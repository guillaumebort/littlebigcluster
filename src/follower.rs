use std::{future::Future, sync::Arc};

use anyhow::Result;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use sqlx::{
    query::QueryScalar, sqlite::SqliteRow, Database, Executor, FromRow, Sqlite, SqlitePool, Type,
};
use tokio::{select, sync::watch};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{client::LeaderClient, config::Config, db::DB, replica::Replica, Node};

pub trait Follower {
    fn watch_leader(&self) -> &watch::Receiver<Option<Node>>;

    fn watch_epoch(&self) -> &watch::Receiver<u32>;

    fn db(&self) -> impl Executor<Database = Sqlite>;

    fn leader_client(&self) -> &LeaderClient;

    fn uuid(&self) -> Uuid;

    fn object_store(&self) -> &Arc<dyn ObjectStore>;

    fn watch<A, F>(&self, f: F) -> impl Future<Output = Result<watch::Receiver<A>>>
    where
        A: Send + Sync + Eq + PartialEq + 'static,
        F: (for<'a> Fn(u32, &'a SqlitePool, &'a Arc<dyn ObjectStore>) -> BoxFuture<'a, Result<A>>)
            + Send
            + 'static;
}

#[derive(Debug)]
pub struct FollowerNode {
    node: Node,
    db: DB,
    object_store: Arc<dyn ObjectStore>,
    watch_leader_node: watch::Receiver<Option<Node>>,
    watch_epoch: watch::Receiver<u32>,
    leader_client: LeaderClient,
    #[allow(unused)]
    cancel: DropGuard,
}

impl FollowerNode {
    pub async fn join(
        node: Node,
        object_store: Arc<dyn ObjectStore>,
        config: Config,
    ) -> Result<Self> {
        debug!(?node, "Joining cluster as follower");

        // Open the replica
        let replica = Replica::open(&node.cluster_id, object_store.clone(), config.clone()).await?;

        // Get a (read-only) DB
        let db = replica.owned_db();

        // Create a watch for the leader node
        let (leader_updates, watch_leader_node) = watch::channel(replica.leader().await?);

        // Create a watch for the epoch
        let (epoch_updates, watch_epoch) = watch::channel(replica.epoch());

        // Keep the replica up-to-date until the follower is dropped
        let cancel = CancellationToken::new();
        tokio::spawn(Self::follow(
            replica,
            leader_updates,
            epoch_updates,
            config.clone(),
            cancel.child_token(),
        ));

        // Start an http2 client always connected to the current leader
        let leader_client = LeaderClient::new(watch_leader_node.clone(), config);

        Ok(Self {
            node,
            db,
            object_store,
            watch_leader_node,
            watch_epoch,
            leader_client,
            cancel: cancel.drop_guard(),
        })
    }

    async fn follow(
        mut replica: Replica,
        leader_updates: watch::Sender<Option<Node>>,
        epoch_updates: watch::Sender<u32>,
        config: Config,
        cancel: CancellationToken,
    ) -> Result<()> {
        while !cancel.is_cancelled() {
            replica.refresh().await?;

            // Notify the epoch change
            let current_epoch = replica.epoch();
            epoch_updates.send_if_modified(|state| {
                if current_epoch != *state {
                    *state = current_epoch;
                    true
                } else {
                    false
                }
            });

            // Notify the leader change
            let current_leader = replica.leader().await?;
            leader_updates.send_if_modified(|state| {
                if current_leader != *state {
                    *state = current_leader;
                    true
                } else {
                    false
                }
            });

            // Wait for the next epoch
            tokio::time::sleep(config.epoch_interval).await;
        }
        Ok(())
    }
}

impl Follower for FollowerNode {
    fn watch_leader(&self) -> &watch::Receiver<Option<Node>> {
        &self.watch_leader_node
    }

    fn watch_epoch(&self) -> &watch::Receiver<u32> {
        &self.watch_epoch
    }

    fn db(&self) -> impl Executor<Database = Sqlite> {
        self.db.read_pool()
    }

    fn leader_client(&self) -> &LeaderClient {
        &self.leader_client
    }

    fn uuid(&self) -> Uuid {
        self.node.uuid
    }

    fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    async fn watch<A, F>(&self, f: F) -> Result<watch::Receiver<A>>
    where
        A: Send + Sync + Eq + PartialEq + 'static,
        F: (for<'a> Fn(u32, &'a SqlitePool, &'a Arc<dyn ObjectStore>) -> BoxFuture<'a, Result<A>>)
            + Send
            + 'static,
    {
        let db = self.db.clone();
        let object_store = self.object_store.clone();
        let mut watch_epoch = self.watch_epoch().clone();
        let current = f(*watch_epoch.borrow(), db.read_pool(), &object_store).await?;
        let (tx, rx) = watch::channel(current);
        tokio::spawn(async move {
            while !tx.is_closed() {
                select! {
                  _ = watch_epoch.changed() => {
                    let epoch = *watch_epoch.borrow();
                    match f(epoch, db.read_pool(), &object_store).await {
                      Ok(current) => {
                        tx.send_if_modified(|state| {
                          if current != *state {
                            *state = current;
                            true
                          } else {
                            false
                          }
                        });
                      }
                      Err(err) => {
                        error!(?err, "Error watching");
                      }
                    }
                  }
                  _ = tx.closed() => {
                    break;
                  }
                }
            }
        });
        Ok(rx)
    }
}
