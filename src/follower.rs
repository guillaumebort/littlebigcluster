use std::sync::Arc;

use anyhow::Result;
use object_store::ObjectStore;
use sqlx::{Executor, Sqlite};
use tokio::sync::watch;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::debug;
use uuid::Uuid;

use crate::{client::LeaderClient, config::Config, db::DB, replica::Replica, Node};

pub trait Follower {
    fn watch_leader(&self) -> &watch::Receiver<Option<Node>>;

    fn db(&self) -> impl Executor<Database = Sqlite>;

    fn leader_client(&self) -> &LeaderClient;

    fn uuid(&self) -> Uuid;

    fn object_store(&self) -> &Arc<dyn ObjectStore>;
}

#[derive(Debug)]
pub struct FollowerNode {
    node: Node,
    db: DB,
    object_store: Arc<dyn ObjectStore>,
    watch_leader_node: watch::Receiver<Option<Node>>,
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

        // Keep the replica up-to-date until the follower is dropped
        let cancel = CancellationToken::new();
        tokio::spawn(Self::follow(
            replica,
            leader_updates,
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
            leader_client,
            cancel: cancel.drop_guard(),
        })
    }

    async fn follow(
        mut replica: Replica,
        tx: watch::Sender<Option<Node>>,
        config: Config,
        cancel: CancellationToken,
    ) -> Result<()> {
        while !cancel.is_cancelled() {
            replica.refresh().await?;

            // Notify the leader change
            let current_leader = replica.leader().await?;
            if current_leader != *tx.borrow() {
                debug!(?current_leader, "Leader changed");
                tx.send(current_leader.clone()).ok();
            }
            tokio::time::sleep(config.epoch_interval).await;
        }
        Ok(())
    }
}

impl Follower for FollowerNode {
    fn watch_leader(&self) -> &watch::Receiver<Option<Node>> {
        &self.watch_leader_node
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
}
