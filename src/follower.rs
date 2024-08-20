use std::{future::Future, net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{debug_handler, extract::State, routing::get, Json, Router};
use futures::future::BoxFuture;
use object_store::ObjectStore;
use serde_json::{json, Value};
use sqlx::{Executor, Sqlite, SqlitePool};
use tokio::{
    select,
    sync::{watch, RwLock},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error};

use crate::{
    config::Config,
    db::DB,
    gossip::{Member, Members, Membership},
    http2_server::{JsonResponse, Server},
    leader_client::LeaderClient,
    replica::Replica,
    Node,
};

#[derive(Debug)]
struct ClusterStateInner {
    node: Node,
    roles: Vec<String>,
    replica: Arc<RwLock<Replica>>,
    db: DB,
}

#[derive(Clone, Debug)]
pub struct ClusterState {
    inner: Arc<ClusterStateInner>,
}

impl ClusterState {
    pub(crate) async fn new(node: Node, roles: Vec<String>, replica: Arc<RwLock<Replica>>) -> Self {
        let db = replica.read().await.db().clone();
        Self {
            inner: Arc::new(ClusterStateInner {
                node,
                roles,
                replica,
                db,
            }),
        }
    }

    pub fn db(&self) -> impl Executor<Database = Sqlite> {
        self.inner.db.read_pool()
    }

    pub fn this(&self) -> &Node {
        &self.inner.node
    }

    pub fn roles(&self) -> &[String] {
        &self.inner.roles
    }

    pub(crate) fn replica(&self) -> Arc<RwLock<Replica>> {
        self.inner.replica.clone()
    }
}

pub trait Follower {
    fn shutdown(self) -> impl Future<Output = Result<()>>;

    fn watch_leader(&self) -> &watch::Receiver<Option<Node>>;

    fn watch_members(&self) -> &watch::Receiver<Members>;

    fn watch_epoch(&self) -> &watch::Receiver<u64>;

    fn db(&self) -> impl Executor<Database = Sqlite>;

    fn leader_client(&self) -> &LeaderClient;

    fn address(&self) -> SocketAddr;

    fn node(&self) -> &Node;

    fn object_store(&self) -> &Arc<dyn ObjectStore>;

    fn watch<A, F>(&self, f: F) -> impl Future<Output = Result<watch::Receiver<A>>>
    where
        A: Send + Sync + Eq + PartialEq + 'static,
        F: (for<'a> Fn(u64, &'a SqlitePool, &'a Arc<dyn ObjectStore>) -> BoxFuture<'a, Result<A>>)
            + Send
            + 'static;
}

#[derive(Debug)]
pub struct FollowerNode {
    node: Node,
    db: DB,
    object_store: Arc<dyn ObjectStore>,
    watch_leader_node: watch::Receiver<Option<Node>>,
    watch_epoch: watch::Receiver<u64>,
    leader_client: LeaderClient,
    membership: Membership,
    server: Server,
    #[allow(unused)]
    cancel: DropGuard,
}

impl FollowerNode {
    pub async fn join(
        mut node: Node,
        cluster_id: &str,
        router: Router<ClusterState>,
        object_store: Arc<dyn ObjectStore>,
        roles: Vec<String>,
        config: Config,
    ) -> Result<Self> {
        debug!(?node, "Joining cluster as follower");

        // Bind server
        let tcp_listener = Server::bind(&mut node).await?;

        // Open the replica
        let replica = Replica::open(cluster_id, &object_store, config.clone()).await?;

        // Track membership
        let membership = Membership::new(
            Member {
                node: node.clone(),
                roles: roles.clone(),
            },
            config.clone(),
        );

        // Get a read only db
        let db: DB = replica.db().clone();

        // Create a watch for the leader node
        let (leader_updates, watch_leader_node) = watch::channel(replica.leader().await?);

        // Create a watch for the epoch
        let (epoch_updates, watch_epoch) = watch::channel(replica.epoch());

        let replica = Arc::new(RwLock::new(replica));

        // Start the server
        let state = ClusterState::new(node.clone(), roles.clone(), replica.clone()).await;
        let system_router = Router::new().route("/status", get(status));
        let router = Router::new()
            .nest("/_lbc", system_router)
            .merge(router)
            .with_state(state);
        let server = Server::start(tcp_listener, router).await?;

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
        let leader_client =
            LeaderClient::new(membership.clone(), watch_leader_node.clone(), config).await;

        Ok(Self {
            node,
            db,
            object_store,
            watch_leader_node,
            watch_epoch,
            leader_client,
            membership,
            server,
            cancel: cancel.drop_guard(),
        })
    }

    async fn follow(
        replica: Arc<RwLock<Replica>>,
        leader_updates: watch::Sender<Option<Node>>,
        epoch_updates: watch::Sender<u64>,
        config: Config,
        cancel: CancellationToken,
    ) -> Result<()> {
        while !cancel.is_cancelled() {
            let mut replica = replica.write().await;
            replica.follow().await?;

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

            // Release the lock
            drop(replica);

            // Wait for the next epoch
            tokio::time::sleep(config.epoch_interval).await;
        }
        Ok(())
    }
}

impl Follower for FollowerNode {
    async fn shutdown(self) -> Result<()> {
        // Gracefully shutdown the server
        self.server.shutdown().await?;

        // Stop refresh task
        drop(self.cancel);

        // Close the leader client
        self.leader_client.shutdown().await?;

        Ok(())
    }

    fn watch_members(&self) -> &watch::Receiver<Members> {
        self.membership.watch()
    }

    fn watch_leader(&self) -> &watch::Receiver<Option<Node>> {
        &self.watch_leader_node
    }

    fn watch_epoch(&self) -> &watch::Receiver<u64> {
        &self.watch_epoch
    }

    fn db(&self) -> impl Executor<Database = Sqlite> {
        self.db.read_pool()
    }

    fn leader_client(&self) -> &LeaderClient {
        &self.leader_client
    }

    fn node(&self) -> &Node {
        &self.node
    }

    fn address(&self) -> SocketAddr {
        self.node.address
    }

    fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    async fn watch<A, F>(&self, f: F) -> Result<watch::Receiver<A>>
    where
        A: Send + Sync + Eq + PartialEq + 'static,
        F: (for<'a> Fn(u64, &'a SqlitePool, &'a Arc<dyn ObjectStore>) -> BoxFuture<'a, Result<A>>)
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

#[debug_handler]
async fn status(State(state): State<ClusterState>) -> JsonResponse<Value> {
    let replica = state.replica();
    let replica = replica.read().await;
    Ok(Json(json!({
        "this": state.this(),
        "roles": state.roles(),
        "leader": replica.leader().await?,
        "replica": {
            "epoch": replica.epoch(),
            "last_update": replica.last_update().await?.to_rfc3339(),
            "snapshot_epoch": replica.snapshot_epoch(),
        },
        "db": {
            "path": replica.db().path().display().to_string(),
            "size": tokio::fs::metadata(replica.db().path()).await?.len(),
        },
    })))
}
