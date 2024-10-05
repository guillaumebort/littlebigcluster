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
    db::DB,
    gossip::{Member, Members, Membership},
    http2_server::{JsonResponse, Server},
    leader::STATUS_URL,
    leader_client::LeaderClient,
    replica::Replica,
    Config, Node,
};

#[derive(Debug)]
struct ClusterStateInner {
    node: Node,
    roles: Vec<String>,
    replica: Arc<RwLock<Replica>>,
    membership: Membership,
    db: DB,
}

#[derive(Clone, Debug)]
pub struct ClusterState {
    inner: Arc<ClusterStateInner>,
}

impl ClusterState {
    pub(crate) async fn new(
        node: Node,
        roles: Vec<String>,
        replica: Arc<RwLock<Replica>>,
        membership: Membership,
    ) -> Self {
        let db = replica.read().await.db().clone();
        Self {
            inner: Arc::new(ClusterStateInner {
                node,
                roles,
                replica,
                membership,
                db,
            }),
        }
    }

    pub fn read(&self) -> impl Executor<Database = Sqlite> {
        self.inner.db.read()
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

    fn read(&self) -> impl Executor<Database = Sqlite>;

    fn leader(&self) -> watch::Ref<'_, Option<Node>>;

    fn leader_client(&self) -> &LeaderClient;

    fn address(&self) -> SocketAddr;

    fn node(&self) -> &Node;

    fn object_store(&self) -> &Arc<dyn ObjectStore>;

    fn wait_for_leader(&self) -> impl Future<Output = Result<Node>>;

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
        let router = compose_router(
            ClusterState::new(
                node.clone(),
                roles.clone(),
                replica.clone(),
                membership.clone(),
            )
            .await,
            router,
        );
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
            LeaderClient::new(membership.clone(), watch_leader_node.clone()).await?;

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

    fn leader(&self) -> watch::Ref<'_, Option<Node>> {
        self.watch_leader_node.borrow()
    }

    fn watch_leader(&self) -> &watch::Receiver<Option<Node>> {
        &self.watch_leader_node
    }

    async fn wait_for_leader(&self) -> Result<Node> {
        let mut watch_leader = self.watch_leader().clone();
        let leader = watch_leader.wait_for(|leader| leader.is_some()).await?;
        leader.clone().ok_or_else(|| anyhow::anyhow!("No leader"))
    }

    fn watch_epoch(&self) -> &watch::Receiver<u64> {
        &self.watch_epoch
    }

    fn read(&self) -> impl Executor<Database = Sqlite> {
        self.db.read()
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
        let current = f(*watch_epoch.borrow(), db.read(), &object_store).await?;
        let (tx, rx) = watch::channel(current);
        tokio::spawn(async move {
            while !tx.is_closed() {
                select! {
                  _ = watch_epoch.changed() => {
                    let epoch = *watch_epoch.borrow();
                    match f(epoch, db.read(), &object_store).await {
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

// --- HTTP API

fn compose_router(state: ClusterState, router: Router<ClusterState>) -> Router {
    let system_router = Router::new().route(STATUS_URL, get(status));
    Router::new()
        .merge(system_router)
        .merge(router)
        .with_state(state)
}

#[debug_handler]
async fn status(State(state): State<ClusterState>) -> JsonResponse<Value> {
    let replica = state.replica();
    let replica = replica.read().await;
    Ok(Json(json!({
        "cluster_id": replica.cluster_id(),
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
        "members": state.inner.membership.watch().borrow().to_vec(),
    })))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{stream::FuturesUnordered, TryStreamExt};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;
    use test_log::test;
    use tokio::task::JoinSet;

    use super::*;
    use crate::{leader::LeaderNode, Leader};

    struct TmpObjectStore {
        #[allow(unused)]
        tmp: TempDir,
        object_store: Arc<dyn ObjectStore>,
    }

    impl TmpObjectStore {
        async fn new() -> Result<Self> {
            let tmp = tempfile::tempdir()?;
            let object_store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path())?);
            let tmp = Self { tmp, object_store };
            // init replica
            Replica::init("test", &tmp.object_store).await?;
            Ok(tmp)
        }
    }

    #[test(tokio::test)]
    async fn start_follower() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;
        let config = Config {
            epoch_interval: Duration::from_millis(100),
            ..Default::default()
        };

        // start a follower node
        let follower = FollowerNode::join(
            Node::new("xx", "127.0.0.1:0".parse()?),
            "test",
            Router::new().route("/yo", get(|| async { "yo" })),
            tmp.object_store,
            vec![],
            config,
        )
        .await?;

        let mut epoch = follower.watch_epoch().clone();

        // the epoch won't move because there is no leader
        assert!(
            tokio::time::timeout(Duration::from_millis(200), epoch.changed())
                .await
                .is_err()
        );

        // there is no leader
        assert_eq!(None, *follower.leader());

        // it is the single member
        assert_eq!(1, follower.watch_members().borrow().len());

        Ok(())
    }

    #[test(tokio::test)]
    async fn follower_join_leader() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;
        let config = Config {
            epoch_interval: Duration::from_millis(100),
            ..Default::default()
        };

        // start a follower node
        let follower = FollowerNode::join(
            Node::new("xx", "127.0.0.1:0".parse()?),
            "test",
            Router::new().route("/yo", get(|| async { "yo" })),
            tmp.object_store.clone(),
            vec![],
            config.clone(),
        )
        .await?;

        let mut watch_epoch = follower.watch_epoch().clone();
        let mut watch_leader = follower.watch_leader().clone();
        let mut watch_members = follower.watch_members().clone();

        // start a leader node
        let leader = LeaderNode::join(
            Node::new("xx", "127.0.0.1:0".parse()?),
            "test",
            Router::new().route("/yo", get(|| async { "yo" })),
            Router::new(),
            tmp.object_store,
            vec![],
            config,
        )
        .await?;
        leader.wait_for_leadership().await?;
        let mut members_as_seen_by_leader = leader.watch_members().clone();

        // the leader will make the epoch move
        watch_epoch.changed().await?;

        // the leader is the leader
        watch_leader.changed().await?;
        assert_eq!(Some(leader.node()), follower.leader().as_ref());

        // there are two members
        watch_members.changed().await?;
        assert_eq!(2, watch_members.borrow_and_update().len());
        assert_eq!(2, members_as_seen_by_leader.borrow_and_update().len());

        // follower is shutdown
        follower.shutdown().await?;

        // leader has seen the follower leave
        members_as_seen_by_leader.changed().await?;
        assert_eq!(1, members_as_seen_by_leader.borrow_and_update().len());

        Ok(())
    }

    #[test(tokio::test)]
    async fn lost_leader() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;
        let config = Config {
            epoch_interval: Duration::from_millis(100),
            ..Default::default()
        };

        // start a leader node
        let leader = LeaderNode::join(
            Node::new("xx", "127.0.0.1:0".parse()?),
            "test",
            Router::new().route("/yo", get(|| async { "yo" })),
            Router::new(),
            tmp.object_store.clone(),
            vec![],
            config.clone(),
        )
        .await?;

        // start a follower node
        let follower = FollowerNode::join(
            Node::new("xx", "127.0.0.1:0".parse()?),
            "test",
            Router::new().route("/yo", get(|| async { "yo" })),
            tmp.object_store.clone(),
            vec![],
            config.clone(),
        )
        .await?;

        // wait for the follower to see the leader
        let mut watch_leader = follower.watch_leader().clone();
        watch_leader.wait_for(|leader| leader.is_some()).await?;

        // shutdown the leader
        leader.shutdown().await?;

        // wait for the follower to see the leader leave
        watch_leader.wait_for(|leader| leader.is_none()).await?;

        Ok(())
    }

    #[ignore = "test uses a lot of resources"]
    #[test(tokio::test)]
    async fn tons_of_followers() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;
        let config = Config {
            epoch_interval: Duration::from_millis(100),
            ..Default::default()
        };

        // start a leader node
        let leader = LeaderNode::join(
            Node::new("xx", "127.0.0.1:0".parse()?),
            "test",
            Router::new().route("/yo", get(|| async { "yo" })),
            Router::new(),
            tmp.object_store.clone(),
            vec![],
            config.clone(),
        )
        .await?;

        // start 100 followers
        let followers = FuturesUnordered::new();
        for _ in 0..100 {
            followers.push(async {
                let follower = FollowerNode::join(
                    Node::new("xx", "127.0.0.1:0".parse()?),
                    "test",
                    Router::new(),
                    tmp.object_store.clone(),
                    vec![],
                    config.clone(),
                )
                .await?;
                follower.wait_for_leader().await?;
                anyhow::Ok(follower)
            });
        }

        // wait for all followers to be ready
        let followers = followers.try_collect::<Vec<_>>().await?;
        assert_eq!(100, followers.len());

        // wait for the membership to stabilize
        let mut members_seen_by_leader = leader.watch_members().clone();
        members_seen_by_leader
            .wait_for(|members| members.len() == 101)
            .await?;

        // let's shutdown half of the followers
        let mut shutting_down = JoinSet::new();
        let mut remaining_followers = Vec::new();
        for (i, follower) in followers.into_iter().enumerate() {
            if i % 2 == 0 {
                shutting_down.spawn(follower.shutdown());
            } else {
                remaining_followers.push(follower);
            }
        }
        shutting_down.join_all().await;

        // wait for the membership to stabilize
        let mut members_seen_by_leader = leader.watch_members().clone();
        members_seen_by_leader
            .wait_for(|members| members.len() == 51)
            .await?;

        // check what remaining followers see
        for follower in remaining_followers.iter() {
            let mut watch_members = follower.watch_members().clone();
            watch_members
                .wait_for(|members| members.len() == 51)
                .await?;
        }

        // now shutdown the leader
        leader.shutdown().await?;

        // check what remaining followers see
        for follower in remaining_followers.iter() {
            let mut watch_leader: watch::Receiver<Option<Node>> = follower.watch_leader().clone();
            watch_leader.wait_for(|leader| leader.is_none()).await?;
        }

        Ok(())
    }
}
