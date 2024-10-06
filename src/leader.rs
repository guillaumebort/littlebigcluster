use std::{
    future::Future,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc, Weak},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use axum::{
    debug_handler,
    extract::{Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use futures::{future::BoxFuture, FutureExt};
use hyper::StatusCode;
use object_store::ObjectStore;
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteQueryResult, Executor, Sqlite, SqliteConnection};
use tokio::{
    select,
    sync::{futures::Notified, watch, Notify, RwLock},
    task::JoinSet,
    time::MissedTickBehavior,
};
use tracing::{debug, error, trace};

use crate::{
    db::{Ack, DB},
    follower::ClusterState,
    gossip::{Gossip, Member, Members, Membership},
    http2_server::{JsonResponse, Server},
    leader_client::LeaderClient,
    replica::Replica,
    Config, Node,
};

#[derive(Debug)]
struct LeaderStateInner {
    node: Node,
    roles: Vec<String>,
    leader_status: LeaderStatus,
    replica: Arc<RwLock<Replica>>,
    membership: Membership,
    graceful_shutdown: Arc<Notify>,
    db: DB,
    config: Config,
}

#[derive(Clone, Debug)]
pub struct LeaderState {
    inner: Arc<LeaderStateInner>,
}

impl LeaderState {
    pub(crate) async fn new(
        node: Node,
        roles: Vec<String>,
        leader_status: LeaderStatus,
        replica: Arc<RwLock<Replica>>,
        membership: Membership,
        graceful_shutdown: Arc<Notify>,
        config: Config,
    ) -> Self {
        let db = replica.read().await.db().clone();
        Self {
            inner: Arc::new(LeaderStateInner {
                node,
                roles,
                leader_status,
                replica,
                membership,
                graceful_shutdown,
                db,
                config,
            }),
        }
    }

    pub fn wait_shutdown(&self) -> Notified {
        self.inner.graceful_shutdown.notified()
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn this(&self) -> &Node {
        &self.inner.node
    }

    pub fn is_leader(&self) -> bool {
        self.inner.leader_status.is_leader()
    }

    pub fn read(&self) -> impl Executor<Database = Sqlite> {
        self.inner.db.read()
    }

    pub async fn read_uncommitted(&self) -> tokio::sync::MappedMutexGuard<'_, SqliteConnection> {
        self.inner.db.read_uncommitted().await
    }

    pub fn roles(&self) -> &[String] {
        &self.inner.roles
    }

    pub async fn transaction<A>(
        &self,
        thunk: impl (for<'c> FnOnce(&'c mut SqliteConnection) -> BoxFuture<'c, Result<A>>) + Send,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
    {
        self.inner.db.transaction(thunk).await
    }

    pub async fn execute<'a>(
        &'a self,
        query: sqlx::query::Query<'a, Sqlite, <Sqlite as sqlx::Database>::Arguments<'a>>,
    ) -> Result<Ack<SqliteQueryResult>> {
        self.inner.db.execute(query).await
    }

    pub async fn execute_batch(
        &self,
        query: sqlx::RawSql<'static>,
    ) -> Result<Ack<SqliteQueryResult>> {
        self.inner.db.execute_batch(query).await
    }

    pub(crate) fn replica(&self) -> &Arc<RwLock<Replica>> {
        &self.inner.replica
    }
}

pub trait Leader {
    fn shutdown(self) -> impl Future<Output = Result<()>>;

    fn wait_for_leadership(&self) -> impl Future<Output = Result<()>>;

    fn wait_for_lost_leadership(&self) -> impl Future<Output = Result<()>>;

    fn object_store(&self) -> &Arc<dyn ObjectStore>;

    fn watch_leader(&self) -> &watch::Receiver<Option<Node>>;

    fn watch_members(&self) -> &watch::Receiver<Members>;

    fn watch_epoch(&self) -> &watch::Receiver<u64>;

    fn address(&self) -> SocketAddr;

    fn node(&self) -> &Node;

    fn read(&self) -> impl Executor<Database = Sqlite>;

    fn read_uncommitted(
        &self,
    ) -> impl Future<Output = tokio::sync::MappedMutexGuard<'_, SqliteConnection>>;

    fn transaction<A>(
        &self,
        thunk: impl (for<'c> FnOnce(&'c mut SqliteConnection) -> BoxFuture<'c, Result<A>>) + Send,
    ) -> impl Future<Output = Result<Ack<A>>>
    where
        A: Send + Unpin + 'static;

    fn execute<'a>(
        &'a self,
        query: sqlx::query::Query<'a, Sqlite, <Sqlite as sqlx::Database>::Arguments<'a>>,
    ) -> impl Future<Output = Result<Ack<SqliteQueryResult>>>;

    fn execute_batch(
        &self,
        query: sqlx::RawSql<'static>,
    ) -> impl Future<Output = Result<Ack<SqliteQueryResult>>>;
}

#[derive(Clone, Debug)]
pub struct LeaderStatus {
    is_leader: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl LeaderStatus {
    pub fn new() -> Self {
        Self {
            is_leader: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader
            .store(is_leader, std::sync::atomic::Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct LeaderNode {
    node: Node,
    replica: Arc<RwLock<Replica>>,
    db: DB,
    object_store: Arc<dyn ObjectStore>,
    server: Server,
    leader_status: LeaderStatus,
    membership: Membership,
    graceful_shutdown: Arc<Notify>,
    watch_leader_node: watch::Receiver<Option<Node>>,
    watch_epoch: watch::Receiver<u64>,
    leader_client: Weak<LeaderClient>,
    tasks: JoinSet<()>,
}

impl LeaderNode {
    pub async fn join(
        mut node: Node,
        cluster_id: &str,
        router: Router<LeaderState>,
        additional_router: Router<ClusterState>,
        object_store: Arc<dyn ObjectStore>,
        roles: Vec<String>,
        config: Config,
    ) -> Result<Self> {
        debug!(?node, "Joining cluster as Leader");

        // Bind the server
        let tcp_listener = Server::bind(&mut node).await?;

        // Keep track of leadership status
        let leader_status = LeaderStatus::new();

        // Open the replica
        let replica = Replica::open(cluster_id, &object_store, config.clone()).await?;
        let db: DB = replica.db().clone();
        let replica = Arc::new(RwLock::new(replica));

        // Track membership
        let membership = Membership::new(
            Member {
                node: node.clone(),
                roles: roles.clone(),
            },
            config.clone(),
        );

        // For graceful shutdown
        let graceful_shutdown = Arc::new(Notify::new());

        // Start the server
        let router = compose_routers(
            LeaderState::new(
                node.clone(),
                roles.clone(),
                leader_status.clone(),
                replica.clone(),
                membership.clone(),
                graceful_shutdown.clone(),
                config.clone(),
            )
            .await,
            router,
            additional_router,
        )
        .await;
        let server = Server::start(tcp_listener, router).await?;

        // Watch leader updates
        let (leader_updates, watch_leader_node) =
            watch::channel(replica.read().await.leader().await?);

        // Create a watch for the epoch
        let (epoch_updates, watch_epoch) = watch::channel(replica.read().await.epoch());

        // HTTP client connected to the leader
        let leader_client =
            Arc::new(LeaderClient::new(membership.clone(), watch_leader_node.clone()).await?);

        // Keep the replica up-to-date and try to acquire leadership
        let mut tasks = JoinSet::new();
        tasks.spawn(
            Self::wait_for_leadership(
                node.clone(),
                leader_client.clone(),
                leader_updates,
                epoch_updates,
                leader_status.clone(),
                replica.clone(),
                config.clone(),
            )
            .map({
                let leader_status = leader_status.clone();
                move |res| {
                    leader_status.set_leader(false);
                    if let Err(err) = res {
                        error!(?err);
                    }
                }
            }),
        );

        Ok(Self {
            node,
            replica,
            db,
            object_store,
            server,
            leader_status,
            membership,
            graceful_shutdown,
            watch_leader_node,
            watch_epoch,
            leader_client: Arc::downgrade(&leader_client),
            tasks,
        })
    }

    async fn wait_for_leadership(
        node: Node,
        leader_client: Arc<LeaderClient>,
        leader_updates: watch::Sender<Option<Node>>,
        epoch_updates: watch::Sender<u64>,
        leader_status: LeaderStatus,
        replica: Arc<RwLock<Replica>>,
        config: Config,
    ) -> Result<()> {
        debug!("Waiting for leadership...");
        loop {
            let mut replica_guard = replica.write().await;

            // Update the replica
            replica_guard.follow().await?;

            // Notify the epoch change
            let current_epoch = replica_guard.epoch();
            epoch_updates.send_if_modified(|state| {
                if current_epoch != *state {
                    *state = current_epoch;
                    true
                } else {
                    false
                }
            });

            // Check current leader
            let current_leader = replica_guard.leader().await?;
            let last_update = replica_guard.last_update().await?;
            leader_updates.send_if_modified(|state| {
                if current_leader != *state {
                    *state = current_leader.clone();
                    true
                } else {
                    false
                }
            });

            // If there is no leader OR the leader is stale
            if current_leader.is_none()
                || (Utc::now() - last_update).to_std().unwrap_or_default() > config.session_timeout
            {
                debug!(
                    ?current_leader,
                    ?last_update,
                    "No leader or leader is stale"
                );

                // Try to acquire leadership (this is a race with other nodes)
                match replica_guard.try_change_leader(Some(node.clone())).await {
                    Ok(()) => {
                        // We are the leader!
                        debug!("Acquired leadership");
                        leader_status.set_leader(true);

                        drop(replica_guard);
                        drop(leader_client);

                        // Spawn a keep-alive task incrementing the epoch and safely synchronizing the DB
                        // until the leader is dropped or explicitly shutdown
                        return Self::keep_alive(epoch_updates, leader_status, replica, config)
                            .await;
                    }
                    Err(err) => {
                        debug!(?err, "Failed to acquire leadership");
                    }
                }
            }
            drop(replica_guard);

            tokio::time::sleep(config.epoch_interval).await;
        }
    }

    async fn keep_alive(
        epoch_updates: watch::Sender<u64>,
        is_leader: LeaderStatus,
        replica: Arc<RwLock<Replica>>,
        config: Config,
    ) -> Result<()> {
        let mut epochs = tokio::time::interval_at(
            (Instant::now() + config.epoch_interval).into(),
            config.epoch_interval,
        );
        epochs.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut vacuums =
            tokio::time::interval(Duration::from_secs(5 * 60).min(config.retention_period));

        loop {
            select! {
                _ = epochs.tick() => {
                    match replica.write().await.incr_epoch().await {
                        Ok(epoch) => {
                            trace!(epoch, "Replica epoch incremented");

                            // Notify the epoch change
                            epoch_updates.send_if_modified(|state| {
                                if epoch != *state {
                                    *state = epoch;
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        Err(err) => {
                            error!(?err, "Cannot increment epoch, leader fenced? Giving up");
                            break;
                        }
                    }

                }

                _ = vacuums.tick() => {
                    if let Err(err) = replica.write().await.schedule_vacuum().await {
                        error!(?err, "Failed to schedule vacuum");
                    }
                }
            }
        }

        // ooops, we lost leadership
        is_leader.set_leader(false);
        Err(anyhow!("Lost leadership"))
    }

    fn check_db_rw(&self) -> Result<()> {
        if self.leader_status.is_leader() {
            Ok(())
        } else {
            Err(anyhow!("Not the leader, cannot write to the DB"))
        }
    }
}

impl Leader for LeaderNode {
    async fn shutdown(mut self) -> Result<()> {
        // Notify everyone we are shutting down
        self.graceful_shutdown.notify_waiters();

        // Gracefully shutdown the server
        self.server.shutdown().await?;

        let leader_client = self.leader_client.upgrade();

        // Stop tasks
        self.tasks.abort_all();
        self.tasks.shutdown().await;

        // If we have a leader client, at this point we are the only owner
        // let's shutdown the client properly
        if let Some(leader_client) = leader_client {
            let client = Arc::try_unwrap(leader_client)
                .map_err(|_| anyhow!("LeaderClient still in use???"))?;
            client.shutdown().await?;
        }

        if self.leader_status.is_leader() {
            self.leader_status.set_leader(false);

            // replica copies are owned by 1) the keep-alive task and 2) the server
            // so when both are shutdown, we should be the only one holding a replica copy
            match Arc::try_unwrap(self.replica) {
                Ok(replica) => {
                    let mut replica = replica.into_inner();
                    debug!("Releasing leadership...");
                    replica.try_change_leader(None).await?;
                    debug!("Released leadership");
                }
                Err(replica) => {
                    self.replica = replica;
                    error!("Failed to release leadership");
                }
            }
        }

        Ok(())
    }

    async fn wait_for_leadership(&self) -> Result<()> {
        while !self.leader_status.is_leader() {
            self.leader_status.notify.notified().await;
        }
        Ok(())
    }

    async fn wait_for_lost_leadership(&self) -> Result<()> {
        while self.leader_status.is_leader() {
            self.leader_status.notify.notified().await;
        }
        Ok(())
    }

    fn address(&self) -> SocketAddr {
        self.node.address
    }

    fn read(&self) -> impl Executor<Database = Sqlite> {
        self.db.read()
    }

    async fn read_uncommitted(&self) -> tokio::sync::MappedMutexGuard<'_, SqliteConnection> {
        self.db.read_uncommitted().await
    }

    fn node(&self) -> &Node {
        &self.node
    }

    async fn transaction<A>(
        &self,
        thunk: impl (for<'c> FnOnce(&'c mut SqliteConnection) -> BoxFuture<'c, Result<A>>) + Send,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
    {
        self.check_db_rw()?;
        self.db.transaction(thunk).await
    }

    async fn execute<'a>(
        &'a self,
        query: sqlx::query::Query<'a, Sqlite, <Sqlite as sqlx::Database>::Arguments<'a>>,
    ) -> Result<Ack<SqliteQueryResult>> {
        self.check_db_rw()?;
        self.db.execute(query).await
    }

    async fn execute_batch(&self, query: sqlx::RawSql<'static>) -> Result<Ack<SqliteQueryResult>> {
        self.check_db_rw()?;
        self.db.execute_batch(query).await
    }

    fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
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
}

// ---- HTTP API

pub const STATUS_URL: &str = "/.lbc/status";
pub const GOSSIP_URL: &str = "/.lbc/gossip";

async fn compose_routers(
    state: LeaderState,
    leader_router: Router<LeaderState>,
    additional_router: Router<ClusterState>,
) -> Router {
    let cluster_state = ClusterState::new(
        state.inner.node.clone(),
        state.inner.roles.clone(),
        state.inner.replica.clone(),
        state.inner.membership.clone(),
    )
    .await;
    let leader_router = {
        let system_router = Router::new()
            .route(STATUS_URL, get(status))
            .route(GOSSIP_URL, post(gossip));
        let leader_router =
            leader_router.layer(middleware::from_fn_with_state(state.clone(), leader_only));
        Router::new()
            .merge(system_router)
            .merge(leader_router)
            .with_state(state.clone())
    };
    let additional_router = additional_router.with_state(cluster_state);
    leader_router.merge(additional_router)
}

async fn leader_only(State(state): State<LeaderState>, request: Request, next: Next) -> Response {
    if state.is_leader() {
        next.run(request).await
    } else {
        (match state.replica().read().await.leader().await {
            Ok(leader) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "Not the leader - try another!",
                    "leader": (if let Some(leader) = leader {
                        json!({
                            "uuid": leader.uuid.to_string(),
                            "az": leader.az,
                            "address": leader.address,
                        })
                    } else {
                        json!(null)
                    })
                })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            ),
        })
        .into_response()
    }
}

#[debug_handler]
async fn status(State(state): State<LeaderState>) -> JsonResponse<Value> {
    let replica = state.inner.replica.read().await;
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
        "is_leader": state.is_leader(),
        "members": *state.inner.membership.watch().borrow().to_vec(),
    })))
}

#[debug_handler]
async fn gossip(
    State(state): State<LeaderState>,
    Json(gossip): Json<Gossip>,
) -> JsonResponse<Gossip> {
    state.inner.membership.gossip(Utc::now(), gossip.clone());

    match gossip {
        Gossip::Alive {
            known_members_hash, ..
        } => {
            if known_members_hash == state.inner.membership.watch().borrow().hash {
                let mut watch_members = state.inner.membership.watch().clone();
                let members_changed =
                    watch_members.wait_for(|members| known_members_hash != members.hash);
                select! {
                    _ = members_changed => {}
                    _ = tokio::time::sleep(state.config().session_timeout / 2) => {}
                    _ = state.inner.graceful_shutdown.notified() => {
                        state.inner.membership.gossip(Utc::now(), Gossip::Dead { node: state.this().clone() });
                    }
                }
            }

            Ok(Json(state.inner.membership.rumors()))
        }
        _ => Ok(Json(Gossip::Noop)),
    }
}

#[cfg(test)]
mod tests {
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;
    use test_log::test;

    use super::*;

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
    async fn start_leader() -> Result<()> {
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
            tmp.object_store,
            vec![],
            config,
        )
        .await?;

        // this leader node is alone and should become the leader
        leader.wait_for_leadership().await?;
        assert!(leader.leader_status.is_leader());

        // this leader is alone in the cluster
        assert_eq!(1, leader.watch_members().borrow().to_vec().len());

        // we can reach it's API
        reqwest::get(format!("http://{}/yo", leader.address()).as_str())
            .await?
            .error_for_status()?;

        // we can get its status
        reqwest::get(format!("http://{}{}", leader.address(), STATUS_URL).as_str())
            .await?
            .error_for_status()?;

        // we can use the DB in RW mode
        let ack = leader.execute(sqlx::query("SELECT 1")).await?;

        // ack will be eventually committed
        assert_eq!(1, ack.await?.rows_affected());

        Ok(())
    }

    #[test(tokio::test)]
    async fn shutdown_leader() -> Result<()> {
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
            tmp.object_store,
            vec![],
            config,
        )
        .await?;

        // wait for leadership
        leader.wait_for_leadership().await?;

        // and shutdown
        leader.shutdown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn standby_leader() -> Result<()> {
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
        leader.wait_for_leadership().await?;

        // let's start a standby leader
        let standby_leader = LeaderNode::join(
            Node::new("xx", "127.0.0.1:0".parse()?),
            "test",
            Router::new().route("/yo", get(|| async { "yo" })),
            Router::new(),
            tmp.object_store,
            vec![],
            config.clone(),
        )
        .await?;

        // the standby leader follows the leader
        let mut epoch_watcher = standby_leader.watch_epoch().clone();
        epoch_watcher.mark_unchanged();
        epoch_watcher.changed().await?;

        // but it's not the leader
        assert!(!standby_leader.leader_status.is_leader());

        // so we can't reach it's API
        assert_eq!(
            reqwest::get(format!("http://{}/yo", standby_leader.address()).as_str())
                .await?
                .status(),
            503
        );

        // but we can get its status
        reqwest::get(format!("http://{}{}", standby_leader.address(), STATUS_URL).as_str())
            .await?
            .error_for_status()?;

        // we can't use the standby DB in RW mode
        assert!(standby_leader
            .execute(sqlx::query("SELECT 1"))
            .await
            .is_err());

        // now the leader shutdown
        leader.shutdown().await?;

        // the standby leader should become the leader
        standby_leader.wait_for_leadership().await?;
        assert!(standby_leader.leader_status.is_leader());

        // we can reach it's API
        reqwest::get(format!("http://{}/yo", standby_leader.address()).as_str())
            .await?
            .error_for_status()?;

        // and we can use the DB in RW mode
        let ack = standby_leader.execute(sqlx::query("SELECT 1")).await?;
        ack.await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn fence_leader() -> Result<()> {
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
        leader.wait_for_leadership().await?;

        // open another replica for the same cluster
        let mut replica = Replica::open("test", &tmp.object_store, config).await?;

        // let force a fence
        while let Err(_) = replica.try_change_leader(None).await {
            debug!("Failed to fence leader, retrying...");
            replica.follow().await?;
        }

        // so now the leader should have lost leadership
        leader.wait_for_lost_leadership().await?;
        assert!(!leader.leader_status.is_leader());

        // so we can't reach it's API
        assert_eq!(
            reqwest::get(format!("http://{}/yo", leader.address()).as_str())
                .await?
                .status(),
            503
        );

        // and we can't use the DB in RW mode
        assert!(leader.execute(sqlx::query("SELECT 1")).await.is_err());

        Ok(())
    }
}
