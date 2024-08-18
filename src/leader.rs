use std::{
    future::Future,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc, Weak},
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
use sqlx::{sqlite::SqliteQueryResult, Executor, Sqlite, Transaction};
use tokio::{
    select,
    sync::{futures::Notified, watch, Notify, RwLock},
    task::JoinSet,
};
use tracing::{debug, error, trace};

use crate::{
    config::Config,
    db::{Ack, DB},
    follower::ClusterState,
    gossip::{Gossip, Member, Members, Membership},
    http2_server::{JsonResponse, Server},
    leader_client::LeaderClient,
    replica::Replica,
    Node,
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

    pub fn shutdown(&self) -> Notified {
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

    pub fn db(&self) -> impl Executor<Database = Sqlite> {
        self.inner.db.read_pool()
    }

    pub fn roles(&self) -> &[String] {
        &self.inner.roles
    }

    pub async fn transaction<A, E>(
        &self,
        thunk: impl (for<'c> FnOnce(Transaction<'c, Sqlite>) -> BoxFuture<'c, Result<A, E>>) + Send,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
        E: Into<anyhow::Error> + Send,
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

    fn db(&self) -> impl Executor<Database = Sqlite>;

    fn object_store(&self) -> &Arc<dyn ObjectStore>;

    fn watch_leader(&self) -> &watch::Receiver<Option<Node>>;

    fn watch_members(&self) -> &watch::Receiver<Members>;

    fn watch_epoch(&self) -> &watch::Receiver<u64>;

    fn address(&self) -> SocketAddr;

    fn node(&self) -> &Node;

    fn transaction<A, E>(
        &self,
        thunk: impl (for<'c> FnOnce(Transaction<'c, Sqlite>) -> BoxFuture<'c, Result<A, E>>) + Send,
    ) -> impl Future<Output = Result<Ack<A>>>
    where
        A: Send + Unpin + 'static,
        E: Into<anyhow::Error> + Send;

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
        let replica = Replica::open(&node.cluster_id, object_store.clone(), config.clone()).await?;
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
        let leader_router = {
            let state = LeaderState::new(
                node.clone(),
                roles.clone(),
                leader_status.clone(),
                replica.clone(),
                membership.clone(),
                graceful_shutdown.clone(),
                config.clone(),
            )
            .await;
            let system_router = Router::new()
                .route("/status", get(status))
                .route("/gossip", post(gossip));
            let router = Router::new()
                .nest("/_lbc", system_router)
                .merge(router)
                .with_state(state.clone());
            router.layer(middleware::from_fn_with_state(state.clone(), leader_only))
        };
        let additional_router = {
            let state = ClusterState::new(node.clone(), roles.clone(), replica.clone()).await;
            additional_router.with_state(state)
        };
        let server = Server::start(tcp_listener, leader_router.merge(additional_router)).await?;

        // Watch leader updates
        let (leader_updates, watch_leader_node) =
            watch::channel(replica.read().await.leader().await?);

        // Create a watch for the epoch
        let (epoch_updates, watch_epoch) = watch::channel(replica.read().await.epoch());

        // HTTP client connected to the leader
        let leader_client = Arc::new(
            LeaderClient::new(
                membership.clone(),
                watch_leader_node.clone(),
                config.clone(),
            )
            .await,
        );

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
                        panic!("Failed to acquire leadership: {err:#?}");
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

            // Refresh the replica
            replica_guard.refresh().await?;

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
        loop {
            tokio::time::sleep(config.epoch_interval).await;
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
        // ooops, we lost leadership
        is_leader.set_leader(false);
        Err(anyhow!("Lost leadership"))
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

    fn db(&self) -> impl Executor<Database = Sqlite> {
        self.db.read_pool()
    }

    fn node(&self) -> &Node {
        &self.node
    }

    async fn transaction<A, E>(
        &self,
        thunk: impl (for<'c> FnOnce(Transaction<'c, Sqlite>) -> BoxFuture<'c, Result<A, E>>) + Send,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
        E: Into<anyhow::Error> + Send,
    {
        if self.leader_status.is_leader() {
            self.db.transaction(thunk).await
        } else {
            Err(anyhow!("Not the leader"))
        }
    }

    async fn execute<'a>(
        &'a self,
        query: sqlx::query::Query<'a, Sqlite, <Sqlite as sqlx::Database>::Arguments<'a>>,
    ) -> Result<Ack<SqliteQueryResult>> {
        if self.leader_status.is_leader() {
            self.db.execute(query).await
        } else {
            Err(anyhow!("Not the leader"))
        }
    }

    async fn execute_batch(&self, query: sqlx::RawSql<'static>) -> Result<Ack<SqliteQueryResult>> {
        if self.leader_status.is_leader() {
            self.db.execute_batch(query).await
        } else {
            Err(anyhow!("Not the leader"))
        }
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

// ----- Server

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
        "members": state.inner.membership.rumors(),
    })))
}

#[debug_handler]
async fn gossip(
    State(state): State<LeaderState>,
    Json(payload): Json<Gossip>,
) -> JsonResponse<Gossip> {
    match payload {
        Gossip::Alive {
            member,
            known_members_hash,
        } => {
            state
                .inner
                .membership
                .update(vec![(Utc::now(), member)], vec![]);

            if known_members_hash == state.inner.membership.watch().borrow().hash {
                let mut watch_members = state.inner.membership.watch().clone();
                let members_changed =
                    watch_members.wait_for(|members| known_members_hash != members.hash);
                select! {
                    _ = members_changed => {}
                    _ = tokio::time::sleep(state.config().session_timeout / 2) => {}
                    _ = state.inner.graceful_shutdown.notified() => {
                        state.inner.membership.update(vec![], vec![state.this().clone()]);
                    }
                }
            }

            Ok(Json(state.inner.membership.rumors()))
        }
        Gossip::Dead { node } => {
            state.inner.membership.update(vec![], vec![node]);
            Ok(Json(Gossip::GoodBye))
        }
        _ => Err(anyhow!("Unexpected gossip message"))?,
    }
}
