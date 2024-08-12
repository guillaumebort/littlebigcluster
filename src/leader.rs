use std::{
    future::Future,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use axum::{
    debug_handler,
    extract::{Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::Utc;
use futures::future::BoxFuture;
use hyper::StatusCode;
use object_store::ObjectStore;
use serde_json::json;
use sqlx::{sqlite::SqliteQueryResult, Executor, Sqlite, Transaction};
use tokio::{
    select,
    sync::{Notify, RwLock},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, trace};

use crate::{
    config::Config,
    db::{Ack, DB},
    replica::{self, Replica},
    server::{JsonResponse, Server},
    Node,
};

#[derive(Debug)]
struct LeaderStateInner {
    node: Node,
    leader_status: LeaderStatus,
    replica: Arc<RwLock<Replica>>,
    db: DB,
}

#[derive(Clone, Debug)]
pub struct LeaderState {
    inner: Arc<LeaderStateInner>,
}

impl LeaderState {
    pub(crate) async fn new(
        node: Node,
        leader_status: LeaderStatus,
        replica: Arc<RwLock<Replica>>,
    ) -> Self {
        let db = replica.read().await.db().clone();
        Self {
            inner: Arc::new(LeaderStateInner {
                node,
                leader_status,
                replica,
                db,
            }),
        }
    }

    pub fn node(&self) -> &Node {
        &self.inner.node
    }

    pub fn is_leader(&self) -> bool {
        self.inner.leader_status.is_leader()
    }

    pub fn db(&self) -> impl Executor<Database = Sqlite> {
        self.inner.db.read_pool()
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

    fn lost_leadership(&self) -> impl Future<Output = ()>;

    fn address(&self) -> SocketAddr;

    fn db(&self) -> impl Executor<Database = Sqlite>;

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

    fn object_store(&self) -> &Arc<dyn ObjectStore>;
}

pub trait StandByLeader {
    fn wait_for_leadership(self) -> impl Future<Output = Result<impl Leader>>;

    fn db(&self) -> impl Executor<Database = Sqlite>;

    fn object_store(&self) -> &Arc<dyn ObjectStore>;
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
    config: Config,
    cancel: DropGuard,
}

impl LeaderNode {
    pub async fn join(
        mut node: Node,
        router: Router<LeaderState>,
        object_store: Arc<dyn ObjectStore>,
        config: Config,
    ) -> Result<Self> {
        debug!(?node, "Joining cluster as Leader");
        let leader_status = LeaderStatus::new();

        // Open the replica
        let replica = Replica::open(&node.cluster_id, object_store.clone(), config.clone()).await?;
        let db: DB = replica.db().clone();
        let replica = Arc::new(RwLock::new(replica));

        // Start the server
        let state = LeaderState::new(node.clone(), leader_status.clone(), replica.clone()).await;
        let system_router = Router::new()
            .route("/status", get(status))
            .route("/gossip", get(gossip));
        let router = Router::new()
            .nest("/_lbc", system_router)
            .merge(router)
            .with_state(state.clone());
        let server = Server::start(
            &mut node,
            router.layer(middleware::from_fn_with_state(state.clone(), leader_only)),
        )
        .await?;

        // drop guard so we stop everything when the leader is dropped
        let cancel = CancellationToken::new().drop_guard();

        Ok(Self {
            node,
            replica,
            db,
            object_store,
            server,
            leader_status,
            config,
            cancel,
        })
    }

    async fn keep_alive(
        is_leader: LeaderStatus,
        replica: Arc<RwLock<Replica>>,
        config: Config,
        cancel: CancellationToken,
    ) {
        while !cancel.is_cancelled() {
            select! {
                _ = tokio::time::sleep(config.epoch_interval) => {
                    match replica.write().await.incr_epoch().await {
                        Ok(epoch) => {
                            trace!(epoch, "Replica epoch incremented");
                        }
                        Err(err) => {
                            error!(?err, "Cannot increment epoch, leader fenced? Giving up");
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }
        // ooops, we lost leadership
        is_leader.set_leader(false);
    }
}

impl StandByLeader for LeaderNode {
    async fn wait_for_leadership(mut self) -> Result<impl Leader> {
        debug!("Waiting for leadership...");
        loop {
            let mut replica = self.replica.write().await;
            // Refresh the replica
            replica.refresh().await?;

            // Check current leader
            let current_leader = replica.leader().await?;
            let last_update = replica.last_update().await?;

            // If there is no leader OR the leader is stale
            if current_leader.is_none()
                || (Utc::now() - last_update) > chrono::Duration::seconds(10)
            {
                debug!(
                    ?current_leader,
                    ?last_update,
                    "No leader or leader is stale"
                );

                // Try to acquire leadership (this is a race with other nodes)
                match replica.try_change_leader(Some(self.node.clone())).await {
                    Ok(()) => {
                        // We are the leader!
                        debug!("Acquired leadership");
                        self.leader_status.set_leader(true);

                        // Spawn a keep-alive task incrementing the epoch and safely synchronizing the DB
                        // until the leader is dropped or explicitly shutdown
                        let cancel = self.cancel.disarm();
                        tokio::spawn(Self::keep_alive(
                            self.leader_status.clone(),
                            self.replica.clone(),
                            self.config.clone(),
                            cancel.child_token(),
                        ));
                        self.cancel = cancel.drop_guard();
                        drop(replica);

                        return Ok(self);
                    }
                    Err(err) => {
                        debug!(?err, "Failed to acquire leadership");
                    }
                }
            }
            drop(replica);

            tokio::time::sleep(self.config.epoch_interval).await;
        }
    }

    fn db(&self) -> impl Executor<Database = Sqlite> {
        self.db.read_pool()
    }

    fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }
}

impl Leader for LeaderNode {
    async fn shutdown(mut self) -> Result<()> {
        // Gracefully shutdown the server
        self.server.shutdown().await?;

        // Stop keep-alive task
        drop(self.cancel);

        // Release leadership
        loop {
            // replica copies are owned by 1) the keep-alive task and 2) the server
            // so when both are shutdown, we should be the only one holding a replica copy
            match Arc::try_unwrap(self.replica) {
                Ok(replica) => {
                    let mut replica = replica.into_inner();
                    assert!(!self.leader_status.is_leader());
                    debug!("Releasing leadership...");
                    replica.try_change_leader(None).await?;
                    debug!("Released leadership");
                    break;
                }
                Err(replica) => {
                    self.replica = replica;
                    tokio::task::yield_now().await;
                }
            }
        }

        Ok(())
    }

    async fn lost_leadership(&self) {
        while self.leader_status.is_leader() {
            self.leader_status.notify.notified().await;
        }
    }

    fn address(&self) -> SocketAddr {
        self.server.address
    }

    fn db(&self) -> impl Executor<Database = Sqlite> {
        self.db.read_pool()
    }

    async fn transaction<A, E>(
        &self,
        thunk: impl (for<'c> FnOnce(Transaction<'c, Sqlite>) -> BoxFuture<'c, Result<A, E>>) + Send,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
        E: Into<anyhow::Error> + Send,
    {
        self.db.transaction(thunk).await
    }

    async fn execute<'a>(
        &'a self,
        query: sqlx::query::Query<'a, Sqlite, <Sqlite as sqlx::Database>::Arguments<'a>>,
    ) -> Result<Ack<SqliteQueryResult>> {
        self.db.execute(query).await
    }

    async fn execute_batch(&self, query: sqlx::RawSql<'static>) -> Result<Ack<SqliteQueryResult>> {
        self.db.execute_batch(query).await
    }

    fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }
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
async fn status(State(state): State<LeaderState>) -> JsonResponse {
    let replica = state.replica();
    let replica = replica.read().await;
    Ok(Json(json!({
        "cluster_id": state.node().cluster_id,
        "node": {
            "uuid": state.node().uuid.to_string(),
            "az": state.node().az,
            "address": state.node().address,
        },
        "leader": (if let Some(leader) = replica.leader().await? {
            json!({
                "uuid": leader.uuid.to_string(),
                "az": leader.az,
                "address": leader.address,
            })
        } else {
            json!(null)
        }),
        "replica": {
            "epoch": replica.epoch(),
            "last_update": replica.last_update().await?.to_rfc3339(),
            "snapshot_epoch": replica.snapshot_epoch(),
        },
        "db": {
            "path": replica.db().path().display().to_string(),
            "size": tokio::fs::metadata(replica.db().path()).await?.len(),
            "SELECT 1": sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(state.db()).await?,
        },
        "status": (if state.is_leader() { "leader" } else { "standby" }),
    })))
}

#[debug_handler]
async fn gossip(State(state): State<LeaderState>) -> JsonResponse {
    todo!()
}
