use std::{future::IntoFuture, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    debug_handler,
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures::{future::BoxFuture, FutureExt};
use serde_json::json;
use sqlx::{sqlite::SqliteQueryResult, Executor, Sqlite, Transaction};
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinHandle,
};
use tracing::{debug, error};

use crate::{
    db::{Ack, DB},
    node::LeaderStatus,
    replica::Replica,
    Node,
};

#[derive(Debug)]
pub struct Server {
    pub address: SocketAddr,
    notify_shutdown: oneshot::Sender<()>,
    serve: JoinHandle<Result<(), std::io::Error>>,
}

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
        let db = replica.read().await.owned_db().clone();
        Self {
            inner: Arc::new(LeaderStateInner {
                node,
                leader_status,
                replica,
                db,
            }),
        }
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
}

impl Server {
    pub async fn start(
        node: &mut Node,
        router: Router<LeaderState>,
        leader_status: LeaderStatus,
        replica: Arc<RwLock<Replica>>,
    ) -> Result<Self> {
        let address = node
            .address
            .ok_or_else(|| anyhow!("Leader node must have an address"))?;
        let listener = tokio::net::TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        node.address = Some(address);

        let state = LeaderState::new(node.clone(), leader_status, replica).await;

        let system_router = Router::new().route("/status", get(status));
        let router = Router::new()
            .nest("/_lbc", system_router)
            .merge(router.layer(middleware::from_fn_with_state(state.clone(), leader_only)))
            .with_state(state);

        let (notify_shutdown, on_shutdow) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            axum::serve(listener, router)
                .with_graceful_shutdown(on_shutdow.map(|_| ()))
                .into_future(),
        );

        debug!(?address, "Started server");

        Ok(Self {
            address,
            notify_shutdown,
            serve,
        })
    }

    pub async fn shutdown(self) -> Result<()> {
        debug!("Shutting down server...");
        self.notify_shutdown
            .send(())
            .map_err(|_| anyhow!("Cannot notify graceful shutdown"))?;
        self.serve.await??;
        Ok(())
    }
}

async fn leader_only(State(state): State<LeaderState>, request: Request, next: Next) -> Response {
    if state.is_leader() {
        next.run(request).await
    } else {
        (match state.inner.replica.read().await.leader().await {
            Ok(leader) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "Not the leader - try another!",
                    "leader": (if let Some(leader) = leader {
                        json!({
                            "uuid": leader.uuid.to_string(),
                            "az": leader.az,
                            "address": leader.address.map(|a| a.to_string()).unwrap_or_default(),
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
    let replica = state.inner.replica.read().await;
    Ok(Json(json!({
        "cluster_id": state.inner.node.cluster_id,
        "node": {
            "uuid": state.inner.node.uuid.to_string(),
            "az": state.inner.node.az,
            "address": state.inner.node.address.map(|a| a.to_string()).unwrap_or_default(),
        },
        "leader": (if let Some(leader) = replica.leader().await? {
            json!({
                "uuid": leader.uuid.to_string(),
                "az": leader.az,
                "address": leader.address.map(|a| a.to_string()).unwrap_or_default(),
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
            "path": state.inner.db.path().display().to_string(),
            "size": tokio::fs::metadata(state.inner.db.path()).await?.len(),
            "SELECT 1": sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(state.db()).await?,
        },
        "status": (if state.is_leader() { "leader" } else { "standby" }),
    })))
}

pub type JsonResponse = Result<Json<serde_json::Value>, ServerError>;

pub struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        error!(err = ?self.0, "Internal server error");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

impl<E> From<E> for ServerError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
