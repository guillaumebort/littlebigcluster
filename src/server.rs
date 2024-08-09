use std::{future::IntoFuture, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    debug_handler,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures::FutureExt;
use serde_json::{json, Value};
use sqlx::any;
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinHandle,
};
use tracing::{debug, error};

use crate::{db::DB, node::LeaderStatus, replica::Replica, Node};

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

        let system_router = Router::new().route("/status", get(status));
        let router = Router::new()
            .nest("/_lbc", system_router)
            .merge(router)
            .with_state(LeaderState::new(node.clone(), leader_status, replica).await);

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
        },
        "db": {
            "path": state.inner.db.path().display().to_string(),
            "size": tokio::fs::metadata(state.inner.db.path()).await?.len(),
        },
        "status": (if state.is_leader() { "leader" } else { "standby" }),
    })))
}

type JsonResponse = Result<Json<serde_json::Value>, ServerError>;

struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
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
