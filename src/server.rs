use std::{future::IntoFuture, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    debug_handler, extract::State, http::StatusCode, response::IntoResponse, routing::get, Json,
    Router,
};
use futures::FutureExt;
use serde_json::{json, Value};
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{debug, error};

use crate::{
    db::{ReadDB, DB},
    node::LeaderStatus,
    Node,
};

#[derive(Debug)]
pub struct Server {
    pub address: SocketAddr,
    notify_shutdown: oneshot::Sender<()>,
    serve: JoinHandle<Result<(), std::io::Error>>,
}

#[derive(Clone, Debug)]
pub struct LeaderState {
    inner: Arc<(Node, LeaderStatus, DB)>,
}

impl LeaderState {
    pub fn new(node: Node, leader_status: LeaderStatus, db: DB) -> Self {
        Self {
            inner: Arc::new((node, leader_status, db)),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.inner.1.is_leader()
    }

    pub async fn debug(&self) -> Result<Value> {
        let (leader, db_path, db_size, epoch, last_update) = self.inner.2.readonly_transaction(|c| {
            let leader: String = c.query_row("SELECT value FROM _lbc WHERE key = 'leader'", [], |row| row.get(0))?;
            let db_path = c.path().map(|p| p.to_owned()).unwrap_or_default();
            let db_size: u64 = c.query_row(r#"SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"#, [], |row| row.get(0))?;
            let epoch: u64 = c.query_row("SELECT value FROM _lbc WHERE key = 'epoch'", [], |row| row.get(0))?;
            let last_update: String = c.query_row("SELECT value FROM _lbc WHERE key = 'last_update'", [], |row| row.get(0))?;
            Ok((leader, db_path, db_size, epoch, last_update))
        }).await?;

        Ok(json!({
          "cluster_id": self.inner.0.cluster_id,
          "leader": leader,
          "node": {
            "uuid": self.inner.0.uuid.to_string(),
            "az": self.inner.0.az,
            "address": self.inner.0.address.map(|a| a.to_string()).unwrap_or_default(),
          },
          "status": if self.is_leader() { "leader" } else { "standby" },
          "db": {
                "path": db_path,
                "size": db_size,
              },
              "replica": {
                "epoch": epoch,
                "last_update": last_update,
              }
        }))
    }
}

impl Server {
    pub async fn start(
        node: &mut Node,
        router: Router<LeaderState>,
        leader_status: LeaderStatus,
        db: DB,
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
            .with_state(LeaderState::new(
                node.clone(),
                leader_status.clone(),
                db.clone(),
            ));

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
async fn status(State(state): State<LeaderState>) -> impl IntoResponse {
    match state.debug().await {
        Ok(value) => Ok(Json(value)),
        Err(e) => {
            error!(?e, "Failed to get status");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
