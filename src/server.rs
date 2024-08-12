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
    leader::LeaderState,
    replica::Replica,
    LeaderStatus, Node,
};

#[derive(Debug)]
pub struct Server {
    pub address: SocketAddr,
    notify_shutdown: oneshot::Sender<()>,
    serve: JoinHandle<Result<(), std::io::Error>>,
}

impl Server {
    pub async fn start(
        node: &mut Node,
        router: Router,
    ) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind(node.address).await?;
        let address = listener.local_addr()?;
        node.address = address;

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
