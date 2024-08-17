use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc, Weak},
    time::Duration,
};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::Request,
    http::Uri,
    response::{IntoResponse, Response},
};
use futures::{
    future::{self, BoxFuture},
    FutureExt,
};
use hyper::{
    client::conn::http2::{self, SendRequest},
    Method,
};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use parking_lot::{lock_api::MappedRwLockReadGuard, Mutex, RawRwLock, RwLock, RwLockReadGuard};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::TcpStream,
    select,
    sync::{watch, Notify},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error};

use crate::Node;

#[derive(Clone, Debug)]
pub struct Http2Client {
    name: String,
    connections: Arc<RwLock<Vec<Http2Connection>>>,
    round_robin_counter: Arc<AtomicUsize>,
    connections_change: Arc<Notify>,
}

impl Http2Client {
    const MAX_JSON_BODY: usize = 5 * 1024 * 1024;

    pub async fn open(name: impl Into<String>, mut to: watch::Receiver<Vec<Node>>) -> Self {
        let name = name.into();
        let round_robin_counter = Arc::new(AtomicUsize::new(0));

        let mut connections = vec![];
        for node in to.borrow_and_update().iter() {
            let connection = match Http2Connection::open(node.clone()).await {
                Ok(connection) => connection,
                Err(err) => {
                    debug!(?err, "failed to open http2 connection");
                    continue;
                }
            };
            connections.push(connection);
        }
        let connections = Arc::new(RwLock::new(connections));

        let connections_change = Arc::new(Notify::new());
        tokio::spawn(Self::reconnect_in_background(
            name.clone(),
            to,
            Arc::downgrade(&connections),
            connections_change.clone(),
        ));
        Self {
            name,
            connections,
            round_robin_counter,
            connections_change,
        }
    }

    async fn reconnect_in_background(
        client: String,
        mut to: watch::Receiver<Vec<Node>>,
        connections: Weak<RwLock<Vec<Http2Connection>>>,
        connections_change: Arc<Notify>,
    ) {
        while let Some(connections) = connections.upgrade() {
            select! {
                _ = to.changed() => {}
                _ = tokio::time::sleep(Http2Connection::KEEP_ALIVE) => {}
            }

            let mut changed = false;
            let nodes_to_reconnect = {
                let mut connections = connections.write();
                let mut connections_by_node_uuid = HashMap::new();
                for connection in connections.drain(0..) {
                    connections_by_node_uuid.insert(connection.node.uuid, connection);
                }
                let mut to_reconnect = Vec::new();
                for node in to.borrow_and_update().iter() {
                    if let Some(existing_connection) = connections_by_node_uuid.remove(&node.uuid) {
                        if existing_connection.is_closed() {
                            to_reconnect.push(node.clone());
                        } else {
                            connections.push(existing_connection);
                        }
                    } else {
                        to_reconnect.push(node.clone());
                    }
                }
                if !connections_by_node_uuid.is_empty() {
                    changed = true;
                    drop(connections_by_node_uuid); // remaining connections to old nodes
                }
                to_reconnect // new nodes or closed connections to existing nodes
            };

            for node in nodes_to_reconnect {
                let connection = match Http2Connection::open(node).await {
                    Ok(connection) => connection,
                    Err(err) => {
                        debug!(?err, "failed to open http2 connection");
                        continue;
                    }
                };
                connections.write().push(connection);
                changed = true;
            }

            if changed {
                connections_change.notify_waiters();
                debug!(?client, connections = ?*connections.read(), "http2 connections updated");
            }
        }
    }

    pub async fn send(
        &self,
        req: Request<Body>,
        retry_timeout: Duration,
    ) -> Result<Response, (anyhow::Error, Option<Request<Body>>)> {
        let mut req = Some(req);
        let mut exponential_backoff = Duration::from_secs(1);
        let retry_deadline = tokio::time::Instant::now() + retry_timeout;
        loop {
            let res = match (self.get_connection(), req.take()) {
                (Some(connection), Some(req)) => Some(connection.send(req)),
                (_, old_req) => {
                    req = old_req;
                    None
                }
            };
            if let Some(res) = res {
                match res.await {
                    Ok(res) => return Ok(res),
                    Err((err, Some(old_req))) => {
                        req = Some(old_req);
                        error!(?err, "request failed, retrying");
                        if let Err(_) = tokio::time::timeout_at(
                            retry_deadline,
                            tokio::time::sleep(exponential_backoff),
                        )
                        .await
                        {
                            return Err((anyhow!("timeout waiting for connections"), req));
                        }
                        exponential_backoff = exponential_backoff.min(Duration::from_secs(30)) * 2;
                    }
                    Err((err, None)) => return Err((err, None)),
                }
            } else {
                debug!(client = ?self.name, "no connection available, waiting for new connections");
                if let Err(_) =
                    tokio::time::timeout_at(retry_deadline, self.connections_change.notified())
                        .await
                {
                    return Err((anyhow!("timeout waiting for connections"), req));
                }
            }
        }
    }

    pub async fn json_request<B, U, R>(
        &self,
        method: Method,
        uri: U,
        body: &B,
        retry_timeout: Duration,
    ) -> Result<R>
    where
        B: Serialize,
        U: TryInto<Uri>,
        <U as TryInto<Uri>>::Error: Into<anyhow::Error>,
        R: DeserializeOwned,
    {
        let uri: Uri = uri.try_into().map_err(|e| e.into())?;
        let json_body = serde_json::to_string(&body)?;
        let body = Body::from(json_body);
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(body)?;
        let response = self
            .send(request, retry_timeout)
            .await
            .map_err(|(e, _)| e)?;
        if response.status().is_success() {
            let body = axum::body::to_bytes(response.into_body(), Self::MAX_JSON_BODY).await?;
            Ok(serde_json::from_slice(&body)?)
        } else {
            Err(anyhow!("request failed: {:?}", response))
        }
    }

    fn get_connection(&self) -> Option<MappedRwLockReadGuard<'_, RawRwLock, Http2Connection>> {
        let connections = self.connections.read();
        let round_robin = self
            .round_robin_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RwLockReadGuard::try_map(connections, |connections| {
            if connections.is_empty() {
                None
            } else {
                connections
                    .get(round_robin % connections.len())
                    .filter(|connection| !connection.is_closed())
            }
        })
        .ok()
    }
}

#[derive(Debug)]
struct Http2Connection {
    node: Node,
    sender: Mutex<SendRequest<Body>>,
    is_closed: CancellationToken,
    #[allow(unused)]
    close: DropGuard,
}

impl Http2Connection {
    const KEEP_ALIVE: Duration = Duration::from_secs(1);

    pub async fn open(node: Node) -> Result<Http2Connection> {
        let stream = TcpStream::connect(node.address).await?;
        let io = TokioIo::new(stream);
        let (mut sender, conn) = http2::Builder::new(TokioExecutor::new())
            .keep_alive_interval(Self::KEEP_ALIVE)
            .keep_alive_timeout(Self::KEEP_ALIVE)
            .keep_alive_while_idle(true)
            .timer(TokioTimer::new())
            .handshake(io)
            .await?;
        let close = CancellationToken::new();

        {
            let close = close.clone();
            tokio::spawn(async move {
                select! {
                    _ = close.cancelled() => {
                      debug!("http2 connection closed by client");
                    }
                    conn = conn => {
                        if let Err(err) = conn {
                            error!(?err, "http2 connection error");
                        } else {
                            debug!("http2 connection closed by server");
                        }
                        close.cancel();
                    }
                }
            });
        }

        sender.ready().await?;
        let is_closed = close.clone();
        let close = close.drop_guard();
        let sender = Mutex::new(sender);

        Ok(Self {
            node,
            sender,
            is_closed,
            close,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.is_cancelled()
    }

    pub fn send(
        &self,
        req: Request<Body>,
    ) -> BoxFuture<'static, Result<Response, (anyhow::Error, Option<Request<Body>>)>> {
        if self.is_closed.is_cancelled() {
            future::ready(Err((anyhow!("http2 connection closed"), Some(req)))).boxed()
        } else {
            let res = {
                let mut sender = self.sender.lock();
                sender.try_send_request(req)
            };
            res.map(|res| match res {
                Ok(res) => Ok(res.into_response()),
                Err(mut err) => {
                    let request = err.take_message();
                    Err((err.into_error().into(), request))
                }
            })
            .boxed()
        }
    }
}
