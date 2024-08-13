use std::{net::SocketAddr, sync::Arc, time::Instant};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::Request,
    response::{IntoResponse, Response},
};
use futures::FutureExt;
use hyper::client::conn::http2::{self, Connection, SendRequest};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::json;
use tokio::{
    net::TcpStream,
    select,
    sync::{watch, Mutex},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, warn};

use crate::{Config, Node};

#[derive(Debug)]
pub struct LeaderClient {
    sender: Arc<Mutex<Option<SendRequest<Body>>>>,
    config: Config,
    #[allow(unused)]
    cancel: DropGuard,
}

type Http2Connection = Connection<TokioIo<TcpStream>, Body, TokioExecutor>;

impl LeaderClient {
    pub fn new(node: Node, watch_leader: watch::Receiver<Option<Node>>, config: Config) -> Self {
        let cancel = CancellationToken::new();
        let sender = Arc::new(Mutex::new(None));

        // will keep trying to reconnect to the leader
        tokio::spawn(Self::reconnect(
            watch_leader,
            sender.clone(),
            config.clone(),
            cancel.child_token(),
        ));

        // will run the gossip protocol
        tokio::spawn(Self::gossip(
            node,
            sender.clone(),
            config.clone(),
            cancel.child_token(),
        ));

        let cancel = cancel.drop_guard();

        Self {
            sender,
            config,
            cancel,
        }
    }

    pub async fn request(&self, req: Request) -> Result<Response> {
        let mut pending_req = Some(req);
        let mut last_error = None;
        let timeout = Instant::now();

        while let Some(req) = pending_req.take() {
            if timeout.elapsed() > self.config.client_retry_timeout() {
                break;
            } else if let Some(sender) = self.sender.lock().await.as_mut() {
                match sender.try_send_request(req).await {
                    Ok(res) => return Ok(res.into_response()),
                    Err(mut e) => {
                        dbg!("oops", &e);
                        pending_req = e.take_message();
                        last_error = Some(anyhow!(e.into_error()));
                    }
                }
            } else {
                last_error = Some(anyhow!("No connection to leader"));
                pending_req = Some(req);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // TODO: fix hard-coded sleep
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Unknown error")))
    }

    async fn reconnect(
        mut watch_leader: watch::Receiver<Option<Node>>,
        sender: Arc<Mutex<Option<SendRequest<Body>>>>,
        config: Config,
        cancel: CancellationToken,
    ) {
        let mut conn: Option<Http2Connection> = None;
        while !cancel.is_cancelled() {
            // this is the address of the leader
            let leader_address = watch_leader
                .borrow_and_update()
                .as_ref()
                .map(|node| node.address);

            // if we don't have a connection and we have a leader, let's try to open one
            if let (None, Some(leader_address)) = (&conn, leader_address) {
                debug!(?leader_address, "Connecting to leader");
                match Self::open_client(leader_address).await {
                    Ok((new_sender, new_conn)) => {
                        conn = Some(new_conn);
                        *sender.lock().await = Some(new_sender);
                        debug!(?leader_address, "Connected to leader!");
                    }
                    Err(err) => {
                        debug!(?err, "Failed to connect to leader");
                    }
                }
            }

            select! {

              // look for changes in the leader
              _ = watch_leader.changed() => {
                conn = None;
                *sender.lock().await = None;
              }

              // look for connection changes
              r = async {
                if let Some(conn) = conn.take() {
                  conn.await
                } else {
                  futures::future::pending().await
                }
              } => {
                if let Err(e) = r {
                  error!(?e, "Connection error");
                } else {
                  warn!("Connection closed");
                }
                conn = None;
                *sender.lock().await = None;
              }

              // look for cancellation
              _ = cancel.cancelled() => {
                break;
              }

              // retry after a second if we don't have a connection
              _ = tokio::time::sleep(config.epoch_interval), if conn.is_none() => {
                continue;
              }
            }
        }
    }

    async fn gossip(
        node: Node,
        sender: Arc<Mutex<Option<SendRequest<Body>>>>,
        config: Config,
        cancel: CancellationToken,
    ) {
        while !cancel.is_cancelled() {
            let res = {
                if let Some(sender) = sender.lock().await.as_mut() {
                    let req = Request::builder()
                        .uri("/_lbc/gossip")
                        .header("Content-Type", "application/json")
                        .body(Body::from(
                            json!({
                                "uuid": node.uuid.to_string(),
                                "address": node.address.to_string(),
                                "az": node.az.to_string(),
                                "role": node.role.to_string(),
                            })
                            .to_string()
                            .as_bytes()
                            .to_vec(),
                        ))
                        .unwrap();
                    sender
                        .send_request(req)
                        .map(|r| r.map_err(|e| e.into()))
                        .boxed()
                } else {
                    futures::future::ready(Err(anyhow!("No connection to leader"))).boxed()
                }
            };

            let result = res.await.and_then(|res| {
                if res.status().is_success() {
                    Ok(())
                } else {
                    Err(anyhow!("{}", res.status()))
                }
            });

            if let Err(err) = result {
                debug!(?err, "Failed to gossip");
            }

            select! {
              _ = cancel.cancelled() => {
                break;
              }
              _ = tokio::time::sleep(config.gossip_interval) => {}
            }
        }
    }

    async fn open_client(addr: SocketAddr) -> Result<(SendRequest<Body>, Http2Connection)> {
        let stream = TcpStream::connect(addr).await?;
        let io = TokioIo::new(stream);
        let exec = TokioExecutor::new();
        let (mut sender, conn) = http2::handshake(exec, io).await?;
        sender.ready().await?;
        Ok((sender, conn))
    }
}
