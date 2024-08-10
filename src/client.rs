use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::Request,
    response::{IntoResponse, Response},
};
use hyper::client::conn::http2::{self, Connection, SendRequest};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{
    net::TcpStream,
    select,
    sync::{watch, Mutex},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, warn};

use crate::Node;

#[derive(Debug)]
pub struct Client {
    sender: Arc<Mutex<Option<SendRequest<Body>>>>,
    #[allow(unused)]
    cancel: DropGuard,
}

type Http2Connection = Connection<TokioIo<TcpStream>, Body, TokioExecutor>;

impl Client {
    pub fn new(watch_leader: watch::Receiver<Option<Node>>) -> Self {
        let cancel = CancellationToken::new();
        let sender = Arc::new(Mutex::new(None));
        tokio::spawn(Self::reconnect(
            watch_leader,
            sender.clone(),
            cancel.child_token(),
        ));
        let cancel = cancel.drop_guard();

        Self { sender, cancel }
    }

    pub async fn request(&self, req: Request) -> Result<Response> {
        let mut pending_req = Some(req);
        let mut last_error = None;
        let timeout = Instant::now();

        while let Some(req) = pending_req.take() {
            if timeout.elapsed() > Duration::from_secs(5) {
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
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Unknown error")))
    }

    async fn reconnect(
        mut watch_leader: watch::Receiver<Option<Node>>,
        sender: Arc<Mutex<Option<SendRequest<Body>>>>,
        cancel: CancellationToken,
    ) {
        let mut conn: Option<Http2Connection> = None;
        while !cancel.is_cancelled() {
            // this is the address of the leader
            let leader_address = watch_leader
                .borrow_and_update()
                .as_ref()
                .and_then(|node| node.address);

            // if we don't have a connection and we have a leader, let's try to open one
            if let (None, Some(leader_address)) = (&conn, leader_address) {
                debug!(?leader_address, "Connecting to leader");
                match Self::open_client(leader_address).await {
                    Ok((new_sender, new_conn)) => {
                        conn = Some(new_conn);
                        *sender.lock().await = Some(new_sender);
                        debug!(?leader_address, "Connected to leader!");
                    }
                    Err(e) => {
                        error!(?e, "Failed to connect to leader");
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
              _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)), if conn.is_none() => {
                continue;
              }
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
