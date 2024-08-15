use std::{
    io::Read,
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
use bytes::Buf;
use futures::FutureExt;
use hyper::{
    body::Incoming,
    client::conn::http2::{self, Connection, SendRequest},
    Method,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::TcpStream,
    select,
    sync::{watch, Mutex, Notify},
    task::JoinHandle,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error};

use crate::{
    members::{Gossip, Member, MembersHash, Membership},
    Config, Node,
};

#[derive(Debug)]
pub struct LeaderClient {
    node: Node,
    sender: Arc<Mutex<Option<SendRequest<Body>>>>,
    gossip_task: JoinHandle<()>,
    config: Config,
    #[allow(unused)]
    cancel: DropGuard,
}

type Http2Connection = Connection<TokioIo<TcpStream>, Body, TokioExecutor>;

impl LeaderClient {
    pub fn new(
        node: Node,
        roles: Vec<String>,
        watch_leader: watch::Receiver<Option<Node>>,
        membership: Membership,
        config: Config,
    ) -> Self {
        let cancel = CancellationToken::new();
        let sender = Arc::new(Mutex::new(None));
        let connection_change = Arc::new(Notify::new());

        // will keep trying to reconnect to the leader
        tokio::spawn(Self::reconnect(
            watch_leader,
            sender.clone(),
            connection_change.clone(),
            config.clone(),
            cancel.child_token(),
        ));

        // will run the gossip protocol
        let gossip_task = tokio::spawn(Self::gossip(
            node.clone(),
            roles,
            sender.clone(),
            connection_change.clone(),
            membership,
            config.clone(),
            cancel.child_token(),
        ));

        let cancel = cancel.drop_guard();

        Self {
            node,
            sender,
            gossip_task,
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

    fn json_request(method: Method, url: &str, payload: &impl Serialize) -> Result<Request> {
        Ok(Request::builder()
            .uri(url)
            .method(method)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(payload)?))?)
    }

    async fn reconnect(
        mut watch_leader: watch::Receiver<Option<Node>>,
        sender: Arc<Mutex<Option<SendRequest<Body>>>>,
        connection_change: Arc<Notify>,
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
                        connection_change.notify_waiters();
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
                if let Err(err) = r {
                  error!(?err, "Connection to leader failed");
                } else {
                  debug!("Connection to leader was closed");
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
        this: Node,
        roles: Vec<String>,
        sender: Arc<Mutex<Option<SendRequest<Body>>>>,
        connection_change: Arc<Notify>,
        membership: Membership,
        config: Config,
        cancel: CancellationToken,
    ) {
        let mut known_members_hash = MembersHash::ZERO;
        let mut sleep_duration = Duration::ZERO;

        while !cancel.is_cancelled() {
            sleep_duration = Duration::from_secs(1); // TODO backoff

            if let Some(sender) = sender.lock().await.as_mut() {
                let res = sender
                    .send_request(
                        Self::json_request(
                            Method::POST,
                            "/_lbc/gossip",
                            &Gossip::Alive {
                                member: Member {
                                    node: this.clone(),
                                    roles: roles.clone(),
                                },
                                known_members_hash: known_members_hash.clone(),
                            },
                        )
                        .unwrap(),
                    )
                    .map(|r| r.map_err(|e| e.into()))
                    .await;

                async fn read_body<A>(res: Result<Response<Incoming>>) -> Result<A>
                where
                    A: DeserializeOwned,
                {
                    let res = res?;
                    if res.status().is_success() {
                        let bytes =
                            axum::body::to_bytes(Body::new(res.into_body()), 50000000).await?;
                        let value = serde_json::from_slice(&bytes)?;
                        Ok(value)
                    } else {
                        Err(anyhow!("{}", res.status()))
                    }
                }

                let result = read_body::<Gossip>(res).await;
                match result {
                    Ok(Gossip::Rumors {
                        alive,
                        dead,
                        members_hash,
                        ..
                    }) => {
                        membership.update(alive, dead);
                        known_members_hash = members_hash;
                        sleep_duration = Duration::ZERO;
                    }
                    Ok(_) => {
                        error!("Unexpected response from leader");
                    }
                    err => {
                        debug!(?err, "Failed to gossip");
                    }
                }
            }

            select! {
              _ = cancel.cancelled() => {
                break;
              }
              _ = connection_change.notified() => {}
              _ = tokio::time::sleep(sleep_duration)=> {}
            }
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        // stop gossip
        self.gossip_task.abort();

        // if we are still connected to the leader, signal cleanly that we are going down
        if let Some(sender) = self.sender.lock().await.as_mut() {
            sender
                .send_request(
                    Self::json_request(
                        Method::POST,
                        "/_lbc/gossip",
                        &Gossip::Dead { node: self.node },
                    )
                    .unwrap(),
                )
                .await?;
        }

        // cancel the reconnect task
        drop(self.cancel);

        Ok(())
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
