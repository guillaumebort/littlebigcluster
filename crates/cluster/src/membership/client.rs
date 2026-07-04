//! Node-side membership client.

use std::sync::Arc;
use std::time::Duration;

use crate::proto::cluster_client::ClusterClient;
use crate::proto::{membership_request, Heartbeat, Join, MembershipEvent, MembershipRequest};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, warn};

use crate::{ClusterView, JoinError, Member, Registry};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

pub struct Client {
    node_id: String,
    addr: String,
    az: String,
    capabilities: Vec<String>,
    view: watch::Receiver<ClusterView>,
    roster_tx: watch::Sender<Arc<Vec<Member>>>,
    registry: Registry,
    initial_backoff: Duration,
}

impl Client {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: impl Into<String>,
        addr: impl Into<String>,
        az: impl Into<String>,
        capabilities: Vec<String>,
        view: watch::Receiver<ClusterView>,
        roster_tx: watch::Sender<Arc<Vec<Member>>>,
        registry: Registry,
        initial_backoff: Duration,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            addr: addr.into(),
            az: az.into(),
            capabilities,
            view,
            roster_tx,
            registry,
            initial_backoff,
        }
    }

    pub async fn run(mut self) {
        let mut backoff = self.initial_backoff;
        loop {
            let view = self.view.borrow().clone();

            // When we're the leader, register in our own roster in-process
            // instead of dialing our own gRPC service over loopback.
            if view.is_leader {
                if !self.run_as_leader().await {
                    return;
                }
                backoff = self.initial_backoff;
                continue;
            }

            let leader_addr = view.leader_addr;
            if leader_addr.is_empty() {
                if self.view.changed().await.is_err() {
                    return;
                }
                continue;
            }

            match self.connect_and_heartbeat(&leader_addr).await {
                Ok(()) => backoff = self.initial_backoff,
                Err(_) => {
                    // Interruptible backoff: wake early on a view change so a
                    // promotion (or new leader) is picked up without waiting.
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = self.view.changed() => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// Leader path: add ourselves to the local registry and mirror the roster
    /// into the node-local channel, without any network round-trip. Runs until
    /// this node stops being the leader or the node shuts down. Returns `false`
    /// when the view channel closed (shutdown) so the caller can stop.
    async fn run_as_leader(&mut self) -> bool {
        let session = match self.registry.try_join(self.self_member()) {
            Ok(session) => {
                debug!(node_id = %self.node_id, "registered self as leader");
                session
            }
            Err(JoinError::DuplicateId { existing_addr }) => {
                warn!(
                    node_id = %self.node_id,
                    addr = %self.addr,
                    existing_addr = %existing_addr,
                    "leader id already registered at another address"
                );
                return true;
            }
        };

        let mut roster = self.registry.subscribe();
        // `send_replace` (not `send`) so the roster is retained even if no
        // receiver has subscribed yet — the local reader may still be starting.
        self.roster_tx
            .send_replace(roster.borrow_and_update().clone());

        loop {
            tokio::select! {
                changed = roster.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.roster_tx.send_replace(roster.borrow_and_update().clone());
                }
                changed = self.view.changed() => {
                    if changed.is_err() {
                        self.registry.leave(&self.node_id, session);
                        return false;
                    }
                    let view = self.view.borrow();
                    if !view.is_leader {
                        break;
                    }
                    // Keep our epoch fresh; heartbeat does not re-broadcast.
                    self.registry.heartbeat(&self.node_id, view.epoch);
                }
            }
        }

        self.registry.leave(&self.node_id, session);
        true
    }

    fn self_member(&self) -> Member {
        Member {
            node_id: self.node_id.clone(),
            addr: self.addr.clone(),
            az: self.az.clone(),
            epoch: self.view.borrow().epoch,
            capabilities: self.capabilities.clone(),
        }
    }

    async fn connect_and_heartbeat(&mut self, leader_addr: &str) -> Result<(), String> {
        let (tx, mut events) = self.connect(leader_addr).await?;

        let connected_leader = leader_addr.to_string();
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    let epoch = self.view.borrow().epoch;
                    let hb = MembershipRequest {
                        request: Some(membership_request::Request::Heartbeat(Heartbeat {
                            epoch,
                        })),
                    };
                    if tx.send(hb).await.is_err() {
                        return Ok(());
                    }
                }
                msg = events.message() => match msg {
                    Ok(Some(event)) => {
                        let members = event.members.into_iter().map(Member::from).collect();
                        self.roster_tx.send_replace(Arc::new(members));
                    }
                    Ok(None) => return Ok(()),
                    Err(e) => {
                        if e.code() == tonic::Code::AlreadyExists {
                            warn!(
                                node_id = %self.node_id,
                                addr = %self.addr,
                                error = %e.message(),
                                "membership rejected: duplicate node id"
                            );
                        }
                        return Err(e.to_string());
                    }
                },
                changed = self.view.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                    if self.view.borrow().leader_addr != connected_leader {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn connect(
        &self,
        leader_addr: &str,
    ) -> Result<(mpsc::Sender<MembershipRequest>, Streaming<MembershipEvent>), String> {
        let uri = format!("http://{leader_addr}");
        let channel = Channel::from_shared(uri)
            .map_err(|e| e.to_string())?
            .connect()
            .await
            .map_err(|e| e.to_string())?;
        let mut client = ClusterClient::new(channel);

        let (tx, rx) = mpsc::channel(32);
        let join = MembershipRequest {
            request: Some(membership_request::Request::Join(Join {
                node_id: self.node_id.clone(),
                addr: self.addr.clone(),
                az: self.az.clone(),
                epoch: self.view.borrow().epoch,
                capabilities: self.capabilities.clone(),
            })),
        };
        tx.send(join)
            .await
            .map_err(|_| "request stream closed".to_string())?;

        let events = client
            .membership(ReceiverStream::new(rx))
            .await
            .map_err(|e| e.to_string())?
            .into_inner();

        debug!(leader_addr = %leader_addr, "connected to leader");
        Ok((tx, events))
    }
}

impl From<crate::proto::Member> for Member {
    fn from(m: crate::proto::Member) -> Self {
        Self {
            node_id: m.node_id,
            addr: m.addr,
            az: m.az,
            epoch: m.epoch,
            capabilities: m.capabilities,
        }
    }
}
