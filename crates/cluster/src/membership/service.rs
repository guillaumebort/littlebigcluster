//! Leader-side gRPC membership service.

use std::pin::Pin;

use crate::proto::cluster_server::{Cluster, ClusterServer};
use crate::proto::{membership_request, Member as ProtoMember, MembershipEvent, MembershipRequest};
use futures::Stream;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::{ClusterView, JoinError, Member, Registry};

pub struct Service {
    registry: Registry,
    view: watch::Receiver<ClusterView>,
}

impl Service {
    pub fn new(registry: Registry, view: watch::Receiver<ClusterView>) -> Self {
        Self { registry, view }
    }

    pub fn into_server(self) -> ClusterServer<Self> {
        ClusterServer::new(self)
    }
}

#[tonic::async_trait]
impl Cluster for Service {
    type MembershipStream = Pin<Box<dyn Stream<Item = Result<MembershipEvent, Status>> + Send>>;

    async fn membership(
        &self,
        request: Request<Streaming<MembershipRequest>>,
    ) -> Result<Response<Self::MembershipStream>, Status> {
        if !self.view.borrow().is_leader {
            let leader = self.view.borrow().leader_addr.clone();
            return Err(Status::failed_precondition(format!(
                "not the cluster leader; connect to the leader at {leader}"
            )));
        }

        let registry = self.registry.clone();
        let in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(handle_stream(in_stream, registry, tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn handle_stream(
    mut in_stream: Streaming<MembershipRequest>,
    registry: Registry,
    tx: mpsc::Sender<Result<MembershipEvent, Status>>,
) {
    let join = match in_stream.next().await {
        Some(Ok(MembershipRequest {
            request: Some(membership_request::Request::Join(join)),
        })) => join,
        Some(Ok(_)) => {
            let _ = tx
                .send(Err(Status::invalid_argument("first message must be Join")))
                .await;
            return;
        }
        Some(Err(e)) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
        None => return,
    };

    let node_id = join.node_id.clone();
    let addr = join.addr.clone();

    let session = match registry.try_join(Member {
        node_id: join.node_id,
        addr: join.addr,
        az: join.az,
        epoch: join.epoch,
        capabilities: join.capabilities,
    }) {
        Ok(session) => {
            debug!(node_id = %node_id, addr = %addr, "member joined");
            session
        }
        Err(JoinError::DuplicateId { existing_addr }) => {
            warn!(
                node_id = %node_id,
                addr = %addr,
                existing_addr = %existing_addr,
                "rejected duplicate node id"
            );
            let _ = tx
                .send(Err(Status::already_exists(format!(
                    "node id {node_id:?} is already registered at {existing_addr}"
                ))))
                .await;
            return;
        }
    };

    let mut roster = registry.subscribe();

    let initial = roster.borrow_and_update().clone();
    if send_roster(&tx, &initial).await.is_err() {
        registry.leave(&node_id, session);
        return;
    }

    loop {
        tokio::select! {
            incoming = in_stream.next() => match incoming {
                Some(Ok(MembershipRequest {
                    request: Some(membership_request::Request::Heartbeat(hb)),
                })) => registry.heartbeat(&node_id, hb.epoch),
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            },
            changed = roster.changed() => {
                if changed.is_err() {
                    break;
                }
                let snapshot = roster.borrow_and_update().clone();
                if send_roster(&tx, &snapshot).await.is_err() {
                    break;
                }
            }
        }
    }

    debug!(node_id = %node_id, "member disconnected");
    registry.leave(&node_id, session);
}

async fn send_roster(
    tx: &mpsc::Sender<Result<MembershipEvent, Status>>,
    members: &[Member],
) -> Result<(), ()> {
    tx.send(Ok(MembershipEvent {
        members: members.iter().map(proto_member).collect(),
    }))
    .await
    .map_err(|_| ())
}

fn proto_member(m: &Member) -> ProtoMember {
    ProtoMember {
        node_id: m.node_id.clone(),
        addr: m.addr.clone(),
        az: m.az.clone(),
        epoch: m.epoch,
        capabilities: m.capabilities.clone(),
    }
}
