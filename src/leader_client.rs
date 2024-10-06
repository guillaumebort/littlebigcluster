use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use hyper::Method;
use tokio::{
    sync::watch,
    task::{yield_now, JoinSet},
};
use tracing::{debug, error};

use crate::{
    gossip::{Gossip, Member, MembersHash, Membership},
    http2_client::Http2Client,
    leader::GOSSIP_URL,
    Node,
};

#[derive(Debug)]
pub struct LeaderClient {
    node: Node,
    http2_client: Http2Client,
    tasks: JoinSet<()>,
}

impl LeaderClient {
    const MAX_EXPONENTIAL_BACKOFF: Duration = Duration::from_secs(30);

    pub async fn new(
        membership: Membership,
        mut watch_leader: watch::Receiver<Option<Node>>,
    ) -> Result<Self> {
        let mut tasks = JoinSet::new();
        let leader_nodes = if let Some(ref node) = *watch_leader.borrow_and_update() {
            vec![node.clone()]
        } else {
            vec![]
        };
        let (tx, rx) = watch::channel(leader_nodes);
        tasks.spawn({
            async move {
                loop {
                    let _ = watch_leader.changed().await;
                    let leader_nodes = if let Some(ref node) = *watch_leader.borrow_and_update() {
                        vec![node.clone()]
                    } else {
                        vec![]
                    };
                    if let Err(err) = tx.send(leader_nodes) {
                        error!(?err, "Failed to send leader nodes to leader client");
                        break;
                    }
                    yield_now().await
                }
            }
        });
        let http2_client =
            Http2Client::open("leader_client", membership.this().node.clone(), rx, None).await?;

        // will run the gossip protocol
        let node = membership.this().node.clone();
        tasks.spawn(Self::gossip(http2_client.clone(), membership));

        Ok(LeaderClient {
            node,
            http2_client,
            tasks,
        })
    }

    async fn gossip(http2_client: Http2Client, membership: Membership) {
        let mut known_members_hash = MembersHash::ZERO;
        let mut sleep_duration = Duration::ZERO;

        loop {
            let this = membership.this();
            let res: Result<Gossip> = http2_client
                .json_request(
                    Method::POST,
                    GOSSIP_URL,
                    &Gossip::Alive {
                        member: Member {
                            node: this.node,
                            roles: this.roles,
                        },
                        known_members_hash: known_members_hash.clone(),
                    },
                )
                .await;

            // exponential backoff
            sleep_duration = (if sleep_duration == Duration::ZERO {
                Duration::from_secs(1)
            } else {
                sleep_duration * 2
            })
            .min(Self::MAX_EXPONENTIAL_BACKOFF);

            match res {
                Ok(ref gossip @ Gossip::Rumors { members_hash, .. }) => {
                    membership.gossip(Utc::now(), gossip.clone());
                    known_members_hash = members_hash;
                }
                err => {
                    debug!(?err, "Failed to gossip")
                }
            }

            tokio::time::sleep(sleep_duration).await;
        }
    }

    pub async fn shutdown(mut self) -> Result<()> {
        // stop tasks
        self.tasks.abort_all();
        self.tasks.shutdown().await;

        // if we are still connected to the leader, signal cleanly that we are going down
        if let Err(err) = self
            .http2_client
            .send_json_request::<_, _, Gossip>(
                Method::POST,
                GOSSIP_URL,
                &Gossip::Dead { node: self.node },
                false,
            )
            .await
        {
            debug!(?err, "Failed to send dead message to leader");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use axum::{debug_handler, extract::State, response::IntoResponse, routing::post, Json};
    use test_log::test;
    use tokio::select;
    use tokio_util::sync::{CancellationToken, DropGuard};
    use tracing::info;

    use super::*;
    use crate::Config;

    struct FakeLeader {
        node: Node,
        membership: Membership,
        #[allow(unused)]
        shutdown: DropGuard,
    }

    async fn start_fake_leader() -> Result<FakeLeader> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let node = Node::new("az".to_owned(), listener.local_addr()?);
        info!(?node, "server started");
        let cancel = CancellationToken::new();
        let shutdown = cancel.clone().drop_guard();
        let membership = Membership::new(
            Member {
                node: node.clone(),
                roles: vec![],
            },
            Default::default(),
        );
        let state = membership.clone();
        tokio::spawn(async move {
            #[debug_handler]
            async fn gossip(
                State(membership): State<Membership>,
                Json(gossip): Json<Gossip>,
            ) -> impl IntoResponse {
                membership.gossip(Utc::now(), gossip);
                Json(membership.rumors())
            }

            let router = axum::Router::new()
                .route(GOSSIP_URL, post(gossip))
                .with_state(state);

            select! {
                _ = cancel.cancelled() => {}
                _ = axum::serve(listener, router) => {}
            }

            info!("server stopped");
        });
        Ok(FakeLeader {
            node,
            membership,
            shutdown,
        })
    }

    #[test(tokio::test)]
    async fn test_leader_client() -> Result<()> {
        let this = Node::new("xx", "127.0.0.1:3333".parse()?);
        let client_membership = Membership::new(
            Member {
                node: this.clone(),
                roles: vec![],
            },
            Config {
                ..Default::default()
            },
        );

        // open a leader client (but we have no leader yet)
        let (tx, rx) = watch::channel(None);
        let client = LeaderClient::new(client_membership.clone(), rx).await?;
        let mut members_seen_by_client = client_membership.watch().clone();

        assert_eq!(1, members_seen_by_client.borrow_and_update().to_vec().len());

        // start a fake leader
        let fake_leader = start_fake_leader().await?;
        let mut members_seen_by_leader = fake_leader.membership.watch().clone();

        // the leader client should connect to the leader
        tx.send(Some(fake_leader.node.clone()))?;

        // so it will eventually tell the leader that it is alive, and the membership will be updated
        members_seen_by_client.changed().await?;
        assert_eq!(2, members_seen_by_client.borrow_and_update().to_vec().len());

        // the server sees 2 members as well
        assert_eq!(2, members_seen_by_leader.borrow_and_update().to_vec().len());

        // now shutdown the leader client
        client.shutdown().await?;

        // the leader should see that the client is dead
        members_seen_by_leader.changed().await?;
        assert_eq!(1, members_seen_by_leader.borrow_and_update().to_vec().len());

        Ok(())
    }
}
