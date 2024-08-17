use std::time::Duration;

use anyhow::Result;
use hyper::Method;
use tokio::{sync::watch, task::JoinSet};
use tracing::{debug, error};

use crate::{
    gossip::{Gossip, Member, MembersHash, Membership},
    http2_client::Http2Client,
    Config, Node,
};

#[derive(Debug)]
pub struct LeaderClient {
    node: Node,
    http2_client: Http2Client,
    tasks: JoinSet<()>,
    config: Config,
}

impl LeaderClient {
    pub async fn new(
        membership: Membership,
        mut watch_leader: watch::Receiver<Option<Node>>,
        config: Config,
    ) -> Self {
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
                    if let Err(err) = watch_leader.changed().await {
                        error!(
                            ?err,
                            "Failed to receive leader nodes update in leader client"
                        );
                        break;
                    }
                    let leader_nodes = if let Some(ref node) = *watch_leader.borrow_and_update() {
                        vec![node.clone()]
                    } else {
                        vec![]
                    };
                    if let Err(err) = tx.send(leader_nodes) {
                        error!(?err, "Failed to update leader nodes in http2 client");
                        break;
                    }
                }
            }
        });
        let http2_client = Http2Client::open("leader_client", rx).await;

        // will run the gossip protocol
        let node = membership.this().node.clone();
        tasks.spawn(Self::gossip(
            http2_client.clone(),
            membership,
            config.clone(),
        ));

        LeaderClient {
            node,
            http2_client,
            config,
            tasks,
        }
    }

    async fn gossip(http2_client: Http2Client, membership: Membership, config: Config) {
        let mut known_members_hash = MembersHash::ZERO;
        let mut sleep_duration = Duration::ZERO;

        loop {
            let this = membership.this();
            let res: Result<Gossip> = http2_client
                .json_request(
                    Method::POST,
                    "/_lbc/gossip",
                    &Gossip::Alive {
                        member: Member {
                            node: this.node,
                            roles: this.roles,
                        },
                        known_members_hash: known_members_hash.clone(),
                    },
                    config.session_timeout,
                )
                .await;

            // exponential backoff
            sleep_duration = (if sleep_duration == Duration::ZERO {
                Duration::from_secs(1)
            } else {
                sleep_duration * 2
            })
            .min(Duration::from_secs(30));

            match res {
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
                    debug!(?err, "Failed to gossip")
                }
            }

            tokio::time::sleep(sleep_duration).await;
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        // stop gossip
        drop(self.tasks);

        // if we are still connected to the leader, signal cleanly that we are going down
        if let Err(err) = self
            .http2_client
            .json_request::<_, _, Gossip>(
                Method::POST,
                "/_lbc/gossip",
                &Gossip::Dead { node: self.node },
                Duration::ZERO,
            )
            .await
        {
            debug!(?err, "Failed to send dead message to leader");
        }

        Ok(())
    }
}
