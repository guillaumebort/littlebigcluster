use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    net::SocketAddr,
    sync::Arc,
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

use crate::Config;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub uuid: Uuid,
    pub az: String,
    pub address: SocketAddr,
}

impl Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}

impl Node {
    pub fn new(az: impl Into<String>, address: SocketAddr) -> Self {
        let az = az.into();
        let uuid = Uuid::now_v7();
        Self { uuid, az, address }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Member {
    pub node: Node,
    pub roles: Vec<String>,
}

#[derive(Debug)]
pub struct Members {
    members: HashSet<Member>,
    pub(crate) hash: MembersHash,
}

impl Members {
    pub fn to_vec(&self) -> Vec<Member> {
        let mut members = self.members.iter().cloned().collect::<Vec<_>>();
        members.sort_by_key(|member| member.node.uuid);
        members
    }
}

#[derive(Debug)]
struct MembershipState {
    alive: HashMap<Uuid, (DateTime<Utc>, Member)>,
    dead: HashMap<Uuid, (DateTime<Utc>, Node)>,
    updates_tx: watch::Sender<Members>,
    this: Member,
}

#[derive(Debug, Clone)]
pub struct Membership {
    state: Arc<Mutex<MembershipState>>,
    updates_rx: watch::Receiver<Members>,
    config: Config,
}

impl Membership {
    pub fn new(this: Member, config: Config) -> Self {
        let mut members = HashSet::new();
        members.insert(this.clone());

        let (updates_tx, updates_rx) = watch::channel(Members {
            hash: MembersHash::from(members.iter()),
            members,
        });
        let state = Arc::new(Mutex::new(MembershipState {
            alive: HashMap::new(),
            dead: HashMap::new(),
            updates_tx,
            this,
        }));
        {
            let state = state.clone();
            let config = config.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(config.session_timeout).await;
                    let state = state.lock();
                    if state.updates_tx.is_closed() {
                        break;
                    }
                    Self::gc(state, Utc::now(), &config);
                }
            });
        }
        Self {
            state,
            updates_rx,
            config,
        }
    }

    pub fn watch(&self) -> &watch::Receiver<Members> {
        &self.updates_rx
    }

    fn update_state(
        now: DateTime<Utc>,
        mut state: MutexGuard<MembershipState>,
        alives: Vec<(DateTime<Utc>, Member)>,
        deads: Vec<Node>,
        config: &Config,
    ) {
        let this_uuid = state.this.node.uuid;
        for alive in alives {
            state.alive.insert(alive.1.node.uuid, alive);
        }
        for dead in deads {
            state.alive.remove(&dead.uuid);
            state.dead.insert(dead.uuid, (Utc::now(), dead));
        }

        {
            let this = state.this.clone();
            state.alive.insert(this.node.uuid, (Utc::now(), this));
        }
        state.alive.retain(|uuid, (seen, _old)| {
            *uuid == this_uuid
                || (now - *seen).to_std().unwrap_or_default() < config.session_timeout
        });
        state.dead.retain(|_, (seen, _)| {
            (now - *seen).to_std().unwrap_or_default() < config.session_timeout
        });

        state.updates_tx.send_if_modified(|current| {
            let hash = MembersHash::from(state.alive.values().map(|(_, member)| member));
            if hash != current.hash {
                *current = Members {
                    members: state
                        .alive
                        .values()
                        .map(|(_, member)| member)
                        .cloned()
                        .collect::<HashSet<_>>(),
                    hash,
                };
                true
            } else {
                false
            }
        });
    }

    pub fn gossip(&self, now: DateTime<Utc>, gossip: Gossip) {
        let state = self.state.lock();
        match gossip {
            Gossip::Alive { member, .. } => {
                Self::update_state(now, state, vec![(now, member)], vec![], &self.config)
            }
            Gossip::Dead { node } => {
                Self::update_state(now, state, vec![], vec![node], &self.config)
            }
            Gossip::Rumors { alive, dead, .. } => {
                Self::update_state(now, state, alive, dead, &self.config)
            }
            Gossip::Noop => {}
        }
    }

    pub fn rumors(&self) -> Gossip {
        let state = self.state.lock();
        let alive = state.alive.values().cloned().collect();
        let dead = state.dead.values().map(|(_, node)| node).cloned().collect();
        let members_hash = state.updates_tx.borrow().hash;
        Gossip::Rumors {
            alive,
            dead,
            members_hash,
        }
    }

    pub fn this(&self) -> Member {
        self.state.lock().this.clone()
    }

    fn gc(mut state: MutexGuard<MembershipState>, now: DateTime<Utc>, config: &Config) {
        {
            let this = state.this.clone();
            state.alive.insert(this.node.uuid, (Utc::now(), this));
        }
        state.alive.retain(|_, (seen, _old)| {
            (now - *seen).to_std().unwrap_or_default() < config.session_timeout
        });
        state.dead.retain(|_, (seen, _)| {
            (now - *seen).to_std().unwrap_or_default() < config.session_timeout
        });

        state.updates_tx.send_if_modified(|current| {
            let hash = MembersHash::from(state.alive.values().map(|(_, member)| member));
            if hash != current.hash {
                *current = Members {
                    members: state
                        .alive
                        .values()
                        .map(|(_, member)| member)
                        .cloned()
                        .collect::<HashSet<_>>(),
                    hash,
                };
                true
            } else {
                false
            }
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembersHash(u64);

impl MembersHash {
    pub const ZERO: Self = Self(0);

    pub fn from<'a>(members: impl Iterator<Item = &'a Member>) -> Self {
        let mut s = DefaultHasher::new();
        let uuids = members.map(|member| member.node.uuid).collect::<Vec<_>>();
        uuids.hash(&mut s);
        Self(s.finish())
    }
}

impl Serialize for MembersHash {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MembersHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self(s.parse().map_err(serde::de::Error::custom)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Gossip {
    Dead {
        node: Node,
    },
    Alive {
        member: Member,
        known_members_hash: MembersHash,
    },
    Rumors {
        alive: Vec<(DateTime<Utc>, Member)>,
        dead: Vec<Node>,
        members_hash: MembersHash,
    },
    Noop,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use test_log::test;

    use super::*;

    #[test(tokio::test)]
    async fn watch_members() -> Result<()> {
        let now = Utc::now();
        let membership = Membership::new(
            Member {
                node: Node::new("az1", "127.0.0.1:8080".parse()?),
                roles: vec![],
            },
            Default::default(),
        );

        let mut members = membership.watch().clone();
        assert!(!members.has_changed()?);
        assert_eq!(1, members.borrow_and_update().members.len());

        // Discovered a new node
        let another_node = Node::new("az1", "127.0.0.1:8081".parse()?);
        membership.gossip(
            now,
            Gossip::Alive {
                member: Member {
                    node: another_node.clone(),
                    roles: vec![],
                },
                known_members_hash: MembersHash::ZERO,
            },
        );

        assert!(members.has_changed()?);
        assert_eq!(2, members.borrow_and_update().members.len());

        // see the same node again
        membership.gossip(
            now,
            Gossip::Alive {
                member: Member {
                    node: another_node.clone(),
                    roles: vec![],
                },
                known_members_hash: MembersHash::ZERO,
            },
        );

        assert!(!members.has_changed()?);
        assert_eq!(2, members.borrow_and_update().members.len());

        // and now the member is gone
        membership.gossip(
            now,
            Gossip::Dead {
                node: another_node.clone(),
            },
        );

        assert!(members.has_changed()?);
        assert_eq!(1, members.borrow_and_update().members.len());

        Ok(())
    }

    #[test(tokio::test)]
    async fn session_timeout() -> Result<()> {
        let membership = Membership::new(
            Member {
                node: Node::new("az1", "127.0.0.1:8080".parse()?),
                roles: vec![],
            },
            Config {
                session_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        );

        let mut members = membership.watch().clone();
        assert!(!members.has_changed()?);
        assert_eq!(1, members.borrow_and_update().members.len());

        // Discovered a new node
        let now = Utc::now();
        let another_node = Node::new("az1", "127.0.0.1:8081".parse()?);
        membership.gossip(
            now,
            Gossip::Alive {
                member: Member {
                    node: another_node.clone(),
                    roles: vec![],
                },
                known_members_hash: MembersHash::ZERO,
            },
        );

        assert!(members.has_changed()?);
        assert_eq!(2, members.borrow_and_update().members.len());

        // But we have not seen the node for a while
        membership.gossip(
            Utc::now() + Duration::from_secs(2),
            Gossip::Rumors {
                alive: vec![],
                dead: vec![],
                members_hash: MembersHash(0),
            },
        );

        assert!(members.has_changed()?);
        assert_eq!(1, members.borrow_and_update().members.len());

        Ok(())
    }
}
