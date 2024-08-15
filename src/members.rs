use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

use crate::Config;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub uuid: Uuid,
    pub cluster_id: String,
    pub az: String,
    pub address: SocketAddr,
}

impl Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}

impl Node {
    pub fn new(cluster_id: String, az: String, address: SocketAddr) -> Self {
        let uuid = Uuid::now_v7();
        Self {
            uuid,
            cluster_id,
            az,
            address,
        }
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
                    let state = state.lock().unwrap();
                    if state.updates_tx.is_closed() {
                        break;
                    }
                    Self::gc(state, &config);
                }
            });
        }
        Self {
            state,
            updates_rx,
            config,
        }
    }

    pub fn rumors(&self) -> Gossip {
        let state = self.state.lock().unwrap();
        let alive = state.alive.values().cloned().collect();
        let dead = state.dead.values().map(|(_, node)| node).cloned().collect();
        let members_hash = state.updates_tx.borrow().hash;
        Gossip::Rumors {
            alive,
            dead,
            members_hash,
        }
    }

    pub fn watch(&self) -> &watch::Receiver<Members> {
        &self.updates_rx
    }

    pub fn update(&self, alives: Vec<(DateTime<Utc>, Member)>, deads: Vec<Node>) {
        let mut state = self.state.lock().unwrap();
        for alive in alives {
            state.alive.insert(alive.1.node.uuid, alive);
        }
        for dead in deads {
            state.alive.remove(&dead.uuid);
            state.dead.insert(dead.uuid, (Utc::now(), dead));
        }
        Self::gc(state, &self.config);
    }

    fn gc(mut state: MutexGuard<MembershipState>, config: &Config) {
        let now = Utc::now();
        let this = state.this.clone();
        state.alive.insert(this.node.uuid, (Utc::now(), this));
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
        let mut uuids = members.map(|member| member.node.uuid).collect::<Vec<_>>();
        uuids.sort();
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
    GoodBye,
}
