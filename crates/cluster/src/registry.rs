//! Transient in-memory roster on the leader.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub node_id: String,
    pub addr: String,
    pub az: String,
    pub epoch: u64,
    pub capabilities: Vec<String>,
    pub schema_version: u32,
    pub schema_min: u32,
    pub schema_max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinError {
    DuplicateId { existing_addr: String },
}

struct Entry {
    member: Member,
    session: u64,
}

#[derive(Clone)]
pub struct Registry {
    members: Arc<DashMap<String, Entry>>,
    roster_tx: Arc<watch::Sender<Arc<Vec<Member>>>>,
    next_session: Arc<AtomicU64>,
}

impl Registry {
    pub fn new() -> Self {
        let (roster_tx, _) = watch::channel(Arc::new(Vec::new()));
        Self {
            members: Arc::new(DashMap::new()),
            roster_tx: Arc::new(roster_tx),
            next_session: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Registers a member. Returns a session token that must be passed to
    /// [`Self::leave`] so a stale disconnect cannot evict a newer connection.
    /// Rejects a second node using the same id at a different address.
    pub fn try_join(&self, member: Member) -> Result<u64, JoinError> {
        if let Some(existing) = self.members.get(&member.node_id) {
            if existing.member.addr != member.addr {
                return Err(JoinError::DuplicateId {
                    existing_addr: existing.member.addr.clone(),
                });
            }
        }

        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        self.members.insert(
            member.node_id.clone(),
            Entry {
                member: member.clone(),
                session,
            },
        );
        self.broadcast();
        Ok(session)
    }

    pub fn heartbeat(&self, node_id: &str, epoch: u64, schema_version: u32) {
        let changed = match self.members.get_mut(node_id) {
            Some(mut entry)
                if entry.member.epoch != epoch || entry.member.schema_version != schema_version =>
            {
                entry.member.epoch = epoch;
                entry.member.schema_version = schema_version;
                true
            }
            _ => false,
        };
        // Re-broadcast so watchers (web UI, followers) observe live epochs.
        // The `get_mut` guard is dropped above before we touch the map again.
        if changed {
            self.broadcast();
        }
    }

    pub fn leave(&self, node_id: &str, session: u64) {
        if self
            .members
            .remove_if(node_id, |_, entry| entry.session == session)
            .is_some()
        {
            self.broadcast();
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<Vec<Member>>> {
        self.roster_tx.subscribe()
    }

    fn broadcast(&self) {
        let mut roster: Vec<Member> = self.members.iter().map(|e| e.member.clone()).collect();
        roster.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        // `send_replace` keeps the cached roster correct even when no receiver
        // is currently subscribed (e.g. a leader with no followers connected);
        // plain `send` would drop the value and leave a stale roster behind.
        self.roster_tx.send_replace(Arc::new(roster));
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, addr: &str) -> Member {
        Member {
            node_id: id.into(),
            addr: addr.into(),
            az: "az-a".into(),
            epoch: 1,
            capabilities: vec![],
            schema_version: 0,
            schema_min: 0,
            schema_max: 0,
        }
    }

    #[test]
    fn rejects_duplicate_id_at_different_addr() {
        let registry = Registry::new();
        registry
            .try_join(member("node-a", "127.0.0.1:5001"))
            .unwrap();

        let err = registry
            .try_join(member("node-a", "127.0.0.1:5002"))
            .unwrap_err();
        assert_eq!(
            err,
            JoinError::DuplicateId {
                existing_addr: "127.0.0.1:5001".into(),
            }
        );
        assert_eq!(registry.subscribe().borrow().len(), 1);
    }

    #[test]
    fn stale_leave_does_not_evict_reconnected_member() {
        let registry = Registry::new();
        let first = registry
            .try_join(member("node-a", "127.0.0.1:5001"))
            .unwrap();
        let second = registry
            .try_join(member("node-a", "127.0.0.1:5001"))
            .unwrap();

        registry.leave("node-a", first);
        assert_eq!(registry.subscribe().borrow().len(), 1);

        registry.leave("node-a", second);
        assert_eq!(registry.subscribe().borrow().len(), 0);
    }
}
