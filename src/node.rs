use std::{
    future::Future,
    net::SocketAddr,
    ops::Deref,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use anyhow::Result;
use axum::Router;
use chrono::Utc;
use object_store::ObjectStore;
use sqlx::{Executor, Sqlite};
use tokio::{
    select,
    sync::{watch, Notify, RwLock},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, trace};
use uuid::Uuid;

use crate::{
    db::DB,
    replica::Replica,
    server::{LeaderState, Server},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub uuid: Uuid,
    pub cluster_id: String,
    pub az: String,
    pub address: Option<SocketAddr>,
}

impl Node {
    pub fn new(cluster_id: String, az: String, address: Option<SocketAddr>) -> Self {
        let uuid = Uuid::now_v7();
        Self {
            uuid,
            cluster_id,
            az,
            address,
        }
    }
}

// ===== Follower =====

pub trait Follower {
    fn watch_leader(&self) -> &watch::Receiver<Option<Node>>;
}

#[derive(Debug)]
pub struct FollowerNode {
    db: DB,
    watch_leader_node: watch::Receiver<Option<Node>>,
    #[allow(unused)]
    cancel: DropGuard,
}

impl FollowerNode {
    pub async fn join(node: Node, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        debug!(?node, "Joining cluster as follower");

        // Open the replica
        let replica = Replica::open(&node.cluster_id, object_store).await?;

        // Get a (read-only) DB
        let db = replica.owned_db();

        // Create a watch for the leader node
        let (tx, rx) = watch::channel(replica.leader().await?);

        // Keep the replica up-to-date until the follower is dropped
        let cancel = CancellationToken::new();
        tokio::spawn(Self::follow(replica, tx, cancel.child_token()));

        Ok(Self {
            db,
            watch_leader_node: rx,
            cancel: cancel.drop_guard(),
        })
    }

    async fn follow(
        mut replica: Replica,
        tx: watch::Sender<Option<Node>>,
        cancel: CancellationToken,
    ) -> Result<()> {
        while !cancel.is_cancelled() {
            replica.refresh().await?;

            // Notify the leader change
            let current_leader = replica.leader().await?;
            if current_leader != *tx.borrow() {
                debug!(?current_leader, "Leader changed");
                tx.send(current_leader.clone()).ok();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    pub async fn db(&self) -> &DB {
        &self.db
    }
}

impl Follower for FollowerNode {
    fn watch_leader(&self) -> &watch::Receiver<Option<Node>> {
        &self.watch_leader_node
    }
}

// ===== Leader =====

pub trait Leader {
    fn shutdown(self) -> impl Future<Output = Result<()>>;

    fn lost_leadership(&self) -> impl Future<Output = ()>;

    fn address(&self) -> SocketAddr;
}

pub trait StandByLeader {
    fn wait_for_leadership(self) -> impl Future<Output = Result<impl Leader>>;
}

#[derive(Clone, Debug)]
pub struct LeaderStatus {
    is_leader: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl LeaderStatus {
    pub fn new() -> Self {
        Self {
            is_leader: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader
            .store(is_leader, std::sync::atomic::Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct LeaderNode {
    node: Node,
    replica: Arc<RwLock<Replica>>,
    db: DB,
    server: Server,
    leader_status: LeaderStatus,
    cancel: DropGuard,
}

impl LeaderNode {
    pub async fn join(
        mut node: Node,
        router: Router<LeaderState>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        debug!(?node, "Joining cluster as Leader");
        let leader_status = LeaderStatus::new();

        // Open the replica
        let replica = Replica::open(&node.cluster_id, object_store).await?;
        let db: DB = replica.owned_db();
        let replica = Arc::new(RwLock::new(replica));

        // Start the server
        let server =
            Server::start(&mut node, router, leader_status.clone(), replica.clone()).await?;

        // drop guard so we stop everything when the leader is dropped
        let cancel = CancellationToken::new().drop_guard();

        Ok(Self {
            node,
            replica,
            db,
            server,
            leader_status,
            cancel,
        })
    }

    async fn keep_alive(
        is_leader: LeaderStatus,
        replica: Arc<RwLock<Replica>>,
        cancel: CancellationToken,
    ) {
        while !cancel.is_cancelled() {
            select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    match replica.write().await.incr_epoch().await {
                        Ok(epoch) => {
                            trace!(epoch, "Replica epoch incremented");
                        }
                        Err(err) => {
                            error!(?err, "Cannot increment epoch, leader fenced? Giving up");
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }
        // ooops, we lost leadership
        is_leader.set_leader(false);
    }
}

impl StandByLeader for LeaderNode {
    async fn wait_for_leadership(mut self) -> Result<impl Leader> {
        debug!("Waiting for leadership...");
        loop {
            let mut replica = self.replica.write().await;
            // Refresh the replica
            replica.refresh().await?;

            // Check current leader
            let current_leader = replica.leader().await?;
            let last_update = replica.last_update().await?;

            // If there is no leader OR the leader is stale
            if current_leader.is_none()
                || (Utc::now() - last_update) > chrono::Duration::seconds(10)
            {
                debug!(
                    ?current_leader,
                    ?last_update,
                    "No leader or leader is stale"
                );

                // Try to acquire leadership (this is a race with other nodes)
                match replica.try_change_leader(Some(self.node.clone())).await {
                    Ok(()) => {
                        // We are the leader!
                        debug!("Acquired leadership");
                        self.leader_status.set_leader(true);

                        // Spawn a keep-alive task incrementing the epoch and safely synchronizing the DB
                        // until the leader is dropped or explicitly shutdown
                        let cancel = self.cancel.disarm();
                        tokio::spawn(Self::keep_alive(
                            self.leader_status.clone(),
                            self.replica.clone(),
                            cancel.child_token(),
                        ));
                        self.cancel = cancel.drop_guard();
                        drop(replica);

                        return Ok(self);
                    }
                    Err(err) => {
                        debug!(?err, "Failed to acquire leadership");
                    }
                }
            }
            drop(replica);

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

impl Leader for LeaderNode {
    async fn shutdown(mut self) -> Result<()> {
        // Gracefully shutdown the server
        self.server.shutdown().await?;

        // Stop keep-alive task
        drop(self.cancel);

        // Release leadership
        loop {
            // replica copies are owned by 1) the keep-alive task and 2) the server
            // so when both are shutdown, we should be the only one holding a replica copy
            match Arc::try_unwrap(self.replica) {
                Ok(replica) => {
                    let mut replica = replica.into_inner();
                    assert!(!self.leader_status.is_leader());
                    debug!("Releasing leadership...");
                    replica.try_change_leader(None).await?;
                    debug!("Released leadership");
                    break;
                }
                Err(replica) => {
                    self.replica = replica;
                    tokio::task::yield_now().await;
                }
            }
        }

        Ok(())
    }

    async fn lost_leadership(&self) {
        while self.leader_status.is_leader() {
            self.leader_status.notify.notified().await;
        }
    }

    fn address(&self) -> SocketAddr {
        self.server.address
    }
}
