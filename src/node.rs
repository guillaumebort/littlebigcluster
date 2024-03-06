use std::ops::{Deref, Not};
use std::sync::atomic::AtomicBool;
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use axum::Router;
use chrono::Utc;
use object_store::ObjectStore;
use tokio::sync::{watch, Notify};
use tokio::{select, task::JoinHandle};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, trace};
use uuid::Uuid;

use crate::db::{ReadDB, WriteDB, DB};
use crate::replica::Replica;
use crate::server::{LeaderState, Server};

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

pub trait Follower: ReadDB {
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
}

impl Deref for FollowerNode {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl Follower for FollowerNode {
    fn watch_leader(&self) -> &watch::Receiver<Option<Node>> {
        &self.watch_leader_node
    }
}

// ===== Leader =====

pub trait Leader: ReadDB + WriteDB {
    fn shutdown(self) -> impl Future<Output = Result<()>>;

    fn wait_lost_leadership(&self) -> impl Future<Output = ()>;

    fn address(&self) -> SocketAddr;
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
    db: DB,
    server: Server,
    leader_status: LeaderStatus,
    keep_alive: JoinHandle<Replica>,
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

        // Start the server
        let server =
            Server::start(&mut node, router, leader_status.clone(), replica.owned_db()).await?;

        // Return the node only after acquiring leadership
        Self::wait_for_leadership(node, leader_status, replica, server).await
    }

    async fn wait_for_leadership(
        node: Node,
        leader_status: LeaderStatus,
        mut replica: Replica,
        server: Server,
    ) -> Result<Self> {
        debug!("Waiting for leadership...");
        loop {
            // Refresh the replica
            replica.refresh().await?;

            // Check current leader
            let current_leader = replica.leader().await?;
            let last_update = replica.last_update();

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
                match replica.try_change_leader(Some(node.clone())).await {
                    Ok(()) => {
                        // We are the leader!
                        debug!("Acquired leadership");
                        leader_status.set_leader(true);

                        // Get a (read-write) DB
                        let db = replica.owned_db();

                        // Spawn a keep-alive task incrementing the epoch and safely synchronizing the DB
                        // until the leader is dropped or explicitly shutdown
                        let cancel = CancellationToken::new();
                        let keep_alive = tokio::spawn(Self::keep_alive(
                            leader_status.clone(),
                            replica,
                            cancel.child_token(),
                        ));
                        let cancel = cancel.drop_guard();

                        return Ok(Self {
                            db,
                            server,
                            leader_status,
                            keep_alive,
                            cancel,
                        });
                    }
                    Err(err) => {
                        debug!(?err, "Failed to acquire leadership");
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn keep_alive(
        is_leader: LeaderStatus,
        mut replica: Replica,
        cancel: CancellationToken,
    ) -> Replica {
        while !cancel.is_cancelled() {
            select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    if let Err(err) = replica.incr_epoch().await {
                        error!(?err, "Cannot increment epoch, leader fenced? Giving up");
                        break;
                    } else {
                        trace!(epoch = replica.epoch(), "Replica epoch incremented");
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }
        is_leader.set_leader(false);
        replica
    }
}

impl Deref for LeaderNode {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl Leader for LeaderNode {
    async fn shutdown(self) -> Result<()> {
        // Gracefully shutdown the server
        self.server.shutdown().await?;

        // Stop keep-alive task
        drop(self.cancel);

        // Release leadership
        if self.leader_status.is_leader() {
            let mut replica = self.keep_alive.await?;
            debug!("Releasing leadership...");
            replica.try_change_leader(None).await?;
            debug!("Released leadership");
        }

        Ok(())
    }

    async fn wait_lost_leadership(&self) {
        while self.leader_status.is_leader() {
            self.leader_status.notify.notified().await;
        }
    }

    fn address(&self) -> SocketAddr {
        self.server.address
    }
}
