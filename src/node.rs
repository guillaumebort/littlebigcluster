use std::borrow::Cow;
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use chrono::Utc;
use object_store::ObjectStore;
use rusqlite::types::FromSql;
use rusqlite::{Params, Transaction};
use tokio::sync::watch;
use tokio::{select, task::JoinHandle};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, trace};
use uuid::Uuid;

use crate::db::{Ack, DB};
use crate::replica::Replica;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub uuid: Uuid,
    pub address: SocketAddr,
    pub cluster_id: String,
}

impl Node {
    pub fn new(cluster_id: String) -> Self {
        let uuid = Uuid::now_v7();
        let address = "127.0.0.1:8000".parse().unwrap();
        Self {
            uuid,
            address,
            cluster_id,
        }
    }
}

// ===== DB Access =====

pub trait ReadDB {
    fn query_row<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: Send + 'static;

    fn query_scalar<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: FromSql + Send + 'static;

    fn readonly_transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: Send + 'static;
}

pub trait WriteDB {
    fn execute(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> impl Future<Output = Result<Ack<usize>>> + Send;

    fn execute_batch(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
    ) -> impl Future<Output = Result<Ack<()>>> + Send;

    fn transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<Ack<A>>> + Send
    where
        A: Send + Unpin + 'static;
}

trait HasDB {
    fn db(&self) -> &DB;
}

impl<X> ReadDB for X
where
    X: HasDB,
{
    fn query_row<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: Send + 'static,
    {
        self.db().query_row(sql, params, f)
    }

    fn query_scalar<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: FromSql + Send + 'static,
    {
        self.db().query_scalar(sql, params)
    }

    fn readonly_transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: Send + 'static,
    {
        self.db().readonly_transaction(thunk)
    }
}

impl<X> WriteDB for X
where
    X: HasDB,
{
    fn execute(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> impl Future<Output = Result<Ack<usize>>> + Send {
        self.db().execute(sql, params)
    }

    fn execute_batch(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
    ) -> impl Future<Output = Result<Ack<()>>> + Send {
        self.db().execute_batch(sql)
    }

    fn transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<Ack<A>>> + Send
    where
        A: Send + Unpin + 'static,
    {
        self.db().transaction(thunk)
    }
}

// ===== Follower =====

pub trait Follower: ReadDB {
    fn watch_leader(&self) -> watch::Receiver<Option<Node>>;
}

#[derive(Debug)]
pub struct FollowerNode {
    db: DB,
    watch_leader_node: watch::Receiver<Option<Node>>,
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

impl HasDB for FollowerNode {
    fn db(&self) -> &DB {
        &self.db
    }
}

impl Follower for FollowerNode {
    fn watch_leader(&self) -> watch::Receiver<Option<Node>> {
        self.watch_leader_node.clone()
    }
}

// ===== Leader =====

pub trait Leader: ReadDB + WriteDB {
    fn shutdown(self) -> impl Future<Output = Result<()>>;
}

#[derive(Debug)]
pub struct LeaderNode {
    db: DB,
    keep_alive: JoinHandle<Replica>,
    cancel: DropGuard,
}

impl LeaderNode {
    pub async fn join(node: Node, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        debug!(?node, "Joining cluster as Leader");

        // Open the replica
        let replica = Replica::open(&node.cluster_id, object_store).await?;

        // Return the node only after acquiring leadership
        Self::wait_for_leadership(node, replica).await
    }

    async fn wait_for_leadership(node: Node, mut replica: Replica) -> Result<Self> {
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

                        // Get a (read-write) DB
                        let db = replica.owned_db();

                        // Spawn a keep-alive task incrementing the epoch and safely synchronizing the DB
                        // until the leader is dropped or explicitly shutdown
                        let cancel = CancellationToken::new();
                        let keep_alive =
                            tokio::spawn(Self::keep_alive(node, replica, cancel.child_token()));
                        let cancel = cancel.drop_guard();

                        return Ok(Self {
                            db,
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

    async fn keep_alive(node: Node, mut replica: Replica, cancel: CancellationToken) -> Replica {
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
        replica
    }

    pub async fn shutdown(self) -> Result<()> {
        drop(self.cancel);
        let mut replica = self.keep_alive.await?;
        replica.try_change_leader(None).await?;
        debug!("Released leadership");
        Ok(())
    }
}

impl HasDB for LeaderNode {
    fn db(&self) -> &DB {
        &self.db
    }
}

impl Leader for LeaderNode {
    async fn shutdown(self) -> Result<()> {
        LeaderNode::shutdown(self).await
    }
}
