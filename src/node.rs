use std::{borrow::Cow, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use chrono::Utc;
use object_store::ObjectStore;
use parking_lot::RwLock;
use rusqlite::{types::FromSql, Params, Transaction};
use serde::de;
use tokio::task::JoinSet;
use tokio::{select, task::JoinHandle};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::replica::Replica;

pub trait Follower {
    // fn query_row<A>(
    //     &self,
    //     sql: impl Into<Cow<'static, str>> + Send,
    //     params: impl Params + Send + 'static,
    //     f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    // ) -> impl Future<Output = Result<A>> + Send
    // where
    //     A: Send + 'static;

    // fn query_scalar<A>(
    //     &self,
    //     sql: impl Into<Cow<'static, str>> + Send,
    //     params: impl Params + Send + 'static,
    // ) -> impl Future<Output = Result<A>> + Send
    // where
    //     A: FromSql + Send + 'static;

    // fn readonly_transaction<A>(
    //     &self,
    //     thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    // ) -> impl Future<Output = Result<A>> + Send
    // where
    //     A: Send + 'static;
}

pub trait Leader: Follower {
    fn shutdown(self) -> impl Future<Output = Result<()>>;

    // fn execute(
    //     &self,
    //     sql: impl Into<Cow<'static, str>> + Send,
    //     params: impl Params + Send + 'static,
    // ) -> impl Future<Output = Result<usize>> + Send;

    // fn execute_batch(
    //     &self,
    //     sql: impl Into<Cow<'static, str>> + Send,
    // ) -> impl Future<Output = Result<()>> + Send;

    // fn transaction<A>(
    //     &self,
    //     thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    // ) -> impl Future<Output = Result<A>> + Send
    // where
    //     A: Send + 'static;
}

#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub struct LeaderNode {
    keep_alive: JoinHandle<Replica>,
    cancel: DropGuard,
}

impl LeaderNode {
    pub async fn join(node: Node, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        info!("Joining cluster...");
        let replica = Replica::open(&node.cluster_id, object_store).await?;
        Self::wait_for_leadership(node, replica).await
    }

    async fn wait_for_leadership(node: Node, mut replica: Replica) -> Result<Self> {
        info!("Waiting for leadership...");
        loop {
            replica.refresh().await?;
            if replica.leader().await?.is_none() || !replica.was_recently_modified() {
                // try to acquire leadership
                match replica.incr_epoch(Some(node.clone())).await {
                    Ok(()) => {
                        info!("Acquired leadership");
                        let cancel = CancellationToken::new();
                        let keep_alive =
                            tokio::spawn(Self::keep_alive(node, replica, cancel.child_token()));
                        let cancel = cancel.drop_guard();
                        return Ok(Self { keep_alive, cancel });
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
                    if let Err(err) = replica.incr_epoch(Some(node.clone())).await {
                        error!(?err, "Failed to sync replica");
                    } else {
                        debug!(epoch = replica.epoch(), "Synced successfully");
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
        replica.incr_epoch(None).await?;
        info!("Released leadership");
        Ok(())
    }
}

impl Follower for LeaderNode {}

impl Leader for LeaderNode {
    async fn shutdown(self) -> Result<()> {
        LeaderNode::shutdown(self).await
    }
}
