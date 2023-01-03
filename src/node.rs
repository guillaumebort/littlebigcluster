use std::{borrow::Cow, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use chrono::Utc;
use object_store::ObjectStore;
use parking_lot::RwLock;
use rusqlite::{types::FromSql, Params, Transaction};
use serde::de;
use tokio::select;
use tokio::task::JoinSet;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::manifest::{Leadership, Manifest, ManifestData};

pub trait Member {
    type Manifest: ManifestData;

    fn manifest(&self) -> Self::Manifest;
}

pub trait Follower: Member {
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
    uuid: Uuid,
    address: SocketAddr,
    cluster_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl Node {
    pub fn new(cluster_id: String, object_store: Arc<dyn ObjectStore>) -> Self {
        let uuid = Uuid::now_v7();
        let address = "127.0.0.1:8000".parse().unwrap();
        Self {
            uuid,
            address,
            cluster_id,
            object_store,
        }
    }
}

#[derive(Debug)]
pub struct LeaderNode<M>
where
    M: ManifestData,
{
    node: Node,
    manifest: Arc<RwLock<Manifest<M>>>,
    tasks: JoinSet<()>,
}

impl<M> LeaderNode<M>
where
    M: ManifestData,
{
    pub async fn wait_for_leadership(node: Node) -> Result<Self> {
        info!("Waiting for leadership...");
        let mut manifest = Manifest::<M>::watch(
            node.cluster_id.clone(),
            node.object_store.clone(),
            Duration::from_secs(1),
        )
        .await?;
        loop {
            let wakeup = match manifest.borrow().leader {
                Leadership::HasLeader { expire_at, .. } => {
                    (expire_at - Utc::now()).to_std().unwrap_or_default()
                }
                _ => Duration::ZERO,
            };
            select! {
                _ = manifest.changed() => {}
                _ = tokio::time::sleep(wakeup) => {}
            }
            let manifest = manifest.borrow();
            if manifest.leader.has_leader() {
                continue;
            } else {
                debug!(manifest.epoch, "No leader for the cluster");
                let mut manifest = manifest.clone();

                manifest.epoch += 1;
                manifest.leader = Leadership::HasLeader {
                    uuid: node.uuid,
                    address: node.address,
                    expire_at: Utc::now() + Duration::from_secs(30),
                };

                match Manifest::write(&node.object_store, &manifest).await {
                    Ok(()) => {
                        info!(manifest.epoch, "Acquired leadership");
                        manifest.epoch += 1;

                        let manifest = Arc::new(RwLock::new(manifest));
                        let mut tasks = JoinSet::new();
                        // fork a task to keep the leadership alive, this will be cancelled when the node is dropped
                        tasks.spawn(Self::keep_alive(node.clone(), manifest.clone()));
                        return Ok(Self {
                            node,
                            manifest,
                            tasks,
                        });
                    }
                    Err(err) => {
                        debug!(?err, "Failed to acquire leadership");
                    }
                }
            }
        }
    }

    pub async fn shutdown(mut self) {
        self.tasks.shutdown().await;
        let mut manifest = Arc::into_inner(self.manifest)
            .expect("leaked manifest?")
            .into_inner();
        manifest.leader = Leadership::NoLeader;
        if let Err(err) = Manifest::write(&self.node.object_store, &manifest).await {
            error!(?err, "Failed to release leadership");
        } else {
            info!(manifest.epoch, "Released leadership");
        }
    }

    async fn keep_alive(node: Node, manifest: Arc<RwLock<Manifest<M>>>) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let current_manifest = manifest.read().clone();
            match Manifest::write(&node.object_store, &current_manifest).await {
                Ok(()) => {
                    let epoch = {
                        let mut manifest = manifest.write();
                        manifest.epoch += 1;
                        manifest.leader = Leadership::HasLeader {
                            uuid: node.uuid,
                            address: node.address,
                            expire_at: Utc::now() + Duration::from_secs(30),
                        };
                        manifest.epoch
                    };

                    if let Some(old_epoch) = epoch.checked_sub(30) {
                        if let Err(e) = Manifest::<M>::delete(&node.object_store, old_epoch).await {
                            error!(?e, "Failed to delete old manifest");
                        }
                    }
                }
                Err(err) => {
                    error!(?err, "Failed to renew leadership");
                }
            }
        }
    }
}

impl<M> Member for LeaderNode<M>
where
    M: ManifestData,
{
    type Manifest = M;

    fn manifest(&self) -> Self::Manifest {
        self.manifest.read().manifest.clone()
    }
}

impl<M> Follower for LeaderNode<M> where M: ManifestData {}

impl<M> Leader for LeaderNode<M>
where
    M: ManifestData,
{
    async fn shutdown(self) -> Result<()> {
        LeaderNode::shutdown(self).await;
        Ok(())
    }
}
