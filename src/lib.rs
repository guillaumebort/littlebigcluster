mod client;
mod config;
mod db;
mod follower;
mod leader;
mod replica;
mod server;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::Router;
pub use config::Config;
pub use follower::Follower;
use follower::{FollowerNode, FollowerState};
use leader::LeaderNode;
pub use leader::{Leader, LeaderState, LeaderStatus, StandByLeader};
use object_store::ObjectStore;
use replica::Replica;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub uuid: Uuid,
    pub cluster_id: String,
    pub az: String,
    pub address: SocketAddr,
    pub role: String,
}

impl Node {
    pub fn new(cluster_id: String, az: String, address: SocketAddr, role: String) -> Self {
        let uuid = Uuid::now_v7();
        Self {
            uuid,
            cluster_id,
            az,
            address,
            role,
        }
    }
}

pub struct LittleBigCluster {
    cluster_id: String,
    object_store: Arc<dyn ObjectStore>,
    config: config::Config,
}

impl LittleBigCluster {
    pub fn at(
        cluster_id: impl Into<String>,
        object_store: impl ObjectStore,
        config: Config,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let object_store = Arc::new(object_store);
        Ok(Self {
            cluster_id,
            object_store,
            config,
        })
    }

    pub fn id(&self) -> &str {
        &self.cluster_id
    }

    pub async fn join_as_leader(
        self,
        az: impl Into<String>,
        address: impl Into<SocketAddr>,
        router: Router<LeaderState>,
    ) -> Result<impl StandByLeader> {
        let node = Node::new(
            self.cluster_id,
            az.into(),
            address.into(),
            "leader(standby)".to_string(),
        );
        LeaderNode::join(node, router, self.object_store, self.config).await
    }

    pub async fn join_as_follower(
        self,
        az: impl Into<String>,
        address: impl Into<SocketAddr>,
        router: Router<FollowerState>,
        role: impl Into<String>,
    ) -> Result<impl Follower> {
        let node = Node::new(self.cluster_id, az.into(), address.into(), role.into());
        FollowerNode::join(node, router, self.object_store, self.config).await
    }

    pub async fn init(&self) -> Result<()> {
        Replica::init(self.cluster_id.clone(), self.object_store.clone()).await?;
        Ok(())
    }
}
