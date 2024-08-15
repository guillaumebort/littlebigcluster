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
use follower::{ClusterState, FollowerNode};
use leader::LeaderNode;
pub use leader::{Leader, LeaderState, LeaderStatus, StandByLeader};
use object_store::ObjectStore;
use replica::Replica;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub uuid: Uuid,
    pub cluster_id: String,
    pub az: String,
    pub address: SocketAddr,
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
        additional_roles: Vec<(String, Router<ClusterState>)>,
    ) -> Result<impl StandByLeader> {
        let az = az.into();
        let address = address.into();
        let (roles, addtional_router) = Self::roles_with_router(additional_roles)?;
        let node = Node::new(self.cluster_id, az, address);
        LeaderNode::join(
            node,
            router,
            addtional_router,
            self.object_store,
            roles,
            self.config,
        )
        .await
    }

    pub async fn join_as_follower(
        self,
        az: impl Into<String>,
        address: impl Into<SocketAddr>,
        roles: Vec<(String, Router<ClusterState>)>,
    ) -> Result<impl Follower> {
        let az = az.into();
        let address = address.into();
        let (roles, router) = Self::roles_with_router(roles)?;
        let node = Node::new(self.cluster_id, az, address);
        FollowerNode::join(node, router, self.object_store, roles, self.config).await
    }

    fn roles_with_router(
        roles_with_routers: Vec<(String, Router<ClusterState>)>,
    ) -> Result<(Vec<String>, Router<ClusterState>)> {
        let mut roles = Vec::with_capacity(roles_with_routers.len());
        let mut router = Router::new();
        for (role, role_router) in roles_with_routers {
            roles.push(role.clone());
            router = router.merge(role_router);
        }
        Ok((roles, router))
    }

    pub async fn init(&self) -> Result<()> {
        Replica::init(self.cluster_id.clone(), self.object_store.clone()).await?;
        Ok(())
    }
}
