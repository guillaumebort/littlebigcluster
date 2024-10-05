mod db;
mod follower;
mod gossip;
mod http2_client;
mod http2_server;
mod leader;
mod leader_client;
mod replica;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::Router;
pub use follower::Follower;
use follower::{ClusterState, FollowerNode};
pub use gossip::{Member, Members, Node};
use leader::LeaderNode;
pub use leader::{Leader, LeaderState, LeaderStatus};
use object_store::ObjectStore;
use replica::Replica;
pub use http2_client::Http2Client;

pub struct LittleBigCluster {
    cluster_id: String,
    object_store: Arc<dyn ObjectStore>,
    config: Config,
}

impl LittleBigCluster {
    pub fn at(
        cluster_id: impl Into<String>,
        object_store: impl ObjectStore,
        config: Config,
    ) -> Self {
        let cluster_id = cluster_id.into();
        let object_store = Arc::new(object_store);
        Self {
            cluster_id,
            object_store,
            config,
        }
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
    ) -> Result<impl Leader> {
        let az = az.into();
        let address = address.into();
        let (roles, addtional_router) = Self::roles_with_router(additional_roles)?;
        let node = Node::new(az, address);
        LeaderNode::join(
            node,
            &self.cluster_id,
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
        let node = Node::new(az, address);
        FollowerNode::join(
            node,
            &self.cluster_id,
            router,
            self.object_store,
            roles,
            self.config,
        )
        .await
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
        Replica::init(self.cluster_id.clone(), &self.object_store).await?;
        Ok(())
    }
}

// -- Config

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub epoch_interval: Duration,
    pub snapshot_interval: Duration,
    pub session_timeout: Duration,
    pub retention_period: Duration,
}

impl Config {
    pub fn snapshot_interval_epochs(&self) -> u64 {
        self.snapshot_interval.as_secs() / self.epoch_interval.as_secs()
    }

    pub fn client_retry_timeout(&self) -> Duration {
        self.epoch_interval * 2
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            epoch_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(30),
            session_timeout: Duration::from_secs(20),
            retention_period: Duration::from_secs(60 * 60 * 24 * 7),
        }
    }
}
