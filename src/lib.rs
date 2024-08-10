mod client;
mod db;
mod node;
mod replica;
mod server;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::Router;
pub use node::{Follower, Leader, Node, StandByLeader};
use node::{FollowerNode, LeaderNode};
use object_store::ObjectStore;
use replica::Replica;
pub use server::{JsonResponse, LeaderState};

pub struct LittleBigCluster {
    cluster_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl LittleBigCluster {
    pub fn at(cluster_id: impl Into<String>, object_store: impl ObjectStore) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let object_store = Arc::new(object_store);
        Ok(Self {
            cluster_id,
            object_store,
        })
    }

    pub fn id(&self) -> &str {
        &self.cluster_id
    }

    pub async fn join_as_leader(
        self,
        az: String,
        address: SocketAddr,
        router: Router<LeaderState>,
    ) -> Result<impl StandByLeader> {
        let node = Node::new(self.cluster_id, az, Some(address));
        LeaderNode::join(node, router, self.object_store).await
    }

    pub async fn join_as_follower(self, az: String) -> Result<impl Follower> {
        let node = Node::new(self.cluster_id, az, None);
        FollowerNode::join(node, self.object_store).await
    }

    pub async fn init(&self) -> Result<()> {
        Replica::init(self.cluster_id.clone(), self.object_store.clone()).await?;
        Ok(())
    }
}
