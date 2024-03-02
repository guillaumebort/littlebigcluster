mod db;
mod node;
mod replica;

use std::sync::Arc;

use anyhow::{Context, Result};
use node::LeaderNode;
pub use node::{Follower, Leader, Node};
use object_store::ObjectStore;
use replica::Replica;
use tracing::debug;

pub struct LiteCluster {
    cluster_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl LiteCluster {
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

    pub async fn join_as_leader(self) -> Result<impl Leader> {
        let node = Node::new(self.cluster_id);
        LeaderNode::join(node, self.object_store).await
    }

    pub async fn join_as_follower(self) -> Result<impl Follower> {
        let node = Node::new(self.cluster_id);
        LeaderNode::join(node, self.object_store).await // FIXME
    }

    pub async fn init(&self) -> Result<()> {
        Replica::init(self.cluster_id.clone(), self.object_store.clone()).await?;
        Ok(())
    }
}
