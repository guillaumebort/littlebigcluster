mod db;
mod manifest;
mod node;

use std::sync::Arc;

use anyhow::{Context, Result};
use manifest::{Leadership, Manifest, ManifestData};
use node::LeaderNode;
pub use node::{Follower, Leader, Member, Node};
use object_store::ObjectStore;
use tracing::debug;

pub struct LiteCluster<M>
where
    M: ManifestData,
{
    cluster_id: String,
    object_store: Arc<dyn ObjectStore>,
    _phantom: std::marker::PhantomData<M>,
}

impl<M> LiteCluster<M>
where
    M: ManifestData,
{
    pub fn open(cluster_id: impl Into<String>, object_store: impl ObjectStore) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let object_store = Arc::new(object_store);
        Ok(Self {
            cluster_id,
            object_store,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn id(&self) -> &str {
        &self.cluster_id
    }

    pub async fn join_as_leader(self) -> Result<impl Leader<Manifest = M>> {
        let node = Node::new(self.cluster_id, self.object_store);
        LeaderNode::wait_for_leadership(node).await
    }

    pub async fn join_as_follower(self) -> Result<impl Follower<Manifest = M>> {
        let node = Node::new(self.cluster_id, self.object_store);
        LeaderNode::wait_for_leadership(node).await // FIXME
    }

    pub async fn join_as_member(self) -> Result<impl Member<Manifest = M>> {
        let node = Node::new(self.cluster_id, self.object_store);
        LeaderNode::wait_for_leadership(node).await // FIXME
    }

    pub async fn init(&self, manifest: M) -> Result<()> {
        Manifest::write(
            &self.object_store,
            &Manifest {
                cluster_id: self.cluster_id.clone(),
                epoch: 0,
                leader: Leadership::NoLeader,
                manifest,
            },
        )
        .await
    }
}
