//! Cluster node orchestration and membership.

pub mod proto {
    tonic::include_proto!("lbc.cluster");
}

pub mod coordinator;
pub mod membership;
pub mod registry;

pub use coordinator::{ClusterView, Node, NodeConfig, NodeHandle};
pub use registry::{JoinError, Member, Registry};
