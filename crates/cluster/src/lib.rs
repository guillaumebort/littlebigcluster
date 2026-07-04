//! Cluster node orchestration and membership.

pub mod proto {
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("lbc.cluster");
}

pub mod coordinator;
pub mod membership;
pub mod registry;

pub use coordinator::{ClusterView, Node, NodeConfig, NodeHandle};
pub use registry::{JoinError, Member, Registry};
