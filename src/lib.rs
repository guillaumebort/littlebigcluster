//! LittleBigCluster — replicated SQLite over an object store.
//!
//! Join a cluster with [`Node`], ship schema as [`Migration`]s, and run SQL via
//! [`NodeHandle::db`]. See the `demo` example.

pub use lbc_cluster::{ClusterView, JoinError, Member, Node, NodeConfig, NodeHandle, Registry};
pub use lbc_db::{
    Clock, Db, Durable, HousekeepingStats, LeaderState, Migration, RetentionConfig, SchemaSupport,
    SqliteStore, SystemClock, TestClock, TickResult, WriteError, DEFAULT_DB_FILE,
    DEFAULT_EPOCH_INTERVAL, DEFAULT_STALE_THRESHOLD_EPOCHS,
};

pub mod housekeeping {
    pub use lbc_db::housekeeping::*;
}
