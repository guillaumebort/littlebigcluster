//! Object-store paths for one replicated DB.

use object_store::path::Path as ObjectPath;

pub fn epoch_file(prefix: &str, epoch: u64) -> ObjectPath {
    ObjectPath::from(format!("{prefix}epochs/{epoch:020}"))
}

pub fn snapshot_file(prefix: &str, epoch: u64) -> ObjectPath {
    ObjectPath::from(format!("{prefix}snapshots/{epoch:020}"))
}

pub fn last_snapshot(prefix: &str) -> ObjectPath {
    ObjectPath::from(format!("{prefix}last_snapshot"))
}

pub fn epoch_watermark(prefix: &str) -> ObjectPath {
    ObjectPath::from(format!("{prefix}epoch_watermark"))
}

pub fn snapshots_prefix(prefix: &str) -> ObjectPath {
    ObjectPath::from(format!("{prefix}snapshots/"))
}

pub fn epochs_prefix(prefix: &str) -> ObjectPath {
    ObjectPath::from(format!("{prefix}epochs/"))
}
