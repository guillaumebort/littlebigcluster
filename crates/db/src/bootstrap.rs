//! Bootstrap one replicated DB from the object store.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use object_store::ObjectStore;
use tracing::{debug, warn};

use crate::epoch::{decode_wal_epoch, validate_format_version, Epoch, WalEpoch, FORMAT_VERSION};
use crate::paths;
use crate::sqlite::SqliteStore;
use crate::{apply_epoch_body, read_last_snapshot};

pub struct BootstrapResult {
    pub last_epoch: Epoch,
    pub store: Arc<SqliteStore>,
    pub snapshot_epoch: u64,
}

pub async fn bootstrap(
    object_store: &dyn ObjectStore,
    prefix: &str,
    local_path: &Path,
    db_file: &str,
) -> Result<BootstrapResult> {
    crate::prepare_local_dir(local_path).await?;

    let snapshot_epoch = read_last_snapshot(object_store, prefix).await?.unwrap_or(0);
    download_snapshot(object_store, prefix, local_path, db_file, snapshot_epoch).await?;

    let db_path = local_path.join(db_file);
    let store = Arc::new(SqliteStore::open(&db_path, false)?);

    let mut current = snapshot_epoch;
    let mut last_meta: Option<Epoch> = None;
    let mut applied: u64 = 0;

    loop {
        match download_wal_epoch(object_store, prefix, current).await {
            Ok(Some(wal_epoch)) => {
                validate_format_version(
                    &format!("epoch {current}"),
                    wal_epoch.metadata.format_version,
                )?;
                let body = wal_epoch.body.clone();
                apply_epoch_body(&store, &body)?;
                store.refresh_read_pool()?;
                last_meta = Some(wal_epoch.metadata);
                applied += 1;
                current += 1;
            }
            Ok(None) => break,
            Err(e) => {
                if current == snapshot_epoch {
                    return Err(e);
                }
                warn!("epoch {current} not available yet, stopping replay: {e}");
                break;
            }
        }
    }

    debug!("bootstrap replayed {applied} epoch(s) from snapshot epoch {snapshot_epoch}");

    let last_epoch = last_meta.unwrap_or_else(|| Epoch {
        epoch: snapshot_epoch,
        leader_id: String::new(),
        leader_addr: String::new(),
        created_at: chrono::Utc::now().to_rfc3339(),
        format_version: FORMAT_VERSION,
    });

    Ok(BootstrapResult {
        last_epoch,
        store,
        snapshot_epoch,
    })
}

async fn download_snapshot(
    object_store: &dyn ObjectStore,
    prefix: &str,
    local_path: &Path,
    db_file: &str,
    epoch: u64,
) -> Result<()> {
    let path = paths::snapshot_file(prefix, epoch);
    let data = object_store.get(&path).await?;
    let bytes = data.bytes().await?;
    let dest = local_path.join(db_file);
    tokio::fs::write(&dest, &bytes).await?;
    Ok(())
}

async fn download_wal_epoch(
    object_store: &dyn ObjectStore,
    prefix: &str,
    epoch: u64,
) -> Result<Option<WalEpoch>> {
    let path = paths::epoch_file(prefix, epoch);
    match object_store.get(&path).await {
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
        Ok(data) => {
            let bytes = data.bytes().await?;
            let wal_epoch = tokio::task::spawn_blocking(move || decode_wal_epoch(&bytes)).await??;
            Ok(Some(wal_epoch))
        }
    }
}
