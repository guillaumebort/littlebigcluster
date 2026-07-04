//! Housekeeping: snapshot and epoch GC.

use std::collections::HashSet;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use futures::TryStreamExt;
use object_store::ObjectStore;
use tracing::info;

use crate::{paths, read_last_snapshot, update_epoch_watermark};

#[derive(Debug, Clone)]
pub struct RetentionConfig {
    pub snapshot_keep_last: usize,
    pub snapshot_keep_within_secs: Vec<u64>,
    pub snapshot_keep_daily_days: u32,
    pub min_epoch_retention_secs: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            snapshot_keep_last: 5,
            snapshot_keep_within_secs: vec![3600, 6 * 3600, 24 * 3600],
            snapshot_keep_daily_days: 7,
            min_epoch_retention_secs: 60,
        }
    }
}

#[derive(Debug, Default)]
pub struct HousekeepingStats {
    pub snapshots_deleted: usize,
    pub epochs_deleted: usize,
    pub oldest_retained_snapshot_epoch: u64,
}

pub fn decide_snapshots_to_keep(
    snapshots: &[(u64, DateTime<Utc>)],
    now: DateTime<Utc>,
    config: &RetentionConfig,
) -> HashSet<u64> {
    let mut keep = HashSet::new();
    if snapshots.is_empty() {
        return keep;
    }

    for &(epoch, _) in snapshots.iter().rev().take(config.snapshot_keep_last) {
        keep.insert(epoch);
    }

    for &target_secs in &config.snapshot_keep_within_secs {
        let target_time = now - Duration::seconds(target_secs as i64);
        if let Some(&(epoch, _)) = snapshots
            .iter()
            .min_by_key(|&&(_, ts)| (ts - target_time).num_seconds().unsigned_abs())
        {
            keep.insert(epoch);
        }
    }

    for day_offset in 1..=config.snapshot_keep_daily_days {
        let day = (now - Duration::days(day_offset as i64)).date_naive();
        if let Some(&(epoch, _)) = snapshots
            .iter()
            .rev()
            .find(|&&(_, ts)| ts.date_naive() == day)
        {
            keep.insert(epoch);
        }
    }

    keep
}

pub async fn run_housekeeping(
    object_store: &dyn ObjectStore,
    prefix: &str,
    current_epoch: u64,
    config: &RetentionConfig,
    now: DateTime<Utc>,
) -> Result<HousekeepingStats> {
    let latest_snapshot = read_last_snapshot(object_store, prefix).await?.unwrap_or(0);

    let mut snapshots: Vec<(u64, DateTime<Utc>)> = Vec::new();
    let mut list = object_store.list(Some(&paths::snapshots_prefix(prefix)));
    while let Some(meta) = list.try_next().await? {
        if let Some(name) = meta.location.filename() {
            if let Ok(epoch) = name.parse::<u64>() {
                snapshots.push((epoch, meta.last_modified));
            }
        }
    }
    snapshots.sort_by_key(|(e, _)| *e);

    let keep = decide_snapshots_to_keep(&snapshots, now, config);
    let mut stats = HousekeepingStats {
        oldest_retained_snapshot_epoch: keep.iter().copied().min().unwrap_or(latest_snapshot),
        ..Default::default()
    };

    for (epoch, _) in &snapshots {
        if keep.contains(epoch) {
            continue;
        }
        if *epoch >= latest_snapshot {
            continue;
        }
        object_store
            .delete(&paths::snapshot_file(prefix, *epoch))
            .await?;
        stats.snapshots_deleted += 1;
    }

    let min_epoch = current_epoch.saturating_sub(config.min_epoch_retention_secs);
    let mut list = object_store.list(Some(&paths::epochs_prefix(prefix)));
    while let Some(meta) = list.try_next().await? {
        if let Some(name) = meta.location.filename() {
            if let Ok(epoch) = name.parse::<u64>() {
                if epoch < latest_snapshot && epoch < min_epoch {
                    object_store
                        .delete(&paths::epoch_file(prefix, epoch))
                        .await?;
                    stats.epochs_deleted += 1;
                }
            }
        }
    }

    update_epoch_watermark(object_store, prefix, current_epoch).await?;
    info!(
        "housekeeping deleted {} snapshots, {} epochs",
        stats.snapshots_deleted, stats.epochs_deleted
    );
    Ok(stats)
}
