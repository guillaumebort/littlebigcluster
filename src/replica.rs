use std::{sync::Arc, time::Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    FutureExt, StreamExt, TryStreamExt,
};
use object_store::{path::Path, ObjectStore, PutMode, PutOptions};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, trace};
use uuid::Uuid;

use crate::{db::DB, Node};

const SNAPSHOT_INTERVAL: u32 = 30;
const KEEP_SNAPSHOTS: usize = 10;

#[derive(Debug)]
pub(crate) struct Replica {
    pub object_store: Arc<dyn ObjectStore>,
    snapshot_epoch: u32,
    epoch: u32,
    db: DB,
}

impl Replica {
    pub const SNAPSHOT_PATH: &'static str = "/snapshots";
    pub const WAL_PATH: &'static str = "/wal";

    pub async fn open(cluster_id: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        // Find latest snapshot
        let snapshot_epoch = Self::latest_snapshot_epoch(&object_store).await?;
        debug!(snapshot_epoch, ?object_store, "Restoring snapshot");
        let snapshot = Self::get(&object_store, Self::snapshot_path(snapshot_epoch)).await?;

        // Create a DB from the snapshot
        let db = DB::open(Some(snapshot.path())).await?;

        // verify cluster ID
        let db_cluster_id: String =
            sqlx::query_scalar(r#"SELECT value FROM litecluster WHERE key = 'cluster_id'"#)
                .fetch_one(db.read_pool())
                .await?;

        if db_cluster_id != cluster_id {
            bail!(
                "Cluster ID mismatch: expected {}, got {}",
                cluster_id,
                db_cluster_id
            )
        }

        // Replay WAL
        let mut replica = Replica {
            object_store,
            snapshot_epoch,
            epoch: snapshot_epoch,
            db,
        };

        // Do a full refresh (find latest WALs and apply them)
        {
            let t = Instant::now();
            replica.full_refresh().await?;
            debug!(
                replica.epoch,
                snapshot_epoch,
                "Opened replica in {:?}",
                t.elapsed()
            );
        }

        Ok(replica)
    }

    pub async fn init(cluster_id: String, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        // Create a new (fresh) DB
        let db = DB::open(None).await?;
        {
            let object_store = object_store.clone();

            // Verify that there is no file in the WAL path already
            if let Some(object) = object_store
                .list(Some(&Path::from(Self::WAL_PATH)))
                .next()
                .await
            {
                bail!("Cluster already initialized? found: {}", object?.location);
            }

            // Initialize the system table
            let ack = db
                .transaction(|mut txn| {
                    async move {
                        sqlx::query(r#"CREATE TABLE litecluster (key TEXT PRIMARY KEY, value ANY)"#)
                            .execute(&mut *txn)
                            .await?;
                        sqlx::query(r#"INSERT INTO litecluster VALUES ('cluster_id', ?1)"#)
                            .bind(cluster_id)
                            .execute(&mut *txn)
                            .await?;
                        sqlx::query(r#"INSERT INTO litecluster VALUES ('epoch', 0)"#)
                            .execute(&mut *txn)
                            .await?;
                        sqlx::query(r#"INSERT INTO litecluster VALUES ('last_update', ?1)"#)
                            .bind(Utc::now().to_rfc3339())
                            .execute(&mut *txn)
                            .await?;

                        txn.commit().await
                    }
                    .boxed()
                })
                .await?;

            // Do a first checkpoint for epoch 0 and push the WAL to the object store
            db.checkpoint(|wal_path| {
                async move {
                    let wal_snapshot_path = Self::wal_path(0);
                    Self::put_if_not_exists(&object_store, &wal_snapshot_path, &wal_path).await
                }
                .boxed()
            })
            .await?;
            ack.await?;
        }

        // Create the initial snapshot for epoch 0
        {
            let snapshost_0 = NamedTempFile::new()?;
            db.snapshot(snapshost_0.path()).await?;
            Self::put_if_not_exists(&object_store, &Self::snapshot_path(0), snapshost_0.path())
                .await?;
        }

        Ok(())
    }

    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    pub fn snapshot_epoch(&self) -> u32 {
        self.snapshot_epoch
    }

    pub fn owned_db(&self) -> DB {
        self.db.clone()
    }

    pub async fn last_update(&self) -> Result<DateTime<Utc>> {
        let last_update: String =
            sqlx::query_scalar(r#"SELECT value FROM litecluster WHERE key = 'last_update'"#)
                .fetch_one(self.db.read_pool())
                .await?;
        Ok(DateTime::parse_from_rfc3339(&last_update)
            .context("cannot parse `last_update` date")?
            .with_timezone(&Utc))
    }

    /// Retrieve the leader for the replica current epoch
    pub async fn leader(&self) -> Result<Option<Node>> {
        let maybe_node: Option<(String, String, String, String)> = sqlx::query_as(
            r#"
            SELECT * FROM
                (SELECT
                    json_extract(value, '$.uuid') AS uuid,
                    json_extract(value, '$.az') AS az,
                    json_extract(value, '$.address') AS address
                FROM litecluster
                WHERE key = 'leader')
                JOIN
                (SELECT value AS cluster_id FROM litecluster WHERE key = 'cluster_id')
            "#,
        )
        .fetch_optional(self.db.read_pool())
        .await?;
        Ok(if let Some((uuid, az, address, cluster_id)) = maybe_node {
            Some(Node {
                uuid: Uuid::parse_str(&uuid)?,
                az,
                address: address.parse().ok(),
                cluster_id,
            })
        } else {
            None
        })
    }

    /// Try to increment the epoch and create a new WAL
    /// This is an atomic operation that only the current leader is allowed to do
    /// If there is a conflict it means that the current node is not the leader anymore
    pub async fn incr_epoch(&mut self) -> Result<u32> {
        let next_epoch = self.epoch + 1;
        let object_store = self.object_store.clone();

        // Snapshot if needed
        if next_epoch - self.snapshot_epoch >= SNAPSHOT_INTERVAL {
            self.snapshot().await?;
        }

        let _ = self
            .db
            .transaction(move |mut txn| {
                async move {
                    sqlx::query(r#"UPDATE litecluster SET value=?1 WHERE key = 'epoch'"#)
                        .bind(next_epoch)
                        .execute(&mut *txn)
                        .await?;
                    sqlx::query(r#"UPDATE litecluster SET value=?1 WHERE key = 'last_update'"#)
                        .bind(Utc::now().to_rfc3339())
                        .execute(&mut *txn)
                        .await?;

                    txn.commit().await
                }
                .boxed()
            })
            .await?;

        match self
            .db
            .checkpoint(move |shallow_wal| {
                async move {
                    // TODO retry on _some_ failures (but not on conflict)
                    Self::put_if_not_exists(
                        &object_store,
                        &Self::wal_path(next_epoch),
                        &shallow_wal,
                    )
                    .await
                }
                .boxed()
            })
            .await
        {
            Ok(()) => {
                self.epoch = next_epoch;
                Ok(next_epoch)
            }
            Err(e) => {
                // So we are not leader anymore
                Err(anyhow!("Failed to increment epoch: {}", e))
            }
        }
    }

    /// Do a snapshot of the current state of the DB and push it to the object store
    async fn snapshot(&mut self) -> Result<()> {
        // First create a local snapshot
        let snapshot = NamedTempFile::new()?;
        self.db.snapshot(snapshot.path()).await?;

        // The snaphshot is for the current epoch
        let snapshot_epoch = self.epoch;
        debug!(snapshot_epoch, "Created snapshot");
        self.snapshot_epoch = snapshot_epoch;

        // We are going to upload the snapshot in the background to avoid blocking the leader
        let object_store = self.object_store.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::put_if_not_exists(
                &object_store,
                &Self::snapshot_path(snapshot_epoch),
                snapshot.path(),
            )
            .await
            {
                error!(?e, "Failed to upload snapshot for epoch {}", snapshot_epoch);
            } else {
                trace!(snapshot_epoch, "Snapshot uploaded");
                // So it's maybe time to GC
                if let Err(e) = Self::gc(&object_store).await {
                    error!(?e, "Failed to GC after snapshot");
                }
            }
        });

        Ok(())
    }

    /// Attempt to change the leader for the next epoch
    pub async fn try_change_leader(&mut self, new_leader: Option<Node>) -> Result<()> {
        let next_epoch = self.epoch + 1;

        // mark us as leader in the DB
        let leader_ack = self.db.transaction(move |mut txn| async move {
            if let Some(leader) = new_leader {
                sqlx::query(
                    r#"
                    INSERT INTO litecluster VALUES('leader', json_object('uuid', ?1, 'address', ?2, 'az', ?3))
                    ON CONFLICT DO UPDATE SET value=json_object('uuid', ?1, 'address', ?2, 'az', ?3)
                    "#,
                )
                .bind(leader.uuid.to_string())
                .bind(leader
                    .address
                    .map(|a| a.to_string()).unwrap_or_default()
                )
                .bind(leader.az).execute(&mut *txn).await?;
            } else {
                sqlx::query(r#"DELETE FROM litecluster WHERE key = 'leader'"#).execute(&mut *txn).await?;
            }

            sqlx::query(r#"UPDATE litecluster SET value=?1 WHERE key = 'epoch'"#).bind(next_epoch).execute(&mut *txn).await?;
            sqlx::query(r#"UPDATE litecluster SET value=?1 WHERE key = 'last_update'"#).bind(Utc::now().to_rfc3339()).execute(&mut *txn).await?;

            txn.commit().await
        }.boxed()).await?;

        // Try to checkpoint for the next epoch
        let object_store = self.object_store.clone();
        match self
            .db
            // If it fails the change in the DB will be rolled back so we can keep applying WALs received by the new leader without starting from scratch
            .try_checkpoint(move |shallow_wal| {
                async move {
                    Self::put_if_not_exists(
                        &object_store,
                        &Self::wal_path(next_epoch),
                        &shallow_wal,
                    )
                    .await
                }
                .boxed()
            })
            .await
        {
            Ok(()) => {
                self.epoch = next_epoch;
                // at this point the leader change is supposed to be durable
                leader_ack.await?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Attempt to refresh the replica by applying the next WAL
    /// Lookup for the next epoch WAL and apply it if it exists
    pub async fn refresh(&mut self) -> Result<()> {
        while let Some(wal) =
            Self::get_if_exists(&self.object_store, Self::wal_path(self.epoch + 1)).await?
        {
            self.db.apply_wal(wal.path()).await?;
            self.epoch += 1;
            trace!(epoch = self.epoch, "Refreshed replica");
        }
        Ok(())
    }

    pub async fn full_refresh(&mut self) -> Result<()> {
        let current_epoch = Self::latest_wal_epoch(&self.object_store).await?;
        if current_epoch < self.epoch {
            bail!("TODO: current_epoch < self.epoch")
        } else {
            let mut wals = FuturesOrdered::new();
            for epoch in (self.epoch + 1)..=current_epoch {
                let wal_path = Self::wal_path(epoch);
                wals.push_back(Self::get(&self.object_store, wal_path).map(move |o| (epoch, o)));
            }
            while let Some((epoch, wal)) = wals.next().await {
                let wal = wal?;
                self.db.apply_wal(wal.path()).await?;
                trace!(epoch, "Refreshed replica");
            }
        }
        self.epoch = current_epoch;
        self.refresh().await
    }

    pub async fn gc(object_store: &impl ObjectStore) -> Result<()> {
        // keep N latest snapshots
        let mut snapshots = object_store.list(Some(&Path::from(Self::SNAPSHOT_PATH)));
        let mut snaphost_epochs = Vec::with_capacity(snapshots.size_hint().0);
        while let Some(snapshot) = snapshots.next().await.transpose()? {
            if let Some(epoch) = snapshot
                .location
                .filename()
                .and_then(|filename| filename.strip_suffix(".db"))
                .and_then(|filename| filename.parse::<u32>().ok())
            {
                snaphost_epochs.push(epoch);
            }
        }

        if snaphost_epochs.is_empty() {
            bail!("No snapshots found");
        }

        snaphost_epochs.sort_unstable();
        snaphost_epochs.reverse();

        let latest_snapshot_epoch = if snaphost_epochs.len() > KEEP_SNAPSHOTS {
            let (keep, delete) = snaphost_epochs.split_at(KEEP_SNAPSHOTS);
            debug!(
                ?keep,
                ?delete,
                "GC Found {} snapshots",
                snaphost_epochs.len()
            );

            let delete_futures = FuturesUnordered::new();
            for epoch in delete {
                let path = Self::snapshot_path(*epoch);
                delete_futures.push(async move {
                    trace!(?path, "Deleting old SNAPSHOT");
                    object_store.delete(&path).await?;
                    anyhow::Ok(())
                });
            }
            if let Err(e) = delete_futures.try_collect::<Vec<_>>().await {
                error!(?e, "Failed to delete all snapshots");
            }
            keep[0]
        } else {
            snaphost_epochs[0]
        };

        // now delete all WALs that are older than the latest snapshot
        let delete_futures = FuturesUnordered::new();
        let mut wals = object_store.list(Some(&Path::from(Self::WAL_PATH)));
        while let Some(wal) = wals.next().await.transpose()? {
            if let Some(epoch) = wal
                .location
                .filename()
                .and_then(|filename| filename.strip_suffix(".wal"))
                .and_then(|filename| filename.parse::<u32>().ok())
            {
                if epoch < latest_snapshot_epoch {
                    let path = wal.location;
                    trace!(?path, "Deleting old WAL");
                    delete_futures.push(async move {
                        object_store.delete(&path).await?;
                        anyhow::Ok(())
                    });
                }
            }
        }
        if let Err(e) = delete_futures.try_collect::<Vec<_>>().await {
            error!(?e, "Failed to delete all WALs");
        }

        Ok(())
    }

    async fn get(object_store: &impl ObjectStore, path: Path) -> Result<NamedTempFile> {
        Self::get_if_exists(object_store, path)
            .await?
            .ok_or_else(|| anyhow!("Not found"))
    }

    async fn get_if_exists(
        object_store: &impl ObjectStore,
        path: Path,
    ) -> Result<Option<NamedTempFile>> {
        match object_store.get(&path).await {
            Ok(get_result) => {
                let tmp = NamedTempFile::new()?;
                let mut bytes = get_result.into_stream();
                let mut file = tokio::fs::File::create(tmp.path()).await?;
                while let Some(chunk) = bytes.next().await {
                    file.write_all(&chunk?.as_ref()).await?;
                }
                file.sync_data().await?;
                Ok(Some(tmp))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_if_not_exists(
        object_store: &impl ObjectStore,
        path: &Path,
        tmp: &std::path::Path,
    ) -> Result<()> {
        const CHUNK_SIZE: usize = 5 * 1_024 * 1024;
        let mut buf = BytesMut::with_capacity(CHUNK_SIZE);
        if tokio::fs::metadata(tmp).await?.len() > CHUNK_SIZE as u64 {
            todo!("multipart upload")
        } else {
            tokio::fs::File::open(tmp).await?.read_buf(&mut buf).await?;
            object_store
                .put_opts(
                    path,
                    buf.freeze().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
        }
        Ok(())
    }

    fn snapshot_path(epoch: u32) -> Path {
        Path::from(Self::SNAPSHOT_PATH).child(format!("{:010}.db", epoch))
    }

    fn wal_path(epoch: u32) -> Path {
        Path::from(Self::WAL_PATH).child(format!("{:010}.wal", epoch))
    }

    async fn latest_snapshot_epoch(object_store: &impl ObjectStore) -> Result<u32> {
        Self::latest_epoch(object_store, &Path::from(Self::SNAPSHOT_PATH), ".db").await
    }

    async fn latest_wal_epoch(object_store: &impl ObjectStore) -> Result<u32> {
        Self::latest_epoch(object_store, &Path::from(Self::WAL_PATH), ".wal").await
    }

    async fn latest_epoch(
        object_store: &impl ObjectStore,
        path: &Path,
        suffix: &str,
    ) -> Result<u32> {
        let mut items = object_store.list(Some(&path));
        let mut current_epoch: Option<u32> = None;
        while let Some(manifest) = match items.next().await.transpose() {
            Ok(file) => file,
            Err(e) => bail!("Failed to list objects: {}", e),
        } {
            if let Some(epoch) = manifest
                .location
                .filename()
                .and_then(|filename| filename.strip_suffix(suffix))
                .and_then(|filename| filename.parse().ok())
            {
                if current_epoch.is_some_and(|current_epoch| current_epoch < epoch) {
                    current_epoch = Some(epoch);
                } else if current_epoch.is_none() {
                    current_epoch = Some(epoch);
                }
            }
        }
        current_epoch.ok_or_else(|| anyhow!("No objects found (was the cluster initialized?)"))
    }
}
