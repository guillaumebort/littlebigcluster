use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::{
    stream::{self, FuturesOrdered},
    FutureExt, StreamExt,
};
use object_store::{path::Path, ObjectStore, PutMode, PutOptions};
use tempfile::NamedTempFile;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};
use tracing::{debug, error, trace};
use uuid::Uuid;

use crate::{config::Config, db::DB, Node};

#[derive(Debug)]
pub(crate) struct Replica {
    pub object_store: Arc<dyn ObjectStore>,
    snapshot_epoch: u64,
    epoch: u64,
    db: DB,
    config: Config,
    tasks: JoinSet<()>,
}

impl Replica {
    pub const PATH: &'static str = "/.lbc/";
    pub const LAST_SNAPSHOT_PATH: &'static str = "/.lbc/.last_snapshot";
    pub const LAST_VACUUM_PATH: &'static str = "/.lbc/.last_vacuum";

    pub async fn open(
        cluster_id: &str,
        object_store: Arc<dyn ObjectStore>,
        config: Config,
    ) -> Result<Self> {
        // Find epochs
        let (snapshot_epoch, current_epoch) = Self::latest_epochs(&object_store).await?;

        // Retrieve the last snapshot
        debug!(snapshot_epoch, ?object_store, "Restoring snapshot");
        let snapshot = Self::get(&object_store, Self::snapshot_path(snapshot_epoch)).await?;

        // Create a DB from the snapshot
        let db = DB::open(Some(snapshot.path())).await?;

        // verify cluster ID
        let db_cluster_id: String =
            sqlx::query_scalar(r#"SELECT value FROM _lbc WHERE key = 'cluster_id'"#)
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
            config,
            tasks: JoinSet::new(),
        };

        // Do a full refresh (find latest WALs and apply them)
        {
            let t = Instant::now();
            replica.full_refresh(current_epoch).await?;
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
                .list(Some(&Path::from(Self::PATH)))
                .next()
                .await
            {
                bail!("Cluster already initialized? found: {}", object?.location);
            }

            // Initialize the system table
            let ack = db
                .transaction(|mut txn| {
                    async move {
                        sqlx::query(r#"CREATE TABLE _lbc (key TEXT PRIMARY KEY, value ANY)"#)
                            .execute(&mut *txn)
                            .await?;
                        sqlx::query(r#"INSERT INTO _lbc VALUES ('cluster_id', ?1)"#)
                            .bind(cluster_id)
                            .execute(&mut *txn)
                            .await?;
                        sqlx::query(r#"INSERT INTO _lbc VALUES ('last_update', ?1)"#)
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

        // Markers
        Self::override_marker(&object_store, Self::LAST_SNAPSHOT_PATH, 0).await?;
        Self::override_marker(&object_store, Self::LAST_VACUUM_PATH, 0).await?;

        Ok(())
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn snapshot_epoch(&self) -> u64 {
        self.snapshot_epoch
    }

    pub fn db(&self) -> &DB {
        &self.db
    }

    pub async fn last_update(&self) -> Result<DateTime<Utc>> {
        let last_update: String =
            sqlx::query_scalar(r#"SELECT value FROM _lbc WHERE key = 'last_update'"#)
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
                FROM _lbc
                WHERE key = 'leader')
                JOIN
                (SELECT value AS cluster_id FROM _lbc WHERE key = 'cluster_id')
            "#,
        )
        .fetch_optional(self.db.read_pool())
        .await?;
        Ok(if let Some((uuid, az, address, cluster_id)) = maybe_node {
            Some(Node {
                uuid: Uuid::parse_str(&uuid)?,
                az,
                address: address.parse()?,
                cluster_id,
            })
        } else {
            None
        })
    }

    /// Try to increment the epoch and create a new WAL
    /// This is an atomic operation that only the current leader is allowed to do
    /// If there is a conflict it means that the current node is not the leader anymore
    pub async fn incr_epoch(&mut self) -> Result<u64> {
        let next_epoch = self.next_epoch()?;
        let object_store = self.object_store.clone();

        // Snapshot if needed
        if next_epoch - self.snapshot_epoch >= self.config.snapshot_interval_epochs().try_into()? {
            self.snapshot().await?;
        }

        let _ = self
            .db
            .transaction(move |mut txn| {
                async move {
                    sqlx::query(r#"UPDATE _lbc SET value=?1 WHERE key = 'last_update'"#)
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
                if let Err(e) =
                    Self::override_marker(&object_store, Self::LAST_SNAPSHOT_PATH, snapshot_epoch)
                        .await
                {
                    error!(?e, "Failed to update last snapshot marker");
                }
            }
        });

        Ok(())
    }

    pub async fn vacuum(&mut self, retention_period: Duration) -> Result<()> {
        let first_epoch_to_vacuum =
            Self::read_marker(&self.object_store, Self::LAST_VACUUM_PATH).await?;
        let last_epoch_to_vacuum = self
            .epoch
            .saturating_sub(retention_period.as_secs() / self.config.epoch_interval.as_secs());

        debug!(first_epoch_to_vacuum, last_epoch_to_vacuum, "Vacuum");
        let objects_to_delete = stream::unfold(first_epoch_to_vacuum, move |epoch| async move {
            if epoch < last_epoch_to_vacuum {
                if let Some(next_epoch) = epoch.checked_add(1) {
                    Some((
                        futures::stream::iter(vec![
                            Ok(Self::snapshot_path(epoch)),
                            Ok(Self::wal_path(epoch)),
                        ]),
                        next_epoch,
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .flatten()
        .boxed();

        // vacuum run in the background
        let object_store: Arc<dyn ObjectStore> = self.object_store.clone();
        self.tasks.spawn(async move {
            let mut results = object_store.delete_stream(objects_to_delete);
            let mut failure = None;
            while let Some(result) = results.next().await {
                match result {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(err) => {
                        failure = Some(err);
                    }
                }
            }
            if let Some(err) = failure {
                error!(?err, "Vacuum failed");
            } else {
                if let Err(e) = Self::override_marker(
                    &object_store,
                    Self::LAST_VACUUM_PATH,
                    last_epoch_to_vacuum,
                )
                .await
                {
                    error!(?e, "Failed to update last vacuum marker");
                }
            }
        });

        Ok(())
    }

    /// Attempt to change the leader for the next epoch
    pub async fn try_change_leader(&mut self, new_leader: Option<Node>) -> Result<()> {
        let next_epoch = self.next_epoch()?;

        // mark us as leader in the DB
        let leader_ack = self.db.transaction(move |mut txn| async move {
            if let Some(leader) = new_leader {
                sqlx::query(
                    r#"
                    INSERT INTO _lbc VALUES('leader', json_object('uuid', ?1, 'address', ?2, 'az', ?3))
                    ON CONFLICT DO UPDATE SET value=json_object('uuid', ?1, 'address', ?2, 'az', ?3)
                    "#,
                )
                .bind(leader.uuid.to_string())
                .bind(leader
                    .address.to_string()
                )
                .bind(leader.az).execute(&mut *txn).await?;
            } else {
                sqlx::query(r#"DELETE FROM _lbc WHERE key = 'leader'"#).execute(&mut *txn).await?;
            }

            sqlx::query(r#"UPDATE _lbc SET value=?1 WHERE key = 'last_update'"#).bind(Utc::now().to_rfc3339()).execute(&mut *txn).await?;

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

    fn next_epoch(&self) -> Result<u64> {
        self.epoch
            .checked_add(1)
            .ok_or_else(|| anyhow!("epoch overflow"))
    }

    pub async fn follow(&mut self) -> Result<()> {
        while let Some(wal) =
            Self::get_if_exists(&self.object_store, Self::wal_path(self.next_epoch()?)).await?
        {
            self.db.apply_wal(wal.path()).await?;
            self.epoch = self.next_epoch()?;
            trace!(epoch = self.epoch, "Refreshed replica");
        }
        Ok(())
    }

    pub async fn full_refresh(&mut self, current_epoch: u64) -> Result<()> {
        if current_epoch < self.epoch {
            bail!("TODO: current_epoch < self.epoch")
        } else {
            let mut wals = FuturesOrdered::new();
            for epoch in (self.next_epoch()?)..=current_epoch {
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
        self.follow().await
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

    async fn override_marker(
        object_store: &impl ObjectStore,
        path: &str,
        epoch: u64,
    ) -> Result<()> {
        object_store
            .put_opts(
                &Path::from(path),
                format!("{epoch:020}").into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn read_marker(object_store: &impl ObjectStore, path: &str) -> Result<u64> {
        match object_store.get(&Path::from(path)).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let epoch = std::str::from_utf8(bytes.as_ref())?.trim().parse()?;
                Ok(epoch)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn snapshot_path(epoch: u64) -> Path {
        Path::from(Self::PATH).child(format!("{epoch:020}.db"))
    }

    fn wal_path(epoch: u64) -> Path {
        Path::from(Self::PATH).child(format!("{epoch:020}.wal"))
    }

    fn parse_epoch(path: &Path) -> Result<u64> {
        Ok(path
            .filename()
            .ok_or_else(|| anyhow!("Invalid path"))?
            .strip_suffix(".db")
            .or_else(|| path.filename().unwrap().strip_suffix(".wal"))
            .ok_or_else(|| anyhow!("Invalid file extension"))?
            .parse()
            .context("Error parsing epoch from path")?)
    }

    async fn latest_epochs(object_store: &impl ObjectStore) -> Result<(u64, u64)> {
        let mut last_snapshot_epoch = Self::read_marker(object_store, Self::LAST_SNAPSHOT_PATH)
            .await
            .context("Error retrieving start epoch from +last_snapshot")?;

        let mut objects = object_store.list_with_offset(
            Some(&Path::from(Self::PATH)),
            &Self::snapshot_path(last_snapshot_epoch),
        );

        let mut current_epoch = last_snapshot_epoch;

        while let Some(object) = match objects.next().await.transpose() {
            Ok(file) => file,
            Err(e) => bail!("Failed to list objects: {}", e),
        } {
            if let Some("db") = object.location.extension() {
                let epoch = Self::parse_epoch(&object.location)?;
                if epoch > last_snapshot_epoch {
                    last_snapshot_epoch = epoch;
                }
            } else if let Some("wal") = object.location.extension() {
                let epoch = Self::parse_epoch(&object.location)?;
                if epoch > current_epoch {
                    current_epoch = epoch;
                }
            }
        }

        Ok((last_snapshot_epoch, current_epoch))
    }
}
