use std::{sync::Arc, time::Instant};

use anyhow::{anyhow, bail, Result};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, FutureExt, StreamExt};
use object_store::{path::Path, ObjectStore, PutMode, PutOptions};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{db::DB, Node};

struct Object {
    tmp: NamedTempFile,
    last_modified: DateTime<Utc>,
}

impl Object {
    pub fn path(&self) -> &std::path::Path {
        self.tmp.path()
    }

    pub fn last_modified(&self) -> DateTime<Utc> {
        self.last_modified
    }
}

#[derive(Debug)]
pub(crate) struct Replica {
    pub object_store: Arc<dyn ObjectStore>,
    last_modified: DateTime<Utc>,
    epoch: u64,
    db: DB,
}

impl Replica {
    pub const SNAPSHOT_PATH: &'static str = "/snapshots";
    pub const WAL_PATH: &'static str = "/wal";

    pub async fn open(cluster_id: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        // Restore latest snapshot
        let snapshot_epoch = Self::latest_snapshot_epoch(&object_store).await?;
        debug!(snapshot_epoch, "Restoring snapshot");
        let snapshot = Self::get(&object_store, Self::snapshot_path(snapshot_epoch)).await?;
        let last_modified = snapshot.last_modified();
        let db = DB::open(Some(snapshot.path())).await?;

        // verify cluster ID
        let db_cluster_id = db
            .query_scalar::<String>(r#"SELECT value FROM litecluster WHERE key = "cluster_id""#, [])
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
            last_modified,
            epoch: snapshot_epoch,
            db,
        };
        let t = Instant::now();
        replica.full_refresh().await?;
        info!(
            replica.epoch,
            snapshot_epoch,
            "Opened replica in {:?}",
            t.elapsed()
        );

        Ok(replica)
    }

    pub async fn init(cluster_id: String, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        let db = DB::open(None).await?;
        {
            let object_store = object_store.clone();
            if let Some(object) = object_store
                .list(Some(&Path::from(Self::WAL_PATH)))
                .next()
                .await
            {
                bail!("Cluster already initialized? found: {}", object?.location);
            }
            let ack = db
                .transaction(|txn| {
                    txn.execute_batch(r#"CREATE TABLE litecluster (key TEXT PRIMARY KEY, value ANY)"#)?;
                    txn.execute(
                        r#"INSERT INTO litecluster VALUES ("cluster_id", ?1);"#,
                        [cluster_id],
                    )?;
                    txn.commit()
                })
                .await?;
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
        {
            let snapshost_0 = NamedTempFile::new()?;
            db.snapshot(snapshost_0.path()).await?;
            Self::put_if_not_exists(&object_store, &Self::snapshot_path(0), snapshost_0.path())
                .await?;
        }
        Ok(())
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn db(&self) -> &DB {
        &self.db
    }

    pub fn was_recently_modified(&self) -> bool {
        self.last_modified > Utc::now() - chrono::Duration::seconds(10)
    }

    pub async fn get_leader(&self) -> Result<Option<Node>> {
        self.db
            .query_row_opt(
                r#"
                    SELECT
                        json_extract(value, '$.uuid') AS uuid,
                        json_extract(value, '$.address') AS address
                    FROM litecluster
                    WHERE key = "leader"
                "#,
                [],
                |row| {
                    let uuid: String = row.get("uuid")?;
                    let address: String = row.get("address")?;
                    let uuid = Uuid::parse_str(&uuid)?;
                    let address = address.parse()?;
                    Ok(Node {
                        cluster_id: "TODO".to_string(),
                        uuid,
                        address,
                    })
                },
            )
            .await
    }

    pub async fn incr_epoch(&mut self, current_leader: Option<Node>) -> Result<()> {
        let next_epoch = self.epoch + 1;
        let leader_ack = if let Some(leader) = current_leader {
            self.db
                .execute(
                    r#"
                INSERT INTO litecluster VALUES("leader", json_object("uuid", ?1, "address", ?2))
                ON CONFLICT DO UPDATE SET value=json_object("uuid", ?1, "address", ?2);
            "#,
                    [leader.uuid.to_string(), leader.address.to_string()],
                )
                .await?
                .boxed()
        } else {
            self.db
                .execute(r#"DELETE FROM litecluster WHERE key = "leader""#, [])
                .await?
                .boxed()
        };
        let object_store = self.object_store.clone();
        match self
            .db
            .checkpoint(move |shallow_wal| {
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
            }
            Err(e) => {
                panic!("TODO: Failed to checkpoint: {}", e);
            }
        }
        // at this point the leader change is supposed to be durable
        leader_ack.await?;
        Ok(())
    }

    pub async fn refresh(&mut self) -> Result<()> {
        while let Some(wal) =
            Self::get_if_exists(&self.object_store, Self::wal_path(self.epoch + 1)).await?
        {
            self.db.apply_wal(wal.path()).await?;
            self.last_modified = wal.last_modified();
            self.epoch += 1;
            debug!(epoch = self.epoch, "Refreshed replica");
        }
        Ok(())
    }

    pub async fn full_refresh(&mut self) -> Result<()> {
        let current_epoch = Self::latest_wal_epoch(&self.object_store).await?;
        {
            let mut wals = FuturesOrdered::new();
            for i in (self.epoch + 1)..=current_epoch {
                let wal_path = Self::wal_path(i);
                wals.push_back(Self::get(&self.object_store, wal_path));
            }
            while let Some(wal) = wals.next().await {
                self.db.apply_wal(wal?.path()).await?;
                debug!(epoch = self.epoch, "Refreshed replica");
            }
        }
        self.epoch = current_epoch;
        self.refresh().await
    }

    async fn get(object_store: &impl ObjectStore, path: Path) -> Result<Object> {
        Self::get_if_exists(object_store, path)
            .await?
            .ok_or_else(|| anyhow!("Not found"))
    }

    async fn get_if_exists(object_store: &impl ObjectStore, path: Path) -> Result<Option<Object>> {
        match object_store.get(&path).await {
            Ok(get_result) => {
                let tmp = NamedTempFile::new()?;
                let last_modified = get_result.meta.last_modified;
                let mut bytes = get_result.into_stream();
                let mut file = tokio::fs::File::create(tmp.path()).await?;
                while let Some(chunk) = bytes.next().await {
                    file.write_all(&chunk?.as_ref()).await?;
                }
                file.sync_data().await?;
                Ok(Some(Object { tmp, last_modified }))
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

    fn snapshot_path(epoch: u64) -> Path {
        Path::from(Self::SNAPSHOT_PATH).child(format!("{:020}.db", epoch))
    }

    fn wal_path(epoch: u64) -> Path {
        Path::from(Self::WAL_PATH).child(format!("{:020}.wal", epoch))
    }

    async fn latest_snapshot_epoch(object_store: &impl ObjectStore) -> Result<u64> {
        Self::latest_epoch(object_store, &Path::from(Self::SNAPSHOT_PATH), ".db").await
    }

    async fn latest_wal_epoch(object_store: &impl ObjectStore) -> Result<u64> {
        Self::latest_epoch(object_store, &Path::from(Self::WAL_PATH), ".wal").await
    }

    async fn latest_epoch(
        object_store: &impl ObjectStore,
        path: &Path,
        suffix: &str,
    ) -> Result<u64> {
        let mut manifests = object_store.list(Some(&path));
        let mut current_epoch: Option<u64> = None;
        while let Some(manifest) = match manifests.next().await.transpose() {
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
