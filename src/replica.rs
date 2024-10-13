use std::{sync::Arc, time::Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::{
    channel::oneshot,
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

use crate::{db::DB, Config, Node};

#[derive(Debug)]
pub(crate) struct Replica {
    cluster_id: String,
    object_store: Arc<dyn ObjectStore>,
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
        object_store: &Arc<dyn ObjectStore>,
        config: Config,
    ) -> Result<Self> {
        // Find epochs
        let (snapshot_epoch, current_epoch) = Self::latest_epochs(object_store).await?;

        // Retrieve the last snapshot
        debug!(snapshot_epoch, ?object_store, "Restoring snapshot");
        let snapshot = Self::get(object_store, Self::snapshot_path(snapshot_epoch)).await?;

        // Create a DB from the snapshot
        let db = DB::open(Some(snapshot.path())).await?;

        // verify cluster ID
        let db_cluster_id: String =
            sqlx::query_scalar(r#"SELECT value FROM _lbc WHERE key = 'cluster_id'"#)
                .fetch_one(db.read())
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
            cluster_id: cluster_id.to_string(),
            object_store: object_store.clone(),
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

    pub async fn init(
        cluster_id: impl Into<String>,
        object_store: &Arc<dyn ObjectStore>,
    ) -> Result<()> {
        // Create a new (fresh) DB
        let db = DB::open(None).await?;
        {
            let object_store = object_store.clone();
            let cluster_id = cluster_id.into();

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
                .transaction(|txn| {
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

                        Ok(())
                    }
                    .boxed()
                })
                .await?;

            // Do a first checkpoint/snapshot for epoch 0 and push the WAL to the object store
            let snapshot_tmp = NamedTempFile::new()?;
            let wal_0 = Self::wal_path(0);
            let snapshot_0 = Self::snapshot_path(0);
            let object_store_clone = object_store.clone();
            db.checkpoint(
                |wal_tmp| {
                    async move {
                    Self::put_if_not_exists(&object_store_clone, &wal_0, &wal_tmp).await
                }
                .boxed()
                },
                Some(snapshot_tmp.path().into()),
            )
            .await?;
            Self::put_if_not_exists(&object_store, &snapshot_0, &snapshot_tmp.path()).await?;
            ack.await?;
        }

        // Markers
        Self::override_marker(object_store, Self::LAST_SNAPSHOT_PATH, 0).await?;
        Self::override_marker(object_store, Self::LAST_VACUUM_PATH, 0).await?;

        Ok(())
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
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
                .fetch_one(self.db.read())
                .await?;
        Ok(DateTime::parse_from_rfc3339(&last_update)
            .context("cannot parse `last_update` date")?
            .with_timezone(&Utc))
    }

    /// Retrieve the leader for the replica current epoch
    pub async fn leader(&self) -> Result<Option<Node>> {
        let maybe_node: Option<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT
                    json_extract(value, '$.uuid') AS uuid,
                    json_extract(value, '$.az') AS az,
                    json_extract(value, '$.address') AS address
                FROM _lbc
                WHERE key = 'leader'
            "#,
        )
        .fetch_optional(self.db.read())
        .await?;
        Ok(if let Some((uuid, az, address)) = maybe_node {
            Some(Node {
                uuid: Uuid::parse_str(&uuid)?,
                az,
                address: address.parse()?,
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
        let snapshot_tmp = if next_epoch - self.snapshot_epoch
            >= self.config.snapshot_interval_epochs().try_into()?
        {
            Some(NamedTempFile::new()?)
        } else {
            None
        };

        let _ = self
            .db
            .transaction(move |txn| {
                async move {
                    sqlx::query(r#"UPDATE _lbc SET value=?1 WHERE key = 'last_update'"#)
                        .bind(Utc::now().to_rfc3339())
                        .execute(txn)
                        .await?;

                    Ok(())
                }
                .boxed()
            })
            .await?;

        match self
            .db
            .checkpoint(
                move |shallow_wal| {
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
                },
                snapshot_tmp.as_ref().map(|tmp| tmp.path().to_owned()),
            )
            .await
        {
            Ok(()) => {
                if let Some(snapshot_tmp) = snapshot_tmp {
                    self.snapshot_epoch = next_epoch;
                    // Snapshot is uploaded in the background
                    let object_store = self.object_store.clone();
                    self.tasks.spawn(async move {
                        if let Err(e) = Self::put_if_not_exists(
                            &object_store,
                            &Self::snapshot_path(next_epoch),
                            snapshot_tmp.path(),
                        )
                        .await
                        {
                            error!(?e, "Failed to upload snapshot for epoch {}", next_epoch);
                        } else {
                            debug!(next_epoch, "Snapshot completed");
                            if let Err(e) = Self::override_marker(
                                &object_store,
                                Self::LAST_SNAPSHOT_PATH,
                                next_epoch,
                            )
                            .await
                            {
                                error!(?e, "Failed to update last snapshot marker");
                            }
                        }
                    });
                }

                self.epoch = next_epoch;
                Ok(next_epoch)
            }
            Err(e) => {
                // So we are not leader anymore
                Err(anyhow!("Failed to increment epoch: {}", e))
            }
        }
    }

    pub async fn schedule_vacuum(&mut self) -> Result<oneshot::Receiver<Result<()>>> {
        let first_epoch_to_vacuum =
            Self::read_marker(&self.object_store, Self::LAST_VACUUM_PATH).await?;
        let mut last_epoch_to_vacuum = self.epoch.saturating_sub(
            self.config.retention_period.as_secs().max(1)
                / self.config.epoch_interval.as_secs().max(1),
        );

        // in any case we don't vacuum past the last snapshot
        if last_epoch_to_vacuum >= self.snapshot_epoch {
            last_epoch_to_vacuum = self.snapshot_epoch;
        }

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
        let (tx, rx) = oneshot::channel();
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
                let _ = tx.send(Err(err.into()));
            } else {
                debug!("Vacuum completed");
                let _ = tx.send(Ok(()));
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

        debug!(
            first_epoch_to_vacuum,
            last_epoch_to_vacuum, "Vacuum scheduled"
        );

        Ok(rx)
    }

    /// Attempt to change the leader for the next epoch
    pub async fn try_change_leader(&mut self, new_leader: Option<Node>) -> Result<()> {
        let next_epoch = self.next_epoch()?;

        // mark us as leader in the DB
        let leader_ack = self.db.transaction(move |txn| async move {
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

            Ok(())
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;
    use test_log::test;

    use super::*;

    struct TmpObjectStore {
        #[allow(unused)]
        tmp: TempDir,
        object_store: Arc<dyn ObjectStore>,
    }

    impl TmpObjectStore {
        async fn new() -> Result<Self> {
            let tmp = tempfile::tempdir()?;
            let object_store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path())?);
            Ok(Self { tmp, object_store })
        }
    }

    #[test(tokio::test)]
    async fn init_replica() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        // and now let's open it
        let replica = Replica::open("lol", &tmp.object_store, Default::default()).await?;
        assert_eq!(0, replica.epoch);
        assert_eq!(0, replica.snapshot_epoch);

        // DB works
        let one: u32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(replica.db.read())
            .await?;
        assert_eq!(1, one);

        Ok(())
    }

    #[test(tokio::test)]
    async fn already_initialized_replica() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        // and try to init it again
        assert!(Replica::init("lol".to_string(), &tmp.object_store)
            .await
            .is_err());

        Ok(())
    }

    #[test(tokio::test)]
    async fn bad_cluster_id() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        // and now let's open it
        assert!(Replica::open("bad", &tmp.object_store, Default::default())
            .await
            .is_err());

        Ok(())
    }

    #[test(tokio::test)]
    async fn leader_follower() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        // let's open 2 replicas
        let mut leader = Replica::open("lol", &tmp.object_store, Default::default()).await?;
        let mut follower = Replica::open("lol", &tmp.object_store, Default::default()).await?;

        assert_eq!(0, leader.epoch);
        assert_eq!(0, leader.snapshot_epoch);
        assert_eq!(0, follower.epoch);
        assert_eq!(0, follower.snapshot_epoch);

        // leader write to the DB
        let ack = leader
            .db
            .transaction(|txn| {
                async move {
                    sqlx::query("CREATE TABLE lol (name TEXT)")
                        .execute(&mut *txn)
                        .await?;

                    sqlx::query("INSERT INTO lol VALUES ('test')")
                        .execute(&mut *txn)
                        .await?;
                    Ok(())
                }
                .boxed()
            })
            .await?;

        // move the leader to epoch 1
        leader.incr_epoch().await?;

        // so now the write above is acknowledged
        ack.await?;

        assert_eq!(1, leader.epoch);
        assert_eq!(0, leader.snapshot_epoch);

        // but follower don't know about it

        assert_eq!(0, follower.epoch);
        assert_eq!(0, follower.snapshot_epoch);

        assert!(sqlx::query("SELECT * FROM lol")
            .fetch_one(follower.db.read())
            .await
            .is_err());

        // now the second replica should be able to follow
        follower.follow().await?;

        // follower is now at epoch 1
        assert_eq!(1, follower.epoch);
        assert_eq!(0, follower.snapshot_epoch);

        // and it can see the data
        let test: String = sqlx::query_scalar("SELECT name FROM lol")
            .fetch_one(follower.db.read())
            .await?;
        assert_eq!("test", test);

        Ok(())
    }

    #[test(tokio::test)]
    async fn acquire_leadership() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        let leader_node = Node::new("A".to_string(), "0.0.0.0:0".parse()?);
        let standby_leader_node = Node::new("A".to_string(), "0.0.0.0:0".parse()?);

        // let's open 2 replicas
        let mut leader = Replica::open("lol", &tmp.object_store, Default::default()).await?;
        let mut standby_leader =
            Replica::open("lol", &tmp.object_store, Default::default()).await?;

        // leader acquire leadership
        leader.try_change_leader(Some(leader_node.clone())).await?;

        assert_eq!(1, leader.epoch);
        assert_eq!(
            Some(leader_node.uuid),
            leader.leader().await?.map(|n| n.uuid)
        );

        // standby leader tries to acquire leadership
        assert_eq!(0, standby_leader.epoch);
        assert!(standby_leader
            .try_change_leader(Some(standby_leader_node.clone()))
            .await
            .is_err());
        assert_eq!(0, standby_leader.epoch);

        // standby leader eventually sees the new leader
        assert_eq!(None, standby_leader.leader().await?);
        standby_leader.follow().await?;
        assert_eq!(
            Some(leader_node.uuid),
            standby_leader.leader().await?.map(|n| n.uuid)
        );
        assert_eq!(1, standby_leader.epoch);

        // standby leader steals leadership
        standby_leader
            .try_change_leader(Some(standby_leader_node.clone()))
            .await?;
        assert_eq!(2, standby_leader.epoch);

        // so original leader has been fenced off
        assert!(leader.incr_epoch().await.is_err());

        Ok(())
    }

    #[test(tokio::test)]
    async fn zombie_leader() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        // let's open 2 replicas
        let mut leader = Replica::open("lol", &tmp.object_store, Default::default()).await?;
        let mut zombie_leader = Replica::open("lol", &tmp.object_store, Default::default()).await?;

        // move the leader to epoch 1
        leader.incr_epoch().await?;

        // zombie leader tries to do the same
        assert!(zombie_leader.incr_epoch().await.is_err());

        Ok(())
    }

    #[test(tokio::test)]
    async fn snapshot_and_vacuum() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        // and now let's open it
        let mut replica = Replica::open(
            "lol",
            &tmp.object_store,
            Config {
                epoch_interval: Duration::from_secs(1),
                snapshot_interval: Duration::from_secs(10),
                retention_period: Duration::from_secs(5),
                ..Default::default()
            },
        )
        .await?;

        // let's push the epoch to 29
        for _ in 0..29 {
            replica.incr_epoch().await?;
        }

        // last snapshot is at 20
        assert_eq!(20, replica.snapshot_epoch);
        assert_eq!(
            b"00000000000000000020",
            tmp.object_store
                .get(&Path::from(Replica::LAST_SNAPSHOT_PATH))
                .await?
                .bytes()
                .await?
                .as_ref()
        );

        // but we have older snapshots in object store
        tmp.object_store
            .get(&Path::from(Replica::snapshot_path(10)))
            .await?;
        tmp.object_store
            .get(&Path::from(Replica::snapshot_path(20)))
            .await?;

        // for example we could start over from 0 and applies subsequent WALs
        for epoch in 0..=29 {
            tmp.object_store
                .get(&Path::from(Replica::wal_path(epoch)))
                .await?;
        }

        // now we vacuum
        assert_eq!(
            b"00000000000000000000",
            tmp.object_store
                .get(&Path::from(Replica::LAST_VACUUM_PATH))
                .await?
                .bytes()
                .await?
                .as_ref()
        );

        let vacuum = replica.schedule_vacuum().await?;

        // vacuum are run in the background, let's wait for it
        vacuum.await??;

        // we have a 5 seconds retention period but we always keep the last snapshot
        assert!(tmp
            .object_store
            .get(&Path::from(Replica::snapshot_path(10)))
            .await
            .is_err());
        tmp.object_store
            .get(&Path::from(Replica::snapshot_path(20)))
            .await?;

        // old WALs are gone
        for epoch in 0..=19 {
            assert!(tmp
                .object_store
                .get(&Path::from(Replica::wal_path(epoch)))
                .await
                .is_err());
        }

        // WALs on top of the snapshot are still there
        for epoch in 20..=29 {
            assert!(tmp
                .object_store
                .get(&Path::from(Replica::wal_path(epoch)))
                .await
                .is_ok());
        }

        assert_eq!(
            b"00000000000000000020",
            tmp.object_store
                .get(&Path::from(Replica::LAST_VACUUM_PATH))
                .await?
                .bytes()
                .await?
                .as_ref()
        );

        Ok(())
    }

    #[test(tokio::test)]
    async fn vacuum_edge_case() -> Result<()> {
        let tmp = TmpObjectStore::new().await?;

        // init the replica
        Replica::init("lol".to_string(), &tmp.object_store).await?;

        // and now let's open it
        let mut replica = Replica::open(
            "lol",
            &tmp.object_store,
            Config {
                epoch_interval: Duration::from_secs(1),
                snapshot_interval: Duration::from_secs(10),
                retention_period: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await?;

        // and vacuum
        replica.schedule_vacuum().await?.await??;

        // We can still open the replica
        Replica::open(
            "lol",
            &tmp.object_store,
            Config {
                epoch_interval: Duration::from_secs(1),
                snapshot_interval: Duration::from_secs(10),
                retention_period: Duration::from_secs(5),
                ..Default::default()
            },
        )
        .await?;

        Ok(())
    }
}
