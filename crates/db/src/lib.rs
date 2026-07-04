//! Generic SQLite replication over an object store via logical changesets.

mod bootstrap;
mod clock;
mod epoch;
pub mod housekeeping;
mod migrations;
mod paths;
mod sqlite;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use epoch::{
    decode_wal_epoch, encode_wal_epoch, validate_format_version, Epoch, EpochBody, WalEpoch,
};
use migrations::{validate_migrations, validate_schema, Migrations};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use rusqlite::Connection;
use tokio::sync::watch;
use tracing::{debug, warn};

pub use clock::{Clock, SystemClock, TestClock};
pub use epoch::WalEpoch as WalEpochFormat;
pub use housekeeping::{HousekeepingStats, RetentionConfig};
pub use migrations::{Migration, SchemaSupport};
pub use sqlite::SqliteStore;

pub const DEFAULT_EPOCH_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_STALE_THRESHOLD_EPOCHS: u64 = 5;
pub const DEFAULT_DB_FILE: &str = "data.db";

#[derive(Clone)]
pub struct Config {
    pub object_store: Arc<dyn ObjectStore>,
    pub prefix: String,
    pub local_path: PathBuf,
    pub db_file: String,
    pub node_id: String,
    pub node_addr: String,
    pub migrations: Migrations,
    pub clock: Arc<dyn Clock>,
    pub epoch_interval: Duration,
    pub stale_threshold_epochs: u64,
    pub leader_eligible: bool,
    pub snapshot_every_epochs: u64,
    pub min_schema_version: u32,
}

impl Config {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        local_path: impl Into<PathBuf>,
        node_id: impl Into<String>,
        node_addr: impl Into<String>,
    ) -> Self {
        Self {
            object_store,
            prefix: prefix.into(),
            local_path: local_path.into(),
            db_file: DEFAULT_DB_FILE.to_string(),
            node_id: node_id.into(),
            node_addr: node_addr.into(),
            migrations: Arc::new([]),
            clock: Arc::new(SystemClock),
            epoch_interval: DEFAULT_EPOCH_INTERVAL,
            stale_threshold_epochs: DEFAULT_STALE_THRESHOLD_EPOCHS,
            leader_eligible: true,
            snapshot_every_epochs: 300,
            min_schema_version: 0,
        }
    }

    pub fn with_min_schema_version(mut self, min: u32) -> Self {
        self.min_schema_version = min;
        self
    }

    pub fn with_migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.migrations = Arc::from(migrations);
        self
    }

    pub fn with_leader_eligible(mut self, eligible: bool) -> Self {
        self.leader_eligible = eligible;
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_snapshot_every_epochs(mut self, n: u64) -> Self {
        self.snapshot_every_epochs = n;
        self
    }

    pub fn with_db_file(mut self, name: impl Into<String>) -> Self {
        self.db_file = name.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickResult {
    Replicated { epoch: u64 },
    Synced { epoch: u64 },
    NoNewEpoch,
    Promoted,
    Fenced { epoch: u64 },
}

pub struct Db {
    object_store: Arc<dyn ObjectStore>,
    prefix: String,
    local_path: PathBuf,
    node_id: String,
    node_addr: String,
    clock: Arc<dyn Clock>,
    epoch_interval: Duration,
    stale_threshold_epochs: u64,
    leader_eligible: bool,
    snapshot_every_epochs: u64,
    migrations: Migrations,
    schema_support: SchemaSupport,

    store: Arc<SqliteStore>,
    last_epoch: Epoch,
    initialized_fresh: bool,
    is_leader: bool,
    generation: u64,
    last_snapshot_epoch: u64,
    last_progress_at: Option<DateTime<Utc>>,
    promote_block_until: Option<DateTime<Utc>>,
    followed_leader: Option<String>,
    leader_state_tx: watch::Sender<LeaderState>,
}

impl Db {
    pub async fn join(config: Config) -> Result<Self> {
        validate_migrations(&config.migrations)?;
        let schema_support =
            SchemaSupport::from_migrations(&config.migrations, config.min_schema_version);

        let Config {
            object_store,
            prefix,
            local_path,
            db_file,
            node_id,
            node_addr,
            migrations,
            clock,
            epoch_interval,
            stale_threshold_epochs,
            leader_eligible,
            snapshot_every_epochs,
            min_schema_version: _,
        } = config;

        let prefix = normalize_prefix(&prefix);
        let mut has_data = check_has_data(&object_store, &prefix).await?;

        if !has_data && !leader_eligible {
            warn!("not leader-eligible and no data under prefix {prefix:?}; waiting for init");
            wait_for_data(&object_store, &prefix, epoch_interval).await?;
            has_data = true;
        }

        if !has_data {
            debug!("no data under prefix {prefix:?}, initializing");
            prepare_local_dir(&local_path).await?;
            let db_path = local_path.join(&db_file);
            let store = Arc::new(SqliteStore::open(&db_path, true)?);
            store.init_meta()?;

            let init_ts = clock.now().to_rfc3339();
            let last_epoch = Epoch::initial(node_id.clone(), node_addr.clone(), init_ts);
            let (leader_state_tx, _) = watch::channel(LeaderState::follower(last_epoch.epoch));

            let mut db = Self {
                object_store,
                prefix,
                local_path,
                node_id,
                node_addr,
                clock,
                epoch_interval,
                stale_threshold_epochs,
                leader_eligible,
                snapshot_every_epochs,
                migrations,
                schema_support,
                store,
                last_epoch,
                initialized_fresh: true,
                is_leader: false,
                generation: 0,
                last_snapshot_epoch: 0,
                last_progress_at: None,
                promote_block_until: None,
                followed_leader: None,
                leader_state_tx,
            };
            db.initialize().await?;
            validate_schema(db.store.schema_version()?, &db.schema_support)?;
            warn!("promoted to leader for {}", db.prefix);
            Ok(db)
        } else {
            debug!("found data under prefix {prefix:?}, bootstrapping");
            let result =
                bootstrap::bootstrap(object_store.as_ref(), &prefix, &local_path, &db_file).await?;
            if read_epoch_watermark(object_store.as_ref(), &prefix)
                .await?
                .is_none()
            {
                update_epoch_watermark(object_store.as_ref(), &prefix, result.last_epoch.epoch)
                    .await?;
            }
            let (leader_state_tx, _) =
                watch::channel(LeaderState::follower(result.last_epoch.epoch));
            let mut db = Self {
                object_store,
                prefix,
                local_path,
                node_id,
                node_addr,
                clock,
                epoch_interval,
                stale_threshold_epochs,
                leader_eligible,
                snapshot_every_epochs,
                migrations,
                schema_support,
                store: result.store,
                last_epoch: result.last_epoch,
                initialized_fresh: false,
                is_leader: false,
                generation: 0,
                last_snapshot_epoch: result.snapshot_epoch,
                last_progress_at: None,
                promote_block_until: None,
                followed_leader: None,
                leader_state_tx,
            };
            validate_schema(db.store.schema_version()?, &db.schema_support)?;
            if leader_eligible
                && db.is_epoch_stale()
                && matches!(db.try_promote().await?, TickResult::Promoted)
            {
                warn!("promoted to leader for {}", db.prefix);
            }
            Ok(db)
        }
    }

    pub fn store(&self) -> Arc<SqliteStore> {
        self.store.clone()
    }

    pub fn schema_support(&self) -> SchemaSupport {
        self.schema_support
    }

    pub fn initialized_fresh(&self) -> bool {
        self.initialized_fresh
    }

    pub fn epoch(&self) -> u64 {
        self.last_epoch.epoch
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn leader(&self) -> (String, String) {
        (
            self.last_epoch.leader_id.clone(),
            self.last_epoch.leader_addr.clone(),
        )
    }

    pub fn subscribe_leader_state(&self) -> watch::Receiver<LeaderState> {
        self.leader_state_tx.subscribe()
    }

    pub async fn tick(&mut self) -> Result<TickResult> {
        if self.is_leader {
            self.tick_leader().await
        } else {
            self.tick_follower().await
        }
    }

    async fn tick_leader(&mut self) -> Result<TickResult> {
        let epoch = self.last_epoch.epoch + 1;
        let is_snapshot_epoch = epoch - self.last_snapshot_epoch >= self.snapshot_every_epochs;

        let changeset = self.store.capture_changeset()?;

        let wal_epoch = WalEpoch::changeset(
            epoch,
            self.node_id.clone(),
            self.node_addr.clone(),
            self.now(),
            changeset,
        );
        let data = encode_wal_epoch(&wal_epoch)?;

        if !self.write_epoch_atomic(epoch, data).await? {
            if let Some(we) = self.download_wal_epoch(epoch).await? {
                self.last_epoch = we.metadata;
            }
            self.store.rollback_epoch()?;
            self.reject_pending_durables();
            self.is_leader = false;
            self.store.set_leader_mode(false)?;
            self.last_progress_at = None;
            self.publish_state();
            warn!("fenced, demoting to follower for {}", self.prefix);
            return Ok(TickResult::Fenced { epoch });
        }

        self.store.commit_epoch()?;
        self.store.refresh_read_pool()?;
        self.resolve_pending_durables(epoch);

        self.last_epoch = wal_epoch.metadata;
        self.publish_state();

        if is_snapshot_epoch {
            self.snapshot_at(epoch).await?;
            self.last_snapshot_epoch = epoch;
            debug!("replicated epoch {epoch} (snapshot)");
        } else {
            debug!("replicated epoch {epoch}");
        }

        Ok(TickResult::Replicated { epoch })
    }

    async fn tick_follower(&mut self) -> Result<TickResult> {
        let mut synced: u64 = 0;

        loop {
            let next = self.last_epoch.epoch + 1;
            match self.download_wal_epoch(next).await? {
                Some(wal_epoch) => {
                    validate_format_version(
                        &format!("epoch {next}"),
                        wal_epoch.metadata.format_version,
                    )?;
                    let body = wal_epoch.body.clone();
                    let store = self.store.clone();
                    apply_epoch_body(&store, &body)?;
                    self.store.refresh_read_pool()?;
                    self.last_epoch = wal_epoch.metadata;
                    self.last_progress_at = Some(self.clock.now());
                    self.promote_block_until = None;
                    synced += 1;
                }
                None => {
                    if synced == 0 {
                        if self.leader_eligible
                            && self.is_epoch_stale()
                            && !self.promote_suppressed()
                        {
                            let result = self.try_promote().await?;
                            if matches!(result, TickResult::Promoted) {
                                warn!("promoted to leader for {}", self.prefix);
                            }
                            return Ok(result);
                        }
                        return Ok(TickResult::NoNewEpoch);
                    }
                    break;
                }
            }
        }

        self.publish_state();
        if self.followed_leader.as_deref() != Some(self.last_epoch.leader_id.as_str()) {
            self.followed_leader = Some(self.last_epoch.leader_id.clone());
            warn!(
                "following {} for {}",
                self.last_epoch.leader_id, self.prefix
            );
        }
        debug!(
            "synced {synced} epoch(s), now at epoch {}",
            self.last_epoch.epoch
        );
        Ok(TickResult::Synced {
            epoch: self.last_epoch.epoch,
        })
    }

    pub async fn try_promote(&mut self) -> Result<TickResult> {
        let watermark = match read_epoch_watermark(&self.object_store, &self.prefix).await? {
            Some(w) => w,
            None => {
                update_epoch_watermark(&self.object_store, &self.prefix, self.last_epoch.epoch)
                    .await?;
                self.last_epoch.epoch
            }
        };
        if watermark > self.last_epoch.epoch {
            panic!(
                "stale node: watermark ({watermark}) ahead of epoch ({})",
                self.last_epoch.epoch
            );
        }

        self.store.set_leader_mode(true)?;
        self.ship_pending_migrations().await?;

        let next = self.last_epoch.epoch + 1;
        let changeset = self.store.capture_changeset()?;

        let wal_epoch = WalEpoch::changeset(
            next,
            self.node_id.clone(),
            self.node_addr.clone(),
            self.now(),
            changeset,
        );
        let data = encode_wal_epoch(&wal_epoch)?;

        if self.write_epoch_atomic(next, data).await? {
            self.store.commit_epoch()?;
            self.store.refresh_read_pool()?;
            self.resolve_pending_durables(next);
            self.last_epoch = wal_epoch.metadata;
            self.is_leader = true;
            self.generation += 1;
            self.last_progress_at = None;
            self.followed_leader = None;
            self.publish_state();
            if next - self.last_snapshot_epoch >= self.snapshot_every_epochs {
                self.snapshot_at(next).await?;
                self.last_snapshot_epoch = next;
            }
            Ok(TickResult::Promoted)
        } else {
            self.store.rollback_epoch()?;
            self.store.set_leader_mode(false)?;
            if let Some(we) = self.download_wal_epoch(next).await? {
                self.last_epoch = we.metadata;
            }
            self.publish_state();
            Ok(TickResult::NoNewEpoch)
        }
    }

    pub fn step_down(&mut self) {
        if !self.is_leader {
            return;
        }
        let now = self.clock.now();
        let grace = self.stale_threshold_epochs * self.epoch_interval.as_secs().max(1) * 2;
        self.is_leader = false;
        let _ = self.store.set_leader_mode(false);
        self.last_progress_at = Some(now);
        self.promote_block_until = Some(now + chrono::Duration::seconds(grace as i64));
        self.publish_state();
        warn!("stepped down from leadership for {}", self.prefix);
    }

    pub fn write<T>(
        &self,
        apply: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
    ) -> Result<Durable<T>>
    where
        T: Send + 'static,
    {
        if !self.is_leader {
            bail!("write called on a non-leader node");
        }
        let value = self.store.with_write_value(apply)?;
        Ok(self.track(value))
    }

    pub fn durable<T>(&self, value: T) -> Result<Durable<T>> {
        if !self.is_leader {
            bail!("durable called on a non-leader node");
        }
        Ok(self.track(value))
    }

    fn track<T>(&self, value: T) -> Durable<T> {
        Durable {
            value: Some(value),
            target_epoch: self.last_epoch.epoch + 1,
            generation: self.generation,
            state: self.leader_state_tx.subscribe(),
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        self.store.set_leader_mode(true)?;

        let wal0 = WalEpoch::changeset(
            0,
            self.node_id.clone(),
            self.node_addr.clone(),
            self.now(),
            vec![],
        );
        let data = encode_wal_epoch(&wal0)?;
        if !self.write_epoch_atomic(0, data).await? {
            bail!("epoch 0 already exists under prefix {:?}", self.prefix);
        }
        self.store.commit_epoch()?;
        self.last_epoch = wal0.metadata;
        self.snapshot_at(0).await?;
        update_epoch_watermark(&self.object_store, &self.prefix, 0).await?;
        self.ship_pending_migrations().await?;
        self.is_leader = true;
        self.generation += 1;
        self.last_snapshot_epoch = 0;
        self.publish_state();
        debug!("initialized new DB at epoch 0");
        Ok(())
    }

    async fn ship_pending_migrations(&mut self) -> Result<()> {
        if self.migrations.is_empty() {
            return Ok(());
        }
        let current = self.store.schema_version()?;
        let mut pending: Vec<Migration> = self
            .migrations
            .iter()
            .filter(|m| m.version > current)
            .cloned()
            .collect();
        pending.sort_by_key(|m| m.version);

        for m in pending {
            self.store.apply_migration(m.version, &m.sql)?;

            let next = self.last_epoch.epoch + 1;
            let wal_epoch = WalEpoch::migration(
                next,
                self.node_id.clone(),
                self.node_addr.clone(),
                self.now(),
                m.version,
                m.sql.clone(),
            );
            let data = encode_wal_epoch(&wal_epoch)?;
            if !self.write_epoch_atomic(next, data).await? {
                bail!("lost fence shipping migration {}", m.version);
            }
            self.store.commit_epoch()?;
            self.last_epoch = wal_epoch.metadata;
            self.publish_state();
            debug!("shipped migration epoch {next} (version {})", m.version);
        }
        Ok(())
    }

    async fn snapshot_at(&self, epoch: u64) -> Result<()> {
        let snap_path = self.local_path.join(format!(".snapshot_{epoch}.db"));
        self.store.vacuum_into(&snap_path)?;

        let bytes = tokio::fs::read(&snap_path).await?;
        self.object_store
            .put(
                &paths::snapshot_file(&self.prefix, epoch),
                PutPayload::from(bytes),
            )
            .await?;
        let _ = tokio::fs::remove_file(&snap_path).await;
        self.write_last_snapshot(epoch).await?;
        Ok(())
    }

    async fn write_epoch_atomic(&self, epoch: u64, data: Vec<u8>) -> Result<bool> {
        let path = paths::epoch_file(&self.prefix, epoch);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .object_store
            .put_opts(&path, PutPayload::from(data), opts)
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_last_snapshot(&self, epoch: u64) -> Result<()> {
        self.object_store
            .put(
                &paths::last_snapshot(&self.prefix),
                PutPayload::from(epoch.to_string()),
            )
            .await?;
        Ok(())
    }

    async fn download_wal_epoch(&self, epoch: u64) -> Result<Option<WalEpoch>> {
        let path = paths::epoch_file(&self.prefix, epoch);
        match self.object_store.get(&path).await {
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
            Ok(data) => {
                let bytes = data.bytes().await?;
                let wal_epoch =
                    tokio::task::spawn_blocking(move || decode_wal_epoch(&bytes)).await??;
                Ok(Some(wal_epoch))
            }
        }
    }

    fn now(&self) -> String {
        self.clock.now().to_rfc3339()
    }

    fn publish_state(&self) {
        self.leader_state_tx.send_replace(LeaderState {
            generation: self.generation,
            epoch: self.last_epoch.epoch,
            is_leader: self.is_leader,
        });
    }

    fn resolve_pending_durables(&self, _epoch: u64) {
        // Durables observe state via watch channel; publish_state already ran.
    }

    fn reject_pending_durables(&self) {
        self.publish_state();
    }

    fn is_epoch_stale(&self) -> bool {
        let threshold = (self.stale_threshold_epochs * self.epoch_interval.as_secs().max(1)) as i64;
        self.epoch_age_secs().is_none_or(|age| age >= threshold)
    }

    fn epoch_age_secs(&self) -> Option<i64> {
        let reference = match self.last_progress_at {
            Some(t) => t,
            None => chrono::DateTime::parse_from_rfc3339(&self.last_epoch.created_at)
                .ok()?
                .with_timezone(&Utc),
        };
        Some((self.clock.now() - reference).num_seconds())
    }

    fn promote_suppressed(&self) -> bool {
        self.promote_block_until
            .is_some_and(|until| self.clock.now() < until)
    }
}

pub(crate) fn apply_epoch_body(store: &SqliteStore, body: &EpochBody) -> Result<()> {
    match body {
        EpochBody::Changeset(bytes) => store.apply_changeset(bytes),
        EpochBody::Migration { version, sql } => store.apply_migration(*version, sql),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderState {
    pub generation: u64,
    pub epoch: u64,
    pub is_leader: bool,
}

impl LeaderState {
    fn follower(epoch: u64) -> Self {
        Self {
            generation: 0,
            epoch,
            is_leader: false,
        }
    }
}

pub struct Durable<T> {
    value: Option<T>,
    target_epoch: u64,
    generation: u64,
    state: watch::Receiver<LeaderState>,
}

impl<T> Durable<T> {
    pub fn target_epoch(&self) -> u64 {
        self.target_epoch
    }

    pub async fn committed(mut self) -> Result<T, WriteError> {
        loop {
            let outcome = {
                let s = self.state.borrow_and_update();
                if s.generation == self.generation && s.is_leader && s.epoch >= self.target_epoch {
                    Outcome::Committed
                } else if s.generation != self.generation || !s.is_leader {
                    Outcome::Demoted
                } else {
                    Outcome::Pending
                }
            };
            match outcome {
                Outcome::Committed => {
                    return Ok(self
                        .value
                        .take()
                        .expect("Durable::committed resolved more than once"))
                }
                Outcome::Demoted => return Err(WriteError::Demoted),
                Outcome::Pending => {}
            }
            if self.state.changed().await.is_err() {
                return Err(WriteError::Stopped);
            }
        }
    }
}

enum Outcome {
    Committed,
    Demoted,
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    Demoted,
    Stopped,
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Demoted => write!(f, "lost leadership before the write became durable"),
            WriteError::Stopped => write!(f, "cluster stopped before the write became durable"),
        }
    }
}

impl std::error::Error for WriteError {}

fn normalize_prefix(p: &str) -> String {
    if p.is_empty() || p.ends_with('/') {
        p.to_string()
    } else {
        format!("{p}/")
    }
}

pub(crate) async fn prepare_local_dir(path: &Path) -> Result<()> {
    if path.exists() {
        tokio::fs::remove_dir_all(path).await?;
    }
    tokio::fs::create_dir_all(path).await?;
    Ok(())
}

async fn check_has_data(store: &dyn ObjectStore, prefix: &str) -> Result<bool> {
    match store.head(&paths::last_snapshot(prefix)).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

async fn wait_for_data(store: &dyn ObjectStore, prefix: &str, interval: Duration) -> Result<()> {
    loop {
        if check_has_data(store, prefix).await? {
            return Ok(());
        }
        tokio::time::sleep(interval).await;
    }
}

pub async fn read_last_snapshot(store: &dyn ObjectStore, prefix: &str) -> Result<Option<u64>> {
    match store.get(&paths::last_snapshot(prefix)).await {
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
        Ok(data) => {
            let s = String::from_utf8(data.bytes().await?.to_vec())?;
            Ok(Some(s.trim().parse()?))
        }
    }
}

pub async fn read_epoch_watermark(store: &dyn ObjectStore, prefix: &str) -> Result<Option<u64>> {
    match store.get(&paths::epoch_watermark(prefix)).await {
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
        Ok(data) => {
            let s = String::from_utf8(data.bytes().await?.to_vec())?;
            Ok(Some(s.trim().parse()?))
        }
    }
}

pub async fn update_epoch_watermark(
    store: &dyn ObjectStore,
    prefix: &str,
    epoch: u64,
) -> Result<()> {
    let path = paths::epoch_watermark(prefix);
    match read_epoch_watermark(store, prefix).await? {
        Some(current) if epoch <= current => Ok(()),
        Some(_) => {
            store
                .put_opts(
                    &path,
                    PutPayload::from(epoch.to_string()),
                    PutOptions {
                        mode: PutMode::Update(UpdateVersion {
                            e_tag: None,
                            version: None,
                        }),
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        }
        None => {
            store
                .put_opts(
                    &path,
                    PutPayload::from(epoch.to_string()),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        }
    }
}

// Snapshot type for API compatibility (single-file snapshots).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub epoch: u64,
    pub created_at: String,
    pub format_version: u32,
}
