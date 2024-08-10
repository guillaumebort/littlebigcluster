use std::{
    fmt::Debug,
    fs::File,
    future::Future,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::Poll,
};

use anyhow::{anyhow, bail, Context, Result};
use futures::{future::BoxFuture, FutureExt};
use sqlx::{
    pool::PoolConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteQueryResult},
    ConnectOptions, Connection, Sqlite, SqliteConnection, SqlitePool, Transaction,
};
use tempfile::{NamedTempFile, TempDir};
use tokio::{runtime::Handle, sync::oneshot};
use tracing::debug;

type DoAck = oneshot::Sender<bool>;

#[must_use = "Acks must be awaited, or explicitly dropped if you don't care about the result"]
pub struct Ack<A>
where
    A: Unpin,
{
    result: Option<A>,
    ack: oneshot::Receiver<bool>,
}

impl<A> Ack<A>
where
    A: Unpin,
{
    /// Lookup the result before it's acked
    pub fn peek(&self) -> &A {
        self.result.as_ref().expect("ack was already consumed")
    }
}

impl<A> Future for Ack<A>
where
    A: Unpin,
{
    type Output = Result<A>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.ack.poll_unpin(cx) {
            Poll::Ready(Ok(true)) => Poll::Ready(Ok(self.result.take().unwrap())),
            Poll::Ready(Ok(false)) => Poll::Ready(Err(anyhow!("checkpoint failed"))),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
pub(crate) struct DB {
    db_path: PathBuf,
    read_pool: SqlitePool,
    write_conn: Arc<tokio::sync::Mutex<(SqliteConnection, Vec<DoAck>)>>,
    #[allow(unused)]
    tmp: Arc<TempDir>,
}

impl Debug for DB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DB")
            .field("db_path", &self.db_path)
            .finish()
    }
}

impl DB {
    pub async fn open(snapshot: Option<&Path>) -> Result<Self> {
        let tmp = Arc::new(TempDir::new()?);

        // to deliver the acks signals
        let pending_acks = Vec::with_capacity(1_024);

        let db_path = tmp.path().join("litecluster.db");
        debug!(?db_path, ?snapshot, "Opening DB");

        // restore snapshot or create empty db
        if let Some(snapshot) = snapshot {
            std::fs::copy(snapshot, &db_path)?;
        } else {
            // create empty db
            Self::connect_options(&db_path)?
                .create_if_missing(true)
                .connect()
                .await
                .context(format!("failed to initialize db at {db_path:?}"))?;
        }

        // we use a single write connection to mitigate lock contention
        let write_conn = Self::connect_options(&db_path)?.connect().await?;

        // the read pool has a connection per core but is read only
        let cpus = num_cpus::get().try_into()?;
        let read_pool = SqlitePoolOptions::new()
            .min_connections(cpus)
            .max_connections(cpus)
            .connect_with(Self::connect_options(&db_path)?.read_only(true))
            .await?;

        // test the connection
        {
            let one: u32 = sqlx::query_scalar("SELECT 1").fetch_one(&read_pool).await?;
            if 1 != one {
                bail!("failed to query 1");
            }
        }

        Ok(Self {
            db_path,
            read_pool,
            write_conn: Arc::new(tokio::sync::Mutex::new((write_conn, pending_acks))),
            tmp,
        })
    }

    pub fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    pub fn path(&self) -> &Path {
        &self.db_path
    }

    fn connect_options(db_path: &Path) -> Result<SqliteConnectOptions> {
        Ok(
            SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))?
                .journal_mode(SqliteJournalMode::Wal)
                .pragma("wal_autocheckpoint", "0"),
        )
    }

    fn wal_path(&self) -> PathBuf {
        PathBuf::from(format!("{}-wal", self.db_path.display()))
    }

    fn shm_path(&self) -> PathBuf {
        PathBuf::from(format!("{}-shm", self.db_path.display()))
    }

    pub(crate) async fn checkpoint<A>(
        &self,
        f: impl (FnOnce(PathBuf) -> BoxFuture<'static, Result<A>>) + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let shadow = NamedTempFile::new_in(self.tmp.path())?;
        let (result, pending_acks) = {
            let shadow = shadow.path().to_owned();
            self.do_checkpoint(|wal| {
                async move {
                    tokio::fs::copy(wal, shadow).await?;
                    Ok(())
                }
                .boxed()
            })
            .await?
        };
        result?;
        match f(shadow.path().to_owned()).await {
            Ok(a) => {
                tokio::task::spawn_blocking(|| {
                    for do_ack in pending_acks {
                        let _ = do_ack.send(true);
                    }
                })
                .await?;
                Ok(a)
            }
            Err(e) => {
                tokio::task::spawn_blocking(|| {
                    for do_ack in pending_acks {
                        let _ = do_ack.send(false);
                    }
                })
                .await?;
                Err(e)
            }
        }
    }

    pub(crate) async fn try_checkpoint<A>(
        &self,
        f: impl (FnOnce(PathBuf) -> BoxFuture<'static, Result<A>>) + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let (result, pending_acks) = self.do_checkpoint(f).await?;
        match result {
            Ok(a) => {
                tokio::task::spawn_blocking(|| {
                    for do_ack in pending_acks {
                        let _ = do_ack.send(true);
                    }
                })
                .await?;
                Ok(a)
            }
            Err(e) => {
                self.rollback().await?;
                tokio::task::spawn_blocking(|| {
                    for do_ack in pending_acks {
                        let _ = do_ack.send(false);
                    }
                })
                .await?;
                Err(e)
            }
        }
    }

    async fn do_checkpoint<A>(
        &self,
        f: impl (FnOnce(PathBuf) -> BoxFuture<'static, Result<A>>) + Send + 'static,
    ) -> Result<(Result<A>, Vec<DoAck>)>
    where
        A: Send + 'static,
    {
        // this will block all concurrent writes
        let (ref mut connection, ref mut pending_acks) = *self.write_conn.lock().await;

        // let's snapshot the pending acks
        let mut prev_pending_acks = Vec::with_capacity(1_024);
        std::mem::swap(&mut prev_pending_acks, pending_acks);

        // run user code
        let wal_path = self.wal_path();
        let result =
            tokio::task::spawn_blocking(move || Handle::current().block_on(f(wal_path))).await?;

        // run the checkpoint
        if result.is_ok() {
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(&mut *connection)
                .await?;
        }

        Ok((result, prev_pending_acks))
    }

    async fn block_all_reads(&self) -> Result<Vec<PoolConnection<Sqlite>>> {
        let pool_size = self.read_pool.size().try_into()?;
        let mut connections = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            connections.push(self.read_pool.acquire().await?);
        }
        Ok(connections)
    }

    async fn rollback(&self) -> Result<()> {
        let mut prev_pending_acks = Vec::with_capacity(1_024);
        {
            // this will block all concurrent writes
            let (ref mut connection, ref mut pending_acks) = *self.write_conn.lock().await;

            // this will block all concurrent reads
            let _read_guard = self.block_all_reads().await?;

            std::mem::swap(&mut prev_pending_acks, pending_acks);

            let wal_path = self.wal_path();
            let shm_path = self.shm_path();
            tokio::task::spawn_blocking(move || {
                std::fs::File::create(wal_path)?; // truncate wal file
                Self::invalidate_shm(&shm_path)
            })
            .await??;

            // truncate properly
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(&mut *connection)
                .await?;
        }
        // Reject all pending acks
        tokio::task::spawn_blocking(move || {
            for do_ack in prev_pending_acks {
                let _ = do_ack.send(false);
            }
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn snapshot(&self, db_snapshot: impl Into<PathBuf>) -> Result<()> {
        // this will block all concurrent writes
        let (ref mut connection, _) = *self.write_conn.lock().await;

        // truncate wal file
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&mut *connection)
            .await?;

        // copy the db file
        let db_path = self.db_path.clone();
        let db_snapshot = db_snapshot.into();
        tokio::task::spawn_blocking(move || std::fs::copy(db_path, db_snapshot)).await??;

        Ok(())
    }

    pub(crate) async fn apply_wal(&self, wal_snapshot: impl Into<PathBuf>) -> Result<()> {
        // this will block all concurrent writes
        let (ref mut connection, _) = *self.write_conn.lock().await;

        // this will block all concurrent reads
        let _read_guard = self.block_all_reads().await?;

        // truncate wal file
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&mut *connection)
            .await?;

        let wal_snapshot = wal_snapshot.into();
        let wal_path = self.wal_path();
        let shm_path = self.shm_path();
        tokio::task::spawn_blocking(move || {
            // replace the wal file
            std::fs::copy(&wal_snapshot, &wal_path)?;

            // invalidate the shm file
            Self::invalidate_shm(&shm_path)
        })
        .await??;

        // truncate wal file again
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&mut *connection)
            .await?;
        Ok(())
    }

    fn invalidate_shm(shm_path: &Path) -> Result<()> {
        // read the -shm file and update the header
        if !shm_path.exists() {
            bail!("missing shm file: {shm_path:?}");
        }
        const WALINDEX_HEADER_SIZE: usize = 136;
        let mut shm_file = File::options().read(true).write(true).open(&shm_path)?;
        let mut buf = vec![0u8; WALINDEX_HEADER_SIZE];
        shm_file.read_exact(&mut buf)?;

        // clears the iVersion fields so the next transaction will rebuild it
        buf[12] = 0;
        buf[60] = 0;
        shm_file.rewind()?;
        shm_file.write_all(&buf)?;

        Ok(())
    }

    pub async fn transaction<A, E>(
        &self,
        thunk: impl (for<'c> FnOnce(Transaction<'c, Sqlite>) -> BoxFuture<'c, Result<A, E>>) + Send,
    ) -> Result<Ack<A>>
    where
        A: Send + Unpin + 'static,
        E: Into<anyhow::Error> + Send,
    {
        let (ref mut conn, ref mut pending_acks) = *self.write_conn.lock().await;
        let txn = conn.begin().await?;
        let result = match thunk(txn).await {
            Ok(result) => Some(result),
            Err(e) => bail!(e.into()),
        };
        let (do_ack, ack) = tokio::sync::oneshot::channel();
        pending_acks.push(do_ack);
        Ok(Ack { result, ack })
    }

    pub async fn execute<'a>(
        &'a self,
        query: sqlx::query::Query<'a, Sqlite, <Sqlite as sqlx::Database>::Arguments<'a>>,
    ) -> Result<Ack<SqliteQueryResult>> {
        let (ref mut conn, ref mut pending_acks) = *self.write_conn.lock().await;
        let result = Some(query.execute(&mut *conn).await?);
        let (do_ack, ack) = tokio::sync::oneshot::channel();
        pending_acks.push(do_ack);
        Ok(Ack { result, ack })
    }

    pub async fn execute_batch(
        &self,
        query: sqlx::RawSql<'static>,
    ) -> Result<Ack<SqliteQueryResult>> {
        let (ref mut conn, ref mut pending_acks) = *self.write_conn.lock().await;
        let result = Some(query.execute(&mut *conn).await?);
        let (do_ack, ack) = tokio::sync::oneshot::channel();
        pending_acks.push(do_ack);
        Ok(Ack { result, ack })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::AtomicUsize,
        time::{Duration, Instant},
    };

    use futures::FutureExt;
    use rand::Rng;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn open_and_query() -> Result<()> {
        let db = DB::open(None).await?;
        let one: i64 = sqlx::query_scalar("select 1")
            .fetch_one(&db.read_pool)
            .await?;
        assert_eq!(1, one);
        Ok(())
    }

    #[tokio::test]
    async fn open_and_insert() -> Result<()> {
        let db = DB::open(None).await?;
        let _ = &db
            .execute_batch(sqlx::raw_sql(
                r#"
                CREATE TABLE batchs(uuid);
                INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0');
                "#,
            ))
            .await?;

        let uuid: String = sqlx::query_scalar("select * from batchs")
            .fetch_one(&db.read_pool)
            .await?;

        assert_eq!("0191b6d0-3d9a-7eb1-88b8-5312737f2ca0", uuid);

        Ok(())
    }

    #[tokio::test]
    async fn write_in_readonly_pool() -> Result<()> {
        let db = DB::open(None).await?;
        assert!(sqlx::query("create table lol(id);")
            .execute(&db.read_pool)
            .await
            .is_err());

        Ok(())
    }

    #[tokio::test]
    async fn block_pool() -> Result<()> {
        let db = DB::open(None).await?;

        let guard = db.block_all_reads().await?;

        // the pool is blocked, the call will timeout
        assert!(tokio::time::timeout(
            Duration::from_millis(100),
            sqlx::query("select 1").execute(&db.read_pool)
        )
        .await
        .is_err());

        drop(guard);

        Ok(())
    }

    #[tokio::test]
    async fn auto_rollback_transactions() -> Result<()> {
        let db = DB::open(None).await?;
        let _ = db.execute(sqlx::query("CREATE TABLE batchs(uuid)")).await?;

        // transaction rollback
        let _ = db
            .transaction(|mut txn| {
                async move {
                    sqlx::query(
                        "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0')",
                    )
                    .execute(&mut *txn)
                    .await?;
                    // oops forgot to commit
                    anyhow::Ok(())
                }
                .boxed()
            })
            .await?;

        // the transaction was rolled back
        assert_eq!(
            0,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn snapshot() -> Result<()> {
        // create a blank db, insert some data
        let db = DB::open(None).await?;
        let _ = db
            .transaction(|mut txn| {
                async {
                    sqlx::query("CREATE TABLE batchs(uuid)")
                        .execute(&mut *txn)
                        .await?;

                    for i in 0..100 {
                        sqlx::query("INSERT INTO batchs VALUES (?1)")
                            .bind(i)
                            .execute(&mut *txn)
                            .await?;
                    }

                    txn.commit().await
                }
                .boxed()
            })
            .await?;

        // snaphot the db
        let tmp = TempDir::new()?;
        let db_snapshot = tmp.path().join("db_snapshot");
        db.snapshot(&db_snapshot).await?;

        // restore the snapshot
        let db = DB::open(Some(&db_snapshot)).await?;
        let count: i32 = sqlx::query_scalar("select count(*) from batchs")
            .fetch_one(&db.read_pool)
            .await?;
        assert_eq!(100, count);

        Ok(())
    }

    #[tokio::test]
    async fn checkpoint() -> Result<()> {
        // create a blank db, create a table
        let db = DB::open(None).await?;
        let _ = db.execute(sqlx::query("CREATE TABLE batchs(uuid)")).await?;

        // checkpoint the db
        let wal_snapshot_1 = db
            .checkpoint(|wal| {
                async {
                    let tmp = NamedTempFile::new()?;
                    std::fs::copy(wal, tmp.path())?;
                    Ok(tmp)
                }
                .boxed()
            })
            .await?;
        assert!(wal_snapshot_1.path().exists());

        // transaction rollback
        let _ = db
            .transaction(|mut txn| {
                async {
                    sqlx::query(
                        "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0')",
                    )
                    .execute(&mut *txn)
                    .await?;
                    txn.rollback().await
                }
                .boxed()
            })
            .await?;

        // checkpoint the db
        let wal_snapshot_2 = db
            .checkpoint(|wal| {
                async {
                    let tmp = NamedTempFile::new()?;
                    std::fs::copy(wal, tmp.path())?;
                    Ok(tmp)
                }
                .boxed()
            })
            .await?;
        assert!(wal_snapshot_2.path().exists());

        // check the db content
        assert_eq!(
            0,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // insert some data
        let ack_3 = db
            .execute(
                sqlx::query("INSERT INTO batchs VALUES (?1)")
                    .bind("0191b6d0-3d9a-7eb1-88b8-5312737f2ca0"),
            )
            .await?;

        // checkpoint the db
        let wal_snapshot_3 = db
            .checkpoint(|wal| {
                async {
                    let tmp = NamedTempFile::new()?;
                    std::fs::copy(wal, tmp.path())?;
                    Ok(tmp)
                }
                .boxed()
            })
            .await?;
        assert!(wal_snapshot_3.path().exists());
        assert_eq!(1, ack_3.await?.rows_affected());

        // check the db content
        assert_eq!(
            1,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // create a new blank db, restore the first checkpoint
        let db = DB::open(None).await?;
        db.apply_wal(&wal_snapshot_1.path()).await?;
        assert_eq!(
            0,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // restore the second checkpoint
        db.apply_wal(&wal_snapshot_2.path()).await?;
        assert_eq!(
            0,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // restore the third checkpoint
        db.apply_wal(&wal_snapshot_3.path()).await?;
        assert_eq!(
            1,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // check the db content
        assert_eq!(
            "0191b6d0-3d9a-7eb1-88b8-5312737f2ca0",
            sqlx::query_scalar::<_, String>("select* from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn rollback() -> Result<()> {
        // create a blank db, create a table
        let db = DB::open(None).await?;
        let _ = db.execute(sqlx::query("CREATE TABLE batchs(uuid)")).await?;

        // checkpoint the db
        let wal_snapshot_1 = db
            .checkpoint(|wal| {
                async {
                    let tmp = NamedTempFile::new()?;
                    std::fs::copy(wal, tmp.path())?;
                    Ok(tmp)
                }
                .boxed()
            })
            .await?;
        assert!(wal_snapshot_1.path().exists());

        // insert some data
        let ack_1 = db
            .execute(sqlx::query(
                "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0')",
            ))
            .await?;

        // check the db content
        assert_eq!(
            1,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // but now rollback the current wal
        db.rollback().await?;

        assert!(ack_1.await.is_err());

        // check the db content
        assert_eq!(
            0,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // insert some data
        let ack_2 = db
            .execute(sqlx::query(
                "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0')",
            ))
            .await?;

        // check the db content
        assert_eq!(
            1,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // checkpoint the db
        let wal_snapshot_2 = db
            .try_checkpoint(|wal| {
                async {
                    let tmp = NamedTempFile::new()?;
                    std::fs::copy(wal, tmp.path())?;
                    Ok(tmp)
                }
                .boxed()
            })
            .await?;
        assert!(wal_snapshot_2.path().exists());

        assert!(ack_2.await.is_ok());

        // insert some data
        let ack_3 = db
            .execute(sqlx::query(
                "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca1')",
            ))
            .await?;

        // check the db content
        assert_eq!(
            2,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        // failed checkpoint
        let wal_snapshot_3: Result<()> = db
            .try_checkpoint(|_| async { Err(anyhow!("oops")) }.boxed())
            .await;
        assert!(wal_snapshot_3.is_err());
        assert!(ack_3.await.is_err());

        // check the db content
        assert_eq!(
            1,
            sqlx::query_scalar::<_, i32>("select count(*) from batchs")
                .fetch_one(&db.read_pool)
                .await?,
        );

        Ok(())
    }

    #[ignore = "manual test (takes 15 seconds)"]
    #[tokio::test(flavor = "multi_thread")]
    async fn replication() -> Result<()> {
        let log = Arc::new(TempDir::new()?);

        let test_duration = Duration::from_secs(15);

        let leader = DB::open(None).await?;
        let _ = leader
            .execute(sqlx::query("CREATE TABLE batchs(uuid)"))
            .await?;

        let snapshot = log.path().join("snapshot");
        leader.snapshot(&snapshot).await?;
        assert!(snapshot.exists());

        let follower = DB::open(Some(&snapshot)).await?;
        let inserted = Arc::new(AtomicUsize::new(0));

        let write_loop = tokio::spawn({
            let leader = leader.clone();
            let inserted = inserted.clone();
            async move {
                let start = Instant::now();
                let mut t = Instant::now();
                let mut i = 0;
                loop {
                    let ack = leader
                        .transaction(|mut txn| {
                            async {
                                let count = rand::thread_rng().gen_range(1..=1_000);
                                for _ in 0..count {
                                    sqlx::query("INSERT INTO batchs VALUES (?1)")
                                        .bind(uuid::Uuid::now_v7().to_string())
                                        .execute(&mut *txn)
                                        .await?;
                                }
                                if rand::thread_rng().gen_bool(0.75) {
                                    txn.commit().await?;
                                    anyhow::Ok(count)
                                } else {
                                    txn.rollback().await?;
                                    anyhow::Ok(0)
                                }
                            }
                            .boxed()
                        })
                        .await
                        .unwrap();
                    i += ack.peek();
                    tokio::spawn({
                        let inserted = inserted.clone();
                        async move {
                            inserted.fetch_add(
                                ack.await.unwrap_or_default(),
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                    });
                    if t.elapsed() > Duration::from_millis(100) {
                        println!("leader: inserted {} rows", i);
                        t = Instant::now();
                        i = 0;
                    }
                    if start.elapsed() > test_duration {
                        break;
                    }
                }
            }
        });

        // checkpoint loop
        tokio::spawn({
            let leader = leader.clone();
            let log = log.clone();
            async move {
                let mut i = 0;
                loop {
                    let shadow = log.path().join(format!("wal_{}", i));
                    let out = shadow.display().to_string();
                    if rand::thread_rng().gen_bool(0.5) {
                        match leader
                            .try_checkpoint(move |wal| {
                                async {
                                    if rand::thread_rng().gen_bool(0.5) {
                                        Err(anyhow!("oops"))
                                    } else {
                                        std::fs::copy(wal, shadow)?;
                                        Ok(())
                                    }
                                }
                                .boxed()
                            })
                            .await
                        {
                            Ok(_) => {
                                println!("leader: checkpointed {out}");
                                i += 1;
                            }
                            Err(e) => {
                                println!("leader: checkpoint was rollbacked: {e}");
                            }
                        }
                    } else {
                        match leader
                            .checkpoint(|wal| {
                                async {
                                    let tmp = NamedTempFile::new().unwrap();
                                    std::fs::copy(wal, tmp.path())?;
                                    Ok(tmp)
                                }
                                .boxed()
                            })
                            .await
                        {
                            Ok(tmp) => {
                                std::fs::rename(tmp.path(), &shadow).unwrap();
                                println!("leader: checkpointed {out}");
                                i += 1;
                            }
                            Err(e) => {
                                println!("leader: checkpoint error: {e}");
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });

        let replication_loop = tokio::spawn({
            let log = log.clone();
            async move {
                let start = Instant::now();
                let mut i = 0;
                loop {
                    let count: i32 = sqlx::query_scalar("select count(*) from batchs")
                        .fetch_one(&follower.read_pool)
                        .await
                        .unwrap();
                    println!("follower sees {} batches", count);

                    loop {
                        let wal = log.path().join(format!("wal_{}", i));
                        if !wal.exists() {
                            break;
                        } else {
                            follower.apply_wal(&wal).await.unwrap();
                            println!("follower: applied {}", wal.display());
                            i += 1;
                        }
                    }

                    tokio::time::sleep(Duration::from_secs(1)).await;

                    // give it 4 more seconds to catch up
                    if start.elapsed() > test_duration + Duration::from_secs(4) {
                        return count;
                    }
                }
            }
        });

        write_loop.await?;
        let total_seen_by_replica: usize = replication_loop.await?.try_into()?;
        let total_inserted = inserted.load(std::sync::atomic::Ordering::Relaxed);
        println!("TOTAL inserted: {}", total_inserted);
        println!("TOTAL seen by replica: {}", total_seen_by_replica);

        assert_eq!(total_inserted, total_seen_by_replica);

        Ok(())
    }
}
