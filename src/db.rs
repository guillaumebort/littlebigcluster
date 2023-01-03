use std::{
    any::Any,
    borrow::Cow,
    fs::File,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{types::FromSql, Connection, DatabaseName, OpenFlags, Params, Transaction};
use tempfile::TempDir;
use tokio::{runtime::Handle, sync::SemaphorePermit};
use tokio_util::sync::{CancellationToken, DropGuard};

type Factory = Arc<dyn Fn() -> Result<Connection> + Send + Sync>;
type Thunk = Box<dyn FnOnce(&mut Connection) -> Result<Box<dyn Any + Send>> + Send>;
type Callback = tokio::sync::oneshot::Sender<Result<Box<dyn Any + Send>>>;

#[derive(Clone)]
pub struct DB {
    internal_factory: Factory,
    read_pool: Pool,
    write_pool: Pool,
    #[allow(unused)]
    tmp: Arc<TempDir>,
}

fn db_path(connection: &Connection) -> &str {
    connection.path().expect("no path?")
}

fn wal_path(connection: &Connection) -> PathBuf {
    PathBuf::from(format!("{}-wal", db_path(connection)))
}

fn shm_path(connection: &Connection) -> PathBuf {
    PathBuf::from(format!("{}-shm", db_path(connection)))
}

impl DB {
    pub async fn open(snapshot: Option<&Path>) -> Result<Self> {
        let tmp = Arc::new(TempDir::new()?);
        let db_path = tmp.path().join("littlecluster.db");
        // restore snapshot or create empty db
        if let Some(snapshot) = snapshot {
            std::fs::copy(snapshot, &db_path)?;
        } else {
            // create empty db
            let path = db_path.clone();
            tokio::task::spawn_blocking(move || {
                let connection = Connection::open_with_flags(
                    &path,
                    OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
                )?;
                connection.pragma_update(None, "journal_mode", "WAL")?;
                anyhow::Ok(())
            })
            .await
            .context(format!("failed to initialize db at {db_path:?}"))??;
        }

        // these connections will be used internally
        let internal_factory = {
            let path = db_path.clone();
            Arc::new(move || {
                let connection =
                    Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
                connection.pragma_update(None, "synchronous", "NORMAL")?;
                connection.pragma_update(None, "wal_autocheckpoint", "0")?;
                if "wal".to_string()
                    != connection
                        .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?
                {
                    Err(anyhow!("database is not in WAL mode"))
                } else {
                    Ok(connection)
                }
            })
        };

        // test the connection
        {
            let factory = internal_factory.clone();
            tokio::task::spawn_blocking(move || {
                let connection = factory()?;
                if 1 != connection.query_row("select 1", [], |row| row.get::<_, i32>(0))? {
                    Err(anyhow!("failed to query 1"))
                } else {
                    Ok(())
                }
            })
            .await??;
        }

        // the write "pool" has actually only one connection to mitigate lock contention
        let write_pool = {
            let path = db_path.clone();
            let factory = Arc::new(move || {
                let connection =
                    Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
                connection.pragma_update(None, "synchronous", "NORMAL")?;
                connection.pragma_update(None, "wal_autocheckpoint", "0")?;
                Ok(connection)
            });
            Pool::new(1, 10, factory)
        }?;

        // the read pool has a connection per core but is read only
        let read_pool = {
            let path = db_path.clone();
            let factory = Arc::new(move || {
                let connection =
                    Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
                assert!(connection.is_readonly(DatabaseName::Main)?);
                Ok(connection)
            });
            Pool::new(num_cpus::get().try_into()?, 10, factory)
        }?;

        Ok(Self {
            tmp,
            internal_factory,
            read_pool,
            write_pool,
        })
    }

    pub async fn execute(
        &self,
        sql: impl Into<Cow<'static, str>>,
        params: impl Params + Send + 'static,
    ) -> Result<usize> {
        let sql = sql.into();
        Self::call(&self.write_pool, move |c| {
            Ok(c.execute(sql.as_ref(), params)?)
        })
        .await
    }

    pub async fn execute_batch(&self, sql: impl Into<Cow<'static, str>>) -> Result<()> {
        let sql = sql.into();
        Self::call(
            &self.write_pool,
            move |c| Ok(c.execute_batch(sql.as_ref())?),
        )
        .await
    }

    pub async fn transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        Self::call(&self.write_pool, move |c| Ok(thunk(c.transaction()?)?)).await
    }

    pub async fn query_row<A>(
        &self,
        sql: impl Into<Cow<'static, str>>,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let sql = sql.into();
        Self::call(&self.read_pool, move |c| {
            c.query_row_and_then(sql.as_ref(), params, |row| f(row))
        })
        .await
    }

    pub async fn query_scalar<A>(
        &self,
        sql: impl Into<Cow<'static, str>>,
        params: impl Params + Send + 'static,
    ) -> Result<A>
    where
        A: FromSql + Send + 'static,
    {
        let sql = sql.into();
        Self::call(&self.read_pool, move |c| {
            Ok(c.query_row(sql.as_ref(), params, |row| row.get(0))?)
        })
        .await
    }

    pub async fn readonly_transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        Self::call(&self.read_pool, move |c| Ok(thunk(c.transaction()?)?)).await
    }

    async fn call<A>(
        pool: &Pool,
        thunk: impl FnOnce(&mut Connection) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        pool.call(
            Box::new(move |connection| {
                let result = thunk(connection)?;
                Ok(Box::new(result) as Box<dyn Any + Send>)
            }),
            tx,
        )
        .await?;
        rx.await
            .context("Failed to receive call result")
            .and_then(|r| r)
            .and_then(|result| {
                let result = result
                    .downcast::<A>()
                    .map_err(|e| anyhow!("downcast error: {e:#?}"))?;
                Ok(*result)
            })
    }

    pub(crate) async fn checkpoint(&self, wal_snapshot: impl Into<PathBuf>) -> Result<()> {
        let _guard = self.write_pool.block().await?;
        let wal_snapshot = wal_snapshot.into();
        let factory = self.internal_factory.clone();
        tokio::task::spawn_blocking(move || {
            let connection = factory()?;
            std::fs::copy(wal_path(&connection), wal_snapshot)?;
            connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }

    pub(crate) async fn snapshot(&self, db_snapshot: impl Into<PathBuf>) -> Result<()> {
        let _guard = self.write_pool.block().await?;
        let db_snapshot = db_snapshot.into();
        let factory = self.internal_factory.clone();
        tokio::task::spawn_blocking(move || {
            let connection = factory()?;
            connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
            std::fs::copy(db_path(&connection), db_snapshot)?;
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }

    pub(crate) async fn apply_wal(&self, wal_snapshot: impl Into<PathBuf>) -> Result<()> {
        let _guard1 = self.write_pool.block().await?;
        let _guard2 = self.read_pool.block().await?;
        let factory = self.internal_factory.clone();
        let wal_snapshot = wal_snapshot.into();
        tokio::task::spawn_blocking(move || {
            let connection = factory()?;

            let shm_path = shm_path(&connection);
            let wal_path = wal_path(&connection);

            // truncate wal file
            connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;

            // replace the wal file
            std::fs::copy(&wal_snapshot, &wal_path)?;

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

            // truncate wal file again
            connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }
}

#[derive(Clone)]
struct Pool {
    tasks_count: u32,
    tx: async_channel::Sender<(Thunk, Callback)>,
    semaphore: Arc<tokio::sync::Semaphore>,
    #[allow(unused)]
    cancel: Arc<DropGuard>,
}

impl Pool {
    pub fn new(tasks_count: u32, max_pending: usize, factory: Factory) -> Result<Self> {
        let (tx, rx) = async_channel::bounded(max_pending);
        let cancel = CancellationToken::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(tasks_count.try_into()?));
        for _ in 0..tasks_count {
            let factory = factory.clone();
            let rx = rx.clone();
            let cancel = cancel.child_token();
            let semaphore = semaphore.clone();
            tokio::task::spawn_blocking(|| {
                Handle::current().block_on(Self::run(factory, rx, cancel, semaphore))
            });
        }
        let cancel = cancel.drop_guard().into();
        Ok(Self {
            tasks_count,
            tx,
            semaphore,
            cancel,
        })
    }

    async fn call(&self, thunk: Thunk, callback: Callback) -> Result<()> {
        self.tx
            .send((thunk, callback))
            .await
            .map_err(|t| anyhow!("{t:?}"))
            .context("failed to send call to pool")
    }

    async fn block(&self) -> Result<SemaphorePermit> {
        Ok(self.semaphore.acquire_many(self.tasks_count).await?)
    }

    async fn run(
        factory: Factory,
        rx: async_channel::Receiver<(Thunk, Callback)>,
        cancel: CancellationToken,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Result<()> {
        let mut connection = factory()?;
        while !cancel.is_cancelled() {
            let (thunk, tx) = rx.recv().await?;
            let result = {
                let _permit = semaphore.acquire().await?;
                thunk(&mut connection)
            };
            tx.send(result)
                .map_err(|t| anyhow!("{t:?}"))
                .context("failed to send call result")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use rand::Rng;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn open_and_query() -> Result<()> {
        let db = DB::open(None).await?;
        assert_eq!(1, db.query_scalar::<i32>("select 1", []).await?);
        Ok(())
    }

    #[tokio::test]
    async fn open_and_insert() -> Result<()> {
        let db = DB::open(None).await?;
        db.execute_batch(
            r#"
            CREATE TABLE batchs(uuid);
            INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0');
        "#,
        )
        .await?;
        assert_eq!(
            "0191b6d0-3d9a-7eb1-88b8-5312737f2ca0",
            db.query_scalar::<String>("select * from batchs", [])
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_in_readonly_txn() -> Result<()> {
        let db = DB::open(None).await?;
        assert!(db
            .readonly_transaction(|txn| txn.execute("create table lol(id);", ()))
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn snapshot() -> Result<()> {
        // create a blank db, insert some data
        let db = DB::open(None).await?;
        db.transaction(|txn| {
            txn.execute("CREATE TABLE batchs(uuid)", [])?;
            {
                let mut stmt = txn.prepare_cached("INSERT INTO batchs VALUES (?1)")?;
                for i in 0..100 {
                    stmt.execute([i.to_string()])?;
                }
            }
            txn.commit()
        })
        .await?;

        // snaphot the db
        let tmp = TempDir::new()?;
        let db_snapshot = tmp.path().join("db_snapshot");
        db.snapshot(&db_snapshot).await?;

        // restore the snapshot
        let db = DB::open(Some(&db_snapshot)).await?;
        let count = db
            .query_scalar::<i32>("select count(*) from batchs", [])
            .await?;
        assert_eq!(100, count);

        Ok(())
    }

    #[tokio::test]
    async fn auto_rollback_transactions() -> Result<()> {
        let db = DB::open(None).await?;
        db.execute_batch("CREATE TABLE batchs(uuid)").await?;

        // transaction rollback
        db.transaction(|txn| {
            txn.execute(
                "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0')",
                [],
            )?;
            // oops forgot to commit
            Ok(())
        })
        .await?;

        // the transaction was rolled back
        assert_eq!(
            0,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn checkpoint() -> Result<()> {
        let tmp = TempDir::new()?;

        // create a blank db, create a table
        let db = DB::open(None).await?;
        db.execute_batch("CREATE TABLE batchs(uuid)").await?;

        // checkpoint the db
        let wal_snapshot_1 = tmp.path().join("wal_snapshot_1");
        db.checkpoint(&wal_snapshot_1).await?;
        assert!(wal_snapshot_1.exists());

        // transaction rollback
        db.transaction(|txn| {
            txn.execute(
                "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0')",
                [],
            )?;
            txn.rollback()
        })
        .await?;

        // checkpoint the db
        let wal_snapshot_2 = tmp.path().join("wal_snapshot_2");
        db.checkpoint(&wal_snapshot_2).await?;
        assert!(wal_snapshot_2.exists());

        // check the db content
        assert_eq!(
            0,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // insert some data
        db.execute(
            "INSERT INTO batchs VALUES (?1)",
            [("0191b6d0-3d9a-7eb1-88b8-5312737f2ca0")],
        )
        .await?;

        // checkpoint the db
        let wal_snapshot_3 = tmp.path().join("wal_snapshot_3");
        db.checkpoint(&wal_snapshot_3).await?;
        assert!(wal_snapshot_3.exists());

        // check the db content
        assert_eq!(
            1,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // create a new blank db, restore the first checkpoint
        let db = DB::open(None).await?;
        db.apply_wal(&wal_snapshot_1).await?;
        assert_eq!(
            0,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // restore the second checkpoint
        db.apply_wal(&wal_snapshot_2).await?;
        assert_eq!(
            0,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // restore the third checkpoint
        db.apply_wal(&wal_snapshot_3).await?;
        assert_eq!(
            1,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // check the db content
        assert_eq!(
            "0191b6d0-3d9a-7eb1-88b8-5312737f2ca0",
            db.query_scalar::<String>("select * from batchs", [])
                .await?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn block_pool() -> Result<()> {
        let leader = DB::open(None).await?;

        let blocking_permits = leader.write_pool.block().await?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        leader
            .clone()
            .write_pool
            .call(
                Box::new(|c| Ok(c.query_row("select 1", [], |_| Ok(Box::new(1)))?)),
                tx,
            )
            .await?;

        // the pool is blocked, the call will timeout
        assert!(tokio::time::timeout(Duration::from_millis(100), rx)
            .await
            .is_err());

        drop(blocking_permits);

        Ok(())
    }

    #[ignore = "manual test (takes 15 seconds)"]
    #[tokio::test(flavor = "multi_thread")]
    async fn replication() -> Result<()> {
        let log = Arc::new(TempDir::new()?);

        let test_duration = Duration::from_secs(15);

        let leader = DB::open(None).await?;
        leader.execute_batch("CREATE TABLE batchs(uuid)").await?;

        let snapshot = log.path().join("snapshot");
        leader.snapshot(&snapshot).await?;
        assert!(snapshot.exists());

        let follower = DB::open(Some(&snapshot)).await?;

        let write_loop = tokio::spawn({
            let leader = leader.clone();
            async move {
                let start = Instant::now();
                let mut t = Instant::now();
                let mut inserted = 0;
                let mut i = 0;
                loop {
                    inserted += leader
                        .transaction(move |txn| {
                            {
                                let mut stmt =
                                    txn.prepare_cached("INSERT INTO batchs VALUES (?1)")?;
                                stmt.execute([uuid::Uuid::now_v7().to_string()])?;
                            }
                            if rand::thread_rng().gen_bool(0.75) {
                                txn.commit()?;
                                Ok(1)
                            } else {
                                txn.rollback()?;
                                Ok(0)
                            }
                        })
                        .await
                        .unwrap();
                    i += 1;
                    if t.elapsed() > Duration::from_millis(100) {
                        println!("leader: inserted {} rows", i);
                        t = Instant::now();
                        i = 0;
                    }
                    if start.elapsed() > test_duration {
                        return inserted;
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
                    let wal = log.path().join(format!("wal_{}", i));
                    let tmp = NamedTempFile::new_in(log.path()).unwrap();
                    if let Err(e) = leader.checkpoint(tmp.path()).await {
                        println!("leader: checkpoint error: {e}");
                    } else {
                        std::fs::rename(tmp.path(), &wal).unwrap();
                        println!("leader: checkpointed {}", wal.display());
                        i += 1;
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
                    let count = follower
                        .query_scalar::<i32>("select count (*) from batchs", [])
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

        let total_inserted = write_loop.await?;
        println!("TOTAL inserted: {}", total_inserted);

        let total_seen_by_replica = replication_loop.await?;
        println!("TOTAL seen by replica: {}", total_seen_by_replica);

        assert_eq!(total_inserted, total_seen_by_replica);

        Ok(())
    }
}
