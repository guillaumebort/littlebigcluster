use std::{
    any::Any,
    borrow::Cow,
    fmt::Debug,
    fs::File,
    future::Future,
    io::{Read, Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use anyhow::{anyhow, bail, Context, Result};
use futures::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;
use rusqlite::{types::FromSql, Connection, DatabaseName, OpenFlags, Params, Transaction};
use tempfile::{NamedTempFile, TempDir};
use tokio::{runtime::Handle, sync::oneshot, sync::SemaphorePermit};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::debug;

type DoAck = oneshot::Sender<bool>;
type Factory = Arc<dyn Fn() -> Result<Connection> + Send + Sync>;
type Thunk = Box<dyn FnOnce(&mut Connection) -> Result<Box<dyn Any + Send>> + Send>;
type Callback = tokio::sync::oneshot::Sender<Result<Box<dyn Any + Send>>>;

#[must_use = "Acks must be awaited, or explicitly dropped if you don't care about the result"]
pub struct Ack<A>
where
    A: Unpin,
{
    result: Option<A>,
    ack: oneshot::Receiver<bool>,
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
pub struct DB {
    internal_factory: Factory,
    read_pool: Pool,
    write_pool: Pool,
    pending_acks: Arc<Mutex<Vec<DoAck>>>,
    #[allow(unused)]
    tmp: Arc<TempDir>,
}

impl Debug for DB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DB").field("tmp", &self.tmp).finish()
    }
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
        let db_path = tmp.path().join("litecluster.db");
        debug!(?db_path, ?snapshot, "Opening DB");
        // restore snapshot or create empty db
        if let Some(snapshot) = snapshot {
            std::fs::copy(snapshot, "/tmp/wat")?;
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

        // to deliver the acks signals
        let pending_acks = Arc::new(Mutex::new(Vec::with_capacity(1_024)));

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
            Pool::new(1, 10, factory, pending_acks.clone())
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
            Pool::new(
                num_cpus::get().try_into()?,
                10,
                factory,
                pending_acks.clone(),
            )
        }?;

        Ok(Self {
            tmp,
            internal_factory,
            read_pool,
            write_pool,
            pending_acks,
        })
    }

    async fn call<A>(
        pool: &Pool,
        thunk: impl FnOnce(&mut Connection) -> Result<A> + Send + 'static,
        maybe_do_ack: Option<DoAck>,
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
            maybe_do_ack,
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
        // this guard will block all concurrent writes
        let _guard = self.write_pool.block().await?;

        // let's snapshot the pending acks
        let mut pending_acks = Vec::with_capacity(1_024);
        std::mem::swap(&mut pending_acks, &mut *self.pending_acks.lock());

        // run the checkpoint
        let factory = self.internal_factory.clone();
        let result = tokio::task::spawn_blocking(move || {
            let connection = factory()?;
            let wal_path = wal_path(&connection);
            let result = Handle::current().block_on(f(wal_path))?;
            connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
            anyhow::Ok(result)
        })
        .await?;

        Ok((result, pending_acks))
    }

    async fn rollback(&self) -> Result<()> {
        let mut pending_acks = Vec::with_capacity(1_024);
        {
            let _write_guard = self.write_pool.block().await?;
            let _read_guard = self.read_pool.block().await?;
            std::mem::swap(&mut pending_acks, &mut *self.pending_acks.lock());
            let factory = self.internal_factory.clone();
            tokio::task::spawn_blocking(move || {
                let connection = factory()?;
                let wal_path = wal_path(&connection);
                std::fs::File::create(wal_path)?; // truncate wal file
                Self::invalidate_shm(&connection)?;
                // truncate properly
                connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
                anyhow::Ok(())
            })
            .await??;
        }
        tokio::task::spawn_blocking(move || {
            for do_ack in pending_acks {
                let _ = do_ack.send(false);
            }
        })
        .await?;
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
            let wal_path = wal_path(&connection);

            // truncate wal file
            connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;

            // replace the wal file
            std::fs::copy(&wal_snapshot, &wal_path)?;

            Self::invalidate_shm(&connection)?;

            // truncate wal file again
            connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }

    fn invalidate_shm(connection: &Connection) -> Result<()> {
        let shm_path = shm_path(&connection);

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
}

#[derive(Clone)]
struct Pool {
    tasks_count: u32,
    tx: async_channel::Sender<(Thunk, Callback, Option<DoAck>)>,
    semaphore: Arc<tokio::sync::Semaphore>,
    #[allow(unused)]
    cancel: Arc<DropGuard>,
}

impl Pool {
    pub fn new(
        tasks_count: u32,
        max_pending: usize,
        factory: Factory,
        pending_acks: Arc<Mutex<Vec<DoAck>>>,
    ) -> Result<Self> {
        let (tx, rx) = async_channel::bounded(max_pending);
        let cancel = CancellationToken::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(tasks_count.try_into()?));
        for _ in 0..tasks_count {
            let factory = factory.clone();
            let rx = rx.clone();
            let cancel = cancel.child_token();
            let semaphore = semaphore.clone();
            let pending_acks = pending_acks.clone();
            tokio::task::spawn_blocking(|| {
                Handle::current().block_on(Self::run(factory, rx, cancel, semaphore, pending_acks))
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

    async fn call(
        &self,
        thunk: Thunk,
        callback: Callback,
        maybe_do_ack: Option<DoAck>,
    ) -> Result<()> {
        self.tx
            .send((thunk, callback, maybe_do_ack))
            .await
            .map_err(|t| anyhow!("{t:?}"))
            .context("failed to send call to pool")
    }

    async fn block(&self) -> Result<SemaphorePermit> {
        Ok(self.semaphore.acquire_many(self.tasks_count).await?)
    }

    async fn run(
        factory: Factory,
        rx: async_channel::Receiver<(Thunk, Callback, Option<DoAck>)>,
        cancel: CancellationToken,
        semaphore: Arc<tokio::sync::Semaphore>,
        pending_acks: Arc<Mutex<Vec<DoAck>>>,
    ) -> Result<()> {
        let mut connection = factory()?;
        while !cancel.is_cancelled() {
            let (thunk, tx, maybe_do_ack) = rx.recv().await?;
            let result = {
                let _permit = semaphore.acquire().await?;
                match thunk(&mut connection) {
                    Ok(a) => {
                        if let Some(do_ack) = maybe_do_ack {
                            pending_acks.lock().push(do_ack);
                        }
                        Ok(a)
                    }
                    Err(e) => Err(e),
                }
            };
            tx.send(result)
                .map_err(|t| anyhow!("{t:?}"))
                .context("failed to send call result")?;
        }
        Ok(())
    }
}

pub trait ReadDB {
    fn query_row<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: Send + 'static;

    fn query_row_opt<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<Option<A>>> + Send
    where
        A: Send + 'static;

    fn query_scalar<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: FromSql + Send + 'static;

    fn readonly_transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<A>> + Send
    where
        A: Send + 'static;
}

impl ReadDB for DB {
    async fn query_row<A>(
        &self,
        sql: impl Into<Cow<'static, str>>,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        let sql = sql.into();
        Self::call(
            &self.read_pool,
            move |c| c.query_row_and_then(sql.as_ref(), params, |row| f(row)),
            None,
        )
        .await
    }

    async fn query_row_opt<A>(
        &self,
        sql: impl Into<Cow<'static, str>>,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> Result<Option<A>>
    where
        A: Send + 'static,
    {
        let sql = sql.into();
        Self::call(
            &self.read_pool,
            move |c| {
                let mut stmt = c.prepare(&sql)?;
                let mut rows = stmt.query(params)?;
                if let Some(row) = rows.next()? {
                    f(row).map(Some)
                } else {
                    Ok(None)
                }
            },
            None,
        )
        .await
    }

    async fn query_scalar<A>(
        &self,
        sql: impl Into<Cow<'static, str>>,
        params: impl Params + Send + 'static,
    ) -> Result<A>
    where
        A: FromSql + Send + 'static,
    {
        let sql = sql.into();
        Self::call(
            &self.read_pool,
            move |c| Ok(c.query_row(sql.as_ref(), params, |row| row.get(0))?),
            None,
        )
        .await
    }

    async fn readonly_transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        Self::call(&self.read_pool, move |c| Ok(thunk(c.transaction()?)?), None).await
    }
}

impl<X> ReadDB for X
where
    X: Deref<Target = DB> + Send + Sync,
{
    async fn query_row<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        self.deref().query_row(sql, params, f).await
    }

    async fn query_row_opt<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
        f: impl FnOnce(&rusqlite::Row<'_>) -> Result<A> + Send + 'static,
    ) -> Result<Option<A>>
    where
        A: Send + 'static,
    {
        self.deref().query_row_opt(sql, params, f).await
    }

    async fn query_scalar<A>(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> Result<A>
    where
        A: FromSql + Send + 'static,
    {
        self.deref().query_scalar(sql, params).await
    }

    async fn readonly_transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> Result<A>
    where
        A: Send + 'static,
    {
        self.deref().readonly_transaction(thunk).await
    }
}

pub trait WriteDB {
    fn execute(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> impl Future<Output = Result<Ack<usize>>> + Send;

    fn execute_batch(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
    ) -> impl Future<Output = Result<Ack<()>>> + Send;

    fn transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> impl Future<Output = Result<Ack<A>>> + Send
    where
        A: Send + Unpin + 'static;
}

impl WriteDB for DB {
    async fn execute(
        &self,
        sql: impl Into<Cow<'static, str>>,
        params: impl Params + Send + 'static,
    ) -> Result<Ack<usize>> {
        // TODO: box the Acks
        let (do_ack, ack) = oneshot::channel();
        let sql = sql.into();
        let result = Self::call(
            &self.write_pool,
            move |c| Ok(c.execute(sql.as_ref(), params)?),
            Some(do_ack),
        )
        .await?;
        Ok(Ack {
            result: Some(result),
            ack,
        })
    }

    async fn execute_batch(&self, sql: impl Into<Cow<'static, str>>) -> Result<Ack<()>> {
        let (do_ack, ack) = oneshot::channel();
        let sql = sql.into();
        Self::call(
            &self.write_pool,
            move |c| Ok(c.execute_batch(sql.as_ref())?),
            Some(do_ack),
        )
        .await?;
        Ok(Ack {
            result: Some(()),
            ack,
        })
    }

    async fn transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> Result<Ack<A>>
    where
        A: Unpin + Send + 'static,
    {
        let (do_ack, ack) = oneshot::channel();
        let result = Self::call(
            &self.write_pool,
            move |c| Ok(thunk(c.transaction()?)?),
            Some(do_ack),
        )
        .await?;
        Ok(Ack {
            result: Some(result),
            ack,
        })
    }
}

impl<X> WriteDB for X
where
    X: Deref<Target = DB> + Send + Sync,
{
    async fn execute(
        &self,
        sql: impl Into<Cow<'static, str>> + Send,
        params: impl Params + Send + 'static,
    ) -> Result<Ack<usize>> {
        self.deref().execute(sql, params).await
    }

    async fn execute_batch(&self, sql: impl Into<Cow<'static, str>> + Send) -> Result<Ack<()>> {
        self.deref().execute_batch(sql).await
    }

    async fn transaction<A>(
        &self,
        thunk: impl FnOnce(Transaction) -> rusqlite::Result<A> + Send + 'static,
    ) -> Result<Ack<A>>
    where
        A: Unpin + Send + 'static,
    {
        self.deref().transaction(thunk).await
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
        assert_eq!(1, db.query_scalar::<i32>("select 1", []).await?);
        Ok(())
    }

    #[tokio::test]
    async fn open_and_insert() -> Result<()> {
        let db = DB::open(None).await?;
        let _ = db
            .execute_batch(
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
                None,
            )
            .await?;

        // the pool is blocked, the call will timeout
        assert!(tokio::time::timeout(Duration::from_millis(100), rx)
            .await
            .is_err());

        drop(blocking_permits);

        Ok(())
    }

    #[tokio::test]
    async fn snapshot() -> Result<()> {
        // create a blank db, insert some data
        let db = DB::open(None).await?;
        let _ = db
            .transaction(|txn| {
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
        let _ = db.execute_batch("CREATE TABLE batchs(uuid)").await?;

        // transaction rollback
        let _ = db
            .transaction(|txn| {
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
        // create a blank db, create a table
        let db = DB::open(None).await?;
        let _ = db.execute_batch("CREATE TABLE batchs(uuid)").await?;

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
            .transaction(|txn| {
                txn.execute(
                    "INSERT INTO batchs VALUES ('0191b6d0-3d9a-7eb1-88b8-5312737f2ca0')",
                    [],
                )?;
                txn.rollback()
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
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // insert some data
        let ack_3 = db
            .execute(
                "INSERT INTO batchs VALUES (?1)",
                [("0191b6d0-3d9a-7eb1-88b8-5312737f2ca0")],
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
        assert_eq!(1, ack_3.await?);

        // check the db content
        assert_eq!(
            1,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // create a new blank db, restore the first checkpoint
        let db = DB::open(None).await?;
        db.apply_wal(&wal_snapshot_1.path()).await?;
        assert_eq!(
            0,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // restore the second checkpoint
        db.apply_wal(&wal_snapshot_2.path()).await?;
        assert_eq!(
            0,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // restore the third checkpoint
        db.apply_wal(&wal_snapshot_3.path()).await?;
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
    async fn rollback() -> Result<()> {
        // create a blank db, create a table
        let db = DB::open(None).await?;
        let _ = db.execute_batch("CREATE TABLE batchs(uuid)").await?;

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
            .execute(
                "INSERT INTO batchs VALUES (?1)",
                [("0191b6d0-3d9a-7eb1-88b8-5312737f2ca0")],
            )
            .await?;

        // check the db content
        assert_eq!(
            1,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // but now rollback the current wal
        db.rollback().await?;

        assert!(ack_1.await.is_err());

        // check the db content
        assert_eq!(
            0,
            db.query_scalar::<i32>("select count(*) from batchs", [])
                .await?,
        );

        // insert some data
        let ack_2 = db
            .execute(
                "INSERT INTO batchs VALUES (?1)",
                [("0191b6d0-3d9a-7eb1-88b8-5312737f2ca0")],
            )
            .await?;

        // check the db content
        assert_eq!(
            1,
            db.query_scalar::<i32>("select count(*) from batchs", [])
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
            .execute(
                "INSERT INTO batchs VALUES (?1)",
                [("0191b6d0-3d9a-7eb1-88b8-5312737f2ca1")],
            )
            .await?;

        // check the db content
        assert_eq!(
            2,
            db.query_scalar::<i32>("select count(*) from batchs", [])
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
            db.query_scalar::<i32>("select count(*) from batchs", [])
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
        let _ = leader.execute_batch("CREATE TABLE batchs(uuid)").await?;

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
                    tokio::spawn({
                        let inserted = inserted.clone();
                        async move {
                            inserted.fetch_add(
                                ack.await.unwrap_or_default(),
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                    });
                    i += 1;
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

        write_loop.await?;
        let total_seen_by_replica: usize = replication_loop.await?.try_into()?;
        let total_inserted = inserted.load(std::sync::atomic::Ordering::Relaxed);
        println!("TOTAL inserted: {}", total_inserted);
        println!("TOTAL seen by replica: {}", total_seen_by_replica);

        assert_eq!(total_inserted, total_seen_by_replica);

        Ok(())
    }
}
