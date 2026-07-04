//! SQLite access: dedicated writer thread with session tracking, per-call read connections.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::session::{ConflictAction, Session};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};

pub fn ensure_meta(conn: &Connection) -> Result<u32> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _lbc_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );",
    )?;
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM _lbc_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("read schema_version")?;
    Ok(version.and_then(|v| v.parse().ok()).unwrap_or(0))
}

pub fn set_schema_version(conn: &Connection, version: u32) -> Result<()> {
    conn.execute(
        "INSERT INTO _lbc_meta(key, value) VALUES('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![version.to_string()],
    )?;
    Ok(())
}

pub fn assert_all_tables_have_primary_key(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table'
           AND name NOT LIKE 'sqlite_%'
           AND name != '_lbc_meta'",
    )?;
    let tables = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for table in tables {
        let pk_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?) WHERE pk > 0",
            params![table],
            |row| row.get(0),
        )?;
        if pk_count == 0 {
            bail!("table `{table}` has no PRIMARY KEY; changeset replication requires one");
        }
    }
    Ok(())
}

fn is_schema_ddl(action: &AuthAction<'_>) -> bool {
    matches!(
        action,
        AuthAction::CreateIndex { .. }
            | AuthAction::CreateTable { .. }
            | AuthAction::CreateTempIndex { .. }
            | AuthAction::CreateTempTable { .. }
            | AuthAction::CreateTempTrigger { .. }
            | AuthAction::CreateTempView { .. }
            | AuthAction::CreateTrigger { .. }
            | AuthAction::CreateView { .. }
            | AuthAction::DropIndex { .. }
            | AuthAction::DropTable { .. }
            | AuthAction::DropTempIndex { .. }
            | AuthAction::DropTempTable { .. }
            | AuthAction::DropTempTrigger { .. }
            | AuthAction::DropTempView { .. }
            | AuthAction::DropTrigger { .. }
            | AuthAction::DropView { .. }
            | AuthAction::AlterTable { .. }
            | AuthAction::CreateVtable { .. }
            | AuthAction::DropVtable { .. }
    )
}

fn install_ddl_guard(conn: &Connection, allow_ddl: Arc<AtomicBool>) {
    conn.authorizer(Some(move |ctx: AuthContext<'_>| {
        if allow_ddl.load(Ordering::Relaxed) || !is_schema_ddl(&ctx.action) {
            Authorization::Allow
        } else {
            Authorization::Deny
        }
    }));
}

fn with_allow_ddl<T>(allow_ddl: &AtomicBool, f: impl FnOnce() -> Result<T>) -> Result<T> {
    allow_ddl.store(true, Ordering::Relaxed);
    let result = f();
    allow_ddl.store(false, Ordering::Relaxed);
    result
}

fn open_connection(path: &Path, read_only: bool) -> Result<Connection> {
    let flags = if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
    };
    let conn = Connection::open_with_flags(path, flags)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    if !read_only {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
    }
    Ok(conn)
}

fn apply_changeset(conn: &Connection, changeset: &[u8]) -> Result<()> {
    if changeset.is_empty() {
        return Ok(());
    }
    let mut cursor = Cursor::new(changeset);
    conn.apply_strm(&mut cursor, None::<fn(&str) -> bool>, |_conflict, _item| {
        ConflictAction::SQLITE_CHANGESET_REPLACE
    })?;
    Ok(())
}

struct WriteState {
    conn: Connection,
    session: Option<Session<'static>>,
    leader_mode: bool,
    allow_ddl: Arc<AtomicBool>,
}

unsafe fn extend_session<'conn>(session: Session<'conn>) -> Session<'static> {
    std::mem::transmute(session)
}

enum WriterRequest {
    CaptureChangeset(mpsc::Sender<Result<Vec<u8>>>),
    CommitEpoch(mpsc::Sender<Result<()>>),
    RollbackEpoch(mpsc::Sender<Result<()>>),
    SetLeaderMode {
        leader: bool,
        reply: mpsc::Sender<Result<()>>,
    },
    WithWriteValue {
        work: Box<dyn FnOnce(&Connection) -> Result<Box<dyn std::any::Any + Send>> + Send>,
        reply: mpsc::Sender<Result<Box<dyn std::any::Any + Send>>>,
    },
    ApplyChangeset {
        bytes: Vec<u8>,
        reply: mpsc::Sender<Result<()>>,
    },
    ApplyMigration {
        version: u32,
        sql: Vec<String>,
        reply: mpsc::Sender<Result<()>>,
    },
    EnsureMeta(mpsc::Sender<Result<u32>>),
    VacuumInto {
        dest: PathBuf,
        reply: mpsc::Sender<Result<()>>,
    },
}

struct WriterHandle {
    tx: mpsc::Sender<WriterRequest>,
    _thread: JoinHandle<()>,
}

fn recv_reply<T>(rx: mpsc::Receiver<Result<T>>) -> Result<T> {
    rx.recv()
        .map_err(|_| anyhow!("sqlite writer thread stopped"))?
}

impl WriterHandle {
    fn send(&self, req: WriterRequest) -> Result<()> {
        self.tx
            .send(req)
            .map_err(|_| anyhow!("sqlite writer thread stopped"))
    }

    fn capture_changeset(&self) -> Result<Vec<u8>> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::CaptureChangeset(reply_tx))?;
        recv_reply(reply_rx)
    }

    fn commit_epoch(&self) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::CommitEpoch(reply_tx))?;
        recv_reply(reply_rx)
    }

    fn rollback_epoch(&self) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::RollbackEpoch(reply_tx))?;
        recv_reply(reply_rx)
    }

    fn set_leader_mode(&self, leader: bool) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::SetLeaderMode {
            leader,
            reply: reply_tx,
        })?;
        recv_reply(reply_rx)
    }

    fn with_write_value<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::WithWriteValue {
            work: Box::new(move |conn| Ok(Box::new(f(conn)?) as Box<dyn std::any::Any + Send>)),
            reply: reply_tx,
        })?;
        let boxed = recv_reply(reply_rx)?;
        boxed
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|_| anyhow!("sqlite writer returned unexpected type"))
    }

    fn apply_changeset(&self, bytes: Vec<u8>) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::ApplyChangeset {
            bytes,
            reply: reply_tx,
        })?;
        recv_reply(reply_rx)
    }

    fn ensure_meta(&self) -> Result<u32> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::EnsureMeta(reply_tx))?;
        recv_reply(reply_rx)
    }

    fn apply_migration(&self, version: u32, sql: Vec<String>) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::ApplyMigration {
            version,
            sql,
            reply: reply_tx,
        })?;
        recv_reply(reply_rx)
    }

    fn vacuum_into(&self, dest: PathBuf) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send(WriterRequest::VacuumInto {
            dest,
            reply: reply_tx,
        })?;
        recv_reply(reply_rx)
    }
}

fn spawn_writer(path: PathBuf, leader_mode: bool) -> WriterHandle {
    let (tx, rx) = mpsc::channel();
    let thread = thread::spawn(move || writer_loop(path, rx, leader_mode));
    WriterHandle {
        tx,
        _thread: thread,
    }
}

fn writer_loop(path: PathBuf, rx: mpsc::Receiver<WriterRequest>, leader_mode: bool) {
    let mut state = match open_writer_state(&path, leader_mode) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("sqlite writer failed to open: {e}");
            return;
        }
    };

    while let Ok(req) = rx.recv() {
        match req {
            WriterRequest::CaptureChangeset(reply) => {
                let _ = reply.send(capture_changeset(&mut state));
            }
            WriterRequest::CommitEpoch(reply) => {
                let _ = reply.send(commit_epoch(&mut state));
            }
            WriterRequest::RollbackEpoch(reply) => {
                let _ = reply.send(rollback_epoch(&mut state));
            }
            WriterRequest::SetLeaderMode { leader, reply } => {
                let _ = reply.send(set_leader_mode(&mut state, leader));
            }
            WriterRequest::WithWriteValue { work, reply } => {
                let _ = reply.send(work(&state.conn));
            }
            WriterRequest::ApplyChangeset { bytes, reply } => {
                let _ = reply.send(apply_changeset(&state.conn, &bytes));
            }
            WriterRequest::ApplyMigration {
                version,
                sql,
                reply,
            } => {
                let _ = reply.send(apply_migration(
                    &mut state.conn,
                    &state.allow_ddl,
                    version,
                    &sql,
                ));
            }
            WriterRequest::EnsureMeta(reply) => {
                let _ = reply.send(with_allow_ddl(&state.allow_ddl, || {
                    ensure_meta(&state.conn)
                }));
            }
            WriterRequest::VacuumInto { dest, reply } => {
                let _ = reply.send(vacuum_into(&mut state, &dest));
            }
        }
    }
}

fn open_writer_state(path: &Path, leader_mode: bool) -> Result<WriteState> {
    let conn = open_connection(path, false)?;
    let allow_ddl = Arc::new(AtomicBool::new(false));
    install_ddl_guard(&conn, allow_ddl.clone());
    let session = if leader_mode {
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        let session: Session<'static> = unsafe {
            let mut s = Session::new(&conn)?;
            s.attach(None)?;
            extend_session(s)
        };
        Some(session)
    } else {
        None
    };
    Ok(WriteState {
        conn,
        session,
        leader_mode,
        allow_ddl,
    })
}

fn reset_session(state: &mut WriteState) -> Result<()> {
    if !state.leader_mode {
        return Ok(());
    }
    let mut session = Session::new(&state.conn)?;
    session.attach(None)?;
    state.session = Some(unsafe { extend_session(session) });
    Ok(())
}

fn capture_changeset(state: &mut WriteState) -> Result<Vec<u8>> {
    if !state.leader_mode {
        bail!("capture_changeset called while not in leader mode");
    }
    let session = state
        .session
        .as_mut()
        .ok_or_else(|| anyhow!("leader session missing"))?;
    let mut buf = Vec::new();
    session.changeset_strm(&mut buf)?;
    Ok(buf)
}

fn commit_epoch(state: &mut WriteState) -> Result<()> {
    if !state.leader_mode {
        bail!("commit_epoch called while not in leader mode");
    }
    state.conn.execute_batch("COMMIT; BEGIN IMMEDIATE;")?;
    reset_session(state)
}

fn rollback_epoch(state: &mut WriteState) -> Result<()> {
    if !state.leader_mode {
        bail!("rollback_epoch called while not in leader mode");
    }
    state.conn.execute_batch("ROLLBACK; BEGIN IMMEDIATE;")?;
    reset_session(state)
}

fn set_leader_mode(state: &mut WriteState, leader: bool) -> Result<()> {
    if leader && !state.leader_mode {
        state.leader_mode = true;
        if state.conn.is_autocommit() {
            state.conn.execute_batch("BEGIN IMMEDIATE;")?;
            reset_session(state)?;
        }
    } else if !leader && state.leader_mode {
        state.leader_mode = false;
        state.session = None;
        if !state.conn.is_autocommit() {
            state.conn.execute_batch("ROLLBACK;")?;
        }
    }
    Ok(())
}

fn apply_migration(
    conn: &mut Connection,
    allow_ddl: &AtomicBool,
    version: u32,
    sql: &[String],
) -> Result<()> {
    with_allow_ddl(allow_ddl, || {
        if conn.is_autocommit() {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for stmt in sql {
                tx.execute_batch(stmt)?;
            }
            set_schema_version(&tx, version)?;
            tx.commit()?;
        } else {
            for stmt in sql {
                conn.execute_batch(stmt)?;
            }
            set_schema_version(conn, version)?;
        }
        assert_all_tables_have_primary_key(conn)?;
        Ok(())
    })
}

fn vacuum_into(state: &mut WriteState, dest: &Path) -> Result<()> {
    let allow_ddl = Arc::clone(&state.allow_ddl);
    with_allow_ddl(&allow_ddl, || {
        if dest.exists() {
            std::fs::remove_file(dest)?;
        }
        let dest_str = dest
            .to_str()
            .ok_or_else(|| anyhow!("snapshot path is not UTF-8"))?;
        if !state.conn.is_autocommit() {
            state.conn.execute_batch("COMMIT;")?;
        }
        state
            .conn
            .execute_batch(&format!("VACUUM INTO '{dest_str}';"))?;
        if state.leader_mode {
            state.conn.execute_batch("BEGIN IMMEDIATE;")?;
            reset_session(state)?;
        }
        Ok(())
    })
}

/// Shared SQLite store; write operations run on a dedicated thread.
#[derive(Clone)]
pub struct SqliteStore {
    path: PathBuf,
    writer: Arc<WriterHandle>,
}

impl SqliteStore {
    pub fn open(path: impl Into<PathBuf>, leader_mode: bool) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if !path.exists() {
            drop(open_connection(&path, false)?);
        }
        let writer = Arc::new(spawn_writer(path.clone(), leader_mode));
        let store = Self { path, writer };
        store.set_leader_mode(leader_mode)?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn init_meta(&self) -> Result<u32> {
        self.writer.ensure_meta()
    }

    pub fn with_read<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let conn = open_connection(&self.path, true)?;
        f(&conn)
    }

    pub fn with_write<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&Connection) -> Result<()> + Send + 'static,
    {
        self.with_write_value(f)
    }

    pub fn with_write_value<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        self.writer.with_write_value(f)
    }

    pub fn capture_changeset(&self) -> Result<Vec<u8>> {
        self.writer.capture_changeset()
    }

    pub fn commit_epoch(&self) -> Result<()> {
        self.writer.commit_epoch()
    }

    pub fn rollback_epoch(&self) -> Result<()> {
        self.writer.rollback_epoch()
    }

    pub fn set_leader_mode(&self, leader: bool) -> Result<()> {
        self.writer.set_leader_mode(leader)
    }

    pub fn apply_changeset(&self, changeset: &[u8]) -> Result<()> {
        self.writer.apply_changeset(changeset.to_vec())
    }

    pub fn apply_migration(&self, version: u32, sql: &[String]) -> Result<()> {
        self.writer.apply_migration(version, sql.to_vec())
    }

    pub fn vacuum_into(&self, dest: &Path) -> Result<()> {
        self.writer.vacuum_into(dest.to_path_buf())
    }

    pub fn refresh_read_pool(&self) -> Result<()> {
        Ok(())
    }

    pub fn schema_version(&self) -> Result<u32> {
        self.with_read(|conn| ensure_meta(conn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Migration;
    use tempfile::TempDir;

    #[test]
    fn migration_allows_ddl() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("test.db");
        let store = SqliteStore::open(&path, true)?;
        store.init_meta()?;
        let migration = Migration {
            version: 1,
            sql: vec!["CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );"
            .into()],
        };
        store.apply_migration(migration.version, &migration.sql)?;
        store
            .with_write(|conn| {
                conn.execute_batch("CREATE TABLE rogue (id INTEGER PRIMARY KEY)")?;
                Ok(())
            })
            .expect_err("ad-hoc DDL should be rejected");
        Ok(())
    }
}
