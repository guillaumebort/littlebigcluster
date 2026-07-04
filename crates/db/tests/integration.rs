use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lbc_db::{Config, Db, Migration, TickResult, WriteError};
use object_store::local::LocalFileSystem;
use rusqlite::params;
use tempfile::TempDir;
use tokio::time::sleep;

fn kv_migration() -> Migration {
    Migration {
        version: 1,
        sql: vec!["CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );"
        .into()],
    }
}

fn store(prefix: &PathBuf) -> Arc<dyn object_store::ObjectStore> {
    std::fs::create_dir_all(prefix).unwrap();
    Arc::new(LocalFileSystem::new_with_prefix(prefix).unwrap())
}

#[tokio::test]
async fn leader_follower_replicates_changeset() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let object_store = store(&root.path().join("store"));
    let migrations = vec![kv_migration()];

    let leader_cfg = Config::new(
        object_store.clone(),
        "test/",
        root.path().join("leader"),
        "node-a",
        "127.0.0.1:5001",
    )
    .with_migrations(migrations.clone())
    .with_snapshot_every_epochs(100);

    let mut leader = Db::join(leader_cfg).await?;
    assert!(leader.is_leader());

    leader.write(|conn| {
        conn.execute(
            "INSERT INTO kv(key, value) VALUES (?1, ?2)",
            params!["hello", "world"],
        )?;
        Ok(())
    })?;

    for _ in 0..5 {
        if matches!(leader.tick().await?, TickResult::Replicated { .. }) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let follower_cfg = Config::new(
        object_store,
        "test/",
        root.path().join("follower"),
        "node-b",
        "127.0.0.1:5002",
    )
    .with_migrations(migrations)
    .with_snapshot_every_epochs(100);

    let mut follower = Db::join(follower_cfg).await?;
    for _ in 0..10 {
        if matches!(follower.tick().await?, TickResult::Synced { .. }) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let value: String = follower.store().with_read(|conn| {
        Ok(
            conn.query_row("SELECT value FROM kv WHERE key = 'hello'", [], |row| {
                row.get(0)
            })?,
        )
    })?;

    assert_eq!(value, "world");
    Ok(())
}

#[tokio::test]
async fn durable_write_commits_on_epoch() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let object_store = store(&root.path().join("store"));

    let mut leader = Db::join(
        Config::new(
            object_store,
            "durable/",
            root.path().join("leader"),
            "node-a",
            "127.0.0.1:5001",
        )
        .with_migrations(vec![kv_migration()]),
    )
    .await?;

    let durable = leader.write(|conn| {
        conn.execute(
            "INSERT INTO kv(key, value) VALUES (?1, ?2)",
            params!["k", "v"],
        )?;
        Ok(42)
    })?;

    let tick = tokio::spawn(async move {
        leader.tick().await.unwrap();
        leader
    });

    assert_eq!(durable.committed().await?, 42);
    let _leader = tick.await?;
    Ok(())
}

#[tokio::test]
async fn migration_epoch_replicates() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let object_store = store(&root.path().join("store"));

    let migrations = vec![
        kv_migration(),
        Migration {
            version: 2,
            sql: vec!["CREATE TABLE IF NOT EXISTS extra (
                    id INTEGER PRIMARY KEY,
                    note TEXT NOT NULL
                );"
            .into()],
        },
    ];

    let leader_cfg = Config::new(
        object_store.clone(),
        "mig/",
        root.path().join("leader"),
        "node-a",
        "127.0.0.1:5001",
    )
    .with_migrations(migrations.clone())
    .with_snapshot_every_epochs(100);

    let leader = Db::join(leader_cfg).await?;
    assert_eq!(leader.store().schema_version()?, 2);

    let follower_cfg = Config::new(
        object_store,
        "mig/",
        root.path().join("follower"),
        "node-b",
        "127.0.0.1:5002",
    )
    .with_migrations(migrations)
    .with_snapshot_every_epochs(100);

    let mut follower = Db::join(follower_cfg).await?;
    for _ in 0..20 {
        follower.tick().await?;
        if follower.store().schema_version()? == 2 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(follower.store().schema_version()?, 2);
    Ok(())
}

#[tokio::test]
async fn demoted_durable_errors() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let object_store = store(&root.path().join("store"));

    let mut leader = Db::join(
        Config::new(
            object_store,
            "demote/",
            root.path().join("leader"),
            "node-a",
            "127.0.0.1:5001",
        )
        .with_migrations(vec![kv_migration()]),
    )
    .await?;

    let durable = leader.write(|conn| {
        conn.execute(
            "INSERT INTO kv(key, value) VALUES (?1, ?2)",
            params!["k", "v"],
        )?;
        Ok(())
    })?;

    leader.step_down();
    assert!(matches!(
        durable.committed().await,
        Err(WriteError::Demoted)
    ));
    Ok(())
}

#[tokio::test]
async fn write_rejects_ad_hoc_ddl() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let object_store = store(&root.path().join("store"));

    let leader = Db::join(
        Config::new(
            object_store,
            "ddl/",
            root.path().join("leader"),
            "node-a",
            "127.0.0.1:5001",
        )
        .with_migrations(vec![kv_migration()]),
    )
    .await?;

    let err = match leader.write(|conn| {
        conn.execute_batch("CREATE TABLE rogue (id INTEGER PRIMARY KEY)")?;
        Ok(())
    }) {
        Err(err) => err,
        Ok(_) => panic!("ad-hoc DDL should be rejected"),
    };

    assert!(
        err.to_string().to_ascii_lowercase().contains("authoriz"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn join_rejects_db_newer_than_binary() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let object_store = store(&root.path().join("store"));

    let migrations = vec![
        kv_migration(),
        Migration {
            version: 2,
            sql: vec!["CREATE TABLE IF NOT EXISTS extra (
                    id INTEGER PRIMARY KEY,
                    note TEXT NOT NULL
                );"
            .into()],
        },
    ];

    let leader_cfg = Config::new(
        object_store.clone(),
        "schema/",
        root.path().join("leader"),
        "node-a",
        "127.0.0.1:5001",
    )
    .with_migrations(migrations.clone())
    .with_snapshot_every_epochs(100);

    let leader = Db::join(leader_cfg).await?;
    assert_eq!(leader.store().schema_version()?, 2);

    let follower_cfg = Config::new(
        object_store,
        "schema/",
        root.path().join("follower"),
        "node-b",
        "127.0.0.1:5002",
    )
    .with_migrations(vec![kv_migration()])
    .with_snapshot_every_epochs(100);

    let err = match Db::join(follower_cfg).await {
        Err(e) => e,
        Ok(_) => panic!("expected join to fail for newer DB schema"),
    };
    assert!(
        err.to_string().contains("newer than this binary supports"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn join_rejects_db_below_minimum() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let object_store = store(&root.path().join("store"));

    let leader_cfg = Config::new(
        object_store.clone(),
        "min/",
        root.path().join("leader"),
        "node-a",
        "127.0.0.1:5001",
    )
    .with_migrations(vec![kv_migration()])
    .with_snapshot_every_epochs(100);

    let leader = Db::join(leader_cfg).await?;
    assert_eq!(leader.store().schema_version()?, 1);

    let follower_cfg = Config::new(
        object_store,
        "min/",
        root.path().join("follower"),
        "node-b",
        "127.0.0.1:5002",
    )
    .with_migrations(vec![kv_migration()])
    .with_min_schema_version(2)
    .with_snapshot_every_epochs(100);

    let err = match Db::join(follower_cfg).await {
        Err(e) => e,
        Ok(_) => panic!("expected join to fail for newer DB schema"),
    };
    assert!(
        err.to_string().contains("below this binary's minimum"),
        "unexpected error: {err}"
    );
    Ok(())
}
