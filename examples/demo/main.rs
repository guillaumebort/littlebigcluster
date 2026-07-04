//! Multi-process cluster demo.
//!
//! ```text
//! # terminal 1 — first node (creates store + initializes DB, becomes leader)
//! cargo run --example demo -- --object-store /tmp/lbc-store --id node-a --addr 127.0.0.1:5001
//! # web UI: http://127.0.0.1:6001
//!
//! # terminal 2 — follower
//! cargo run --example demo -- --object-store /tmp/lbc-store --id node-b --addr 127.0.0.1:5002
//! # web UI: http://127.0.0.1:6002
//! ```

mod utils;
mod web;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use lbc_cluster::{Node, NodeConfig};
use lbc_db::Migration;
use object_store::local::LocalFileSystem;
use object_store::{parse_url, ObjectStore};
use tempfile::TempDir;
use tracing::info;
use url::Url;

use utils::{log_members, roster_topology, setup_tracing, wait_exit_signal};
use web::{http_addr, router, App};

const DEMO_PREFIX: &str = "demo/";

#[derive(Parser)]
#[command(version, about = "Multi-process littlebigcluster demo")]
struct Args {
    /// Object store URL or local path (`/tmp/store`, `file://…`, `s3://bucket/prefix`).
    #[arg(long)]
    object_store: String,

    #[arg(short = 'i', long = "id")]
    node_id: String,

    /// gRPC membership address.
    #[arg(short, long)]
    addr: String,

    #[arg(long, default_value = "")]
    az: String,

    /// Skip leadership election (follower-only node).
    #[arg(long)]
    follower_only: bool,

    /// Increase log verbosity: `-v` enables debug logs, `-vv` uses `RUST_LOG`.
    #[arg(
        short = 'v',
        long,
        action = clap::ArgAction::Count,
    )]
    verbose: u8,
}

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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_tracing(args.verbose);

    let local_dir = TempDir::new()?;
    let local_path = local_dir.path().to_path_buf();

    let (store, prefix) = open_object_store(&args.object_store)?;
    let store_label = format!(
        "{}/{}",
        args.object_store.trim_end_matches('/'),
        prefix.trim_end_matches('/')
    );

    info!(
        node_id = %args.node_id,
        addr = %args.addr,
        store = %store_label,
        local = %local_path.display(),
        "starting"
    );

    let mut config = NodeConfig::new(
        args.node_id.clone(),
        args.addr.clone(),
        args.az.clone(),
        store,
        local_path,
    )
    .with_migrations(vec![kv_migration()])
    .with_prefix(prefix);

    if args.follower_only {
        config = config.with_leader_eligible(false);
    }

    let (node, _task) = Node::join(config).await?;
    let _local_dir = local_dir;

    let http = http_addr(&args.addr)?;
    let listener = tokio::net::TcpListener::bind(http).await?;
    let web_addr = listener.local_addr()?;

    let app = App {
        node: node.clone(),
        node_id: args.node_id.clone(),
        last_result: Arc::new(tokio::sync::Mutex::new(None)),
    };
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router(app)).await {
            tracing::warn!(error = %e, "web server stopped");
        }
    });

    info!(url = %format!("http://{web_addr}"), "ready — open in browser, Ctrl-C to exit");

    let mut view_rx = node.subscribe_view();
    let mut members_rx = node.subscribe_members();
    let mut last_view = view_rx.borrow_and_update().clone();
    let mut last_members = members_rx.borrow_and_update().clone();
    log_members("initial", &args.node_id, &last_view, &last_members);

    loop {
        tokio::select! {
            _ = wait_exit_signal() => {
                info!("bye");
                break;
            }
            changed = view_rx.changed() => {
                if changed.is_ok() {
                    let view = view_rx.borrow_and_update().clone();
                    if view.leader_id != last_view.leader_id
                        || view.is_leader != last_view.is_leader
                    {
                        log_members("leadership changed", &args.node_id, &view, &members_rx.borrow());
                        last_view = view;
                    }
                }
            }
            changed = members_rx.changed() => {
                if changed.is_ok() {
                    let members = members_rx.borrow_and_update().clone();
                    if roster_topology(&members) != roster_topology(&last_members) {
                        log_members("members changed", &args.node_id, &view_rx.borrow(), &members);
                        last_members = members;
                    }
                }
            }
        }
    }

    Ok(())
}

fn open_object_store(spec: &str) -> Result<(Arc<dyn ObjectStore>, String)> {
    let spec = spec.trim();
    if !spec.contains("://") {
        let dir = Path::new(spec);
        std::fs::create_dir_all(dir).with_context(|| format!("create {spec}"))?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir)?);
        return Ok((store, DEMO_PREFIX.to_string()));
    }

    let url: Url = spec
        .parse()
        .with_context(|| format!("invalid object store URL: {spec}"))?;
    let (store, path) = parse_url(&url)?;
    Ok((Arc::from(store), object_store_prefix(path.as_ref())))
}

fn object_store_prefix(path: &str) -> String {
    if path.is_empty() {
        return DEMO_PREFIX.to_string();
    }
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}
