use std::{collections::HashMap, net::SocketAddr};

use anyhow::Result;
use axum::{
    body::Body,
    extract::{Query, Request},
    Json,
};
use clap::{Parser, Subcommand};
use futures::{select, FutureExt};
use littlebigcluster::{Follower, JsonResponse, Leader, LittleBigCluster, StandByLeader};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tracing::{error, info, warn, Level};
use tracing_subscriber::{
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    cluster_id: String,

    #[arg(short, long)]
    path: std::path::PathBuf,

    #[arg(
        long,
        short = 'v',
        action = clap::ArgAction::Count,
        global = true,
    )]
    verbose: u8,

    #[arg(short, long)]
    #[clap(default_value = "127.0.0.1:0")]
    address: SocketAddr,

    #[arg(long)]
    #[clap(default_value = "default")]
    az: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq, Eq)]
enum Command {
    /// Initialize a new cluster
    Init,
    /// Join the cluster as a leader
    Leader,
    /// Join the cluster as a follower
    Follower,
}

#[tokio::main]
pub async fn main() {
    let args = Args::parse();

    match args.verbose {
        0 => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(tracing_subscriber::fmt::format().compact())
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        |metadata| match *metadata.level() {
                            Level::WARN | Level::ERROR => true,
                            Level::INFO if metadata.target().starts_with("lol_cluster") => true,
                            _ => false,
                        },
                    )),
            )
            .init(),
        1 => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(tracing_subscriber::fmt::format())
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        |metadata| match *metadata.level() {
                            Level::WARN | Level::ERROR => true,
                            Level::INFO | Level::DEBUG
                                if metadata.target().starts_with("lol_cluster")
                                    || metadata.target().starts_with("littlecluster") =>
                            {
                                true
                            }
                            _ => false,
                        },
                    )),
            )
            .init(),
        2 => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(tracing_subscriber::fmt::format().pretty())
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        |metadata| match *metadata.level() {
                            Level::WARN | Level::ERROR | Level::INFO => true,
                            Level::DEBUG | Level::TRACE
                                if metadata.target().starts_with("lol_cluster")
                                    || metadata.target().starts_with("littlecluster") =>
                            {
                                true
                            }
                            _ => false,
                        },
                    )),
            )
            .init(),
        _ => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(tracing_subscriber::fmt::format().pretty()),
            )
            .init(),
    };

    let cluster = match open_cluster(&args.cluster_id, &args.path) {
        Ok(cluster) => cluster,
        Err(err) => {
            error!(?err);
            std::process::exit(1);
        }
    };

    let command_result = match args.command {
        Command::Init => init_cluster(&cluster).await,
        Command::Leader => leader(cluster, args.az, args.address).await,
        Command::Follower => follower(cluster, args.az).await,
    };

    if let Err(err) = command_result {
        error!(?err);
        std::process::exit(1);
    }
}

async fn leader(cluster: LittleBigCluster, az: String, address: SocketAddr) -> Result<()> {
    async fn hello(Query(params): Query<HashMap<String, String>>) -> JsonResponse {
        let name = params
            .get("name")
            .ok_or_else(|| anyhow::anyhow!("name is required"))?;
        let count = params
            .get("count")
            .ok_or_else(|| anyhow::anyhow!("count is required"))?;
        info!(?name, ?count, "Received Hello request");
        Ok(Json(json!({
            "message": format!("Hello, {}!", name),
        })))
    }

    let router = axum::Router::new().route("/hello", axum::routing::get(hello));
    info!("Joining cluster as leader...");
    let node = cluster.join_as_leader(az, address, router).await?;
    info!("Joined cluster! Waiting for leadership...");
    let node = node.wait_for_leadership().await?;
    info!("We are the new leader!");
    info!("Listening on http://{}", node.address());

    select! {
        _ = wait_exit_signal().fuse() => {
            info!("Exiting...");
        }
        _ = node.lost_leadership().fuse() => {
            warn!("Lost leadership!?");
        }
    }

    node.shutdown().await?;
    info!("Exited gracefully");

    Ok(())
}

async fn follower(cluster: LittleBigCluster, az: String) -> Result<()> {
    info!("Joining cluster as follower...");
    let node = cluster.join_as_follower(az).await?;
    info!("Joined cluster!");

    let mut watch_leader = node.watch_leader().clone();
    let leader = watch_leader.borrow_and_update().clone();
    info!(?leader, "Leader is");

    let mut count = 0;
    loop {
        select! {
            _ = watch_leader.changed().fuse() => {
                let leader = watch_leader.borrow_and_update().clone();
                info!(?leader, "Leader changed");
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)).fuse() => {
                count += 1;
                let req = Request::builder()
                    .method("GET")
                    .uri(format!("/hello?name={}&count={}", node.uuid().to_string(), count))
                    .header("X-Custom-Foo", "Bar")
                    .body(Body::empty())?;
                match node.leader_client().request(req).await {
                    Ok(res) => info!(status = ?res.status(), count, "Ok"),
                    Err(err) => error!(?err, count, "Error contacting leader"),
                }
            }

            _ = wait_exit_signal().fuse() => {
                info!("Exiting...");
                break;
            }
        }
    }

    Ok(())
}

fn open_cluster(cluster_id: &str, path: &std::path::Path) -> Result<LittleBigCluster> {
    LittleBigCluster::at(cluster_id, LocalFileSystem::new_with_prefix(path)?)
}

async fn init_cluster(cluster: &LittleBigCluster) -> Result<()> {
    cluster.init().await
}

async fn wait_exit_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sig_term = signal(SignalKind::terminate())?;

    select! {
        _ = sig_term.recv().fuse() => {}
        _ = tokio::signal::ctrl_c().fuse() => {}
    }

    Ok(())
}
