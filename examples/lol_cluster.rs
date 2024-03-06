use std::net::SocketAddr;

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::{select, FutureExt};
use litecluster::Follower;
use litecluster::{Leader, LiteCluster};
use object_store::local::LocalFileSystem;
use tracing::{error, info, warn, Level};
use tracing_subscriber::fmt;

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
        0 => tracing_subscriber::fmt()
            .event_format(fmt::format().compact())
            .init(),
        1 => tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .event_format(fmt::format())
            .init(),
        _ => tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .event_format(fmt::format().pretty())
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

async fn leader(cluster: LiteCluster, az: String, address: SocketAddr) -> Result<()> {
    let router = axum::Router::new().route("/", axum::routing::get(|| async { "LOL" }));
    info!("Waiting to join cluster as leader...");
    let node = cluster.join_as_leader(az, address, router).await?;
    info!("Joined cluster! We are the new leader");
    info!("Listening on http://{}", node.address());

    select! {
        _ = wait_exit_signal().fuse() => {
            info!("Exiting...");
        }
        _ = node.wait_lost_leadership().fuse() => {
            warn!("Lost leadership!?");
        }
    }

    node.shutdown().await?;
    info!("Exited gracefully");

    Ok(())
}

async fn follower(cluster: LiteCluster, az: String) -> Result<()> {
    info!("Joining cluster as follower...");
    let node = cluster.join_as_follower(az).await?;
    info!("Joined cluster!");

    let mut watch_leader = node.watch_leader().clone();
    let leader = watch_leader.borrow_and_update().clone();
    info!(?leader, "Leader is");

    loop {
        select! {
            _ = watch_leader.changed().fuse() => {
                let leader = watch_leader.borrow_and_update().clone();
                info!(?leader, "Leader changed");
            }
            _ = wait_exit_signal().fuse() => {
                info!("Exiting...");
                break;
            }
        }
    }

    Ok(())
}

fn open_cluster(cluster_id: &str, path: &std::path::Path) -> Result<LiteCluster> {
    LiteCluster::at(cluster_id, LocalFileSystem::new_with_prefix(path)?)
}

async fn init_cluster(cluster: &LiteCluster) -> Result<()> {
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
