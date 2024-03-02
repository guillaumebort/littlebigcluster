use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::{select, FutureExt};
use litecluster::{Leader, LiteCluster};
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use tracing::{error, info, Level};
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
        2 => tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .event_format(fmt::format().pretty())
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
        Command::Leader => leader(cluster).await,
        Command::Follower => follower(cluster).await,
    };

    if let Err(err) = command_result {
        error!(?err);
        std::process::exit(1);
    }
}

async fn leader(cluster: LiteCluster) -> Result<()> {
    let node = cluster.join_as_leader().await?;
    info!("Joined cluster");

    use tokio::signal::unix::{signal, SignalKind};
    let mut sig_term = signal(SignalKind::terminate())?;

    select! {
        _ = sig_term.recv().fuse() => {}
        _ = tokio::signal::ctrl_c().fuse() => {}
    }

    node.shutdown().await?;

    Ok(())
}

async fn follower(cluster: LiteCluster) -> Result<()> {
    let node = cluster.join_as_follower().await?;
    info!("Joined cluster");

    Ok(())
}

fn open_cluster(cluster_id: &str, path: &std::path::Path) -> Result<LiteCluster> {
    LiteCluster::at(cluster_id, LocalFileSystem::new_with_prefix(path)?)
}

async fn init_cluster(cluster: &LiteCluster) -> Result<()> {
    cluster.init().await
}
