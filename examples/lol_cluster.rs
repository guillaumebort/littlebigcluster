mod utils;

use std::{net::SocketAddr, time::Duration};

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::{select, FutureExt};
use littlebigcluster::{Config, Follower, Leader, LittleBigCluster};
use object_store::local::LocalFileSystem;
use tracing::{error, info, warn};
use utils::{setup_tracing, wait_exit_signal};

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
    #[clap(default_value = "0.0.0.0:0")]
    address: SocketAddr,

    #[arg(long)]
    #[clap(default_value = "default")]
    az: String,

    #[arg(short, long)]
    roles: Vec<String>,

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

    setup_tracing(args.verbose);

    let cluster = match open_cluster(&args.cluster_id, &args.path) {
        Ok(cluster) => cluster,
        Err(err) => {
            error!(?err);
            std::process::exit(1);
        }
    };

    let command_result = match args.command {
        Command::Init => init_cluster(&cluster).await,
        Command::Leader => leader::join(cluster, args.az, args.address, args.roles).await,
        Command::Follower => follower::join(cluster, args.az, args.address, args.roles).await,
    };

    if let Err(err) = command_result {
        error!(?err);
        std::process::exit(1);
    }
}

fn open_cluster(cluster_id: &str, path: &std::path::Path) -> Result<LittleBigCluster> {
    LittleBigCluster::at(
        cluster_id,
        LocalFileSystem::new_with_prefix(path)?,
        Config {
            epoch_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(30),
            snapshots_to_keep: 5,
            session_timeout: Duration::from_secs(20),
        },
    )
}

async fn init_cluster(cluster: &LittleBigCluster) -> Result<()> {
    info!("Initializing cluster...");
    cluster.init().await?;
    info!("Cluster initialized!");
    Ok(())
}

mod follower {
    use axum::Router;
    use futures::FutureExt;
    use tokio::select;

    use super::*;

    pub async fn join(
        cluster: LittleBigCluster,
        az: String,
        address: SocketAddr,
        roles: Vec<String>,
    ) -> Result<()> {
        info!("Joining cluster...");
        let follower = cluster
            .join_as_follower(
                az,
                address,
                roles
                    .into_iter()
                    .map(|role| (role, Router::new()))
                    .collect(),
            )
            .await?;
        info!("Joined cluster! Listening on http://{}", follower.address());

        let mut watch_leader = follower.watch_leader().clone();
        let mut watch_members = follower.watch_members().clone();

        utils::info_members(
            "Intial members",
            follower.node(),
            &*watch_members.borrow_and_update(),
            &*watch_leader.borrow_and_update(),
        );

        loop {
            select! {

                _ = watch_leader.changed().fuse() => {
                    utils::info_members("Leader changed", follower.node(), &*watch_members.borrow_and_update(), &*watch_leader.borrow_and_update());
                }

                _ = watch_members.changed().fuse() => {
                    utils::info_members("Members changed", follower.node(), &*watch_members.borrow_and_update(), &*watch_leader.borrow_and_update());
                }

                _ = wait_exit_signal().fuse() => {
                    info!("Exiting...");
                    break;
                }
            }
        }

        follower.shutdown().await?;
        info!("Exited gracefully");

        Ok(())
    }
}

mod leader {

    use axum::{routing::get, Router};

    use super::*;

    pub async fn join(
        cluster: LittleBigCluster,
        az: String,
        address: SocketAddr,
        roles: Vec<String>,
    ) -> Result<()> {
        let router =
            axum::Router::new().route("/hello", get(|| async { "Hello from the leader!" }));

        let leader = cluster
            .join_as_leader(
                az,
                address,
                router,
                roles
                    .into_iter()
                    .map(|role| (role, Router::new()))
                    .collect(),
            )
            .await?;
        info!("Joining cluster...");
        info!("Joined cluster! Listening on http://{}", leader.address());

        let mut watch_leader = leader.watch_leader().clone();
        let mut watch_members = leader.watch_members().clone();

        utils::info_members(
            "Intial members",
            leader.node(),
            &*watch_members.borrow_and_update(),
            &*watch_leader.borrow_and_update(),
        );

        info!("Waiting for leadership...");

        loop {
            select! {

                _ = watch_leader.changed().fuse() => {
                    utils::info_members("Leader changed", leader.node(), &*watch_members.borrow_and_update(), &*watch_leader.borrow_and_update());
                }

                _ = watch_members.changed().fuse() => {
                    utils::info_members("Members changed", leader.node(), &*watch_members.borrow_and_update(), &*watch_leader.borrow_and_update());
                }

                _ = leader.wait_for_leadership().fuse() => {
                    break;
                }

                _ = wait_exit_signal().fuse() => {
                    info!("Shutting down...");
                    leader.shutdown().await?;
                    info!("Exited gracefully");
                    return Ok(());
                }
            }
        }

        info!("We are the new leader!");

        utils::info_members(
            "Leader changed",
            leader.node(),
            &*watch_members.borrow_and_update(),
            &Some(leader.node().clone()),
        );

        loop {
            select! {
                _ = watch_members.changed().fuse() => {
                    utils::info_members("Members changed",leader.node(), &*watch_members.borrow_and_update(), &Some(leader.node().clone()));
                }

                _ = wait_exit_signal().fuse() => {
                    info!("Shutting down...");
                    leader.shutdown().await?;
                    info!("Exited gracefully");
                    return Ok(());
                }

                _ = leader.wait_for_lost_leadership().fuse() => {
                    warn!("Lost leadership!?");
                    return Ok(());
                }
            }
        }
    }
}
