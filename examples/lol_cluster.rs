use std::{net::SocketAddr, time::Duration};

use anyhow::Result;
use ascii_table::AsciiTable;
use clap::{Parser, Subcommand};
use colored::Colorize;
use futures::{select, FutureExt};
use littlebigcluster::{Config, Follower, Leader, LittleBigCluster, Members, Node, StandByLeader};
use object_store::local::LocalFileSystem;
use tracing::{error, info, warn, Level};
use tracing_subscriber::{
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};
use uuid::Uuid;

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
                                    || metadata.target().starts_with("littlebigcluster") =>
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
                            Level::WARN | Level::ERROR | Level::INFO | Level::DEBUG => true,
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
    cluster.init().await
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
        info!("Joining cluster as follower...");
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
        info!("Joined cluster!");
        info!("Listening on http://{}", follower.address());

        let mut watch_leader = follower.watch_leader().clone();
        let mut watch_members = follower.watch_members().clone();

        print_members(
            follower.node().uuid,
            &*watch_members.borrow_and_update(),
            &*watch_leader.borrow_and_update(),
        );

        loop {
            select! {

                _ = watch_leader.changed().fuse() => {
                    print_members(follower.node().uuid, &*watch_members.borrow_and_update(), &*watch_leader.borrow_and_update());
                }

                _ = watch_members.changed().fuse() => {
                    print_members(follower.node().uuid, &*watch_members.borrow_and_update(), &*watch_leader.borrow_and_update());
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

        let standby = cluster
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
        info!("Waiting for leadership...");

        let leader = standby.wait_for_leadership().await?;
        info!("We are the new leader!");
        info!("Listening on http://{}", leader.address());

        let mut watch_members = leader.watch_members().clone();

        print_members(
            leader.node().uuid,
            &*watch_members.borrow_and_update(),
            &Some(leader.node().clone()),
        );

        loop {
            select! {
                _ = watch_members.changed().fuse() => {
                    print_members(leader.node().uuid, &*watch_members.borrow_and_update(), &Some(leader.node().clone()));
                }

                _ = wait_exit_signal().fuse() => {
                    info!("Shutting down...");
                    leader.shutdown().await?;
                    info!("Exited gracefully");
                    return Ok(());
                }

                _ = leader.lost_leadership().fuse() => {
                    warn!("Lost leadership!?");
                    return Ok(());
                }
            }
        }
    }
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

fn print_members(this: Uuid, members: &Members, leader: &Option<Node>) {
    let members = members.to_vec();
    let members_len = members.len();
    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(200);
    ascii_table.column(0).set_header("");
    ascii_table.column(1).set_header("UUID");
    ascii_table.column(2).set_header("Address");
    ascii_table.column(3).set_header("AZ");
    ascii_table.column(4).set_header("Roles");
    let mut data = Vec::with_capacity(members.len());
    for member in members {
        let mut row = vec![
            (if member.node.uuid == this {
                "*".to_string()
            } else {
                "".to_string()
            })
            .normal(),
            member.node.uuid.to_string().normal(),
            member.node.address.to_string().normal(),
            member.node.az.to_string().normal(),
            member.roles.join(", ").normal(),
        ];
        if member.node.uuid == leader.as_ref().map(|node| node.uuid).unwrap_or_default() {
            for s in row.iter_mut() {
                *s = s.clone().bold();
            }
        }
        data.push(row);
    }
    let members_table = ascii_table.format(data);
    info!(
        "Cluster Memberhip ({}):{}\n\n{}",
        "\x1B[0m", members_len, members_table
    );
}
