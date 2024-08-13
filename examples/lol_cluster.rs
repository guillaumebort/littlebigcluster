use std::{net::SocketAddr, time::Duration};

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::{select, FutureExt};
use littlebigcluster::{Config, Follower, Leader, LittleBigCluster, StandByLeader};
use object_store::local::LocalFileSystem;
use tracing::{debug, error, info, warn, Level};
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
        Command::Leader => leader::join(cluster, args.az, args.address).await,
        Command::Follower => follower::join(cluster, args.az, args.address).await,
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
            gossip_interval: Duration::from_secs(1),
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

    pub async fn join(cluster: LittleBigCluster, az: String, address: SocketAddr) -> Result<()> {
        info!("Joining cluster as follower...");
        let follower = cluster
            .join_as_follower(az, address, Router::new(), "follower")
            .await?;
        info!("Joined cluster!");
        info!("Listening on http://{}", follower.address());

        let mut watch_leader = follower.watch_leader().clone();
        let mut watch_value = follower
            .watch(|_epoc, db, _object_store| {
                async move {
                    let value: Option<String> = sqlx::query_scalar(
                        r#"SELECT value FROM all_values ORDER BY id DESC LIMIT 1"#,
                    )
                    .fetch_optional(db)
                    .await?;
                    debug!(?value);
                    Ok(value)
                }
                .boxed()
            })
            .await?;

        let leader = watch_leader.borrow_and_update().clone();
        let value: Option<String> = watch_value.borrow_and_update().clone();
        info!(?leader, ?value);

        loop {
            select! {
                _ = watch_leader.changed().fuse() => {
                    let leader = watch_leader.borrow_and_update().clone();
                    let value: Option<String> = watch_value.borrow().clone();
                    info!(?leader, ?value, "Leader changed!");
                }

                _ = watch_value.changed().fuse() => {
                    let value: Option<String> = watch_value.borrow_and_update().clone();
                    info!(?value, "Value changed!");
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
    use std::collections::HashMap;

    use axum::{
        extract::{Query, State},
        response::{IntoResponse, Response},
        routing::{get, post},
        Json,
    };
    use hyper::StatusCode;
    use littlebigcluster::LeaderState;
    use serde_json::json;

    use super::*;

    pub async fn join(cluster: LittleBigCluster, az: String, address: SocketAddr) -> Result<()> {
        let router = axum::Router::new()
            .route("/value/set", post(set_value))
            .route("/value/get", get(get_value));

        let standby = cluster.join_as_leader(az, address, router).await?;
        info!("Joined cluster! Waiting for leadership...");

        let leader = standby.wait_for_leadership().await?;
        info!("We are the new leader!");

        // ensure that the leader schema is created (a real application would use sqlx migrations)
        let _ = leader
            .execute_batch(sqlx::raw_sql(
                r#"
                CREATE TABLE IF NOT EXISTS all_values (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    value TEXT NOT NULL
                )
                "#,
            ))
            .await?;

        info!("Listening on http://{}", leader.address());

        select! {
            _ = wait_exit_signal().fuse() => {
                info!("Shutting down...");
            }
            _ = leader.lost_leadership().fuse() => {
                warn!("Lost leadership!?");
            }
        }

        leader.shutdown().await?;
        info!("Exited gracefully");

        Ok(())
    }

    async fn set_value(
        State(state): State<LeaderState>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Json<serde_json::Value>, LolServerError> {
        // retrieve the value parameter from the query string
        let value = params
            .get("value")
            .ok_or_else(|| anyhow::anyhow!("please provide a value parameter!"))?;

        // insert the value into the database
        let ack = state
            .execute(sqlx::query("INSERT INTO all_values (value) VALUES (?)").bind(value))
            .await?;

        info!(?value, "Value changed!");

        // wait for the value to be safely replicated
        ack.await?;

        Ok(Json(json!({
            "value": value,
        })))
    }

    async fn get_value(
        State(state): State<LeaderState>,
    ) -> Result<Json<serde_json::Value>, LolServerError> {
        // fetch the (last) value from the database
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM all_values ORDER BY id DESC LIMIT 1")
                .fetch_optional(state.db())
                .await?;

        Ok(Json(json!({
            "value": value,
        })))
    }

    struct LolServerError(anyhow::Error);

    impl IntoResponse for LolServerError {
        fn into_response(self) -> Response {
            error!(err = ?self.0, "Internal server error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": self.0.to_string() })),
            )
                .into_response()
        }
    }

    impl<E> From<E> for LolServerError
    where
        E: Into<anyhow::Error>,
    {
        fn from(err: E) -> Self {
            Self(err.into())
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
