use std::sync::Arc;

use anyhow::Result;
use lbc_cluster::{ClusterView, Member};
use tracing::info;

pub fn setup_tracing(verbose: u8) {
    use tracing_subscriber::fmt::time::ChronoLocal;
    use tracing_subscriber::EnvFilter;

    let filter = match verbose {
        0 => EnvFilter::new("warn,demo=info,lbc_db=info,lbc_cluster=info"),
        1 => EnvFilter::new("warn,demo=info,lbc_db=debug,lbc_cluster=debug"),
        _ => EnvFilter::from_default_env(),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_timer(ChronoLocal::new("%H:%M:%S%.3f".to_owned()))
        .with_target(false)
        .compact()
        .init();
}

pub fn log_members(msg: &str, self_id: &str, view: &ClusterView, members: &Arc<Vec<Member>>) {
    let role = if view.is_leader { "leader" } else { "follower" };

    if members.is_empty() {
        info!("{msg}: {role} @ epoch {} — roster empty", view.epoch);
        return;
    }

    // Keep the whole roster on a single line: a multi-line event leaves the
    // continuation lines without a timestamp/level prefix, which looks broken.
    let count = members.len();
    let noun = if count == 1 { "node" } else { "nodes" };
    let roster = members
        .iter()
        .map(|m| {
            let leader = if m.node_id == view.leader_id {
                " ★"
            } else {
                ""
            };
            let me = if m.node_id == self_id { " (self)" } else { "" };
            format!("{} {} {}{leader}{me}", m.node_id, m.addr, m.az)
        })
        .collect::<Vec<_>>()
        .join(", ");

    info!(
        "{msg}: {role} @ epoch {} ({count} {noun}): {roster}",
        view.epoch
    );
}

/// Roster identity, ignoring per-member epoch (which bumps every tick). Used to
/// decide whether a membership change is worth logging.
pub fn roster_topology(members: &[Member]) -> Vec<(String, String, String)> {
    members
        .iter()
        .map(|m| (m.node_id.clone(), m.addr.clone(), m.az.clone()))
        .collect()
}

pub async fn wait_exit_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
    }
    Ok(())
}
