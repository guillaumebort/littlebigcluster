use anyhow::Result;
use ascii_table::AsciiTable;
use colored::Colorize;
use littlebigcluster::{Members, Node};
use tokio::select;
use tracing::{info, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

pub fn info_members(msg: &str, this: &Node, members: &Members, leader: &Option<Node>) {
    let members = members.to_vec();
    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(200);
    ascii_table.column(0).set_header("");
    ascii_table
        .column(1)
        .set_header(format!("UUID({})", members.len()));
    ascii_table.column(2).set_header("Address");
    ascii_table.column(3).set_header("AZ");
    ascii_table.column(4).set_header("Roles");
    let mut data = Vec::with_capacity(members.len());
    for member in members {
        let mut row = vec![
            (if member.node.uuid == this.uuid {
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
    info!("{msg}:{}\n\n{}", "\x1B[0m", members_table);
}

pub async fn wait_exit_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sig_term = signal(SignalKind::terminate())?;

    select! {
        _ = sig_term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }

    Ok(())
}

pub fn setup_tracing(verbosity: u8) {
    match verbosity {
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
}
