use std::{future::IntoFuture, net::IpAddr, sync::OnceLock};

use anyhow::{anyhow, bail, Result};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json, Router,
};
use futures::FutureExt;
use itertools::Itertools;
use pnet::ipnetwork::IpNetwork;
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tracing::{debug, error};

use crate::Node;

#[derive(Debug)]
pub struct Server {
    notify_shutdown: oneshot::Sender<()>,
    serve: JoinHandle<Result<(), std::io::Error>>,
}

impl Server {
    pub async fn bind(node: &mut Node) -> Result<TcpListener> {
        let listener = tokio::net::TcpListener::bind(node.address).await?;
        let mut address = listener.local_addr()?;

        // Fix the node address with the correct server address to advertise
        if address.ip().is_unspecified() {
            if let Some(ip) = best_ip_address() {
                address.set_ip(ip);
            } else {
                bail!("Cannot advertise {} as IP address", address.ip());
            }
        }
        node.address = address;

        Ok(listener)
    }

    pub async fn start(listener: TcpListener, router: Router) -> Result<Self> {
        let address = listener.local_addr()?;
        let (notify_shutdown, on_shutdow) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            axum::serve(listener, router)
                .tcp_nodelay(true)
                .with_graceful_shutdown(on_shutdow.map(|_| ()))
                .into_future(),
        );

        debug!(?address, "Started server");

        Ok(Self {
            notify_shutdown,
            serve,
        })
    }

    pub async fn shutdown(self) -> Result<()> {
        debug!("Shutting down server...");
        self.notify_shutdown
            .send(())
            .map_err(|_| anyhow!("Cannot notify graceful shutdown"))?;
        self.serve.await??;
        Ok(())
    }
}

pub type JsonResponse<A> = Result<Json<A>, ServerError>;

pub struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        error!(err = ?self.0, "Internal server error");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

impl<E> From<E> for ServerError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

// Find the best non-local IP address
fn best_ip_address() -> Option<IpAddr> {
    fn is_forwardable_ip(ip_addr: &IpAddr) -> bool {
        static NON_FORWARDABLE_NETWORKS: OnceLock<Vec<IpNetwork>> = OnceLock::new();
        NON_FORWARDABLE_NETWORKS
            .get_or_init(|| {
                [
                    "0.0.0.0/8",
                    "127.0.0.0/8",
                    "169.254.0.0/16",
                    "192.0.0.0/24",
                    "192.0.2.0/24",
                    "198.51.100.0/24",
                    "2001:10::/28",
                    "2001:db8::/32",
                    "203.0.113.0/24",
                    "240.0.0.0/4",
                    "255.255.255.255/32",
                    "::/128",
                    "::1/128",
                    "::ffff:0:0/96",
                    "fe80::/10",
                ]
                .iter()
                .map(|network| network.parse().unwrap())
                .collect()
            })
            .iter()
            .all(|network| !network.contains(*ip_addr))
    }

    fn is_private_ip(ip_addr: &IpAddr) -> bool {
        static PRIVATE_NETWORKS: OnceLock<Vec<IpNetwork>> = OnceLock::new();
        PRIVATE_NETWORKS
            .get_or_init(|| {
                ["192.168.0.0/16", "172.16.0.0/12", "10.0.0.0/8", "fc00::/7"]
                    .iter()
                    .map(|network| network.parse().unwrap())
                    .collect()
            })
            .iter()
            .any(|network| network.contains(*ip_addr))
    }

    pnet::datalink::interfaces()
        .iter()
        .filter(|interface| interface.is_up())
        .flat_map(|interface| {
            interface
                .ips
                .iter()
                .filter(|ip_net| is_forwardable_ip(&ip_net.ip()) && is_private_ip(&ip_net.ip()))
                .map(move |ip_net| (interface, ip_net))
        })
        .sorted_by_key(|(interface, ip_net)| {
            (
                ip_net.is_ipv6(),
                interface.is_dormant(),
                std::cmp::Reverse(ip_net.prefix()),
            )
        })
        .next()
        .map(|(_, ip_net)| ip_net.ip())
}
