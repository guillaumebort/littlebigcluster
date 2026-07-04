//! Node coordinator: owns the replicated DB tick loop and publishes cluster view.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use lbc_db::{Config as DbConfig, Db, Migration, TickResult, DEFAULT_STALE_THRESHOLD_EPOCHS};
use object_store::ObjectStore;
use tokio::sync::{watch, Mutex};
use tracing::{debug, warn};

use crate::membership::{Client, Service};
use crate::{Member, Registry};

const DEFAULT_PREFIX: &str = "cluster/";

/// Live view of DB leadership for membership and clients.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterView {
    pub is_leader: bool,
    pub leader_id: String,
    pub leader_addr: String,
    pub epoch: u64,
    pub schema_version: u32,
}

/// Configuration for joining a cluster node.
pub struct NodeConfig {
    pub node_id: String,
    pub node_addr: String,
    pub az: String,
    pub capabilities: Vec<String>,
    pub object_store: Arc<dyn ObjectStore>,
    pub local_root: PathBuf,
    pub migrations: Vec<Migration>,
    pub min_schema_version: u32,
    pub leader_eligible: bool,
    pub epoch_interval: Duration,
    pub snapshot_every_epochs: u64,
    pub prefix: String,
}

impl NodeConfig {
    pub fn new(
        node_id: impl Into<String>,
        node_addr: impl Into<String>,
        az: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        local_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            node_addr: node_addr.into(),
            az: az.into(),
            capabilities: Vec::new(),
            object_store,
            local_root: local_root.into(),
            migrations: Vec::new(),
            min_schema_version: 0,
            leader_eligible: true,
            epoch_interval: Duration::from_secs(1),
            snapshot_every_epochs: 300,
            prefix: DEFAULT_PREFIX.to_string(),
        }
    }

    pub fn with_migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.migrations = migrations;
        self
    }

    pub fn with_min_schema_version(mut self, min: u32) -> Self {
        self.min_schema_version = min;
        self
    }

    pub fn with_leader_eligible(mut self, eligible: bool) -> Self {
        self.leader_eligible = eligible;
        self
    }

    pub fn with_capabilities(mut self, capabilities: Vec<String>) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn stale_threshold(&self) -> Duration {
        self.epoch_interval * DEFAULT_STALE_THRESHOLD_EPOCHS as u32
    }
}

/// Handle to a running cluster node.
#[derive(Clone)]
pub struct NodeHandle {
    db: Arc<Mutex<Db>>,
    view_tx: watch::Sender<ClusterView>,
    registry: Registry,
    members_tx: watch::Sender<Arc<Vec<Member>>>,
}

impl NodeHandle {
    pub fn db(&self) -> Arc<Mutex<Db>> {
        self.db.clone()
    }

    pub fn subscribe_view(&self) -> watch::Receiver<ClusterView> {
        self.view_tx.subscribe()
    }

    pub fn subscribe_members(&self) -> watch::Receiver<Arc<Vec<Member>>> {
        self.members_tx.subscribe()
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn membership_service(&self) -> Service {
        Service::new(self.registry.clone(), self.view_tx.subscribe())
    }
}

/// Starts a cluster node: joins the DB and runs tick + membership loops.
pub struct Node;

impl Node {
    pub async fn join(config: NodeConfig) -> Result<(NodeHandle, tokio::task::JoinHandle<()>)> {
        let stale_threshold = config.stale_threshold();
        let db_config = DbConfig::new(
            config.object_store.clone(),
            &config.prefix,
            config.local_root.join("db"),
            &config.node_id,
            &config.node_addr,
        )
        .with_migrations(config.migrations.clone())
        .with_min_schema_version(config.min_schema_version)
        .with_leader_eligible(config.leader_eligible)
        .with_snapshot_every_epochs(config.snapshot_every_epochs);

        let db = Arc::new(Mutex::new(Db::join(db_config).await?));
        let schema_support = {
            let db = db.lock().await;
            db.schema_support()
        };
        let (view_tx, view_rx) = watch::channel(ClusterView::default());
        let registry = Registry::new();
        let (members_tx, _) = watch::channel(Arc::new(Vec::new()));

        {
            let db = db.lock().await;
            publish_view(&view_tx, &db);
        }

        let node_id = config.node_id.clone();
        let db_task = db.clone();
        let view_tx_task = view_tx.clone();
        let epoch_interval = config.epoch_interval;

        let membership_client = Client::new(
            config.node_id.clone(),
            config.node_addr.clone(),
            config.az.clone(),
            config.capabilities.clone(),
            schema_support,
            view_rx.clone(),
            members_tx.clone(),
            registry.clone(),
            stale_threshold,
        );

        let grpc_addr: SocketAddr = config
            .node_addr
            .parse()
            .with_context(|| format!("invalid node_addr for gRPC: {}", config.node_addr))?;
        let grpc_registry = registry.clone();
        let grpc_view = view_tx.subscribe();
        tokio::spawn(async move {
            let service = Service::new(grpc_registry, grpc_view);
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(service.into_server())
                .serve(grpc_addr)
                .await
            {
                warn!(error = %e, %grpc_addr, "membership gRPC server stopped");
            }
        });

        let members_tx_loop = members_tx.clone();
        let registry_loop = registry.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(epoch_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut membership = tokio::spawn(membership_client.run());

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut db = db_task.lock().await;
                        match db.tick().await {
                            Ok(TickResult::Replicated { epoch }) => debug!(node_id = %node_id, epoch, "replicated"),
                            Ok(TickResult::Synced { epoch }) => debug!(node_id = %node_id, epoch, "synced"),
                            Ok(TickResult::Promoted) => warn!(node_id = %node_id, "promoted to leader"),
                            Ok(TickResult::Fenced { epoch }) => warn!(node_id = %node_id, epoch, "fenced"),
                            Ok(TickResult::NoNewEpoch) => {}
                            Err(e) => warn!(node_id = %node_id, error = %e, "tick failed"),
                        }
                        publish_view(&view_tx_task, &db);
                    }
                    r = &mut membership => {
                        if let Err(e) = r {
                            warn!(node_id = %node_id, error = %e, "membership task failed");
                        }
                        membership = tokio::spawn(Client::new(
                            config.node_id.clone(),
                            config.node_addr.clone(),
                            config.az.clone(),
                            config.capabilities.clone(),
                            schema_support,
                            view_rx.clone(),
                            members_tx_loop.clone(),
                            registry_loop.clone(),
                            stale_threshold,
                        ).run());
                    }
                }
            }
        });

        Ok((
            NodeHandle {
                db,
                view_tx,
                registry,
                members_tx,
            },
            handle,
        ))
    }
}

fn publish_view(tx: &watch::Sender<ClusterView>, db: &Db) {
    let (leader_id, leader_addr) = db.leader();
    let schema_version = db.store().schema_version().unwrap_or(0);
    let _ = tx.send(ClusterView {
        is_leader: db.is_leader(),
        leader_id,
        leader_addr,
        epoch: db.epoch(),
        schema_version,
    });
}
