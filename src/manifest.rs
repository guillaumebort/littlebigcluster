use std::{fmt::Debug, net::SocketAddr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, PutMode, PutOptions};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokio::sync::watch;
use tokio_util::bytes::Bytes;
use tracing::{debug, error};
use uuid::Uuid;

pub trait ManifestData:
    Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

impl<A> ManifestData for A where
    A: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Leadership {
    HasLeader {
        uuid: Uuid,
        address: SocketAddr,
        expire_at: DateTime<Utc>,
    },
    NoLeader,
}

impl Leadership {
    pub fn filter_expired_leader(&self) -> Self {
        match self {
            Leadership::HasLeader { expire_at, .. } if *expire_at < Utc::now() => {
                Leadership::NoLeader
            }
            _ => self.clone(),
        }
    }

    pub fn has_leader(&self) -> bool {
        self.filter_expired_leader() != Leadership::NoLeader
    }

    pub fn is_leader(&self, node_uuid: Uuid) -> bool {
        match self.filter_expired_leader() {
            Leadership::HasLeader { uuid, .. } => uuid == node_uuid,
            Leadership::NoLeader => false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Manifest<M>
where
    M: ManifestData,
{
    pub cluster_id: String,
    pub leader: Leadership,
    pub manifest: M,
    pub epoch: u64,
}

impl<M> Manifest<M>
where
    M: ManifestData,
{
    const PATH: &'static str = "/manifests";

    pub async fn open(
        cluster_id: impl Into<String>,
        object_store: &impl ObjectStore,
    ) -> Result<Manifest<M>> {
        Self::fetch_current_manifest(cluster_id.into(), object_store).await
    }

    pub async fn watch(
        cluster_id: impl Into<String>,
        object_store: impl ObjectStore,
        interval: Duration,
    ) -> Result<watch::Receiver<Manifest<M>>> {
        let manifest = Self::fetch_current_manifest(cluster_id.into(), &object_store).await?;
        let current_epoch = manifest.epoch;
        let cluster_id = manifest.cluster_id.clone();
        let (tx, rx) = watch::channel(manifest);
        tokio::spawn(async move {
            let mut current_epoch = current_epoch;
            while !tx.is_closed() {
                loop {
                    if let Ok(get_result) = object_store
                        .get(&Self::manifest_path(current_epoch + 1))
                        .await
                    {
                        // we have a new manifest
                        let cluster_id: String = cluster_id.clone();
                        match get_result
                            .bytes()
                            .await
                            .map_err(|e| anyhow!("Failed to fetch data: {e:#?}"))
                            .and_then(move |bytes| {
                                Self::read_manifest(bytes, cluster_id, current_epoch + 1)
                            }) {
                            Ok(new_manifest) => {
                                current_epoch = new_manifest.epoch;
                                tx.send_replace(new_manifest);
                                debug!(current_epoch, "New manifest");
                            }
                            Err(e) => {
                                error!(?e, "Failed to process new manifest");
                            }
                        }
                    } else {
                        break;
                    }
                }
                tokio::time::sleep(interval).await;
            }
        });

        Ok(rx)
    }

    pub async fn write(object_store: &impl ObjectStore, manifest: &Manifest<M>) -> Result<()> {
        let leader_json = match manifest.leader {
            Leadership::HasLeader {
                uuid,
                address,
                expire_at,
            } => json!({
                "uuid": uuid.to_string(),
                "address": address.to_string(),
                "expire_at": expire_at.to_rfc3339(),
            }),
            Leadership::NoLeader => Value::Null,
        };
        let manifest_json = json!({
            "cluster_id": manifest.cluster_id,
            "leader": leader_json,
            "manifest": manifest.manifest,
        });
        object_store
            .put_opts(
                &Self::manifest_path(manifest.epoch),
                Bytes::from(
                    serde_json::to_string_pretty(&manifest_json)
                        .context("Failed to serialize manifest to JSON")?,
                ),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .context("Failed to write new manifest")?;
        Ok(())
    }

    pub async fn delete(object_store: &impl ObjectStore, epoch: u64) -> Result<bool> {
        let path = Self::manifest_path(epoch);
        match object_store.delete(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(anyhow!(e)).context("Failed to delete manifest"),
        }
    }

    async fn fetch_current_manifest(
        cluster_id: String,
        object_store: &impl ObjectStore,
    ) -> Result<Manifest<M>> {
        let current_epoch = Self::fetch_current_epoch(object_store).await?;
        let manifest = Self::fetch_manifest(cluster_id, object_store, current_epoch).await?;
        Ok(manifest)
    }

    fn manifest_path(epoch: u64) -> Path {
        Path::from(Self::PATH).child(format!("{:020}.json", epoch))
    }

    async fn fetch_manifest(
        cluster_id: String,
        object_store: &impl ObjectStore,
        epoch: u64,
    ) -> Result<Manifest<M>> {
        let bytes = object_store
            .get(&Self::manifest_path(epoch))
            .await
            .context("Failed to fetch manifest")?
            .bytes()
            .await
            .context("Failed to read manifest data")?;
        Self::read_manifest(bytes, cluster_id, epoch)
    }

    fn read_manifest(bytes: Bytes, cluster_id: String, epoch: u64) -> Result<Manifest<M>> {
        Self::parse_manifest(
            cluster_id,
            epoch,
            &serde_json::from_slice(&bytes).context("Failed to parse JSON")?,
        )
    }

    fn parse_manifest(
        cluster_id: String,
        epoch: u64,
        manifest_json: &Value,
    ) -> Result<Manifest<M>> {
        let found_cluster_id = manifest_json["cluster_id"].as_str().unwrap_or_default();
        if found_cluster_id != cluster_id {
            error!(found_cluster_id, cluster_id, "Cluster id mismatch");
            bail!("Cluster id mismatch");
        }

        let leader = match manifest_json.get("leader") {
            Some(Value::Object(ref leader_json)) => {
                let expire_at: DateTime<Utc> = leader_json
                    .get("expire_at")
                    .unwrap_or(&Value::Null)
                    .as_str()
                    .ok_or_else(|| anyhow!("Invalid JSON: leader.expire_at"))?
                    .parse()?;
                if expire_at < Utc::now() {
                    anyhow::Ok(Leadership::NoLeader)
                } else {
                    let uuid = Uuid::parse_str(
                        leader_json
                            .get("uuid")
                            .unwrap_or(&Value::Null)
                            .as_str()
                            .ok_or_else(|| anyhow!("Invalid JSON: leader.uuid"))?,
                    )?;
                    let address = leader_json
                        .get("address")
                        .unwrap_or(&Value::Null)
                        .as_str()
                        .ok_or_else(|| anyhow!("Invalid JSON: leader.address"))?
                        .parse()?;

                    Ok(Leadership::HasLeader {
                        uuid,
                        address,
                        expire_at,
                    })
                }
            }
            Some(Value::Null) => Ok(Leadership::NoLeader),
            _ => bail!("Invalid JSON: leader"),
        }?;

        Ok(Manifest {
            cluster_id,
            leader,
            manifest: serde_json::from_value(manifest_json["manifest"].clone())?,
            epoch,
        })
    }

    async fn fetch_current_epoch(object_store: &impl ObjectStore) -> Result<u64> {
        let mut manifests = object_store.list(Some(&Path::from(Self::PATH)));
        let mut current_epoch: Option<u64> = None;
        while let Some(manifest) = match manifests.next().await.transpose() {
            Ok(file) => file,
            Err(e) => bail!("Failed to list manifest files: {}", e),
        } {
            if let Some(epoch) = manifest
                .location
                .filename()
                .and_then(|filename| filename.strip_suffix(".json"))
                .and_then(|filename| filename.parse().ok())
            {
                if current_epoch.is_some_and(|current_epoch| current_epoch < epoch) {
                    current_epoch = Some(epoch);
                } else if current_epoch.is_none() {
                    current_epoch = Some(epoch);
                }
            }
        }
        current_epoch.ok_or_else(|| anyhow!("No manifests found (was the cluster initialized?)"))
    }
}
