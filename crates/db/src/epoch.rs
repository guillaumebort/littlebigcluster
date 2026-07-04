//! On-disk epoch and snapshot formats.

use anyhow::{anyhow, bail, Result};
use prost::Message;

mod wire {
    include!(concat!(env!("OUT_DIR"), "/lbc.epoch.rs"));
}

use wire::{Changeset, EpochMetadata, Migration, WalEpoch as ProtoWalEpoch};

pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct Epoch {
    pub epoch: u64,
    pub leader_id: String,
    pub leader_addr: String,
    pub created_at: String,
    pub format_version: u32,
}

impl Epoch {
    pub fn initial(leader_id: String, leader_addr: String, timestamp: String) -> Self {
        Self {
            epoch: 0,
            leader_id,
            leader_addr,
            created_at: timestamp,
            format_version: FORMAT_VERSION,
        }
    }

    pub fn new(epoch: u64, leader_id: String, leader_addr: String, timestamp: String) -> Self {
        Self {
            epoch,
            leader_id,
            leader_addr,
            created_at: timestamp,
            format_version: FORMAT_VERSION,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalEpoch {
    pub metadata: Epoch,
    pub body: EpochBody,
}

#[derive(Debug, Clone)]
pub enum EpochBody {
    /// Session-extension changeset bytes (may be empty).
    Changeset(Vec<u8>),
    /// Ordered DDL statements applied at this epoch boundary.
    Migration { version: u32, sql: Vec<String> },
}

impl WalEpoch {
    pub fn changeset(
        epoch: u64,
        leader_id: String,
        leader_addr: String,
        timestamp: String,
        changeset: Vec<u8>,
    ) -> Self {
        Self {
            metadata: Epoch::new(epoch, leader_id, leader_addr, timestamp),
            body: EpochBody::Changeset(changeset),
        }
    }

    pub fn migration(
        epoch: u64,
        leader_id: String,
        leader_addr: String,
        timestamp: String,
        version: u32,
        sql: Vec<String>,
    ) -> Self {
        Self {
            metadata: Epoch::new(epoch, leader_id, leader_addr, timestamp),
            body: EpochBody::Migration { version, sql },
        }
    }
}

fn epoch_to_proto(epoch: &Epoch) -> EpochMetadata {
    EpochMetadata {
        epoch: epoch.epoch,
        leader_id: epoch.leader_id.clone(),
        leader_addr: epoch.leader_addr.clone(),
        created_at: epoch.created_at.clone(),
        format_version: epoch.format_version,
    }
}

fn epoch_from_proto(metadata: EpochMetadata) -> Epoch {
    Epoch {
        epoch: metadata.epoch,
        leader_id: metadata.leader_id,
        leader_addr: metadata.leader_addr,
        created_at: metadata.created_at,
        format_version: metadata.format_version,
    }
}

fn wal_epoch_to_proto(wal_epoch: &WalEpoch) -> ProtoWalEpoch {
    let body = match &wal_epoch.body {
        EpochBody::Changeset(bytes) => Some(wire::wal_epoch::Body::Changeset(Changeset {
            data: bytes.clone(),
        })),
        EpochBody::Migration { version, sql } => {
            Some(wire::wal_epoch::Body::Migration(Migration {
                version: *version,
                sql: sql.clone(),
            }))
        }
    };

    ProtoWalEpoch {
        metadata: Some(epoch_to_proto(&wal_epoch.metadata)),
        body,
    }
}

fn wal_epoch_from_proto(proto: ProtoWalEpoch) -> Result<WalEpoch> {
    let metadata = proto
        .metadata
        .ok_or_else(|| anyhow!("WalEpoch missing metadata"))?;
    let body = match proto.body {
        Some(wire::wal_epoch::Body::Changeset(changeset)) => EpochBody::Changeset(changeset.data),
        Some(wire::wal_epoch::Body::Migration(migration)) => EpochBody::Migration {
            version: migration.version,
            sql: migration.sql,
        },
        None => bail!("WalEpoch missing body"),
    };

    Ok(WalEpoch {
        metadata: epoch_from_proto(metadata),
        body,
    })
}

pub fn encode_wal_epoch(wal_epoch: &WalEpoch) -> Result<Vec<u8>> {
    let bytes = wal_epoch_to_proto(wal_epoch).encode_to_vec();
    Ok(lz4_flex::compress_prepend_size(&bytes))
}

pub fn decode_wal_epoch(data: &[u8]) -> Result<WalEpoch> {
    let bytes = lz4_flex::decompress_size_prepended(data)
        .map_err(|e| anyhow!("failed to decompress WalEpoch: {e}"))?;
    let proto = ProtoWalEpoch::decode(bytes.as_slice())
        .map_err(|e| anyhow!("failed to decode WalEpoch: {e}"))?;
    wal_epoch_from_proto(proto)
}

pub fn validate_format_version(label: &str, version: u32) -> Result<()> {
    if version != FORMAT_VERSION {
        bail!("format mismatch: {label} has version {version}, expected {FORMAT_VERSION}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let wal = WalEpoch::changeset(
            3,
            "a".into(),
            "127.0.0.1:1".into(),
            "2026-01-01T00:00:00Z".into(),
            vec![1, 2, 3],
        );
        let back = decode_wal_epoch(&encode_wal_epoch(&wal).unwrap()).unwrap();
        assert_eq!(back.metadata.epoch, 3);
        match back.body {
            EpochBody::Changeset(b) => assert_eq!(b, vec![1, 2, 3]),
            _ => panic!("expected changeset"),
        }
    }

    #[test]
    fn round_trip_migration() {
        let wal = WalEpoch::migration(
            1,
            "leader".into(),
            "127.0.0.1:1".into(),
            "2026-01-01T00:00:00Z".into(),
            2,
            vec!["CREATE TABLE t (id INTEGER PRIMARY KEY);".into()],
        );
        let back = decode_wal_epoch(&encode_wal_epoch(&wal).unwrap()).unwrap();
        match back.body {
            EpochBody::Migration { version, sql } => {
                assert_eq!(version, 2);
                assert_eq!(sql.len(), 1);
            }
            _ => panic!("expected migration"),
        }
    }
}
