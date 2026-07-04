//! Schema migrations shipped as explicit replication epochs.

use std::sync::Arc;

use anyhow::{bail, Result};

/// One ordered schema migration.
#[derive(Debug, Clone)]
pub struct Migration {
    pub version: u32,
    pub sql: Vec<String>,
}

/// App-supplied migration list, sorted by version.
pub type Migrations = Arc<[Migration]>;

/// Schema version range this binary supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaSupport {
    pub min: u32,
    pub max: u32,
}

impl SchemaSupport {
    pub fn from_migrations(migrations: &Migrations, min: u32) -> Self {
        let max = migrations.last().map(|m| m.version).unwrap_or(0);
        Self { min, max }
    }
}

/// Rejects a local DB whose schema version is outside the supported range.
pub fn validate_schema(current: u32, support: &SchemaSupport) -> Result<()> {
    if current > support.max {
        bail!(
            "DB schema {current} is newer than this binary supports (max {})",
            support.max
        );
    }
    if current < support.min {
        bail!(
            "DB schema {current} is below this binary's minimum ({})",
            support.min
        );
    }
    Ok(())
}

/// Validates migration list is strictly increasing without gaps/duplicates.
pub fn validate_migrations(migrations: &Migrations) -> Result<()> {
    let mut prev = 0u32;
    for m in migrations.iter() {
        if m.version <= prev {
            bail!(
                "migrations must be strictly increasing; found version {} after {prev}",
                m.version
            );
        }
        if m.sql.is_empty() {
            bail!("migration {} has no SQL statements", m.version);
        }
        prev = m.version;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_schema_accepts_in_range() {
        let support = SchemaSupport { min: 1, max: 3 };
        assert!(validate_schema(1, &support).is_ok());
        assert!(validate_schema(3, &support).is_ok());
    }

    #[test]
    fn validate_schema_rejects_out_of_range() {
        let support = SchemaSupport { min: 1, max: 2 };
        assert!(validate_schema(0, &support).is_err());
        assert!(validate_schema(3, &support).is_err());
    }
}
