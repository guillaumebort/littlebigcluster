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
