use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub epoch_interval: Duration,
    pub snapshot_interval: Duration,
    pub session_timeout: Duration,
    pub retention_period: Duration,
}

impl Config {
    pub fn snapshot_interval_epochs(&self) -> u64 {
        self.snapshot_interval.as_secs() / self.epoch_interval.as_secs()
    }

    pub fn client_retry_timeout(&self) -> Duration {
        self.epoch_interval * 2
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            epoch_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(30),
            session_timeout: Duration::from_secs(20),
            retention_period: Duration::from_secs(60 * 60 * 24 * 7),
        }
    }
}
