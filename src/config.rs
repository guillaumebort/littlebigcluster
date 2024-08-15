use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub epoch_interval: Duration,
    pub snapshot_interval: Duration,
    pub snapshots_to_keep: usize,
    pub session_timeout: Duration,
}

impl Config {
    pub fn snapshot_interval_epochs(&self) -> u64 {
        self.snapshot_interval.as_secs() / self.epoch_interval.as_secs()
    }

    pub fn client_retry_timeout(&self) -> Duration {
        self.epoch_interval * 2
    }
}
