//! Wall clock abstraction for tests.

use chrono::{DateTime, Utc};

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Default)]
pub struct TestClock {
    now: parking_lot::Mutex<DateTime<Utc>>,
}

impl TestClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: parking_lot::Mutex::new(now),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        *self.now.lock() = t;
    }

    pub fn advance_secs(&self, secs: i64) {
        *self.now.lock() += chrono::Duration::seconds(secs);
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock()
    }
}
