use crate::types::TimestampMs;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> TimestampMs {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_the_epoch.as_millis() as TimestampMs
}
