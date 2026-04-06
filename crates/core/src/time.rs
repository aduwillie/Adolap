use crate::types::TimestampMs;
use std::time::{SystemTime, UNIX_EPOCH};

// Time utility for getting current timestamp in milliseconds since the Unix epoch.
pub fn now_ms() -> TimestampMs {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_the_epoch.as_millis() as TimestampMs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TimestampMs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn epoch_ms(time: SystemTime) -> TimestampMs {
        time.duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as TimestampMs
    }

    #[test]
    fn now_ms_returns_non_negative_timestamp() {
        let timestamp = now_ms();
        assert!(timestamp >= 0);
    }

    #[test]
    fn now_ms_falls_within_call_time_window() {
        let lower_bound = epoch_ms(SystemTime::now());
        let timestamp = now_ms();
        let upper_bound = epoch_ms(SystemTime::now());

        assert!(timestamp >= lower_bound);
        assert!(timestamp <= upper_bound);
    }
}
