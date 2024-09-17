use chrono::{NaiveDateTime, Timelike};

/// Rounds the timestamp to the nearest period (minute, hour, etc.)
//  #[allow(dead_code)]
pub fn round_to_period(timestamp: NaiveDateTime, period: &str) -> NaiveDateTime {
    match period {
        "second" => timestamp
            .date()
            .and_hms_opt(timestamp.hour(), timestamp.minute(), timestamp.second())
            .unwrap(),
        "minute" => timestamp
            .date()
            .and_hms_opt(timestamp.hour(), timestamp.minute(), 0)
            .unwrap(),
        "hour" => timestamp
            .date()
            .and_hms_opt(timestamp.hour(), 0, 0)
            .unwrap(),
        "day" => timestamp.date().and_hms_opt(0, 0, 0).unwrap(),
        _ => timestamp
            .date()
            .and_hms_opt(timestamp.hour(), 0, 0)
            .unwrap(), // Default to 'hour'
    }
}
