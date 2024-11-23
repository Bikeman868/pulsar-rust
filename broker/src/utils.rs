use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::data_types::Timestamp;

pub fn now_epoc_millis() -> Timestamp {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()) as u64
}
