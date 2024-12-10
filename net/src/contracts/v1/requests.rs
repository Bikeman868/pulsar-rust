/*
Version 1 data contracts for serializing request body
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::data_types::{PartitionId, Timestamp, TopicId};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub key: Option<String>,
    pub timestamp: Option<Timestamp>,
    pub attributes: HashMap<String, String>,
}
