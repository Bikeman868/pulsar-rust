use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use pulsar_rust_net::data_types::{CatalogId, MessageId, PartitionId, Timestamp, TopicId};

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub struct MessageRef {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalog_id: CatalogId,
    pub message_id: MessageId,
}

impl MessageRef {
    pub fn to_key(self: &Self) -> String {
        self.topic_id.to_string()
            + ":"
            + &self.partition_id.to_string()
            + ":"
            + &self.catalog_id.to_string()
            + ":"
            + &self.message_id.to_string()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub message_ref: MessageRef,
    pub key: String,
    pub published: Timestamp,
    pub attributes: HashMap<String, String>,
}
