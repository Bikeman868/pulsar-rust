pub mod data_types;
pub mod database_entities;
pub mod events;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub struct MessageRef {
    pub topic_id: data_types::TopicId,
    pub partition_id: data_types::PartitionId,
    pub catalog_id: data_types::CatalogId,
    pub message_id: data_types::MessageId,
}

impl MessageRef {
    fn to_key(self: &Self) -> String {
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
    pub published: data_types::Timestamp,
    pub attributes: HashMap<String, String>,
}
