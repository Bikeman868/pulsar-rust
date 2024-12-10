use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use pulsar_rust_net::{
    contracts::v1::responses, 
    data_types::{LedgerId, MessageId, PartitionId, Timestamp, TopicId}
};

#[derive(Debug, Copy, Clone, Deserialize, Serialize, Default)]
pub struct MessageRef {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub message_id: MessageId,
}

impl MessageRef {
    pub fn to_key(self: &Self) -> String {
        self.topic_id.to_string()
            + ":"
            + &self.partition_id.to_string()
            + ":"
            + &self.ledger_id.to_string()
            + ":"
            + &self.message_id.to_string()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub message_ref: MessageRef,
    pub key: String,
    pub timestamp: Timestamp,
    pub published: Timestamp,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostResult {
    pub success: bool,
    pub error: String,
}

impl Into<MessageRef> for responses::MessageRef {
    fn into(self) -> MessageRef {
        MessageRef {
            topic_id: self.topic_id, 
            partition_id: self.partition_id, 
            ledger_id: self.ledger_id, 
            message_id: self.message_id,
        }
    }
}

impl Into<responses::MessageRef> for MessageRef {
    fn into(self) -> responses::MessageRef {
        responses::MessageRef {
            topic_id: self.topic_id, 
            partition_id: self.partition_id, 
            ledger_id: self.ledger_id, 
            message_id: self.message_id,
        }
    }
}

impl From<&MessageRef> for responses::MessageRef {
    fn from(value: &MessageRef) -> Self {
        Self {
            topic_id: value.topic_id,
            partition_id: value.partition_id,
            ledger_id: value.ledger_id,
            message_id: value.message_id,
        }
    }
}
