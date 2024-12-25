use pulsar_rust_net::{
    contracts::v1::responses,
    data_types::{ConsumerId, LedgerId, MessageId, PartitionId, Timestamp, TopicId},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Serialize, Deserialize, Clone, Copy, Default)]
pub struct MessageRef {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub message_id: MessageId,
}

impl MessageRef {
    pub fn from_key(key: &str) -> MessageRef {
        let splits = key.split(':').collect::<Vec<&str>>();

        let topic_id: TopicId = splits[0].parse().unwrap();
        let partition_id: PartitionId = splits[1].parse().unwrap();
        let ledger_id: LedgerId = splits[2].parse().unwrap();
        let message_id: MessageId = splits[3].parse().unwrap();

        Self {
            topic_id,
            partition_id,
            ledger_id,
            message_id,
        }
    }

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

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Serialize, Deserialize, Clone)]
pub struct PublishedMessage {
    pub message_ref: MessageRef,
    pub key: String,
    pub timestamp: Timestamp,
    pub published: Timestamp,
    pub attributes: HashMap<String, String>,
    pub subscriber_count: usize,
    pub ack_count: usize,
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone)]
pub struct SubscribedMessage {
    pub message_ref_key: String,
    pub key: String,
    pub consumer_id: Option<ConsumerId>,
    pub delivered_timestamp: Option<Timestamp>,
    pub delivery_count: usize,
}

impl SubscribedMessage {
    pub fn new(message_ref_key: &str, key: &str) -> Self {
        Self {
            message_ref_key: message_ref_key.to_owned(),
            key: key.to_owned(),
            consumer_id: None,
            delivered_timestamp: None,
            delivery_count: 0,
        }
    }
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

impl From<&PublishedMessage> for SubscribedMessage {
    fn from(message: &PublishedMessage) -> Self {
        Self {
            message_ref_key: message.message_ref.to_key(),
            key: message.key.clone(),
            consumer_id: None,
            delivered_timestamp: None,
            delivery_count: 0,
        }
    }
}
