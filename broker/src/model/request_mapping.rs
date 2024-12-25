use crate::utils::now_epoc_millis;
use pulsar_rust_net::{
    contracts::v1::requests,
    data_types::{LedgerId, MessageId, Timestamp},
};

use super::messages::MessageRef;

impl From<&super::messages::PublishedMessage> for requests::Message {
    fn from(value: &super::messages::PublishedMessage) -> Self {
        Self {
            topic_id: value.message_ref.topic_id,
            partition_id: value.message_ref.partition_id,
            key: if value.key.len() == 0 {
                None
            } else {
                Some(value.key.clone())
            },
            timestamp: Some(value.timestamp),
            attributes: value.attributes.clone(),
        }
    }
}

impl Into<super::messages::PublishedMessage> for requests::Message {
    fn into(self) -> super::messages::PublishedMessage {
        super::messages::PublishedMessage {
            message_ref: MessageRef {
                topic_id: self.topic_id,
                partition_id: self.partition_id,
                ledger_id: LedgerId::default(),
                message_id: MessageId::default(),
            },
            key: match self.key {
                Some(key) => key.clone(),
                None => String::default(),
            },
            timestamp: match self.timestamp {
                Some(epoch_time) => epoch_time,
                None => now_epoc_millis(),
            },
            published: Timestamp::default(),
            attributes: self.attributes.clone(),
            subscriber_count: usize::default(),
            ack_count: usize::default(),
        }
    }
}
