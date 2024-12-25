use crate::{
    model::messages::{MessageRef, PublishedMessage},
    persistence::Keyed,
};
use pulsar_rust_net::data_types::{ConsumerId, SubscriptionId, TopicId};
use serde::{Deserialize, Serialize};

use super::log_entries::LogEntry;

#[derive(Debug, Deserialize, Serialize)]
pub struct AckEvent {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NackEvent {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PublishEvent {
    pub message: PublishedMessage,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NewConsumerEvent {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DropConsumerEvent {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KeyAffinityEvent {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
    pub message_key: String,
}

impl AckEvent {
    pub fn new(
        message_ref: MessageRef,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> Self {
        AckEvent {
            message_ref,
            subscription_id,
            consumer_id,
        }
    }
}

impl NackEvent {
    pub fn new(
        message_ref: MessageRef,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> Self {
        NackEvent {
            message_ref,
            subscription_id,
            consumer_id,
        }
    }
}

impl PublishEvent {
    pub fn new(message: &PublishedMessage) -> Self {
        PublishEvent {
            message: message.clone(),
        }
    }
}

impl Keyed for AckEvent {
    fn type_name(self: &Self) -> &'static str {
        LogEntry::ACK_TYPE_NAME
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}

impl Keyed for NackEvent {
    fn type_name(self: &Self) -> &'static str {
        LogEntry::NACK_TYPE_NAME
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}

impl Keyed for PublishEvent {
    fn type_name(self: &Self) -> &'static str {
        LogEntry::PUBLISH_TYPE_NAME
    }
    fn key(self: &Self) -> String {
        self.message.message_ref.to_key()
    }
}

impl Keyed for NewConsumerEvent {
    fn type_name(self: &Self) -> &'static str {
        LogEntry::NEW_CONSUMER_TYPE_NAME
    }
    fn key(self: &Self) -> String {
        self.topic_id.to_string()
            + ":"
            + &self.subscription_id.to_string()
            + ":"
            + &self.consumer_id.to_string()
    }
}

impl Keyed for DropConsumerEvent {
    fn type_name(self: &Self) -> &'static str {
        LogEntry::DROP_CONSUMER_TYPE_NAME
    }
    fn key(self: &Self) -> String {
        self.topic_id.to_string()
            + ":"
            + &self.subscription_id.to_string()
            + ":"
            + &self.consumer_id.to_string()
    }
}

impl Keyed for KeyAffinityEvent {
    fn type_name(self: &Self) -> &'static str {
        LogEntry::KEY_AFFINITY_TYPE_NAME
    }
    fn key(self: &Self) -> String {
        self.topic_id.to_string()
            + ":"
            + &self.subscription_id.to_string()
            + ":"
            + &self.consumer_id.to_string()
    }
}
