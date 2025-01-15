use std::collections::HashMap;

use pulsar_rust_net::{
    contracts::v1,
    data_types::{ConsumerId, LedgerId, MessageId, PartitionId, Timestamp, TopicId},
};

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct MessageRef {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub message_id: MessageId,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Message {
    pub message_ref: MessageRef,
    pub message_key: String,
    pub message_ref_key: String,
    pub published: Timestamp,
    pub delivered: Timestamp,
    pub delivery_count: usize,
    pub attributes: HashMap<String, String>,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PublishResult {
    pub message_ref: MessageRef,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ConsumeResult {
    pub consumer_id: ConsumerId,
    pub messages: Vec<Message>,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct AckResult {
    pub success: bool,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct NackResult {
    pub success: bool,
}

impl From<&v1::responses::MessageRef> for MessageRef {
    fn from(message_ref: &v1::responses::MessageRef) -> Self {
        Self {
            topic_id: message_ref.topic_id,
            partition_id: message_ref.partition_id,
            ledger_id: message_ref.ledger_id,
            message_id: message_ref.message_id,
        }
    }
}

impl From<&v1::responses::PublishResult> for PublishResult {
    fn from(result: &v1::responses::PublishResult) -> Self {
        Self {
            message_ref: MessageRef::from(&result.message_ref),
        }
    }
}

impl From<&v1::responses::Message> for Message {
    fn from(message: &v1::responses::Message) -> Self {
        Self {
            message_ref: MessageRef::from(&message.message_ref),
            message_key: message.message_key.clone(),
            message_ref_key: message.message_ack_key.clone(),
            published: message.published,
            delivered: message.delivered,
            delivery_count: message.delivery_count,
            attributes: message.attributes.clone(),
        }
    }
}

impl From<&v1::responses::ConsumeResult> for ConsumeResult {
    fn from(result: &v1::responses::ConsumeResult) -> Self {
        Self {
            consumer_id: result.consumer_id,
            messages: result.messages.iter().map(|m| Message::from(m)).collect(),
        }
    }
}

impl From<&v1::responses::AckResult> for AckResult {
    fn from(result: &v1::responses::AckResult) -> Self {
        AckResult {
            success: result.success,
        }
    }
}

impl From<&v1::responses::NackResult> for NackResult {
    fn from(result: &v1::responses::NackResult) -> Self {
        NackResult {
            success: result.success,
        }
    }
}
