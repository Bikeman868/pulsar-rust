use pulsar_rust_net::{
    contracts::v1,
    data_types::{LedgerId, MessageId, PartitionId, TopicId},
};

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct MessageRef {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub message_id: MessageId,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PublishResult {
    pub message_ref: MessageRef,
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
    fn from(publish_result: &v1::responses::PublishResult) -> Self {
        Self {
            message_ref: MessageRef::from(&publish_result.message_ref),
        }
    }
}
