/*
Version 1 data contracts for serializing response body
*/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::data_types::{
    ConsumerId, LedgerId, MessageId, NodeId, PartitionId, PortNumber, SubscriptionId, Timestamp,
    TopicId,
};

#[derive(Deserialize, Serialize, Clone)]
pub struct ClusterSummary {
    pub name: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ClusterDetail {
    pub name: String,
    pub nodes: Vec<NodeSummary>,
    pub topics: Vec<TopicSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct NodeSummary {
    pub node_id: NodeId,
    pub ip_address: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct NodeDetail {
    pub node_id: NodeId,
    pub ip_address: String,
    pub admin_port: PortNumber,
    pub pubsub_port: PortNumber,
    pub sync_port: PortNumber,
    pub ledgers: Vec<LedgerSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TopicSummary {
    pub topic_id: TopicId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TopicDetail {
    pub topic_id: TopicId,
    pub name: String,
    pub partitions: Vec<PartitionSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PartitionSummary {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PartitionDetail {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledgers: Vec<LedgerSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LedgerSummary {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub node_id: NodeId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LedgerDetail {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub node_id: NodeId,
    pub next_message_id: MessageId,
    pub message_count: usize,
    pub create_timestamp: Timestamp,
    pub last_update_timestamp: Timestamp,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct NodeList {
    pub nodes: Vec<NodeSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TopicList {
    pub topics: Vec<TopicSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PartitionList {
    pub partitions: Vec<PartitionSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TopicPartitionMap {
    pub topic: TopicSummary,
    pub partitions: Vec<PartitionDetail>,
    pub nodes: Vec<NodeDetail>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LedgerList {
    pub ledgers: Vec<LedgerSummary>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PublishResult {
    pub message_ref: MessageRef,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct AllocateConsumerResult {
    pub consumer_id: ConsumerId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct AckResult {}

#[derive(Deserialize, Serialize, Clone)]
pub struct NackResult {}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct MessageRef {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub message_id: MessageId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Message {
    pub message_ref: MessageRef,
    pub message_key: String,
    pub message_ack_key: String,
    pub published: Timestamp,
    pub delivered: Timestamp,
    pub delivery_count: usize,
    pub attributes: HashMap<String, String>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LogEntrySummary {
    pub timestamp: Timestamp,
    pub event_type: String,
    pub event_key: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PublishLogEntry {
    pub message: Message,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct AckLogEntry {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct NackLogEntry {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct NewConsumerLogEntry {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct DropConsumerLogEntry {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct KeyAffinityLogEntry {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
    pub message_key: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub enum LogEntryDetail {
    Publish(PublishLogEntry),
    Ack(AckLogEntry),
    Nack(NackLogEntry),
    NewConsumer(NewConsumerLogEntry),
    DropConsumer(DropConsumerLogEntry),
    KeyAffinity(KeyAffinityLogEntry),
}

#[derive(Deserialize, Serialize, Clone)]
pub struct LogEntry {
    pub timestamp: Timestamp,
    pub event_type: String,
    pub event_key: String,
    pub details: Option<LogEntryDetail>,
}

#[derive(Deserialize, Serialize, Clone)]
pub enum RequestOutcome {
    Success,
    NoData,
    Warning,
    Error,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Response<T> {
    pub outcome: RequestOutcome,
    pub message: Option<String>,
    pub data: Option<T>,
}

impl<T> Response<T> {
    pub fn success(data: T) -> Self {
        Self {
            outcome: RequestOutcome::Success,
            message: None,
            data: Some(data),
        }
    }
    pub fn no_data(msg: &str) -> Self {
        Self {
            outcome: RequestOutcome::NoData,
            message: Some(msg.to_owned()),
            data: None,
        }
    }
    pub fn warning(msg: &str) -> Self {
        Self {
            outcome: RequestOutcome::Warning,
            message: Some(msg.to_owned()),
            data: None,
        }
    }
    pub fn error(msg: &str) -> Self {
        Self {
            outcome: RequestOutcome::Error,
            message: Some(msg.to_owned()),
            data: None,
        }
    }
}
