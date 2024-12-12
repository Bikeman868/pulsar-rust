/*
Version 1 data contracts for serializing response body
*/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::data_types::{ConsumerId, LedgerId, MessageId, NodeId, PartitionId, PortNumber, SubscriptionId, Timestamp, TopicId};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClusterSummary {
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClusterDetail {
    pub name: String,
    pub nodes: Vec<NodeSummary>,
    pub topics: Vec<TopicSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeSummary {
    pub node_id: NodeId,
    pub ip_address: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeDetail {
    pub node_id: NodeId,
    pub ip_address: String,
    pub admin_port: PortNumber,
    pub pubsub_port: PortNumber,
    pub sync_port: PortNumber,
    pub ledgers: Vec<LedgerSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicSummary {
    pub topic_id: TopicId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicDetail {
    pub topic_id: TopicId,
    pub name: String,
    pub partitions: Vec<PartitionSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PartitionSummary {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PartitionDetail {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledgers: Vec<LedgerSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LedgerSummary {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub node_id: NodeId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeList {
    pub nodes: Vec<NodeSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicList {
    pub topics: Vec<TopicSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PartitionList {
    pub partitions: Vec<PartitionSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicPartitionMap {
    pub topic: TopicSummary,
    pub partitions: Vec<PartitionDetail>,
    pub nodes: Vec<NodeDetail>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LedgerList {
    pub ledgers: Vec<LedgerSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PublishResult {
    pub result: PostResult,
    pub message_ref: Option<MessageRef>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct MessageRef {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub message_id: MessageId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message {
    pub message_ref: MessageRef,
    pub message_key: String,
    pub published: Timestamp,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LogEntrySummary {
    pub timestamp: Timestamp,
    pub event_type: String,
    pub event_key: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PublishLogEntry {
    pub message: Message,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AckLogEntry {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NackLogEntry {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum LogEntryDetail {
    Publish(PublishLogEntry),
    Ack(AckLogEntry),
    Nack(NackLogEntry),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LogEntry {
    pub timestamp: Timestamp,
    pub event_type: String,
    pub event_key: String,
    pub details: Option<LogEntryDetail>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PostResult {
    pub success: bool,
    pub error: Option<String>,
}

impl PostResult {
    pub fn error(msg: &str) -> Self {
        Self {
            success: false,
            error: Some(msg.to_owned()),
        }
    }
    pub fn success() -> Self {
        Self {
            success: true,
            error: None,
        }
    }
}
