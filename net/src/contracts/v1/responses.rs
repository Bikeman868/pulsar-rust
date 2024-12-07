/*
Version 1 data contracts for serializing response body
*/

use serde::{Deserialize, Serialize};

use crate::data_types::{CatalogId, NodeId, PartitionId, PortNumber, TopicId};

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
    pub catalogs: Vec<CatalogSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicSummary {
    pub topic_id: TopicId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicDetail {
    pub topic_id: TopicId,
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
    pub catalogs: Vec<CatalogSummary>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CatalogSummary {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalog_id: CatalogId,
    pub node_id: NodeId,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CatalogDetail {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalog_id: CatalogId,
    pub node_id: NodeId,
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
pub struct CatalogList {
    pub catalogs: Vec<CatalogSummary>,
}
