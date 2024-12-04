use std::{collections::HashMap, sync::Arc};

use crate::model::{
    self,
    data_types::{CatalogId, NodeId, PartitionId, TopicId},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeSummary {
    pub node_id: NodeId,
    pub ip_address: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeDetail {
    pub node_id: NodeId,
    pub ip_address: String,
    pub catalogs: Vec<CatalogSummary>,
}

impl From<&model::Node> for self::NodeSummary {
    fn from(node: &model::Node) -> Self {
        Self {
            node_id: node.node_id,
            ip_address: node.ip_address.clone(),
        }
    }
}

impl From<&model::Node> for self::NodeDetail {
    fn from(node: &model::Node) -> Self {
        Self {
            node_id: node.node_id,
            ip_address: node.ip_address.clone(),
            catalogs: Vec::default(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicSummary {
    pub topic_id: TopicId,
}

impl From<&model::Topic> for self::TopicSummary {
    fn from(topic: &model::Topic) -> Self {
        Self {
            topic_id: topic.topic_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicDetail {
    pub topic_id: TopicId,
}

impl From<&model::Topic> for self::TopicDetail {
    fn from(topic: &model::Topic) -> Self {
        Self {
            topic_id: topic.topic_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PartitionSummary {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
}

impl From<&model::Partition> for self::PartitionSummary {
    fn from(partition: &model::Partition) -> Self {
        Self {
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PartitionDetail {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
}

impl From<&model::Partition> for self::PartitionDetail {
    fn from(partition: &model::Partition) -> Self {
        Self {
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CatalogSummary {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalog_id: CatalogId,
    pub node_id: NodeId,
}

impl From<&model::Catalog> for self::CatalogSummary {
    fn from(catalog: &model::Catalog) -> Self {
        Self {
            topic_id: catalog.topic_id,
            partition_id: catalog.partition_id,
            catalog_id: catalog.catalog_id,
            node_id: catalog.node_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CatalogDetail {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalog_id: CatalogId,
    pub node_id: NodeId,
}

impl From<&model::Catalog> for self::CatalogDetail {
    fn from(catalog: &model::Catalog) -> Self {
        Self {
            topic_id: catalog.topic_id,
            partition_id: catalog.partition_id,
            catalog_id: catalog.catalog_id,
            node_id: catalog.node_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeList {
    pub nodes: Vec<NodeSummary>,
}

impl From<&Arc<HashMap<NodeId, model::Node>>> for NodeList {
    fn from(nodes: &Arc<HashMap<NodeId, model::Node>>) -> Self {
        Self {
            nodes: nodes.values().map(|p| NodeSummary::from(p)).collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicList {
    pub topics: Vec<TopicSummary>,
}

impl From<&Arc<HashMap<TopicId, model::Topic>>> for TopicList {
    fn from(topics: &Arc<HashMap<TopicId, model::Topic>>) -> Self {
        Self {
            topics: topics.values().map(|p| TopicSummary::from(p)).collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PartitionList {
    pub partitions: Vec<PartitionSummary>,
}

impl From<&Arc<HashMap<PartitionId, model::Partition>>> for PartitionList {
    fn from(partitions: &Arc<HashMap<PartitionId, model::Partition>>) -> Self {
        Self {
            partitions: partitions
                .values()
                .map(|p| PartitionSummary::from(p))
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CatalogList {
    pub catalogs: Vec<CatalogSummary>,
}

impl From<&Arc<HashMap<CatalogId, model::Catalog>>> for CatalogList {
    fn from(catalogs: &Arc<HashMap<CatalogId, model::Catalog>>) -> Self {
        Self {
            catalogs: catalogs.values().map(|p| CatalogSummary::from(p)).collect(),
        }
    }
}
