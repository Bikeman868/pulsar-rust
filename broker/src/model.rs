pub mod cluster;
pub mod data_types;
pub mod messages;

use crate::{data::DataLayer, persistence::persisted_entities};
use data_types::{CatalogId, NodeId, PartitionId, SubscriptionId, TopicId};

#[derive(Debug)]
pub enum RefreshStatus {
    /// Entity was successfully refreshed from the database
    Updated,

    /// Entity contains stale data because the refresh failed
    Stale,

    /// This entity was not found in the database
    Deleted,
}

#[derive(Debug)]
pub struct Node {
    persisted_data: persisted_entities::Node,
    refresh_status: RefreshStatus,
}

impl Node {
    pub fn new(data_layer: &DataLayer, node_id: NodeId) -> Self {
        Self {
            persisted_data: data_layer.get_node(node_id).unwrap(),
            refresh_status: RefreshStatus::Updated,
        }
    }

    pub fn refresh(self: &mut Self, data_layer: &DataLayer) {
        match data_layer.get_node(self.persisted_data.node_id) {
            Ok(node) => self.persisted_data = node,
            Err(err) => match err {
                crate::data::DataErr::Duplicate { .. } => {
                    self.refresh_status = RefreshStatus::Stale
                }
                crate::data::DataErr::PersistenceFailure { .. } => {
                    self.refresh_status = RefreshStatus::Stale
                }
                crate::data::DataErr::NotFound { .. } => {
                    self.refresh_status = RefreshStatus::Deleted
                }
            },
        }
    }
}

#[derive(Debug)]
pub struct Topic {
    persisted_data: persisted_entities::Topic,
    refresh_status: RefreshStatus,
}

impl Topic {
    pub fn new(data_layer: &DataLayer, topic_id: TopicId) -> Self {
        Self {
            persisted_data: data_layer.get_topic(topic_id).unwrap(),
            refresh_status: RefreshStatus::Updated,
        }
    }

    pub fn refresh(self: &mut Self, data_layer: &DataLayer) {}
}

#[derive(Debug)]
pub struct Partition {
    persisted_data: persisted_entities::Partition,
    refresh_status: RefreshStatus,
}

impl Partition {
    pub fn new(data_layer: &DataLayer, topic_id: TopicId, partition_id: PartitionId) -> Self {
        Self {
            persisted_data: data_layer.get_partition(topic_id, partition_id).unwrap(),
            refresh_status: RefreshStatus::Updated,
        }
    }

    pub fn refresh(self: &mut Self, data_layer: &DataLayer) {}
}

#[derive(Debug)]
pub struct Catalog {
    persisted_data: persisted_entities::Catalog,
    refresh_status: RefreshStatus,
}

impl Catalog {
    pub fn new(
        data_layer: &DataLayer,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> Self {
        Self {
            persisted_data: data_layer
                .get_catalog(topic_id, partition_id, catalog_id)
                .unwrap(),
            refresh_status: RefreshStatus::Updated,
        }
    }

    pub fn refresh(self: &mut Self, data_layer: &DataLayer) {}
}

#[derive(Debug)]
pub struct Subscription {
    persisted_data: persisted_entities::Subscription,
    refresh_status: RefreshStatus,
}

impl Subscription {
    pub fn new(data_layer: &DataLayer, topic_id: TopicId, subscription_id: SubscriptionId) -> Self {
        Self {
            persisted_data: data_layer
                .get_subscription(topic_id, subscription_id)
                .unwrap(),
            refresh_status: RefreshStatus::Updated,
        }
    }

    pub fn refresh(self: &mut Self, data_layer: &DataLayer) {}
}
