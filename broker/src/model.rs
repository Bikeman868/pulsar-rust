/*
Defines the shared internal state of the application. Some of this state is
persisted to the database, and some is just updated in memory.
To recover the proper state after a restart, the applications persists an event
log that can be replayed at startup.
*/

pub mod cluster;
pub mod data_types;
pub mod messages;

use std::{collections::HashMap, sync::Arc};

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
    pub node_id: NodeId,
    pub ip_address: String,
}

impl Node {
    pub fn new(data_layer: Arc<DataLayer>, node_id: NodeId) -> Self {
        let persisted_data = data_layer.get_node(node_id).unwrap();
        let ip_address = persisted_data.ip_address.clone();
        Self {
            node_id,
            persisted_data,
            refresh_status: RefreshStatus::Updated,
            ip_address,
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
    pub topic_id: TopicId,
    pub name: String,
    pub partitions: Arc<HashMap<PartitionId, Partition>>,
}

impl Topic {
    pub fn new(data_layer: Arc<DataLayer>, topic_id: TopicId) -> Self {
        let persisted_data = data_layer.get_topic(topic_id).unwrap();

        let partitions: Arc<HashMap<PartitionId, Partition>> = Arc::new(
            persisted_data
                .partitions
                .iter()
                .map(|&partition_id| {
                    (
                        partition_id,
                        Partition::new(data_layer.clone(), topic_id, partition_id),
                    )
                })
                .collect(),
        );
        let name = persisted_data.name.clone();

        Self {
            persisted_data,
            refresh_status: RefreshStatus::Updated,
            topic_id,
            name,
            partitions,
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: Arc<DataLayer>) {}
}

#[derive(Debug)]
pub struct Partition {
    persisted_data: persisted_entities::Partition,
    refresh_status: RefreshStatus,
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalogs: Arc<HashMap<CatalogId, Catalog>>,
}

impl Partition {
    pub fn new(data_layer: Arc<DataLayer>, topic_id: TopicId, partition_id: PartitionId) -> Self {
        let persisted_data = data_layer.get_partition(topic_id, partition_id).unwrap();

        let catalogs: Arc<HashMap<CatalogId, Catalog>> = Arc::new(
            persisted_data
                .catalogs
                .iter()
                .map(|&catalog_id| {
                    (
                        catalog_id,
                        Catalog::new(data_layer.clone(), topic_id, partition_id, catalog_id),
                    )
                })
                .collect(),
        );

        Self {
            persisted_data,
            refresh_status: RefreshStatus::Updated,
            topic_id,
            partition_id,
            catalogs,
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: Arc<DataLayer>) {}
}

#[derive(Debug)]
pub struct Catalog {
    persisted_data: persisted_entities::Catalog,
    refresh_status: RefreshStatus,
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalog_id: CatalogId,
    pub node_id: NodeId,
}

impl Catalog {
    pub fn new(
        data_layer: Arc<DataLayer>,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> Self {
        let persisted_data = data_layer
            .get_catalog(topic_id, partition_id, catalog_id)
            .unwrap();
        let node_id = persisted_data.node_id;

        Self {
            persisted_data,
            topic_id,
            partition_id,
            catalog_id,
            node_id,
            refresh_status: RefreshStatus::Updated,
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: Arc<DataLayer>) {}
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

    pub fn refresh(self: &mut Self, _data_layer: Arc<DataLayer>) {}
}
