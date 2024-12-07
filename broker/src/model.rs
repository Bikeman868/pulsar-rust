/*
Defines the shared internal state of the application. Some of this state is
persisted to the database, and some is just updated in memory.
To recover the proper state after a restart, the applications persists an event
log that can be replayed at startup.
*/

pub mod cluster;
pub mod messages;
pub mod responses;

use std::{collections::{hash_map, HashMap}, sync::Arc};
use pulsar_rust_net::data_types::{CatalogId, NodeId, PartitionId, PortNumber, SubscriptionId, TopicId};
use crate::{data::DataLayer, persistence::persisted_entities};

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
    pub admin_port: PortNumber,
    pub pubsub_port: PortNumber,
    pub sync_port: PortNumber,
}

impl Node {
    pub fn new(data_layer: Arc<DataLayer>, node_id: NodeId) -> Self {
        let persisted_data = data_layer.get_node(node_id).unwrap();
        let ip_address = persisted_data.ip_address.clone();
        let admin_port = persisted_data.admin_port;
        let pubsub_port = persisted_data.pubsub_port;
        let sync_port = persisted_data.sync_port;
        Self {
            node_id,
            persisted_data,
            refresh_status: RefreshStatus::Updated,
            ip_address,
            admin_port,
            pubsub_port,
            sync_port,
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
    pub partitions: Arc<PartitionList>,
}

impl Topic {
    pub fn new(data_layer: Arc<DataLayer>, topic_id: TopicId) -> Self {
        let persisted_data = data_layer.get_topic(topic_id).unwrap();

        let partitions = Arc::new(PartitionList::new(persisted_data
            .partitions
            .iter()
            .map(|&partition_id| Partition::new(data_layer.clone(), topic_id, partition_id))));

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
    pub catalogs: Arc<CatalogList>,
}

impl Partition {
    pub fn new(data_layer: Arc<DataLayer>, topic_id: TopicId, partition_id: PartitionId) -> Self {
        let persisted_data = data_layer.get_partition(topic_id, partition_id).unwrap();

        let catalogs = Arc::new(CatalogList::new(
            persisted_data
                .catalogs
                .iter()
                .map(|&catalog_id| Catalog::new(data_layer.clone(), topic_id, partition_id, catalog_id))));

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

#[derive(Debug)]
pub struct NodeList {
    hash_by_node_id: HashMap<NodeId, Node>,
}

impl NodeList {
    pub fn new (nodes: impl Iterator<Item = Node>) -> Self{
        Self { hash_by_node_id: nodes.map(|n|(n.node_id, n)).collect() }
    }

    pub fn by_id(self: &Self, node_id: NodeId) -> Option<&Node> {
        Some(self.hash_by_node_id.get(&node_id)?)
    }

    pub fn iter(self: &Self) -> hash_map::Values<'_, NodeId, Node>  {
        self.hash_by_node_id.values()
    }
}

#[derive(Debug)]
pub struct TopicList {
    hash_by_topic_id: HashMap<TopicId, Topic>,
}

impl TopicList {
    pub fn new (topics: impl Iterator<Item = Topic>) -> Self{
        Self { hash_by_topic_id: topics.map(|t|(t.topic_id, t)).collect() }
    }

    pub fn by_id(self: &Self, topic_id: TopicId) -> Option<&Topic> {
        Some(self.hash_by_topic_id.get(&topic_id)?)
    }

    pub fn iter(self: &Self) -> hash_map::Values<'_, TopicId, Topic>  {
        self.hash_by_topic_id.values()
    }
}

#[derive(Debug)]
pub struct PartitionList {
    hash_by_partition_id: HashMap<PartitionId, Partition>,
}

impl PartitionList {
    pub fn new (partitions: impl Iterator<Item = Partition>) -> Self{
        Self { hash_by_partition_id: partitions.map(|p|(p.partition_id, p)).collect() }
    }

    pub fn by_id(self: &Self, partition_id: PartitionId) -> Option<&Partition> {
        Some(self.hash_by_partition_id.get(&partition_id)?)
    }

    pub fn iter(self: &Self) -> hash_map::Values<'_, PartitionId, Partition>  {
        self.hash_by_partition_id.values()
    }
}

#[derive(Debug)]
pub struct CatalogList {
    hash_by_catalog_id: HashMap<CatalogId, Catalog>,
}

impl CatalogList {
    pub fn new (catalogs: impl Iterator<Item = Catalog>) -> Self{
        Self { hash_by_catalog_id: catalogs.map(|c|(c.catalog_id, c)).collect() }
    }

    pub fn by_id(self: &Self, catalog_id: CatalogId) -> Option<&Catalog> {
        Some(self.hash_by_catalog_id.get(&catalog_id)?)
    }

    pub fn iter(self: &Self) -> hash_map::Values<'_, CatalogId, Catalog>  {
        self.hash_by_catalog_id.values()
    }
}