use serde::{Deserialize, Serialize};

use crate::{
    model::data_types::{CatalogId, NodeId, PartitionId, SubscriptionId, TopicId, VersionNumber},
    persistence::{Keyed, Versioned},
};

use super::Key;

// TODO: Write a macro to generate entities
// =====

/// Represents a collection of machines that collborate in serving broker functionality. There is
/// only one cluster record in the database.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Cluster {
    pub version: VersionNumber,
    pub name: String,
    pub nodes: Vec<NodeId>,
    pub topics: Vec<TopicId>,
    pub next_node_id: NodeId,
    pub next_topic_id: TopicId,
}

#[rustfmt::skip]
impl Cluster {
    pub fn new(
        name: &str,
        nodes: Vec<NodeId>,
        topics: Vec<TopicId>,
        next_node_id: NodeId,
        next_topic_id: TopicId,
    ) -> Self {
        Self {
            version: 0,
            name: name.to_string(),
            nodes,
            topics,
            next_node_id,
            next_topic_id,
        }
    }
    pub fn key(name: &str) -> impl Keyed {
        Key {
            type_name: <Cluster>::type_name(),
            key: Cluster::key_from(name),
        }
    }
    fn type_name() -> &'static str { "Cluster" }
    fn key_from(name: &str) -> String { name.to_owned() }
}

#[rustfmt::skip]
impl Keyed for Cluster {
    fn type_name(self: &Self) -> &'static str { Cluster::type_name() }
    fn key(self: &Self) -> String { Cluster::key_from(&self.name) }
}

#[rustfmt::skip]
impl Versioned for Cluster {
    fn version(self: &Self) -> VersionNumber { self.version }
    fn set_version(self: &mut Self, version: VersionNumber) { self.version = version }
}

/// Represents a machine in the cluster that runs the application. Each topic/partition
/// is handled by one node at any point in time, but partitions can transition between nodes
/// to balance the load. When partitions transition between nodes, a new catalog is created.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Node {
    pub version: VersionNumber,
    pub node_id: NodeId,
    pub ip_address: String,
}

#[rustfmt::skip]
impl Node {
    pub fn new(node_id: NodeId, ip_address: &str) -> Self {
        Self {
            version: 0,
            node_id,
            ip_address: ip_address.to_owned(),
        }
    }
    pub fn key(node_id: NodeId) -> impl Keyed {
        Key {
            type_name: <Node>::type_name(),
            key: Node::key_from(node_id),
        }
    }
    fn type_name() -> &'static str { "Node" }
    fn key_from(node_id: NodeId) -> String { node_id.to_string() }
}

#[rustfmt::skip]
impl Keyed for Node {
    fn type_name(self: &Self) -> &'static str { Node::type_name() }
    fn key(self: &Self) -> String { Node::key_from(self.node_id) }
}

#[rustfmt::skip]
impl Versioned for Node {
    fn version(self: &Self) -> VersionNumber { self.version }
    fn set_version(self: &mut Self, version: VersionNumber) { self.version = version }
}

/// Topics are pub/sub channels with multiple publishers and multiple subscribers
/// Every message that is published to the topic will be delivered to every subscriber
/// of that topic at least once
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Topic {
    pub version: VersionNumber,
    pub topic_id: TopicId,
    pub name: String,
    pub partitions: Vec<PartitionId>,
    pub subscriptions: Vec<SubscriptionId>,
    pub next_partition_id: PartitionId,
    pub next_subscription_id: SubscriptionId,
}

#[rustfmt::skip]
impl Topic {
    pub fn new(
        topic_id: TopicId,
        name: String,
        partitions: Vec<PartitionId>,
        subscriptions: Vec<SubscriptionId>,
        next_partition_id: PartitionId,
        next_subscription_id: SubscriptionId,
    ) -> Self {
        Self {
            version: 0,
            topic_id,
            name,
            partitions,
            subscriptions,
            next_partition_id,
            next_subscription_id,
        }
    }
    pub fn key(topic_id: TopicId) -> impl Keyed {
        Key {
            type_name: <Topic>::type_name(),
            key: Topic::key_from(topic_id),
        }
    }
    fn type_name() -> &'static str { "Topic" }
    fn key_from(topic_id: TopicId) -> String { topic_id.to_string() }
}

#[rustfmt::skip]
impl Keyed for Topic {
    fn type_name(self: &Self) -> &'static str { Topic::type_name() }
    fn key(self: &Self) -> String { Topic::key_from(self.topic_id) }
}

#[rustfmt::skip]
impl Versioned for Topic {
    fn version(self: &Self) -> VersionNumber { self.version }
    fn set_version(self: &mut Self, version: VersionNumber) { self.version = version }
}

/// Topics are sharded into partitions by hashing the message key. When the number
/// of partitions is changed, all message consumption has to stop until existing
/// messages are consumed. Message publishing can continue, and is accumulated
/// as a backlog until older messages are drained.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Partition {
    pub version: VersionNumber,
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalogs: Vec<CatalogId>,
    pub next_catalog_id: CatalogId,
}

#[rustfmt::skip]
impl Partition {
    pub fn new(topic_id: TopicId, partition_id: PartitionId, catalog_ids: Vec<CatalogId>, next_catalog_id: CatalogId) -> Self {
        Self {
            version: 0,
            partition_id,
            topic_id,
            catalogs: catalog_ids,
            next_catalog_id,
        }
    }
    pub fn key(topic_id: TopicId, partition_id: PartitionId) -> impl Keyed {
        Key {
            type_name: <Partition>::type_name(),
            key: Partition::key_from(topic_id, partition_id),
        }
    }
    fn type_name() -> &'static str { "Partition" }
    fn key_from(topic_id: TopicId, partition_id: PartitionId) -> String { 
        topic_id.to_string() + ":" + &partition_id.to_string()
    }
}

#[rustfmt::skip]
impl Keyed for Partition {
    fn type_name(self: &Self) -> &'static str { Partition::type_name() }
    fn key(self: &Self) -> String { Partition::key_from(self.topic_id, self.partition_id) }
}

#[rustfmt::skip]
impl Versioned for Partition {
    fn version(self: &Self) -> VersionNumber { self.version }
    fn set_version(self: &mut Self, version: VersionNumber) { self.version = version }
}

/// A catalog is a collection of messages that belong to a partition and a time
/// interval. When partitions are transitioned to a new node, a new catalog is created.
/// New catalogs can also be created if we run out of message ids within the catalog.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Catalog {
    pub version: VersionNumber,
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub catalog_id: CatalogId,
    pub node_id: NodeId,
}

#[rustfmt::skip]
impl Catalog {
    pub fn new(
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
        node_id: NodeId,
    ) -> Self {
        Self {
            version: 0,
            topic_id,
            partition_id,
            catalog_id,
            node_id,
        }
    }
    pub fn key(topic_id: TopicId, partition_id: PartitionId, catalog_id: CatalogId) -> impl Keyed {
        Key {
            type_name: <Catalog>::type_name(),
            key: Catalog::key_from(topic_id, partition_id, catalog_id),
        }
    }
    fn type_name() -> &'static str { "Catalog" }
    fn key_from(topic_id: TopicId, partition_id: PartitionId, catalog_id: CatalogId) -> String { 
        topic_id.to_string()
            + ":"
            + &partition_id.to_string()
            + ":"
            + &catalog_id.to_string()
    }
}

#[rustfmt::skip]
impl Keyed for Catalog {
    fn type_name(self: &Self) -> &'static str { Catalog::type_name() }
    fn key(self: &Self) -> String { Catalog::key_from(self.topic_id, self.partition_id, self.catalog_id) }
}

#[rustfmt::skip]
impl Versioned for Catalog {
    fn version(self: &Self) -> VersionNumber { self.version }
    fn set_version(self: &mut Self, version: VersionNumber) { self.version = version }
}

/// A subscription connects an application to a topic. Each message published to the
/// topic will be delivered at least once to each sunscription associated with that topic
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Subscription {
    pub version: VersionNumber,
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub name: String,
}

#[rustfmt::skip]
impl Subscription {
    pub fn new(topic_id: TopicId, subscription_id: SubscriptionId, name: String) -> Self {
        Self {
            version: 0,
            topic_id,
            subscription_id,
            name,
        }
    }
    pub fn key(topic_id: TopicId, subscription_id: SubscriptionId) -> impl Keyed {
        Key {
            type_name: <Subscription>::type_name(),
            key: Subscription::key_from(topic_id, subscription_id),
        }
    }
    fn type_name() -> &'static str { "Subscription" }
    fn key_from(topic_id: TopicId, subscription_id: SubscriptionId) -> String { 
        topic_id.to_string() + ":" + &subscription_id.to_string()
    }
}

#[rustfmt::skip]
impl Keyed for Subscription {
    fn type_name(self: &Self) -> &'static str { Subscription::type_name() }
    fn key(self: &Self) -> String { Subscription::key_from(self.topic_id, self.subscription_id) }
}

#[rustfmt::skip]
impl Versioned for Subscription {
    fn version(self: &Self) -> VersionNumber { self.version }
    fn set_version(self: &mut Self, version: VersionNumber) { self.version = version }
}
