use serde::{Deserialize, Serialize};

use crate::persistence::{Keyed, Versioned};
use pulsar_rust_net::data_types::{
    ConsumerId, LedgerId, NodeId, PartitionId, PortNumber, SubscriptionId, TopicId, VersionNumber,
};

use super::Key;

// TODO: Write a macro to generate entities

/// Represents a collection of machines that collborate in serving broker functionality. There is
/// only one cluster record in the database.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Cluster {
    pub version: VersionNumber,
    pub name: String,
    pub node_ids: Vec<NodeId>,
    pub topic_ids: Vec<TopicId>,
    pub next_node_id: NodeId,
    pub next_topic_id: TopicId,
}

#[rustfmt::skip]
impl Cluster {
    pub fn new(
        name: &str,
        node_ids: Vec<NodeId>,
        topic_ids: Vec<TopicId>,
        next_node_id: NodeId,
        next_topic_id: TopicId,
    ) -> Self {
        Self {
            version: 0,
            name: name.to_string(),
            node_ids,
            topic_ids,
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
/// to balance the load. When partitions transition between nodes, a new ledger is created.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Node {
    pub version: VersionNumber,
    pub node_id: NodeId,
    pub ip_address: String,
    pub admin_port: PortNumber,
    pub pubsub_port: PortNumber,
    pub sync_port: PortNumber,
}

#[rustfmt::skip]
impl Node {
    pub fn new(
        node_id: NodeId, 
        ip_address: &str,
        admin_port: PortNumber,
        pubsub_port: PortNumber,
        sync_port: PortNumber,
    ) -> Self {
        Self {
            version: 0,
            node_id,
            ip_address: ip_address.to_owned(),
            admin_port,
            pubsub_port,
            sync_port,
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
    pub partition_ids: Vec<PartitionId>,
    pub subscription_ids: Vec<SubscriptionId>,
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
            partition_ids: partitions,
            subscription_ids: subscriptions,
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
    pub ledger_ids: Vec<LedgerId>,
    pub next_ledger_id: LedgerId,
    pub node_id: NodeId,
}

#[rustfmt::skip]
impl Partition {
    pub fn new(topic_id: TopicId, partition_id: PartitionId, ledger_ids: Vec<LedgerId>, next_ledger_id: LedgerId, node_id: NodeId) -> Self {
        Self {
            version: 0,
            partition_id,
            topic_id,
            ledger_ids,
            next_ledger_id,
            node_id,
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

/// A ledger is a collection of messages that belong to a partition and a time
/// interval. When partitions are transitioned to a new node, a new ledger is created.
/// New ledgers can also be created if we run out of message ids within the ledger.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Ledger {
    pub version: VersionNumber,
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub ledger_id: LedgerId,
    pub node_id: NodeId,
}

#[rustfmt::skip]
impl Ledger {
    pub fn new(
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
        node_id: NodeId,
    ) -> Self {
        Self {
            version: 0,
            topic_id,
            partition_id,
            ledger_id,
            node_id,
        }
    }
    pub fn key(topic_id: TopicId, partition_id: PartitionId, ledger_id: LedgerId) -> impl Keyed {
        Key {
            type_name: <Ledger>::type_name(),
            key: Ledger::key_from(topic_id, partition_id, ledger_id),
        }
    }
    fn type_name() -> &'static str { "Ledger" }
    fn key_from(topic_id: TopicId, partition_id: PartitionId, ledger_id: LedgerId) -> String { 
        topic_id.to_string()
            + ":"
            + &partition_id.to_string()
            + ":"
            + &ledger_id.to_string()
    }
}

#[rustfmt::skip]
impl Keyed for Ledger {
    fn type_name(self: &Self) -> &'static str { Ledger::type_name() }
    fn key(self: &Self) -> String { Ledger::key_from(self.topic_id, self.partition_id, self.ledger_id) }
}

#[rustfmt::skip]
impl Versioned for Ledger {
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
    pub has_key_affinity: bool,
    pub next_consumer_id: ConsumerId,
}

#[rustfmt::skip]
impl Subscription {
    pub fn new(
        topic_id: TopicId, 
        subscription_id: SubscriptionId, 
        name: String, 
        has_key_affinity: bool,
        next_consumer_id: ConsumerId,
    ) -> Self {
        Self {
            version: 0,
            topic_id,
            subscription_id,
            name,
            has_key_affinity,
            next_consumer_id,
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
