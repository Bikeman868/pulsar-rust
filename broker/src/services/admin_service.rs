/*
This module provides a service for cluster administration. Various APIs can be built on
top of this service layer to provide access to the admin functionallity. When the cluster
configuration is changed, this service persists the changes to the database, communicates
the change to other nodes in the cluster, and coordinates the handoff between nodes if
appropriate.
*/

use std::sync::Arc;

use pulsar_rust_net::data_types::{
    LedgerId, NodeId, PartitionId, TopicId
};
use crate::model::{
    cluster::Cluster, 
    ledger::{Ledger, LedgerList}, 
    node::{Node, NodeList}, 
    partition::{Partition, PartitionList}, 
    topic::{Topic, TopicList},
};

pub struct AdminService {
    cluster: Arc<Cluster>,
}

impl AdminService {
    pub fn new(cluster: &Arc<Cluster>) -> Self {
        Self { cluster: Arc::clone(cluster) }
    }

    pub fn all_nodes(self: &Self) -> &Arc<NodeList> {
        self.cluster.nodes()
    }

    pub fn all_topics(self: &Self) -> &Arc<TopicList> {
        self.cluster.topics()
    }

    pub fn node_by_id(self: &Self, node_id: NodeId) -> Option<&Node> {
        self.cluster.nodes().by_id(node_id)
    }

    pub fn topic_by_id(self: &Self, topic_id: TopicId) -> Option<&Topic> {
        self.cluster.topics().by_id(topic_id)
    }

    pub fn partition_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<&Partition> {
        self.cluster.topics().by_id(topic_id)?.partitions().by_id(partition_id)
    }

    pub fn ledger_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
    ) -> Option<&Ledger> {
        self.cluster.topics().by_id(topic_id)?.partitions().by_id(partition_id)?.ledgers().by_id(ledger_id)
    }

    pub fn partitions_by_topic_name<'a>(
        self: &Self,
        topic_name: &'a str,
    ) -> Option<&Arc<PartitionList>> {
        Some(self.cluster.topics().iter().find(|topic| topic.name() == topic_name)?.partitions())
    }

    pub fn partitions_by_topic_id<'a>(
        self: &Self,
        topic_id: TopicId,
    ) -> Option<&Arc<PartitionList>> {
        Some(self.topic_by_id(topic_id)?.partitions())
    }

    pub fn ledgers_by_partition_id<'a>(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<&Arc<LedgerList>> {
        Some(self.partition_by_id(topic_id, partition_id)?.ledgers())
    }
}
