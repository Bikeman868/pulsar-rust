/*
This module provides a service for cluster administration. Various APIs can be built on
top of this service layer to provide access to the admin functionallity. When the cluster
configuration is changed, this service persists the changes to the database, communicates
the change to other nodes in the cluster, and coordinates the handoff between nodes if
appropriate.
*/

use std::sync::Arc;

use crate::model::{
    cluster::Cluster,
    ledger::LedgerRef,
    node::{NodeList, NodeRef},
    partition::PartitionRef,
    topic::{TopicList, TopicRef},
};
use pulsar_rust_net::data_types::{LedgerId, NodeId, PartitionId, TopicId};

pub struct AdminService {
    cluster: Arc<Cluster>,
}

impl AdminService {
    pub fn new(cluster: &Arc<Cluster>) -> Self {
        Self {
            cluster: Arc::clone(cluster),
        }
    }

    pub fn all_nodes(self: &Self) -> &NodeList {
        self.cluster.nodes()
    }

    pub fn all_topics(self: &Self) -> &TopicList {
        self.cluster.topics()
    }

    pub fn node_by_id(self: &Self, node_id: NodeId) -> Option<NodeRef> {
        self.cluster.nodes().get(&node_id)
    }

    pub fn topic_by_id(self: &Self, topic_id: TopicId) -> Option<TopicRef> {
        self.cluster.topics().get(&topic_id)
    }

    pub fn topic_by_name(self: &Self, name: &str) -> Option<TopicRef> {
        self.cluster.topics().find(|topic|topic.name() == name)
    }

    pub fn partition_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<PartitionRef> {
        self.cluster
            .topics()
            .get(&topic_id)?
            .partitions()
            .get(&partition_id)
    }

    pub fn ledger_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
    ) -> Option<LedgerRef> {
        self.cluster
            .topics()
            .get(&topic_id)?
            .partitions()
            .get(&partition_id)?
            .ledgers()
            .get(&ledger_id)
    }
}
