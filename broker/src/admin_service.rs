/*
This module provides a service for cluster administration. Various APIs can be built on
top of this service layer to provide access to the admin functionallity. When the cluster
configuration is changed, this service persists the changes to the database, communicates
the change to other nodes in the cluster, and coordinates the handoff between nodes if
appropriate.
*/

use std::{collections::HashMap, sync::Arc};

use pulsar_rust_net::data_types::{CatalogId, NodeId, PartitionId, TopicId};
use crate::model::{
    cluster::Cluster, Catalog, CatalogList, Node, NodeList, Partition, PartitionList, Topic, TopicList
};

pub struct AdminService {
    cluster: Arc<Cluster>,
}

impl AdminService {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }

    pub fn all_nodes(self: &Self) -> Arc<NodeList> {
        self.cluster.nodes.clone()
    }

    pub fn all_topics(self: &Self) -> Arc<TopicList> {
        self.cluster.topics.clone()
    }

    pub fn node_by_id(self: &Self, node_id: NodeId) -> Option<&Node> {
        self.cluster.nodes.by_id(node_id)
    }

    pub fn topic_by_id(self: &Self, topic_id: TopicId) -> Option<&Topic> {
        self.cluster.topics.by_id(topic_id)
    }

    pub fn partition_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<&Partition> {
        self.cluster.topics.by_id(topic_id)?.partitions.by_id(partition_id)
    }

    pub fn catalog_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> Option<&Catalog> {
        self.cluster.topics.by_id(topic_id)?.partitions.by_id(partition_id)?.catalogs.by_id(catalog_id)
    }

    pub fn partitions_by_topic_name<'a>(
        self: &Self,
        topic_name: &'a str,
    ) -> Option<Arc<PartitionList>> {
        Some(self.cluster.topics.iter().find(|topic| topic.name == topic_name)?.partitions.clone())
    }

    pub fn partitions_by_topic_id<'a>(
        self: &Self,
        topic_id: TopicId,
    ) -> Option<Arc<PartitionList>> {
        Some(self.topic_by_id(topic_id)?.partitions.clone())
    }

    pub fn catalogs_by_partition_id<'a>(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<Arc<CatalogList>> {
        Some(self.partition_by_id(topic_id, partition_id)?.catalogs.clone())
    }
}
