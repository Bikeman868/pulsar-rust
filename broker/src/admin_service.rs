/*
This module provides a service for cluster administration. Various APIs can be built on
top of this service layer to provide access to the admin functionallity. When the cluster
configuration is changed, this service persists the changes to the database, communicates
the change to other nodes in the cluster, and coordinates the handoff between nodes if
appropriate.
*/

use std::{collections::HashMap, sync::Arc};

use crate::model::{
    cluster::Cluster,
    data_types::{CatalogId, NodeId, PartitionId, TopicId},
    Catalog, Node, Partition, Topic,
};

pub struct AdminService {
    cluster: Arc<Cluster>,
}

impl AdminService {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }

    pub fn all_nodes(self: &Self) -> Arc<HashMap<NodeId, Node>> {
        self.cluster.nodes.clone()
    }

    pub fn all_topics(self: &Self) -> Arc<HashMap<TopicId, Topic>> {
        self.cluster.topics.clone()
    }

    pub fn node_by_id(self: &Self, node_id: NodeId) -> Option<&Node> {
        self.cluster.nodes.get(&node_id)
    }

    pub fn topic_by_id(self: &Self, topic_id: TopicId) -> Option<&Topic> {
        self.cluster.topics.get(&topic_id)
    }

    pub fn partition_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<&Partition> {
        self.cluster
            .topics
            .get(&topic_id)?
            .partitions
            .get(&partition_id)
    }

    pub fn catalog_by_id(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> Option<&Catalog> {
        self.cluster
            .topics
            .get(&topic_id)?
            .partitions
            .get(&partition_id)?
            .catalogs
            .get(&catalog_id)
    }

    pub fn partitions_by_topic_name<'a>(
        self: &Self,
        topic_name: &'a str,
    ) -> Option<Arc<HashMap<PartitionId, Partition>>> {
        Some(
            self.cluster
                .topics
                .values()
                .find(|topic| topic.name == topic_name)?
                .partitions
                .clone(),
        )
    }

    pub fn partitions_by_topic_id<'a>(
        self: &Self,
        topic_id: TopicId,
    ) -> Option<Arc<HashMap<PartitionId, Partition>>> {
        Some(self.topic_by_id(topic_id)?.partitions.clone())
    }

    pub fn catalogs_by_partition_id<'a>(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<Arc<HashMap<CatalogId, Catalog>>> {
        Some(
            self.partition_by_id(topic_id, partition_id)?
                .catalogs
                .clone(),
        )
    }
}
