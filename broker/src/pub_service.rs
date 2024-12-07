/*
Provides the functionallity needed to support message publishers. Various APIs can
be layered on top of this service to expose this funtionallity to applicatins.
*/

use std::sync::Arc;
use pulsar_rust_net::data_types::TopicId;
use crate::model::{
    cluster::Cluster,
    PartitionList,
};

pub struct PubService {
    cluster: Arc<Cluster>,
}

impl PubService {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
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
        Some(self.cluster.topics.by_id(topic_id)?.partitions.clone())
    }
}
