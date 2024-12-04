/*
Provides the functionallity needed to support message publishers. Various APIs can
be layered on top of this service to expose this funtionallity to applicatins.
*/

use crate::model::{
    cluster::Cluster,
    data_types::{PartitionId, TopicId},
    Partition,
};
use std::{collections::HashMap, sync::Arc};

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
        Some(self.cluster.topics.get(&topic_id)?.partitions.clone())
    }
}
