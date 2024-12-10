use std::{
    collections::{hash_map, HashMap}, 
    sync::Arc
};
use pulsar_rust_net::data_types::TopicId;
use crate::data::DataLayer;
use super::partition::{Partition, PartitionList};

/// Represents a queue of messages sharded into partitions for scale. Publishers can publish
/// messages to the topic, and each message is guaranteed to be delivered at least once to
/// each subscriber.
#[derive(Debug)]
pub struct Topic {
    topic_id: TopicId,
    name: String,
    partitions: Arc<PartitionList>,
}

impl Topic {
    pub fn topic_id(self: &Self) -> TopicId { self.topic_id }
    pub fn name(self: &Self) -> &str { &self.name }
    pub fn partitions(self: &Self) -> &Arc<PartitionList> { &self.partitions }

    pub fn new(data_layer: &Arc<DataLayer>, topic_id: TopicId) -> Self {
        let topic = data_layer.get_topic(topic_id).unwrap();

        let partitions = Arc::new(PartitionList::new(
            topic.partition_ids
            .iter()
            .map(|&partition_id| Partition::new(data_layer, topic_id, partition_id))));

        let name = topic.name.clone();

        Self {
            topic_id,
            name,
            partitions,
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}
}

/// Represents the list of topics that can be pushished to on a cluster
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
