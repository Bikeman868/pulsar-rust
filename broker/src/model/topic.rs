use super::{
    partition::{Partition, PartitionList, PartitionStats}, 
    subscription::{
        key_shared, shared, Subscription, SubscriptionList, SubscriptionStats
    }, 
    Entity, EntityList, EntityRef
};
use crate::{data::DataLayer, formatting::plain_text_builder::{PlainTextBuilder, ToPlainText}};
use pulsar_rust_net::data_types::{PartitionId, SubscriptionId, TopicId};
use serde::Serialize;
use std::sync::Arc;

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Serialize)]
pub struct TopicPartitionStats {
    pub partition_id: PartitionId,
    pub partition_stats: PartitionStats,
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Serialize)]
pub struct TopicSubscriptionStats {
    pub subscription_id: SubscriptionId,
    pub subscription_stats: SubscriptionStats,
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Serialize)]
pub struct TopicStats {
    pub name: String,
    pub partitions: Vec<TopicPartitionStats>,
    pub subscriptions: Vec<TopicSubscriptionStats>,
}

impl ToPlainText for TopicStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Name", 15);
        builder.new_line();
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.str_left(&self.name, 15);
        builder.new_line();

        builder.indent();

        if self.partitions.len() > 0 {
            TopicPartitionStats::to_plain_text_header(builder);
            for partition in &self.partitions {
                partition.to_plain_text(builder);
            }
        }

        if self.subscriptions.len() > 0 {
            TopicSubscriptionStats::to_plain_text_header(builder);
            for subscription in &self.subscriptions {
                subscription.to_plain_text(builder);
            }
        }

        builder.outdent();
    }
}

impl ToPlainText for TopicPartitionStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Partition id", 15);
        PartitionStats::to_plain_text_header(builder);
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.u16_left(self.partition_id, 15);
        self.partition_stats.to_plain_text(builder);
    }
}

impl ToPlainText for TopicSubscriptionStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Subsription id", 17);
        SubscriptionStats::to_plain_text_header(builder);
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.u32_left(self.subscription_id, 17);
        self.subscription_stats.to_plain_text(builder);
    }
}

/// Represents a queue of messages sharded into partitions for scale. Publishers can publish
/// messages to the topic, and each message is guaranteed to be delivered at least once to
/// each subscriber.
pub struct Topic {
    topic_id: TopicId,
    name: String,
    partitions: PartitionList,
    subscriptions: SubscriptionList,
}

impl Entity<TopicId> for Topic { 
    fn key(self: &Self) -> TopicId { self.topic_id }
}

pub type TopicRef = EntityRef<TopicId, Topic>;
pub type TopicList = EntityList<TopicId, Topic>;

impl Topic {
    pub fn topic_id(self: &Self) -> TopicId {
        self.topic_id
    }

    pub fn name(self: &Self) -> &str {
        &self.name
    }

    pub fn partitions(self: &Self) ->  &PartitionList {
        &self.partitions
    }

    pub fn subscriptions(self: &Self) ->  &SubscriptionList {
        &self.subscriptions
    }

    pub fn new(data_layer: &Arc<DataLayer>, topic_id: TopicId) -> Self {
        let topic = data_layer.get_topic(topic_id).unwrap();

        let partitions = 
            EntityList::from_iter(topic.partition_ids.iter().map(
            |&partition_id| Partition::new(data_layer, topic_id, partition_id)),
        );

        let subscriptions = EntityList::from_iter(topic.subscription_ids.iter().map(
            |&subscription_id| {
                let subscription = data_layer
                    .get_subscription(topic_id, subscription_id)
                    .unwrap();
                if subscription.has_key_affinity {
                    Subscription::KeyShared(key_shared::Subscription::new(
                        data_layer,
                        topic_id,
                        subscription_id,
                    ))
                } else {
                    Subscription::Shared(shared::Subscription::new(
                        data_layer,
                        topic_id,
                        subscription_id,
                    ))
                }
            },
        ));

        let name = topic.name.clone();

        Self {
            topic_id,
            name,
            partitions,
            subscriptions,
        }
    }

    pub fn active_subscription_ids(self: &Self) -> Vec<SubscriptionId> {
        self.subscriptions.keys()
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}

    pub fn stats(self: &Self) -> TopicStats {
        let partitions = self.partitions.values().iter()
            .map(|partition| TopicPartitionStats {
                    partition_id: partition.partition_id(),
                    partition_stats: partition.stats(),
            })
            .collect();
        let subscriptions = self.subscriptions.values().iter()
            .map(|subscription| TopicSubscriptionStats {
                    subscription_id: subscription.subscription_id(),
                    subscription_stats: subscription.stats(),
            })
            .collect();
        TopicStats {
            name: self.name.clone(),
            partitions,
            subscriptions,
        }
    }
}
