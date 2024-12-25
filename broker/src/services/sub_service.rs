/*
Provides the functionallity needed to support message subscribers. Various APIs can
be layered on top of this service to expose this funtionallity to applicatins.
*/

use std::sync::Arc;

use pulsar_rust_net::data_types::{ConsumerId, SubscriptionId, TopicId};

use crate::{
    model::{
        cluster::Cluster,
        messages::{MessageRef, PublishedMessage, SubscribedMessage},
        node::NodeList,
        subscription::SubscriptionRef,
        topic::{TopicList, TopicRef},
    },
    persistence::{log_entries::LoggedEvent, logged_events, PersistenceLayer},
};

pub enum SubError {
    Error(String),
    TopicNotFound,
    SubscriptionNotFound,
    PartitionNotFound,
    LedgerNotFound,
    MessageNotFound,
    NoneAvailable,
}

pub type SubOk = (SubscribedMessage, PublishedMessage);

pub type SubResult = Result<SubOk, SubError>;
pub type AckResult = Result<bool, SubError>;
pub type NackResult = Result<bool, SubError>;

pub struct SubService {
    persistence: Arc<PersistenceLayer>,
    cluster: Arc<Cluster>,
}

impl SubService {
    pub fn new(persistence: &Arc<PersistenceLayer>, cluster: &Arc<Cluster>) -> Self {
        Self {
            persistence: Arc::clone(persistence),
            cluster: Arc::clone(cluster),
        }
    }

    pub fn all_nodes(self: &Self) -> &NodeList {
        self.cluster.nodes()
    }

    pub fn all_topics(self: &Self) -> &TopicList {
        self.cluster.topics()
    }

    pub fn topic_by_name(self: &Self, name: &str) -> Option<TopicRef> {
        self.cluster.topics().find(|topic|topic.name() == name)
    }

    pub fn subscription_by_name<'a>(self: &Self, topic_name: &'a str, subscription_name: &'a str) -> Option<SubscriptionRef> {
        let topic = self.topic_by_name(topic_name)?;
        topic.subscriptions().find(|subscription| subscription.name() == subscription_name)
    }

    pub fn allocate_consumer_id(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
    ) -> Option<ConsumerId> {
        self.cluster
            .topics()
            .get(&topic_id)?
            .subscriptions()
            .get(&subscription_id)?
            .connect_consumer()
    }

    pub fn next_message(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> SubResult {
        match self.cluster.topics().get(&topic_id) {
            Some(topic) => match topic.subscriptions().get(&subscription_id) {
                Some(subscription) => match subscription.pop(consumer_id) {
                    Some(message) => {
                        let message_ref = MessageRef::from_key(&message.message_ref_key);
                        match topic.partitions().get(&message_ref.partition_id) {
                            Some(partition) => {
                                match partition.ledgers().get(&message_ref.ledger_id) {
                                    Some(ledger) => {
                                        match ledger.get_message(&message_ref.message_id) {
                                            Some(published_message) => {
                                                Ok((message, published_message))
                                            }
                                            None => Err(SubError::LedgerNotFound),
                                        }
                                    }
                                    None => Err(SubError::LedgerNotFound),
                                }
                            }
                            None => Err(SubError::PartitionNotFound),
                        }
                    }
                    None => Err(SubError::NoneAvailable),
                },
                None => Err(SubError::SubscriptionNotFound),
            },
            None => Err(SubError::TopicNotFound),
        }
    }

    pub fn ack(
        self: &Self,
        message_ref_key: String,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> AckResult {
        let message_ref = MessageRef::from_key(&message_ref_key);
        match self.cluster.topics().get(&message_ref.topic_id) {
            Some(topic) => match topic.subscriptions().get(&subscription_id) {
                Some(subscription) => match topic.partitions().get(&message_ref.partition_id) {
                    Some(partition) => match partition.ledgers().get(&message_ref.ledger_id) {
                        Some(ledger) => {
                            if subscription.ack(consumer_id, &message_ref_key) {
                                ledger.ack(&message_ref.message_id);
                                Ok(true)
                            } else { Ok(false) }
                        }
                        None => Err(SubError::LedgerNotFound),
                    },
                    None => Err(SubError::PartitionNotFound),
                },
                None => Err(SubError::SubscriptionNotFound),
            },
            None => Err(SubError::TopicNotFound),
        }
    }

    pub fn nack(
        self: &Self,
        message_ref_key: String,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> NackResult {
        let message_ref = MessageRef::from_key(&message_ref_key);
        match self.cluster.topics().get(&message_ref.topic_id) {
            Some(topic) => match topic.subscriptions().get(&subscription_id) {
                Some(subscription) => {
                    let _ =
                        self.persistence
                            .log_event(&LoggedEvent::Nack(logged_events::NackEvent {
                                message_ref,
                                subscription_id,
                                consumer_id,
                            }));
                    Ok(subscription.nack(consumer_id, &message_ref_key))
                }
                None => Err(SubError::SubscriptionNotFound),
            },
            None => Err(SubError::TopicNotFound),
        }
    }
}
