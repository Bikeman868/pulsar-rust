use crate::{data::DataLayer, utils::now_epoc_millis};
use pulsar_rust_net::data_types::{ConsumerId, SubscriptionId, TopicId};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use super::*;

pub struct Subscription {
    data_layer: Arc<DataLayer>,
    name: String,
    topic_id: TopicId,
    subscription_id: SubscriptionId,

    /// Newly published messages are initially added to this queue, and represent
    /// a subscription backlog
    queued_messages: RwLock<VecDeque<SubscribedMessage>>,

    /// When messages are delivered to the consumer, they are dequeued from above,
    /// and added to this map so that we can find the message related to ack/nack.
    /// This map is also used by a background thread that looks for Nack timeouts.
    delivered_messages: RwLock<HashMap<String, SubscribedMessage>>,
}

/// Implements semantics for shared subscriptions where messages do not have consumer affinity
impl Subscription {
    pub fn topic_id(self: &Self) -> TopicId {
        self.topic_id
    }
    pub fn subscription_id(self: &Self) -> SubscriptionId {
        self.subscription_id
    }
    pub fn name(self: &Self) -> String {
        self.name.clone()
    }

    pub fn new(
        data_layer: &Arc<DataLayer>,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
    ) -> Self {
        let subscription = data_layer
            .get_subscription(topic_id, subscription_id)
            .unwrap();
        let name = subscription.name;

        Self {
            data_layer: data_layer.clone(),
            name,
            topic_id,
            subscription_id,
            queued_messages: RwLock::new(VecDeque::new()),
            delivered_messages: RwLock::new(HashMap::new()),
        }
    }

    pub fn stats(self: &Self) -> SubscriptionStats {
        SubscriptionStats {
            queued_count: self.queued_messages.read().unwrap().len(),
            unacked_count: self.delivered_messages.read().unwrap().len(),
            assigned_count: 0,
            affinity_count: 0,
        }
    }

    /// Adds a message to the queue for delivery to this subscriber
    pub fn push(self: &Self, message: SubscribedMessage) {
        let mut queue = self.queued_messages.write().unwrap();
        queue.push_back(message);
    }

    /// Removes a message from the front of the queue for this subscription if there is one
    pub fn pop(self: &Self, consumer_id: ConsumerId) -> Option<SubscribedMessage> {
        let mut queue = self.queued_messages.write().unwrap();
        let mut message = queue.pop_front()?;
        drop(queue);

        message.consumer_id = Some(consumer_id);
        message.delivery_count += 1;
        message.delivered_timestamp = Some(now_epoc_millis());

        let result = Some(message.clone());
        let mut delivered_messages = self.delivered_messages.write().unwrap();
        delivered_messages.insert(message.message_ref_key.clone(), message);
        result
    }

    pub fn connect_consumer(self: &Self) -> Option<ConsumerId> {
        let mut next_consumer_id: ConsumerId = 0;

        match self.data_layer.update_subscription(
            self.topic_id,
            self.subscription_id,
            |subscription| {
                next_consumer_id = subscription.next_consumer_id;
                if next_consumer_id == 0 {
                    return false;
                }
                subscription.next_consumer_id = if next_consumer_id == ConsumerId::MAX {
                    0
                } else {
                    next_consumer_id + 1
                };
                true
            },
        ) {
            Ok(_) => Some(next_consumer_id),
            Err(_) => None,
        }
    }

    pub fn disconnect_consumer(self: &Self, _consumer_id: ConsumerId) {}

    pub fn ack(self: &Self, _consumer_id: ConsumerId, message_ref_key: &str) -> bool {
        let mut delivered_messages = self.delivered_messages.write().unwrap();
        delivered_messages.remove(message_ref_key).is_some()
    }

    pub fn nack(self: &Self, _consumer_id: ConsumerId, message_ref_key: &str) -> bool {
        let mut delivered_messages = self.delivered_messages.write().unwrap();
        if let Some(message) = delivered_messages.remove(message_ref_key) {
            let mut queue = self.queued_messages.write().unwrap();
            queue.push_front(message);
            true
        } else {
            false
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}
}
