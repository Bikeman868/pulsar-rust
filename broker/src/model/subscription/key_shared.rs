use super::*;
use crate::{data::DataLayer, utils::now_epoc_millis};
use pulsar_rust_net::data_types::{ConsumerId, SubscriptionId, TopicId};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

type MessageKey = String;
type MessageRefKey = String;

/// Used to associate message keys to consumers. Keeps track of the number of
/// in-flight messages because when this goes to zero the association can be
/// removed.
#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Default)]
struct MessageAffinity {
    consumer_id: ConsumerId,
    message_count: usize,
}

/// Implememts the semantic of key-shared subscriptions where 2 messages with the same
/// key can not be in-flight with different consumers at the same point in time.
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
    delivered_messages: RwLock<HashMap<MessageRefKey, SubscribedMessage>>,

    /// These are messages that have the same key, and have an affinity to a consumer
    assigned_messages: RwLock<HashMap<ConsumerId, VecDeque<SubscribedMessage>>>,

    /// If we receive a message where there is already a delivered message with the same key.
    /// then we create an affinity mapping to the consumer so that subsequent messages with
    /// the same key will be sent to the same consumer
    affinity_map: RwLock<HashMap<MessageKey, MessageAffinity>>,
}

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
            assigned_messages: RwLock::new(HashMap::new()),
            affinity_map: RwLock::new(HashMap::new()),
        }
    }

    pub fn stats(self: &Self) -> SubscriptionStats {
        SubscriptionStats {
            queued_count: self.queued_messages.read().unwrap().len(),
            unacked_count: self.delivered_messages.read().unwrap().len(),
            assigned_count: self.assigned_messages.read().unwrap().iter().fold(0, |sum, entry|sum + entry.1.len()),
            affinity_count: self.affinity_map.read().unwrap().len(),
        }
    }

    /// Increments the next consumer id in the database and returns the original value
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

    pub fn disconnect_consumer(self: &Self, consumer_id: ConsumerId) {
        // Make a list of the keys with affinity to this consumer
        let affinity_map = self.affinity_map.read().unwrap();
        let keys: Vec<&String> = affinity_map
            .iter()
            .filter_map(|a| {
                if a.1.consumer_id == consumer_id {
                    Some(a.0)
                } else {
                    None
                }
            })
            .collect();

        // Remove all of these keys from the affinity map
        let mut affinity_map = self.affinity_map.write().unwrap();
        for key in keys {
            affinity_map.remove(key);
        }

        // Push messages assigned to this consumer back into the input queue
        let mut assigned_messages = self.assigned_messages.write().unwrap();
        if let Some(queue) = assigned_messages.get_mut(&consumer_id) {
            queue.into_iter().for_each(|message| {
                self.queued_messages
                    .write()
                    .unwrap()
                    .push_front(message.to_owned());
            });
        }

        assigned_messages.remove(&consumer_id);
    }

    /// Queues a message for delivery to this subscription
    pub fn push(self: &Self, message: SubscribedMessage) {
        self.queued_messages.write().unwrap().push_back(message);
    }

    /// Retrieves the next message for a consumer if there is one
    pub fn pop(self: &Self, consumer_id: ConsumerId) -> Option<SubscribedMessage> {
        // Loop until we dequeue a message that is deliverable to this consumer
        loop {
            // 1. deliver messages that are queued for this consumer specifically
            if let Some(mut message) = self.pop_assigned(consumer_id) {
                self.message_delivered_to(&mut message, consumer_id);
                return Some(message);
            }

            // 2. Get a message from the general input queue
            let mut queue = self.queued_messages.write().unwrap();
            let message = queue.pop_front()?;
            drop(queue);

            // 3. If this message has an affinity to a consumer then assign it to that consumer
            if let Some(assigned_consumer_id) = self.increment_affinity(&message) {
                self.assign_consumer(message, assigned_consumer_id);
                continue;
            }

            // 4. Create an affinity so other messages with the same key will be processed by this consumer
            self.create_affinity(&message, consumer_id);

            // 5. Queue this message for processing by this consumer
            self.assign_consumer(message, consumer_id);
            continue;
        }
    }

    pub fn ack(self: &Self, consumer_id: ConsumerId, message_ref_key: &str) -> bool {
        self.decrement_affinity(message_ref_key, consumer_id)
            .is_some()
    }

    pub fn nack(self: &Self, consumer_id: ConsumerId, message_ref_key: &str) -> bool {
        if let Some((message, count)) = self.decrement_affinity(message_ref_key, consumer_id) {
            if count == 0 {
                self.queued_messages.write().unwrap().push_front(message);
            } else {
                let mut assigned_messages = self.assigned_messages.write().unwrap();
                let consumer_queue = assigned_messages.get_mut(&consumer_id);
                match consumer_queue {
                    Some(queue) => queue.push_front(message),
                    None => {
                        let mut queue = VecDeque::new();
                        queue.push_front(message);
                        assigned_messages.insert(consumer_id, queue);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    fn message_delivered_to(self: &Self, message: &mut SubscribedMessage, consumer_id: ConsumerId) {
        message.delivered_timestamp = Some(now_epoc_millis());
        message.consumer_id = Some(consumer_id);
        message.delivery_count += 1;

        let mut delivered_messages = self.delivered_messages.write().unwrap();
        delivered_messages.insert(message.message_ref_key.clone(), message.clone());
    }

    fn create_affinity(self: &Self, message: &SubscribedMessage, consumer_id: ConsumerId) {
        let mut affinity_map = self.affinity_map.write().unwrap();
        affinity_map.insert(
            message.key.clone(),
            MessageAffinity {
                consumer_id,
                message_count: 1,
            },
        );
    }

    fn increment_affinity(self: &Self, message: &SubscribedMessage) -> Option<ConsumerId> {
        let mut affinity_map = self.affinity_map.write().unwrap();
        if let Some(affinity) = affinity_map.get_mut(&message.key) {
            affinity.message_count += 1;
            Some(affinity.consumer_id)
        } else {
            None
        }
    }

    fn decrement_affinity(
        self: &Self,
        message_ref_key: &str,
        consumer_id: ConsumerId,
    ) -> Option<(SubscribedMessage, usize)> {
        let mut delivered_messages = self.delivered_messages.write().unwrap();
        match delivered_messages.remove(message_ref_key) {
            Some(message) => {
                let mut count = 0;
                let mut affinity_map = self.affinity_map.write().unwrap();
                if let Some(affinity) = affinity_map.get_mut(&message.key) {
                    if consumer_id == affinity.consumer_id {
                        if affinity.message_count == 1 {
                            affinity_map.remove(&message.key);
                        } else {
                            count = affinity.message_count - 1;
                            affinity.message_count = count;
                        };
                    }
                }
                Some((message, count))
            }
            None => None,
        }
    }

    // Assign a message to the consumer that it has an affinity with
    fn assign_consumer(self: &Self, message: SubscribedMessage, consumer_id: ConsumerId) {
        let mut assigned_messages = self.assigned_messages.write().unwrap();
        let consumer_queue = assigned_messages.get_mut(&consumer_id);
        match consumer_queue {
            Some(queue) => queue.push_back(message),
            None => {
                let mut queue = VecDeque::new();
                queue.push_back(message);
                assigned_messages.insert(consumer_id, queue);
            }
        }
    }

    // Return the next message that is assigned to a consumer
    fn pop_assigned(self: &Self, consumer_id: ConsumerId) -> Option<SubscribedMessage> {
        let mut assigned_messages = self.assigned_messages.write().unwrap();
        let consumer_queue = assigned_messages.get_mut(&consumer_id)?;
        consumer_queue.pop_front()
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}
}
