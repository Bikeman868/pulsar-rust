/*
Provides the functionallity needed to support message publishers. Various APIs can
be layered on top of this service to expose this funtionallity to applicatins.
*/

use crate::{
    model::{
        cluster::Cluster,
        messages::{MessageRef, PublishedMessage, SubscribedMessage},
        node::NodeRef,
        topic::TopicRef,
    },
    persistence::{log_entries::LoggedEvent, logged_events::PublishEvent, PersistenceLayer},
    utils::now_epoc_millis,
};
use pulsar_rust_net::data_types::Timestamp;
use std::sync::{Arc, Mutex};

pub enum PubError {
    Error(String),
    TopicNotFound,
    PartitionNotFound,
    NodeNotFound,
    WrongNode(NodeRef),
    BacklogCapacityExceeded,
    NoSubscribers,
}

pub type PubResult<'a> = Result<MessageRef, PubError>;

pub struct PubService {
    persistence: Arc<PersistenceLayer>,
    cluster: Arc<Cluster>,
    new_ledger_lock: Mutex<()>,
}

impl PubService {
    pub fn new(persistence: &Arc<PersistenceLayer>, cluster: &Arc<Cluster>) -> Self {
        Self {
            persistence: Arc::clone(persistence),
            cluster: Arc::clone(cluster),
            new_ledger_lock: Mutex::new(()),
        }
    }

    pub fn topic_by_name(self: &Self, name: &str) -> Option<TopicRef> {
        self.cluster.topics().find(|topic| topic.name() == name)
    }

    pub fn publish_message(self: &Self, mut message: PublishedMessage) -> PubResult {
        // Find the topic
        let topic = match self.cluster.topics().get(&message.message_ref.topic_id) {
            Some(topic) => topic,
            None => return PubResult::Err(PubError::TopicNotFound),
        };

        // Make a list of subscribers to this topic
        let subscrition_ids = topic.active_subscription_ids();
        if subscrition_ids.len() == 0 {
            return PubResult::Err(PubError::NoSubscribers);
        }

        // Find the partition within this topic
        let partition = match topic.partitions().get(&message.message_ref.partition_id) {
            Some(partition) => partition,
            None => return PubResult::Err(PubError::PartitionNotFound),
        };

        // See if the current ledger is owned by this node
        match partition.current_ledger(self.cluster.my_node_id()) {
            Some(ledger) => {
                // We own the ledger, try to allocate a new message id
                if let Some(message_id) = ledger.allocate_message_id() {
                    let topic_id = ledger.topic_id();
                    let partition_id = ledger.partition_id();
                    let ledger_id = ledger.ledger_id();

                    message.message_ref = MessageRef {
                        topic_id,
                        partition_id,
                        ledger_id,
                        message_id,
                    };
                    message.subscriber_count = subscrition_ids.len();
                    message.published = now_epoc_millis();

                    if message.timestamp == Timestamp::default() {
                        message.timestamp = message.published;
                    }

                    let message_ref = message.message_ref;
                    match self
                        .persistence
                        .log_event(&LoggedEvent::Publish(PublishEvent::new(&message)))
                    {
                        Ok(_) => {
                            // We must add the message to the ledger first becuase subscribers could immediately
                            // try to send the message to consumers
                            let message_ref_key = message.message_ref.to_key();
                            let key = message.key.clone();
                            ledger.publish_message(message);

                            // Add the message to all subscribers
                            for subscription_id in subscrition_ids {
                                if let Some(subscription) =
                                    topic.subscriptions().get(&subscription_id)
                                {
                                    let subscribed_message =
                                        SubscribedMessage::new(&message_ref_key, &key);
                                    subscription.push(subscribed_message);
                                }
                            }

                            // Confirm that the message was sucesfully published
                            Ok(message_ref)
                        }
                        Err(err) => PubResult::Err(PubError::Error(format!(
                            "Failed to write publish event to transaction log. {:?}",
                            err
                        ))),
                    }
                } else {
                    let _lock = self
                        .new_ledger_lock
                        .lock()
                        .expect("New ledger lock is poisoned");
                    PubResult::Err(PubError::BacklogCapacityExceeded) // TODO: Create a new ledger if this one is full
                }
            }
            None => {
                // We don't own this partition, tell the caller to post to the broker that does
                let node_id = partition.node_id();
                let node = self.cluster.nodes().get(&node_id);
                match node {
                    Some(node) => PubResult::Err(PubError::WrongNode(node)),
                    None => PubResult::Err(PubError::NodeNotFound),
                }
            }
        }
    }
}
