/*
Provides the functionallity needed to support message publishers. Various APIs can
be layered on top of this service to expose this funtionallity to applicatins.
*/

use std::sync::{Arc, Mutex};
use pulsar_rust_net::data_types::{Timestamp, TopicId};
use crate::{
    model::{
        cluster::Cluster, 
        messages::{Message, MessageRef}, 
        node::Node, 
        partition::PartitionList, 
        topic::Topic 
    }, 
    persistence::{log_entries::LoggedEvent, logged_events::PublishEvent, PersistenceLayer},
    utils::now_epoc_millis
};

pub enum PubError<'a> {
    Error(String),
    TopicNotFound,
    PartitionNotFound,
    NodeNotFound,
    WrongNode(Arc<&'a Node>),
    BacklogCapacityExceeded,
}

pub type PubResult<'a> = Result<MessageRef, PubError<'a>>;

pub struct PubService {
    persistence: Arc<PersistenceLayer>,
    cluster: Arc<Cluster>,
    new_ledger_lock: Mutex<()>
}

impl PubService {
    pub fn new(
        persistence: &Arc<PersistenceLayer>, 
        cluster: &Arc<Cluster>) -> Self {
        Self { 
            persistence: Arc::clone(persistence),
            cluster: Arc::clone(cluster), 
            new_ledger_lock: Mutex::new(()),
        }
    }

    pub fn topic_by_name<'a>(self: &Self, topic_name: &'a str) -> Option<&Topic> {
        self.cluster.topics().iter().find(|topic| topic.name() == topic_name)
    }
    
    pub fn partitions_by_topic_name<'a>(
        self: &Self,
        topic_name: &'a str,
    ) -> Option<&Arc<PartitionList>> {
        Some(self.cluster.topics().iter().find(|topic| topic.name() == topic_name)?.partitions())
    }

    pub fn partitions_by_topic_id<'a>(
        self: &Self,
        topic_id: TopicId,
    ) -> Option<&Arc<PartitionList>> {
        Some(self.cluster.topics().by_id(topic_id)?.partitions())
    }

    pub fn publish_message(self: &Self, mut message: Message) -> PubResult {
        let topic = match self.cluster.topics().by_id(message.message_ref.topic_id) {
            Some(topic) => topic,
            None => return PubResult::Err(PubError::TopicNotFound),
        };
        let partition = match topic.partitions().by_id(message.message_ref.partition_id) {
            Some(partition) => partition,
            None => return PubResult::Err(PubError::PartitionNotFound),
        };
        match partition.current_ledger(self.cluster.my_node_id()) {
            Some(ledger) => {
                if let Some(message_id) = ledger.allocate_message_id() {
                    let topic_id = ledger.topic_id();
                    let partition_id = ledger.partition_id();
                    let ledger_id = ledger.ledger_id();

                    message.message_ref = MessageRef{ topic_id, partition_id, ledger_id, message_id };
                    message.subscriber_count = 1; // TODO: Subscribers
                    message.published = now_epoc_millis();
                    
                    if message.timestamp == Timestamp::default() { 
                        message.timestamp = message.published;
                    }

                    let message_ref = message.message_ref;
                    match self.persistence.log_event(&LoggedEvent::Publish(PublishEvent::new(&message))) {
                        Ok(_) => {
                            ledger.publish_message(message);
                            Ok(message_ref)
                        },
                        Err(err) => PubResult::Err(PubError::Error(format!("Failed to write publish event to transaction log. {:?}", err))),
                    }
                } else {
                    let _lock = self.new_ledger_lock.lock().expect("New ledger lock is poisoned");
                    PubResult::Err(PubError::BacklogCapacityExceeded) // TODO: Create a new ledger if this one is full
                }
            }
            None => {
                let node_id = partition.node_id();
                let node = self.cluster.nodes().by_id(node_id);
                match node {
                    Some(node) => PubResult::Err(PubError::WrongNode(Arc::new(node))),
                    None => PubResult::Err(PubError::NodeNotFound),
                }
            }
        }
    }
}
