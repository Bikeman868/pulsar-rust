/*
This file provides mappings between the API contracts and the internal model so that we can query the model and
return results according to the API contract.
Note that the pulsar_rust_net crate always maps requests and responses to the latest version of the contract,
and this source file only has to map the latest version of the contract onto the internal model.
*/
use super::{
    ledger::{LedgerList, LedgerRef}, 
    messages::PublishedMessage, 
    node::{NodeList, NodeRef}, 
    partition::{PartitionList, PartitionRef}, 
    topic::{TopicList, TopicRef},
};
use crate::persistence::{
    log_entries::{LogEntry, LoggedEvent},
    logged_events::{
        AckEvent, 
        DropConsumerEvent, 
        KeyAffinityEvent, 
        NackEvent,
        NewConsumerEvent,
        PublishEvent,
    },
};
use pulsar_rust_net::contracts::v1::responses;

impl From<&NodeRef> for responses::NodeSummary {
    fn from(node: &NodeRef) -> Self {
        Self {
            node_id: node.node_id(),
            ip_address: node.ip_address().clone(),
        }
    }
}

impl From<&NodeRef> for responses::NodeDetail {
    fn from(node: &NodeRef) -> Self {
        Self {
            node_id: node.node_id(),
            ip_address: node.ip_address().clone(),
            admin_port: node.admin_port(),
            pubsub_port: node.pubsub_port(),
            sync_port: node.sync_port(),
            ledgers: Vec::default(), // TODO
        }
    }
}

impl From<&TopicRef> for responses::TopicSummary {
    fn from(topic: &TopicRef) -> Self {
        Self {
            topic_id: topic.topic_id(),
        }
    }
}

impl From<&TopicRef> for responses::TopicDetail {
    fn from(topic: &TopicRef) -> Self {
        Self {
            topic_id: topic.topic_id(),
            name: topic.name().to_owned(),
            partitions: Vec::default(), // TODO
        }
    }
}

impl From<&PartitionRef> for responses::PartitionSummary {
    fn from(partition: &PartitionRef) -> Self {
        Self {
            topic_id: partition.topic_id(),
            partition_id: partition.partition_id(),
        }
    }
}

impl From<&PartitionRef> for responses::PartitionDetail {
    fn from(partition: &PartitionRef) -> Self {
        Self {
            topic_id: partition.topic_id(),
            partition_id: partition.partition_id(),
            ledgers: Vec::default(), // TODO
        }
    }
}

impl From<&LedgerRef> for responses::LedgerSummary {
    fn from(ledger: &LedgerRef) -> Self {
        Self {
            topic_id: ledger.topic_id(),
            partition_id: ledger.partition_id(),
            ledger_id: ledger.ledger_id(),
            node_id: ledger.node_id(),
        }
    }
}

impl From<&LedgerRef> for responses::LedgerDetail {
    fn from(ledger: &LedgerRef) -> Self {
        Self {
            topic_id: ledger.topic_id(),
            partition_id: ledger.partition_id(),
            ledger_id: ledger.ledger_id(),
            node_id: ledger.node_id(),
            next_message_id: ledger.next_message_id(),
            message_count: ledger.message_count(),
            create_timestamp: ledger.create_timestamp(),
            last_update_timestamp: ledger.last_update_timestamp(),
        }
    }
}

impl From<&PublishedMessage> for responses::Message {
    fn from(message: &PublishedMessage) -> Self {
        Self {
            message_ref: message.message_ref.into(),
            message_key: message.key.clone(),
            message_ack_key: message.message_ref.to_key(),
            published: message.published,
            attributes: message.attributes.clone(),
            delivered: 0,
            delivery_count: 0,
        }
    }
}

impl From<&NodeList> for responses::NodeList {
    fn from(nodes: &NodeList) -> Self {
        Self {
            nodes: nodes.values().iter().map(
                |node_ref| responses::NodeSummary::from(node_ref)
            )
            .collect(),
        }
    }
}

impl From<&TopicList> for responses::TopicList {
    fn from(topics: &TopicList) -> Self {
        Self {
            topics: topics.values().iter().map(
                |topic| responses::TopicSummary::from(topic)
            )
            .collect(),
        }
    }
}

impl From<&PartitionList> for responses::PartitionList {
    fn from(partitions: &PartitionList) -> Self {
        Self {
            partitions: partitions.values().iter().map(
                |p| responses::PartitionSummary::from(p)
            )
            .collect(),
        }
    }
}

impl From<&LedgerList> for responses::LedgerList {
    fn from(ledgers: &LedgerList) -> Self {
        Self {
            ledgers: ledgers.values().iter().map(
                |ledger| responses::LedgerSummary::from(ledger)
            )
            .collect(),
        }
    }
}

impl From<&TopicRef> for responses::TopicPartitionMap {
    fn from(topic: &TopicRef) -> Self {
        Self {
            topic: responses::TopicSummary::from(topic),
            partitions: topic
                .partitions().values()
                .iter()
                .map(|partition| responses::PartitionDetail::from(partition))
                .collect(),
            nodes: Vec::new(),
        }
    }
}

impl From<&LogEntry> for responses::LogEntrySummary {
    fn from(entry: &LogEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            event_type: entry.type_name.clone(),
            event_key: entry.key.clone(),
        }
    }
}

impl From<&LogEntry> for responses::LogEntry {
    fn from(entry: &LogEntry) -> Self {
        let details = match entry.deserialize() {
            Some(logged_event) => Some(responses::LogEntryDetail::from(&logged_event)),
            None => None,
        };
        Self {
            timestamp: entry.timestamp,
            event_type: entry.type_name.clone(),
            event_key: entry.key.clone(),
            details,
        }
    }
}

impl From<&LoggedEvent> for responses::LogEntryDetail {
    fn from(event: &LoggedEvent) -> Self {
        match event {
            LoggedEvent::Publish(event) => {
                responses::LogEntryDetail::Publish(responses::PublishLogEntry::from(event))
            }
            LoggedEvent::Ack(event) => {
                responses::LogEntryDetail::Ack(responses::AckLogEntry::from(event))
            }
            LoggedEvent::Nack(event) => {
                responses::LogEntryDetail::Nack(responses::NackLogEntry::from(event))
            }
            LoggedEvent::NewConsumer(event) => {
                responses::LogEntryDetail::NewConsumer(responses::NewConsumerLogEntry::from(event))
            }
            LoggedEvent::DropConsumer(event) => responses::LogEntryDetail::DropConsumer(
                responses::DropConsumerLogEntry::from(event),
            ),
            LoggedEvent::KeyAffinity(event) => {
                responses::LogEntryDetail::KeyAffinity(responses::KeyAffinityLogEntry::from(event))
            }
        }
    }
}

impl From<&PublishEvent> for responses::PublishLogEntry {
    fn from(entry: &PublishEvent) -> Self {
        Self {
            message: responses::Message::from(&entry.message),
        }
    }
}

impl From<&AckEvent> for responses::AckLogEntry {
    fn from(entry: &AckEvent) -> Self {
        Self {
            message_ref: responses::MessageRef::from(&entry.message_ref),
            subscription_id: entry.subscription_id,
            consumer_id: entry.consumer_id,
        }
    }
}

impl From<&NackEvent> for responses::NackLogEntry {
    fn from(entry: &NackEvent) -> Self {
        Self {
            message_ref: responses::MessageRef::from(&entry.message_ref),
            subscription_id: entry.subscription_id,
            consumer_id: entry.consumer_id,
        }
    }
}

impl From<&NewConsumerEvent> for responses::NewConsumerLogEntry {
    fn from(entry: &NewConsumerEvent) -> Self {
        Self {
            topic_id: entry.topic_id,
            subscription_id: entry.subscription_id,
            consumer_id: entry.consumer_id,
        }
    }
}

impl From<&DropConsumerEvent> for responses::DropConsumerLogEntry {
    fn from(entry: &DropConsumerEvent) -> Self {
        Self {
            topic_id: entry.topic_id,
            subscription_id: entry.subscription_id,
            consumer_id: entry.consumer_id,
        }
    }
}

impl From<&KeyAffinityEvent> for responses::KeyAffinityLogEntry {
    fn from(entry: &KeyAffinityEvent) -> Self {
        Self {
            topic_id: entry.topic_id,
            subscription_id: entry.subscription_id,
            consumer_id: entry.consumer_id,
            message_key: entry.message_key.clone(),
        }
    }
}
