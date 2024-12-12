/*
This file provides mappings between the API contracts and the internal model so that we can query the model and
return results according to the API contract.
Note that the pulsar_rust_net crate always maps requests and responses to the latest version of the contract,
and this source file only has to map the latest version of the contract onto the internal model.
*/
use pulsar_rust_net::contracts::v1::responses;
use crate::persistence::{logged_events::{AckEvent, NackEvent, PublishEvent},log_entries::{LogEntry, LoggedEvent}};
use super::{
    ledger::{Ledger, LedgerList}, messages::Message, node::{Node, NodeList}, partition::{Partition, PartitionList}, topic::{Topic, TopicList}
};

impl From<&Node> for responses::NodeSummary {
    fn from(node: &Node) -> Self {
        Self {
            node_id: node.node_id(),
            ip_address: node.ip_address().clone(),
        }
    }
}

impl From<&Node> for responses::NodeDetail {
    fn from(node: &Node) -> Self {
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

impl From<&Topic> for responses::TopicSummary {
    fn from(topic: &Topic) -> Self {
        Self {
            topic_id: topic.topic_id(),
        }
    }
}

impl From<&Topic> for responses::TopicDetail {
    fn from(topic: &Topic) -> Self {
        Self {
            topic_id: topic.topic_id(),
            name: topic.name().to_owned(),
            partitions: Vec::default(), // TODO
        }
    }
}

impl From<&Partition> for responses::PartitionSummary {
    fn from(partition: &Partition) -> Self {
        Self {
            topic_id: partition.topic_id(),
            partition_id: partition.partition_id(),
        }
    }
}

impl From<&Partition> for responses::PartitionDetail {
    fn from(partition: &Partition) -> Self {
        Self {
            topic_id: partition.topic_id(),
            partition_id: partition.partition_id(),
            ledgers: Vec::default(), // TODO
        }
    }
}

impl From<&Ledger> for responses::LedgerSummary {
    fn from(ledger: &Ledger) -> Self {
        Self {
            topic_id: ledger.topic_id(),
            partition_id: ledger.partition_id(),
            ledger_id: ledger.ledger_id(),
            node_id: ledger.node_id(),
        }
    }
}

impl From<&Ledger> for responses::LedgerDetail {
    fn from(ledger: &Ledger) -> Self {
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

impl From<&Message> for responses::Message {
    fn from(message: &Message) -> Self {
        Self {
            message_ref: message.message_ref.into(),
            message_key: message.key.clone(),
            published: message.published,
            attributes: message.attributes.clone(),
        }
    }
}

impl From<&NodeList> for responses::NodeList {
    fn from(nodes: &NodeList) -> Self {
        Self {
            nodes: nodes.iter().map(|p| responses::NodeSummary::from(p)).collect(),
        }
    }
}

impl From<&TopicList> for responses::TopicList {
    fn from(topics: &TopicList) -> Self {
        Self {
            topics: topics.iter().map(|p| responses::TopicSummary::from(p)).collect(),
        }
    }
}

impl From<&PartitionList> for responses::PartitionList {
    fn from(partitions: &PartitionList) -> Self {
        Self {
            partitions: partitions.iter().map(|p| responses::PartitionSummary::from(p)).collect(),
        }
    }
}

impl From<&LedgerList> for responses::LedgerList {
    fn from(ledgers: &LedgerList) -> Self {
        Self {
            ledgers: ledgers.iter().map(|p| responses::LedgerSummary::from(p)).collect(),
        }
    }
}

impl From <&Topic> for responses::TopicPartitionMap {
    fn from(topic: &Topic) -> Self {
        Self {
            topic: responses::TopicSummary::from(topic),
            partitions: topic.partitions().iter().map(|p| responses::PartitionDetail::from(p)).collect(),
            nodes: Vec::new(),
        }
    }
}

impl From <&LogEntry> for responses::LogEntrySummary {
    fn from(entry: &LogEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            event_type: entry.type_name.clone(),
            event_key: entry.key.clone(),
        }
    }
}

impl From <&LogEntry> for responses::LogEntry {
    fn from(entry: &LogEntry) -> Self {
        let details = match entry.deserialize()
        {
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

impl From <&LoggedEvent> for responses::LogEntryDetail {
    fn from(event: &LoggedEvent) -> Self {
        match event {
            LoggedEvent::Publish(event) => responses::LogEntryDetail::Publish(responses::PublishLogEntry::from(event)),
            LoggedEvent::Ack(event) => responses::LogEntryDetail::Ack(responses::AckLogEntry::from(event)),
            LoggedEvent::Nack(event) => responses::LogEntryDetail::Nack(responses::NackLogEntry::from(event)),
        }
    }
}

impl From <&PublishEvent> for responses::PublishLogEntry {
    fn from(entry: &PublishEvent) -> Self {
        Self {
            message: responses::Message::from(&entry.message),
        }
    }
}

impl From <&AckEvent> for responses::AckLogEntry {
    fn from(entry: &AckEvent) -> Self {
        Self {
            message_ref: responses::MessageRef::from(&entry.message_ref),
            subscription_id: entry.subscription_id,
            consumer_id: entry.consumer_id,
        }
    }
}

impl From <&NackEvent> for responses::NackLogEntry {
    fn from(entry: &NackEvent) -> Self {
        Self {
            message_ref: responses::MessageRef::from(&entry.message_ref),
            subscription_id: entry.subscription_id,
            consumer_id: entry.consumer_id,
        }
    }
}

