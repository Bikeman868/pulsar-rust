use super::responses::{
    AckLogEntry, DropConsumerLogEntry, KeyAffinityLogEntry, LogEntry, LogEntryDetail,
    LogEntrySummary, Message, MessageRef, NackLogEntry, NewConsumerLogEntry, PublishLogEntry,
};
use crate::display::JoinableToString;
use std::fmt::Display;

impl JoinableToString for Vec<LogEntry> {
    fn join(&self, sep: &str) -> String {
        self.iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
            .join(sep)
    }
}

impl JoinableToString for Vec<LogEntrySummary> {
    fn join(&self, sep: &str) -> String {
        self.iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
            .join(sep)
    }
}

impl Display for MessageRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "topic:{} partition:{} ledger:{} message:{}",
            self.topic_id, self.partition_id, self.ledger_id, self.message_id
        )
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} key:{} attributes:{:?}",
            self.message_ref, self.message_key, self.attributes
        )
    }
}

impl Display for PublishLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Display for AckLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} subscription:{} consumer:{}",
            self.message_ref, self.subscription_id, self.consumer_id
        )
    }
}

impl Display for NackLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} subscription:{} consumer:{}",
            self.message_ref, self.subscription_id, self.consumer_id
        )
    }
}

impl Display for NewConsumerLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "topic:{} subscription:{} consumer:{}",
            self.topic_id, self.subscription_id, self.consumer_id
        )
    }
}

impl Display for DropConsumerLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "topic:{} subscription:{} consumer:{}",
            self.topic_id, self.subscription_id, self.consumer_id
        )
    }
}

impl Display for KeyAffinityLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "topic:{} subscription:{} consumer:{} key:{}",
            self.topic_id, self.subscription_id, self.consumer_id, self.message_key
        )
    }
}

impl Display for LogEntryDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogEntryDetail::Publish(entry) => write!(f, "{}", entry),
            LogEntryDetail::Ack(entry) => write!(f, "{}", entry),
            LogEntryDetail::Nack(entry) => write!(f, "{}", entry),
            LogEntryDetail::NewConsumer(entry) => write!(f, "{}", entry),
            LogEntryDetail::DropConsumer(entry) => write!(f, "{}", entry),
            LogEntryDetail::KeyAffinity(entry) => write!(f, "{}", entry),
        }
    }
}

impl ToString for LogEntry {
    fn to_string(&self) -> String {
        match &self.details {
            Some(detail) => format!(
                "{} {} {} {}",
                self.timestamp, self.event_type, self.event_key, detail
            ),
            None => format!("{} {} {}", self.timestamp, self.event_type, self.event_key),
        }
    }
}

impl ToString for LogEntrySummary {
    fn to_string(&self) -> String {
        format!("{} {} {}", self.timestamp, self.event_type, self.event_key)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        contracts::v1::responses::{AckLogEntry, LogEntry, LogEntryDetail, MessageRef},
        display::JoinableToString,
    };

    #[test]
    fn should_join_log_entries() {
        let entries: Vec<LogEntry> = vec![
            LogEntry {
                timestamp: 1,
                event_type: String::from("Ack"),
                event_key: String::from("123"),
                details: None,
            },
            LogEntry {
                timestamp: 2,
                event_type: String::from("Ack"),
                event_key: String::from("abc"),
                details: Some(LogEntryDetail::Ack(AckLogEntry {
                    message_ref: MessageRef {
                        topic_id: 5,
                        partition_id: 2,
                        ledger_id: 243,
                        message_id: 98678,
                    },
                    subscription_id: 3,
                    consumer_id: 99,
                })),
            },
        ];
        let output = entries.join("\n");
        assert_eq!("1 Ack 123\n2 Ack abc topic:5 partition:2 ledger:243 message:98678 subscription:3 consumer:99", output);
    }
}
