use serde::{Deserialize, Serialize};
use rmp_serde::{Deserializer, Serializer};
use pulsar_rust_net::data_types::Timestamp;
use super::{logged_events::{AckEvent, NackEvent, PublishEvent}, Keyed};

#[derive(Debug)]
pub struct LogEntry {
    pub timestamp: Timestamp,
    pub type_name: String,
    pub key: String,
    pub serialization: Option<Vec<u8>>,
}

#[derive(Debug)]
pub enum LoggedEvent {
    Publish(PublishEvent),
    Ack(AckEvent),
    Nack(NackEvent),
}

impl LogEntry {
    pub const PUBLISH_TYPE_NAME: &'static str = "Pub";
    pub const ACK_TYPE_NAME: &'static str = "Ack";
    pub const NACK_TYPE_NAME: &'static str = "Nack";

    pub fn new(event: &LoggedEvent, timestamp: Timestamp) -> Self {
        let type_name: String;
        let key: String;

        let mut serialization = Vec::new();
        let mut serializer = Serializer::new(&mut serialization);

        match event {
            LoggedEvent::Publish(publish) => {
                type_name = LogEntry::PUBLISH_TYPE_NAME.to_owned();
                key = publish.key();
                publish.serialize(&mut serializer).unwrap();
            }
            LoggedEvent::Ack(ack) => {
                type_name = LogEntry::ACK_TYPE_NAME.to_owned();
                key = ack.key();
                ack.serialize(&mut serializer).unwrap();
            }
            LoggedEvent::Nack(nack) => {
                type_name = LogEntry::NACK_TYPE_NAME.to_owned();
                key = nack.key();
                nack.serialize(&mut serializer).unwrap();
            }
        }

        Self {
            timestamp,
            type_name,
            key,
            serialization: Some(serialization),
        }
    }

    pub fn deserialize(self: &Self) -> Option<LoggedEvent> {
        match &self.serialization {
            Some(serialization) => {
                let mut deserializer = Deserializer::new(&serialization[..]);

                match self.type_name.as_str() {
                    LogEntry::ACK_TYPE_NAME =>{
                        let ack_event: AckEvent = Deserialize::deserialize(&mut deserializer).unwrap();
                        Some(LoggedEvent::Ack(ack_event))
                    }
                    LogEntry::NACK_TYPE_NAME =>{
                        let nack_event: NackEvent = Deserialize::deserialize(&mut deserializer).unwrap();
                        Some(LoggedEvent::Nack(nack_event))
                    }
                    LogEntry::PUBLISH_TYPE_NAME => {
                        let publish_event: PublishEvent = Deserialize::deserialize(&mut deserializer).unwrap();
                        Some(LoggedEvent::Publish(publish_event))
                    }
                    &_ => None // TODO: Log this as an error
                }
            }
            None => None,
        }
    }
}
