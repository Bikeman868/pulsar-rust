use super::{file_system, in_memory};
use crate::model::data_types::{self, Timestamp};
use serde::Serialize;

pub struct LogEntry {
    pub timestamp: data_types::Timestamp,
    pub type_name: String,
    pub key: String,
    pub serialization: Option<Vec<u8>>,
}

pub struct EventQueryOptions {
    pub include_serialization: bool,
}

pub type EventQueryResult = Box<dyn Iterator<Item = LogEntry>>;

pub enum EventPersister {
    InMmeory(in_memory::events::EventPersister),
    FileSystem(file_system::events::EventPersister),
}

pub type LogResult = Result<(), ()>;

impl EventPersister {
    pub fn log<T: super::Keyed + Serialize>(
        self: &mut Self,
        event: &T,
        timestamp: &Timestamp,
    ) -> LogResult {
        match self {
            EventPersister::InMmeory(p) => p.log(event, timestamp),
            EventPersister::FileSystem(_) => todo!(),
        }
    }

    pub fn query_by_timestamp<'a>(
        self: &'a Self,
        start: &Timestamp,
        end: &Timestamp,
        options: &EventQueryOptions,
    ) -> EventQueryResult {
        match self {
            EventPersister::InMmeory(p) => p.query_by_timestamp(start, end, options),
            EventPersister::FileSystem(_) => todo!(),
        }
    }

    pub fn query_by_key_prefix<'a>(
        self: &'a Self,
        key_prefix: &str,
        options: &EventQueryOptions,
    ) -> EventQueryResult {
        match self {
            EventPersister::InMmeory(p) => p.query_by_key_prefix(key_prefix, options),
            EventPersister::FileSystem(_) => todo!(),
        }
    }
}
