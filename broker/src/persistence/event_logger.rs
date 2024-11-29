use serde::Serialize;

use super::{file_system, in_memory, Keyed};
use crate::model::data_types::Timestamp;

#[derive(Debug)]
pub struct LogEntry {
    pub timestamp: Timestamp,
    pub type_name: String,
    pub key: String,
    pub serialization: Option<Vec<u8>>,
}

pub struct EventQueryOptions {
    pub include_serialization: bool,
    pub skip: usize,
    pub take: usize,
}

impl EventQueryOptions {
    pub fn default() -> Self {
        Self {
            include_serialization: false,
            skip: 0,
            take: 0,
        }
    }
    pub fn range(skip: usize, take: usize) -> Self {
        Self {
            include_serialization: false,
            skip,
            take,
        }
    }
    pub fn limit(take: usize) -> Self {
        Self {
            include_serialization: false,
            skip: 0,
            take,
        }
    }
    pub fn with_serialization() -> Self {
        Self {
            include_serialization: true,
            skip: 0,
            take: 0,
        }
    }
}

pub enum EventLogger {
    InMemory(in_memory::event_logger::EventLogger),
    FileSystem(file_system::event_logger::EventLogger),
}

#[derive(Debug, PartialEq)]
pub enum LoggingError {
    Error,
}

pub type LogResult = Result<(), LoggingError>;

/// This is a generic wrapper around the supported event log persistence mechanisms. You can call
/// methods of this type to persist events and query the event log without knowning how it
/// is stored.
impl EventLogger {
    #[cfg(debug_assertions)]
    pub fn delete_all(self: &Self) {
        match self {
            EventLogger::InMemory(p) => p.delete_all(),
            EventLogger::FileSystem(_) => todo!(),
        }
    }

    pub fn log<T: Keyed + Serialize>(self: &Self, event: &T, timestamp: Timestamp) -> LogResult {
        match self {
            EventLogger::InMemory(p) => p.log(event, timestamp),
            EventLogger::FileSystem(_) => todo!(),
        }
    }

    pub fn query_by_timestamp<'a>(
        self: &'a Self,
        start: Timestamp,
        end: Timestamp,
        options: &'a EventQueryOptions,
    ) -> impl Iterator<Item = LogEntry> + use<'a> {
        match self {
            EventLogger::InMemory(p) => p.query_by_timestamp(start, end, options),
            EventLogger::FileSystem(_) => todo!(),
        }
    }

    pub fn query_by_key_prefix<'a>(
        self: &'a Self,
        key_prefix: &'a str,
        options: &'a EventQueryOptions,
    ) -> impl Iterator<Item = LogEntry> + use<'a> {
        match self {
            EventLogger::InMemory(p) => p.query_by_key_prefix(key_prefix, options),
            EventLogger::FileSystem(_) => todo!(),
        }
    }
}
