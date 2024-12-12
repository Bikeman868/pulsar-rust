use pulsar_rust_net::data_types::Timestamp;

use super::{file_system, in_memory, log_entries::LogEntry};

pub struct EventQueryOptions {
    pub include_serialization: bool,
    pub descending: bool,
    pub skip: usize,
    pub take: usize,
}

pub type LogDeleteResult = Result<(), ()>;

impl EventQueryOptions {
    pub fn default() -> Self {
        Self {
            include_serialization: false,
            descending: true,
            skip: 0,
            take: 0,
        }
    }
    pub fn range(skip: usize, take: usize) -> Self {
        Self {
            include_serialization: false,
            descending: true,
            skip,
            take,
        }
    }
    pub fn limit(take: usize) -> Self {
        Self {
            include_serialization: false,
            descending: true,
            skip: 0,
            take,
        }
    }
    pub fn replay() -> Self {
        Self {
            include_serialization: true,
            descending: false,
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
pub enum LogEventError {
    Error,
}

pub type LogEventResult = Result<(), LogEventError>;

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

    pub fn log(self: &Self, log_entry: LogEntry) -> LogEventResult {
        match self {
            EventLogger::InMemory(p) => p.log(log_entry),
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

    pub fn delete_before(self: &Self, end: Timestamp) -> LogDeleteResult {
        match self {
            EventLogger::InMemory(p) => p.delete_before(end),
            EventLogger::FileSystem(_) => todo!(),
        }
    }

    pub fn delete_by_key_prefix(self: &Self, key_prefix: &str) -> LogDeleteResult {
        match self {
            EventLogger::InMemory(p) => p.delete_by_key_prefix(key_prefix),
            EventLogger::FileSystem(_) => todo!(),
        }
    }
}
