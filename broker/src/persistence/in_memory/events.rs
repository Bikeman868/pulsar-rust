use std::time::{Duration, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use rmp_serde::Serializer;
use serde::Serialize;

use crate::{
    model::data_types,
    persistence::{
        events::{EventQueryOptions, EventQueryResult, LogEntry},
        Keyed,
    },
};

pub struct EventPersister {
    entries: Vec<LogEntry>,
}

impl EventPersister {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn log<T: Keyed + Serialize>(
        self: &mut Self,
        event: &T,
        timestamp: &data_types::Timestamp,
    ) -> Result<(), ()> {
        let mut buffer = Vec::new();
        let mut serializer = Serializer::new(&mut buffer);
        event.serialize(&mut serializer).unwrap();

        let type_name = T::type_name().to_owned();
        let key = event.key();
        let system_time = UNIX_EPOCH + Duration::from_millis(u64::from(*timestamp));
        let date_time: DateTime<Utc> = system_time.into();
        println!("EVENT {date_time} {type_name}:{key} => {buffer:?}");

        let log_entry = LogEntry {
            timestamp: timestamp.clone(),
            type_name,
            key,
            serialization: Some(buffer),
        };

        self.entries.push(log_entry);
        Result::Ok(())
    }

    pub fn query_by_timestamp(
        self: &Self,
        start: &data_types::Timestamp,
        end: &data_types::Timestamp,
        _options: &EventQueryOptions,
    ) -> EventQueryResult {
        let _iter = LogEntryIterator::new(&self.entries, move |entry| {
            entry.timestamp >= *start && entry.timestamp < *end
        });
        todo!();
    }

    pub fn query_by_key_prefix(
        self: &Self,
        key_prefix: &str,
        _options: &EventQueryOptions,
    ) -> EventQueryResult {
        let _iter = LogEntryIterator::new(&self.entries, move |entry| {
            entry.key.starts_with(key_prefix)
        });
        todo!();
    }
}

struct LogEntryIterator<'a, F>
where
    F: Fn(&'a LogEntry) -> bool,
{
    entries: &'a Vec<LogEntry>,
    index: usize,
    filter: F,
}

impl<'a, F> LogEntryIterator<'a, F>
where
    F: Fn(&'a LogEntry) -> bool,
{
    pub fn new(entries: &'a Vec<LogEntry>, filter: F) -> Self {
        Self {
            entries,
            filter,
            index: entries.len(),
        }
    }
}

impl<'a, F> Iterator for LogEntryIterator<'a, F>
where
    F: Fn(&'a LogEntry) -> bool,
{
    type Item = LogEntry;

    fn next(self: &mut Self) -> Option<Self::Item> {
        loop {
            self.index = self.index - 1;
            match self.entries.get(self.index) {
                Some(entry) if (self.filter)(entry) => {
                    return Some(LogEntry {
                        key: entry.key.clone(),
                        timestamp: entry.timestamp.clone(),
                        type_name: entry.type_name.clone(),
                        serialization: entry.serialization.clone(),
                    })
                }
                Some(_) => continue,
                None => return None,
            }
        }
    }
}
