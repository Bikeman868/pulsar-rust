use std::{
    sync::RwLock,
    time::{Duration, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use rmp_serde::Serializer;
use serde::Serialize;
use pulsar_rust_net::data_types::Timestamp;
use crate::persistence::{
    event_logger::{EventQueryOptions, LogDeleteResult, LogEntry, LogResult},
    Keyed,
};

pub struct EventLogger {
    entries: RwLock<Vec<LogEntry>>,
}

impl EventLogger {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
        }
    }

    #[cfg(debug_assertions)]
    pub fn delete_all(self: &Self) {
        self.entries.write().unwrap().clear();
        println!("Event log cleared");
    }

    pub fn log<T: Keyed + Serialize>(self: &Self, event: &T, timestamp: Timestamp) -> LogResult {
        let mut buffer = Vec::new();
        let mut serializer = Serializer::new(&mut buffer);
        event.serialize(&mut serializer).unwrap();

        let type_name = event.type_name().to_owned();
        let key = event.key();
        let system_time = UNIX_EPOCH + Duration::from_millis(timestamp);
        let date_time: DateTime<Utc> = system_time.into();
        println!("EVENT {date_time} {type_name}:{key} => {buffer:?}");

        let log_entry = LogEntry {
            timestamp: timestamp.clone(),
            type_name,
            key,
            serialization: Some(buffer),
        };

        self.entries.write().unwrap().push(log_entry);
        Result::Ok(())
    }

    pub fn query_by_timestamp<'a, 'b>(
        self: &'a Self,
        start: Timestamp,
        end: Timestamp,
        options: &'b EventQueryOptions,
    ) -> Box<dyn Iterator<Item = LogEntry> + 'b>
    where
        'a: 'b,
    {
        if options.descending {
            Box::new(DescendingLogEntryIterator::new(
                &self.entries,
                options,
                move |entry| entry.timestamp >= start && entry.timestamp < end,
            ))
        } else {
            Box::new(AscendingLogEntryIterator::new(
                &self.entries,
                options,
                move |entry| entry.timestamp >= start && entry.timestamp < end,
            ))
        }
    }

    pub fn query_by_key_prefix<'a, 'b>(
        self: &'a Self,
        key_prefix: &'b str,
        options: &'b EventQueryOptions,
    ) -> Box<dyn Iterator<Item = LogEntry> + 'b>
    where
        'a: 'b,
    {
        if options.descending {
            Box::new(DescendingLogEntryIterator::new(
                &self.entries,
                options,
                move |entry| entry.key.starts_with(key_prefix),
            ))
        } else {
            Box::new(AscendingLogEntryIterator::new(
                &self.entries,
                options,
                move |entry| entry.key.starts_with(key_prefix),
            ))
        }
    }

    pub fn delete_before(self: &Self, end: Timestamp) -> LogDeleteResult {
        self.entries
            .write()
            .unwrap()
            .retain(|log_entry| log_entry.timestamp >= end);
        LogDeleteResult::Ok(())
    }

    pub fn delete_by_key_prefix(self: &Self, key_prefix: &str) -> LogDeleteResult {
        self.entries
            .write()
            .unwrap()
            .retain(|log_entry| !log_entry.key.starts_with(key_prefix));
        LogDeleteResult::Ok(())
    }
}

pub struct AscendingLogEntryIterator<'a, F: Fn(&LogEntry) -> bool> {
    entries: &'a RwLock<Vec<LogEntry>>,
    count: usize,
    index: usize,
    taken: usize,
    filter: F,
    options: &'a EventQueryOptions,
}

impl<'a, F: Fn(&LogEntry) -> bool> AscendingLogEntryIterator<'a, F> {
    pub fn new(
        entries: &'a RwLock<Vec<LogEntry>>,
        options: &'a EventQueryOptions,
        filter: F,
    ) -> Self {
        let count = entries.read().unwrap().len();
        let index = if count > options.skip {
            options.skip
        } else {
            count
        };
        Self {
            entries,
            filter: filter,
            count,
            index,
            options,
            taken: 0,
        }
    }
}

impl<'a, F: Fn(&LogEntry) -> bool> Iterator for AscendingLogEntryIterator<'a, F> {
    type Item = LogEntry;

    fn next(self: &mut Self) -> Option<Self::Item> {
        loop {
            if (self.index == self.count)
                || (self.options.take > 0 && self.taken == self.options.take)
            {
                return None;
            }

            let entries = self.entries.read().unwrap();
            let log_entry = entries.get(self.index);
            self.index = self.index + 1;
            match log_entry {
                Some(entry) if (self.filter)(&entry) => {
                    self.taken += 1;
                    return Some(LogEntry {
                        key: entry.key.clone(),
                        timestamp: entry.timestamp,
                        type_name: entry.type_name.clone(),
                        serialization: match self.options.include_serialization {
                            true => entry.serialization.clone(),
                            false => None,
                        },
                    });
                }
                Some(_) => continue,
                None => return None,
            }
        }
    }
}

pub struct DescendingLogEntryIterator<'a, F: Fn(&LogEntry) -> bool> {
    entries: &'a RwLock<Vec<LogEntry>>,
    index: usize,
    taken: usize,
    filter: F,
    options: &'a EventQueryOptions,
}

impl<'a, F: Fn(&LogEntry) -> bool> DescendingLogEntryIterator<'a, F> {
    pub fn new(
        entries: &'a RwLock<Vec<LogEntry>>,
        options: &'a EventQueryOptions,
        filter: F,
    ) -> Self {
        let len = entries.read().unwrap().len();
        let index = if len > options.skip {
            len - options.skip
        } else {
            0
        };
        Self {
            entries,
            filter: filter,
            index,
            options,
            taken: 0,
        }
    }
}

impl<'a, F: Fn(&LogEntry) -> bool> Iterator for DescendingLogEntryIterator<'a, F> {
    type Item = LogEntry;

    fn next(self: &mut Self) -> Option<Self::Item> {
        loop {
            if (self.index == 0) || (self.options.take > 0 && self.taken == self.options.take) {
                return None;
            }
            self.index = self.index - 1;

            let entries = self.entries.read().unwrap();
            let log_entry = entries.get(self.index);
            match log_entry {
                Some(entry) if (self.filter)(&entry) => {
                    self.taken += 1;
                    return Some(LogEntry {
                        key: entry.key.clone(),
                        timestamp: entry.timestamp,
                        type_name: entry.type_name.clone(),
                        serialization: match self.options.include_serialization {
                            true => entry.serialization.clone(),
                            false => None,
                        },
                    });
                }
                Some(_) => continue,
                None => return None,
            }
        }
    }
}
