pub mod entity_persister;
pub mod persisted_entities;

pub mod event_logger;
pub mod logged_events;

mod file_system;
mod in_memory;

use serde::{Deserialize, Serialize};

use self::entity_persister::{DeleteResult, EntityPersister, LoadResult, SaveResult};
use self::event_logger::{EventLogger, EventQueryOptions, LogEntry, LogResult};

use crate::model::data_types::VersionNumber;
use crate::{model::data_types::Timestamp, utils::now_epoc_millis};

pub enum PersistenceScheme {
    InMemory,
    FileSystem,
}

impl PersistenceScheme {
    const IN_MEMORY: &str = "in-memory";
    const FILE_SYSTEM: &str = "file-system";

    pub fn as_string(self: &Self) -> &'static str {
        match self {
            PersistenceScheme::InMemory => PersistenceScheme::IN_MEMORY,
            PersistenceScheme::FileSystem => PersistenceScheme::FILE_SYSTEM,
        }
    }

    pub fn from_string(value: &str) -> PersistenceScheme {
        match value {
            PersistenceScheme::IN_MEMORY => PersistenceScheme::InMemory,
            PersistenceScheme::FILE_SYSTEM => PersistenceScheme::FileSystem,
            _ => panic!("Unknown persistence scheme {value}"),
        }
    }
}

pub trait Versioned {
    fn version(self: &Self) -> VersionNumber;
    fn set_version(self: &mut Self, version: VersionNumber);
}

pub trait Keyed {
    fn type_name(self: &Self) -> &'static str;
    fn key(self: &Self) -> String;
}

pub struct Key {
    type_name: &'static str,
    key: String,
}

impl Keyed for Key {
    fn type_name(self: &Self) -> &'static str {
        self.type_name
    }

    fn key(self: &Self) -> String {
        self.key.clone()
    }
}

pub struct PersistenceLayer {
    event_persister: EventLogger,
    entity_persister: EntityPersister,
}

impl PersistenceLayer {
    pub fn new(
        event_persistence: PersistenceScheme,
        entity_persistence: PersistenceScheme,
    ) -> Self {
        Self {
            event_persister: match event_persistence {
                PersistenceScheme::InMemory => {
                    EventLogger::InMemory(in_memory::event_logger::EventLogger::new())
                }

                PersistenceScheme::FileSystem => {
                    EventLogger::FileSystem(file_system::event_logger::EventLogger::new())
                }
            },
            entity_persister: match entity_persistence {
                PersistenceScheme::InMemory => {
                    EntityPersister::InMemory(in_memory::entity_persister::EntityPersister::new())
                }

                PersistenceScheme::FileSystem => EntityPersister::FileSystem(
                    file_system::entity_persister::EntityPersister::new(),
                ),
            },
        }
    }

    #[cfg(debug_assertions)]
    pub fn delete_all(self: &Self) {
        self.event_persister.delete_all();
        self.entity_persister.delete_all();
    }

    pub fn load<'a, TEntity>(self: &Self, key: &impl Keyed) -> LoadResult<TEntity>
    where
        TEntity: Deserialize<'a>,
    {
        self.entity_persister.load(key)
    }

    pub fn save<T: Versioned + Keyed + Serialize>(self: &Self, entity: &mut T) -> SaveResult {
        self.entity_persister.save(entity)
    }

    pub fn delete(self: &Self, key: &impl Keyed) -> DeleteResult {
        self.entity_persister.delete(key)
    }

    pub fn log<T: Keyed + Serialize>(self: &Self, event: &T) -> LogResult {
        self.event_persister.log(event, now_epoc_millis())
    }

    pub fn log_with_timestamp<T: Keyed + Serialize>(
        self: &Self,
        event: &T,
        timestamp: Timestamp,
    ) -> LogResult {
        self.event_persister.log(event, timestamp)
    }

    pub fn events_by_key_prefix<'a>(
        self: &'a Self,
        key_prefix: &'a str,
        options: &'a EventQueryOptions,
    ) -> impl Iterator<Item = LogEntry> + use<'a> {
        self.event_persister
            .query_by_key_prefix(key_prefix, options)
    }

    pub fn events_by_timestamp<'a>(
        self: &'a Self,
        start: Timestamp,
        end: Timestamp,
        options: &'a EventQueryOptions,
    ) -> impl Iterator<Item = LogEntry> + use<'a> {
        self.event_persister.query_by_timestamp(start, end, options)
    }
}
