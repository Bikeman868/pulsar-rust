/*
This node provides an abstraction over the supported persistence mechanisms, so that the
rest of the application doesn't need to know if state is persisted in memord, on disk,
or in a database.
*/
pub mod entity_persister;
pub mod persisted_entities;

pub mod event_logger;
pub mod logged_events;

mod file_system;
mod in_memory;

use event_logger::LogDeleteResult;
use serde::{Deserialize, Serialize};

use self::entity_persister::{DeleteResult, EntityPersister, LoadResult, SaveResult};
use self::event_logger::{EventLogger, EventQueryOptions, LogEntry, LogResult};

use crate::model::data_types::{CatalogId, MessageId, PartitionId, TopicId, VersionNumber};
use crate::model::messages::MessageRef;
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
    event_logger: EventLogger,
    entity_persister: EntityPersister,
}

impl PersistenceLayer {
    pub fn new(
        event_persistence: PersistenceScheme,
        entity_persistence: PersistenceScheme,
    ) -> Self {
        Self {
            event_logger: match event_persistence {
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
        self.event_logger.delete_all();
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
        self.event_logger.log(event, now_epoc_millis())
    }

    pub fn log_with_timestamp<T: Keyed + Serialize>(
        self: &Self,
        event: &T,
        timestamp: Timestamp,
    ) -> LogResult {
        self.event_logger.log(event, timestamp)
    }

    pub fn events_by_key_prefix<'a>(
        self: &'a Self,
        key_prefix: &'a str,
        options: &'a EventQueryOptions,
    ) -> impl Iterator<Item = LogEntry> + use<'a> {
        self.event_logger.query_by_key_prefix(key_prefix, options)
    }

    pub fn build_topic_prefix(topic_id: TopicId) -> String {
        topic_id.to_string() + ":"
    }

    pub fn build_partition_prefix(topic_id: TopicId, partition_id: PartitionId) -> String {
        topic_id.to_string() + ":" + &partition_id.to_string() + ":"
    }

    pub fn build_catalog_prefix(
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> String {
        topic_id.to_string() + ":" + &partition_id.to_string() + ":" + &catalog_id.to_string() + ":"
    }

    pub fn build_message_prefix(
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
        message_id: MessageId,
    ) -> String {
        topic_id.to_string()
            + ":"
            + &partition_id.to_string()
            + ":"
            + &catalog_id.to_string()
            + ":"
            + &message_id.to_string()
    }

    pub fn events_by_timestamp<'a>(
        self: &'a Self,
        start: Timestamp,
        end: Timestamp,
        options: &'a EventQueryOptions,
    ) -> impl Iterator<Item = LogEntry> + use<'a> {
        self.event_logger.query_by_timestamp(start, end, options)
    }

    pub fn delete_events_before(self: &Self, end: Timestamp) -> LogDeleteResult {
        self.event_logger.delete_before(end)
    }

    pub fn delete_events_for_topic(self: &Self, topic_id: TopicId) -> LogDeleteResult {
        self.event_logger
            .delete_by_key_prefix(&Self::build_topic_prefix(topic_id))
    }

    pub fn delete_events_for_partition(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> LogDeleteResult {
        self.event_logger
            .delete_by_key_prefix(&Self::build_partition_prefix(topic_id, partition_id))
    }

    pub fn delete_events_for_catalog(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> LogDeleteResult {
        self.event_logger
            .delete_by_key_prefix(&Self::build_catalog_prefix(
                topic_id,
                partition_id,
                catalog_id,
            ))
    }

    pub fn delete_events_for_message(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
        message_id: MessageId,
    ) -> LogDeleteResult {
        self.event_logger
            .delete_by_key_prefix(&Self::build_message_prefix(
                topic_id,
                partition_id,
                catalog_id,
                message_id,
            ))
    }

    pub fn delete_events_for_message_ref(self: &Self, message_ref: MessageRef) -> LogDeleteResult {
        self.event_logger
            .delete_by_key_prefix(&message_ref.to_key())
    }
}
