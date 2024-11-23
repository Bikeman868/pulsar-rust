pub mod entities;
pub mod events;
mod file_system;
mod in_memory;

use entities::EntityPersister;
use events::EventPersister;

use crate::model::data_types;

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

pub fn build_event_persister(scheme: PersistenceScheme) -> EventPersister {
    match scheme {
        PersistenceScheme::InMemory => {
            EventPersister::InMmeory(in_memory::events::EventPersister::new())
        }

        PersistenceScheme::FileSystem => {
            EventPersister::FileSystem(file_system::events::EventPersister::new())
        }
    }
}

pub fn build_entity_persister(scheme: PersistenceScheme) -> EntityPersister {
    match scheme {
        PersistenceScheme::InMemory => {
            EntityPersister::InMemory(in_memory::entities::EntityPersister::new())
        }

        PersistenceScheme::FileSystem => {
            EntityPersister::FileSystem(file_system::entities::EntityPersister::new())
        }
    }
}

pub trait Versioned {
    fn version(self: &Self) -> data_types::VersionNumber;
    fn set_version(self: &mut Self, version: data_types::VersionNumber);
}

pub trait Keyed {
    fn type_name() -> &'static str;
    fn key(self: &Self) -> String;
}
