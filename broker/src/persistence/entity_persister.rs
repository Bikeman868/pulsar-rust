use serde::{Deserialize, Serialize};

use super::{file_system, in_memory, Keyed};

#[derive(Debug, PartialEq)]
pub enum SaveError {
    Error { msg: String },
    VersionMissmatch,
}

pub type SaveResult = Result<(), SaveError>;

#[derive(Debug, PartialEq)]
pub enum LoadError {
    Error {
        msg: String,
    },
    NotFound {
        entity_type: String,
        entity_key: String,
    },
}

pub type LoadResult<T> = Result<T, LoadError>;

#[derive(Debug, PartialEq)]
pub enum DeleteError {
    Error {
        msg: String,
    },
    NotFound {
        entity_type: String,
        entity_key: String,
    },
}

pub type DeleteResult = Result<(), DeleteError>;

/// This is a generic wrapper around the supported entity persistence mechanisms. You can call
/// methods of this type to persist, load and delete stored data without knowning how it
/// is stored.
pub enum EntityPersister {
    InMemory(in_memory::entity_persister::EntityPersister),
    FileSystem(file_system::entity_persister::EntityPersister),
}

impl EntityPersister {
    #[cfg(debug_assertions)]
    pub fn delete_all(self: &Self) {
        match self {
            EntityPersister::InMemory(p) => p.delete_all(),
            EntityPersister::FileSystem(_) => todo!(),
        }
    }

    pub fn save<T>(self: &Self, entity: &mut T) -> SaveResult
    where
        T: super::Versioned + super::Keyed + Serialize,
    {
        match self {
            EntityPersister::InMemory(p) => p.save(entity),
            EntityPersister::FileSystem(_) => todo!(),
        }
    }

    pub fn load<'a, TEntity>(self: &Self, key: &impl Keyed) -> LoadResult<TEntity>
    where
        TEntity: Deserialize<'a>,
    {
        match self {
            EntityPersister::InMemory(p) => p.load(key),
            EntityPersister::FileSystem(_) => todo!(),
        }
    }

    pub fn delete(self: &Self, key: &impl Keyed) -> DeleteResult {
        match self {
            EntityPersister::InMemory(p) => p.delete(key),
            EntityPersister::FileSystem(_) => todo!(),
        }
    }
}
