use super::{file_system, in_memory};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum SaveError {
    Error,
    VersionMissmatch,
}

pub type SaveResult = Result<(), SaveError>;

#[derive(Debug)]
pub enum LoadError {
    Error,
    NotFound,
}

pub type LoadResult<T> = Result<T, LoadError>;

#[derive(Debug)]
pub enum DeleteError {
    Error,
    NotFound,
}

pub type DeleteResult = Result<(), DeleteError>;

pub enum EntityPersister {
    InMemory(in_memory::entities::EntityPersister),
    FileSystem(file_system::entities::EntityPersister),
}

impl EntityPersister {
    pub fn save<T>(self: &mut Self, entity: &mut T) -> SaveResult
    where
        T: super::Versioned + super::Keyed + Serialize,
    {
        match self {
            EntityPersister::InMemory(p) => p.save(entity),
            EntityPersister::FileSystem(_) => todo!(),
        }
    }

    pub fn load<'a, TKey, TEntity>(
        self: &Self,
        type_name: &'static str,
        key: &'a TKey,
    ) -> LoadResult<TEntity>
    where
        TKey: super::Keyed,
        TEntity: Deserialize<'a>,
    {
        match self {
            EntityPersister::InMemory(p) => p.load(type_name, key),
            EntityPersister::FileSystem(_) => todo!(),
        }
    }

    pub fn delete<T: super::Keyed>(
        self: &mut Self,
        _type_name: &'static str,
        key: &T,
    ) -> DeleteResult {
        match self {
            EntityPersister::InMemory(p) => p.delete(_type_name, key),
            EntityPersister::FileSystem(_) => todo!(),
        }
    }
}
