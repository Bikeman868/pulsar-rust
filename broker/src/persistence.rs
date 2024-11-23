mod in_memory;
mod file_system;

use serde::{Serialize, Deserialize};
use crate::model::data_types;

pub fn build_event_persister() -> impl EventPersister { 
    // TODO: Examine the config and choose which persister to build
    in_memory::InMemoryEventPersister { }
}

pub fn build_state_persister() -> impl StatePersister { 
    // TODO: Examine the config and choose which persister to build
    in_memory::InMemoryStatePersister { }
}

pub trait EventPersister {
    fn log<T>(self: &Self, event: &T, timestamp: &data_types::Timestamp) -> Result<(), ()> where T: Keyed + Serialize;
}

pub trait StatePersister {
    fn save<T>(self: &Self, entity: &T) -> Result<(), SaveError>
        where T: Versioned + Keyed + Serialize;
    
    fn load<'a, T>(self: &Self, entity: & 'a mut T) -> Result<(), LoadError>
        where T: Keyed + Deserialize<'a>;

    fn delete<T>(self: &Self, entity: &T) -> Result<(), DeleteError>
        where T: Keyed;
}

pub trait Versioned {
    fn version(self: &Self) -> data_types::VersionNumber;
    fn set_version(self: &mut Self, version: data_types::VersionNumber);
}

pub trait Keyed {
    fn type_name(self: &Self) -> &'static str;
    fn key(self: &Self) -> String;
}

#[derive(Debug)]
pub enum SaveError {
    Error,
    VersionMissmatch,
}

#[derive(Debug)]
pub enum LoadError {
    Error,
    NotFound,
}

#[derive(Debug)]
pub enum DeleteError {
    Error,
    NotFound,
}
