/*
Provides functions for adding, retrieving, updating, and deleting entities that are persisted to the database.

If two threads (even ones running on different pods) simultaneously read-modify-write the same entity, then
one thread will succeed and the other will retry the whole read-modify-write operation. This guarantees that
all updates are persisted.

Ensures referential integrity between entities. For example adding or deleting a partition updates the
partition list of the topic it belongs to. Also does cascading deletes, so deleting a topic will delete all of
the partitions and subscriptions, and for each partition, the ledgers will be deleted.

Note that this data layer is designed for storing configuration data only, and is not suitable for high throughput.
As originally implemented, we store topics, partitions etc and these things change very rarely. Do not add volatile
information to these entities and try to update them thousands of times per second, it won't work.
*/

pub mod cluster;
pub mod node;
pub mod topic;
pub mod partition;
pub mod ledger;
pub mod subscription;

use std::{fmt::Debug, sync::Arc};
use serde::Deserialize;
use crate::persistence::{
    entity_persister::LoadError,
    Keyed, PersistenceLayer,    
};

#[derive(Debug, PartialEq)]
pub enum DataUpdateError {
    Unmodified,
    PersistenceFailure { msg: String },
    NotFound,
}
pub type DataUpdateResult<T> = Result<T, DataUpdateError>;

#[derive(Debug, PartialEq)]
pub enum DataReadError {
    PersistenceFailure { msg: String },
    NotFound,
}
pub type DataReadResult<T> = Result<T, DataReadError>;

#[derive(Debug, PartialEq)]
pub enum DataAddError {
    Duplicate { msg: String },
    PersistenceFailure { msg: String },
}
pub type DataAddResult<T> = Result<T, DataAddError>;

pub struct DataLayer {
    cluster_name: String,
    persistence: Arc<PersistenceLayer>,
}

impl DataLayer {
    pub fn new(cluster_name: String, persistence: &Arc<PersistenceLayer>) -> Self {
        Self {
            cluster_name,
            persistence: Arc::clone(persistence),
        }
    }

    fn get_entity<'b, T: Deserialize<'b>>(self: &Self, key: &impl Keyed) -> DataReadResult<T> {
        match self.persistence.load::<T>(key) {
            Ok(entity) => DataReadResult::Ok(entity),
            Err(e) => match e {
                LoadError::Error { msg } => DataReadResult::Err(DataReadError::PersistenceFailure { msg }),
                LoadError::NotFound {
                    entity_type,
                    entity_key,
                } => DataReadResult::Err(DataReadError::PersistenceFailure {
                    msg: format!(
                        "{} entity with id={} was not found",
                        entity_type, entity_key
                    ),
                }),
            },
        }
    }
}
