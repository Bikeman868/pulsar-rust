use std::{collections::HashMap, sync::RwLock};

use crate::persistence::{
    entity_persister::{DeleteError, DeleteResult, LoadError, LoadResult, SaveError, SaveResult},
    Keyed, Versioned,
};
use pulsar_rust_net::data_types::VersionNumber;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

pub struct StoredEntity {
    pub version: VersionNumber,
    pub serialization: Vec<u8>,
}

pub struct EntityPersister {
    type_map: RwLock<HashMap<&'static str, RwLock<HashMap<String, StoredEntity>>>>,
}

impl EntityPersister {
    pub fn new() -> Self {
        Self {
            type_map: RwLock::new(HashMap::new()),
        }
    }

    #[cfg(debug_assertions)]
    pub fn delete_all(self: &Self) {
        self.type_map.write().unwrap().clear();
        // println!("Persisted state cleared");
    }

    pub fn save<T: Versioned + Keyed + Serialize>(self: &Self, entity: &mut T) -> SaveResult {
        let type_name = entity.type_name();
        let key = entity.key();
        let mut version = entity.version();

        let mut type_map = self.type_map.write().unwrap();
        let mut entity_map = type_map
            .entry(type_name)
            .or_insert_with(|| RwLock::new(HashMap::new()))
            .write()
            .expect("Poisoned entity map");

        match entity_map.get(&key) {
            Some(saved_entity) => {
                if saved_entity.version != entity.version() {
                    return SaveResult::Err(SaveError::VersionMissmatch);
                }
                version = version + 1;
            }
            None => {
                version = 1;
            }
        }
        entity.set_version(version);

        let mut buffer = Vec::new();
        let mut serializer = Serializer::new(&mut buffer);
        entity.serialize(&mut serializer).unwrap();

        // println!("SAVE {type_name}:{key} V{version} => {buffer:?}");

        let stored_entity = StoredEntity {
            version,
            serialization: buffer,
        };
        entity_map.insert(key, stored_entity);

        SaveResult::Ok(())
    }

    pub fn load<'a, TEntity>(self: &Self, keyed: &impl Keyed) -> LoadResult<TEntity>
    where
        TEntity: Deserialize<'a>,
    {
        let mut type_map = self.type_map.write().unwrap();
        let entity_map = type_map
            .entry(keyed.type_name())
            .or_insert_with(|| RwLock::new(HashMap::new()))
            .read()
            .unwrap();

        match entity_map.get(&keyed.key()) {
            Some(stored_entity) => {
                // println!(
                //     "LOAD {}:{} FOUND V{}",
                //     keyed.type_name(),
                //     keyed.key(),
                //     stored_entity.version
                // );
                let mut deserializer = Deserializer::new(&stored_entity.serialization[..]);
                let entity: TEntity = Deserialize::deserialize(&mut deserializer).unwrap();
                LoadResult::Ok(entity)
            }
            None => {
                // println!("LOAD {}:{} NOT FOUND", keyed.type_name(), keyed.key());
                LoadResult::Err(LoadError::NotFound {
                    entity_type: keyed.type_name().to_string(),
                    entity_key: keyed.key().to_string(),
                })
            }
        }
    }

    pub fn delete(self: &Self, keyed: &impl Keyed) -> DeleteResult {
        let mut type_map = self.type_map.write().unwrap();
        let mut entity_map = type_map
            .entry(keyed.type_name())
            .or_insert_with(|| RwLock::new(HashMap::new()))
            .write()
            .unwrap();

        // println!("DELETE {}:{}", keyed.type_name(), keyed.key());

        match entity_map.remove(&keyed.key()) {
            Some(_) => DeleteResult::Ok(()),
            None => DeleteResult::Err(DeleteError::NotFound {
                entity_type: keyed.type_name().to_owned(),
                entity_key: keyed.key(),
            }),
        }
    }
}
