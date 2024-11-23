use std::collections::HashMap;

use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};

use crate::{
    model::data_types::VersionNumber,
    persistence::{
        entities::{DeleteResult, LoadError, LoadResult, SaveError, SaveResult},
        Keyed, Versioned,
    },
};

pub struct StoredEntity {
    pub version: VersionNumber,
    pub _serialization: Vec<u8>,
}

pub struct EntityPersister {
    type_map: HashMap<&'static str, HashMap<String, StoredEntity>>,
}

impl EntityPersister {
    pub fn new() -> Self {
        Self {
            type_map: HashMap::new(),
        }
    }

    pub fn save<T: Versioned + Keyed + Serialize>(self: &mut Self, entity: &mut T) -> SaveResult {
        let type_name = T::type_name();
        let key = entity.key();
        let mut version = entity.version();

        let entity_map = self
            .type_map
            .entry(type_name)
            .or_insert_with(|| HashMap::new());

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

        println!("SAVE {type_name}:{key} V{version} => {buffer:?}");

        let stored_entity = StoredEntity {
            version,
            _serialization: buffer,
        };
        entity_map.insert(key, stored_entity);

        SaveResult::Ok(())
    }

    pub fn load<'a, TKey, TEntity>(
        self: &Self,
        _type_name: &'static str,
        _key: &'a TKey,
    ) -> LoadResult<TEntity>
    where
        TKey: Keyed,
        TEntity: Deserialize<'a>,
    {
        LoadResult::Err(LoadError::NotFound)
    }

    pub fn delete<T>(self: &mut Self, _type_name: &'static str, _key: &T) -> DeleteResult
    where
        T: Keyed,
    {
        DeleteResult::Ok(())
    }
}
