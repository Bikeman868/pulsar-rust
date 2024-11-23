use serde::{Serialize, Deserialize};
use rmp_serde::{Deserializer, Serializer};

use super::{ Keyed, Versioned, SaveError, LoadError, DeleteError };
use crate::model::data_types;

pub struct FileSystemEventPersister {
}

pub struct FileSystemStatePersister {
}

impl super::EventPersister for FileSystemEventPersister {
    fn log<T>(self: &Self, event: &T, timestamp: &data_types::Timestamp) -> Result<(), ()> 
        where T: Keyed + Serialize {
        let type_name = event.type_name();
        let key = event.key();

        let mut buffer = Vec::new();
        let mut serializer = Serializer::new(&mut buffer);
        event.serialize(&mut serializer).unwrap();

        println!("EVENT @{timestamp} {type_name}:{key} => {buffer:?}");
        Result::Ok(())
    }
}

impl super::StatePersister for FileSystemStatePersister {
    fn save<T>(self: &Self, entity: &T) -> Result<(), SaveError>
        where T: Versioned + Keyed + Serialize {
            let type_name = entity.type_name();
            let key = entity.key();
            let version = entity.version();
    
            let mut buffer = Vec::new();
            let mut serializer = Serializer::new(&mut buffer);
            entity.serialize(&mut serializer).unwrap();
    
            println!("SAVE {type_name}:{key}:{version} => {buffer:?}");
            Result::Ok(())
    }

    fn load<'a, T>(self: &Self, entity: & 'a mut T) -> Result<(), LoadError>
        where T: Keyed + Deserialize<'a> {
        Result::Ok(())
    }

    fn delete<T>(self: &Self, entity: &T) -> Result<(), DeleteError>
        where T: Keyed {
        Result::Ok(())
    }
}
