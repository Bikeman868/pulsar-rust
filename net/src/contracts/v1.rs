use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

pub mod display;
pub mod requests;
pub mod responses;
pub struct ContractSerializer {}

const BUFFER_CAPACITY: usize = 2048;

impl ContractSerializer {
    // TODO: maintain a pool of buffers

    pub fn serialize<T: Serialize>(self: &Self, entity: T) -> super::SerializeResult {
        let mut buffer = Vec::with_capacity(BUFFER_CAPACITY);
        let mut serializer = Serializer::new(&mut buffer);
        match entity.serialize(&mut serializer) {
            Ok(_) => Ok(buffer),
            Err(err) => Err(super::SerializeError::Error {
                msg: format!("{err:?}"),
            }),
        }
    }

    pub fn deserialize<'a, T>(self: &Self, buffer: Vec<u8>) -> super::DeserializeResult<T>
    where
        T: Deserialize<'a>,
    {
        let mut deserializer = Deserializer::new(&buffer[..]);
        match Deserialize::deserialize(&mut deserializer) {
            Ok(entity) => super::DeserializeResult::Ok(entity),
            Err(err) => Err(super::DeserializeError::Error {
                msg: format!("{err:?}"),
            }),
        }
    }
}
