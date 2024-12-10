use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use crate::contracts::v1;

pub mod requests;
pub mod responses;

pub struct ContractSerializer {}

impl ContractSerializer {
    fn serialize<T: Serialize>(entity: T) -> super::SerializeResult {
        let mut buffer = Vec::with_capacity(super::BUFFER_CAPACITY);
        let mut serializer = Serializer::new(&mut buffer);
        match entity.serialize(&mut serializer) {
            Ok(_) => Ok(buffer),
            Err(err) => Err(super::SerializeError::Error { msg: format!("{err:?}")}),
        }
    }

    fn deserialize<'a, T>(buffer: Vec<u8>) -> super::DeserializeResult<T> where T: Deserialize<'a>{
        let mut deserializer = Deserializer::new(&buffer[..]);
        match Deserialize::deserialize(&mut deserializer) {
            Ok(entity) => super::DeserializeResult::Ok(entity),
            Err(err) => Err(super::DeserializeError::Error { msg: format!("{err:?}")}),
        }
    }

    pub fn serialize_node_detail_response(node_detail: &v1::responses::NodeDetail) -> super::SerializeResult {
        Self::serialize(node_detail)
    }

    pub fn deserialize_node_detail_response(buffer: Vec<u8>) -> super::DeserializeResult<v1::responses::NodeDetail> {
        Self::deserialize(buffer)
    } 
}

