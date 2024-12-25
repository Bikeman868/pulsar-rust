use serde::{Deserialize, Serialize};

pub mod v1;

pub type SerializeResult = Result<Vec<u8>, SerializeError>;
pub type DeserializeResult<T> = Result<T, DeserializeError>;
pub type ContractVersionNumber = u16;

#[derive(Debug, PartialEq)]
pub enum SerializeError {
    Error { msg: String },
}

#[derive(Debug, PartialEq)]
pub enum DeserializeError {
    Error { msg: String },
}

pub enum Version {
    V1(v1::ContractSerializer),
}

/// These are used to negotiate a contract version number to use in client server comms
impl Version {
    pub const MIN: ContractVersionNumber = 1;
    pub const MAX: ContractVersionNumber = 1;

    pub fn serialize<T: Serialize>(self: &Self, entity: T) -> SerializeResult {
        match self {
            Version::V1(serializer) => serializer.serialize(entity),
        }
    }

    pub fn deserialize<'a, T>(self: &Self, buffer: Vec<u8>) -> DeserializeResult<T>
    where
        T: Deserialize<'a>,
    {
        match self {
            Version::V1(serializer) => serializer.deserialize(buffer),
        }
    }
}
