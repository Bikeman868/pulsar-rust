pub mod v1;

#[derive(Debug, PartialEq)]
pub enum SerializeError {
    Error { msg: String },
}

pub type SerializeResult = Result<Vec<u8>, SerializeError>;


#[derive(Debug, PartialEq)]
pub enum DeserializeError {
    Error { msg: String },
}

pub type DeserializeResult<T> = Result<T, DeserializeError>;


const BUFFER_CAPACITY: usize = 2048;

pub type ContractVersionNumber = u16;

pub enum Version {
    V1(v1::ContractSerializer),
}

impl Version {
    pub fn min() -> ContractVersionNumber { 1 }
    pub fn max() -> ContractVersionNumber { 1 }
}
