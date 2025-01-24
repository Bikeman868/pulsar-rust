/*
For APIs that use binary serialization, wraps the request and response in an envelope that contains
- The type that was serialized
- The version of the data contract
- A unique ID for the request so that responses can be matched up with requests
Serializes and deserilizes these messages to byte arrays for transmission over Tcp
*/

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    contracts::v1::{self},
    sockets::{buffer_pool::BufferPool, MessageLength},
};

pub type SerializeResult = Result<Vec<u8>, SerializeError>;
pub type DeserializeResult<T> = Result<T, DeserializeError>;

#[derive(Debug, PartialEq)]
pub enum SerializeError {
    Error { msg: String },
}

#[derive(Debug, PartialEq)]
pub enum DeserializeError {
    Error { msg: String },
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum RequestPayload {
    NegotiateVersion(v1::requests::NegotiateVersion),
    V1Publish(v1::requests::Publish),
    V1Consume(v1::requests::Consume),
    V1Ack(v1::requests::Ack),
    V1Nack(v1::requests::Nack),
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Request {
    pub request_id: RequestId,
    pub payload: RequestPayload,
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum ResponsePayload {
    NegotiateVersion(v1::responses::Response<v1::responses::NegotiateVersionResult>),
    V1Publish(v1::responses::Response<v1::responses::PublishResult>),
    V1Consume(v1::responses::Response<v1::responses::ConsumeResult>),
    V1Ack(v1::responses::Response<v1::responses::AckResult>),
    V1Nack(v1::responses::Response<v1::responses::NackResult>),
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct BrokerResponse {
    pub request_id: RequestId,
    pub payload: ResponsePayload,
}

pub struct ContractSerializer {
    buffer_pool: Arc<BufferPool>,
}

type MessageTypeId = u16;
pub type RequestId = u32;

const BUFFER_CAPACITY: MessageLength = 2048;
const MESSAGE_TYPE_SIZE: usize = size_of::<MessageTypeId>();
const REQUEST_ID_SIZE: usize = size_of::<RequestId>();

const NEGOTIATE_VERSION_MESSAGE_TYPE_ID: MessageTypeId = 1;
const V1_PUBLISH_MESSAGE_TYPE_ID: MessageTypeId = 2;
const V1_CONSUMER_MESSAGE_TYPE_ID: MessageTypeId = 3;
const V1_ACK_MESSAGE_TYPE_ID: MessageTypeId = 4;
const V1_NACK_MESSAGE_TYPE_ID: MessageTypeId = 5;

impl BrokerResponse {
    pub fn new(request_id: RequestId, payload: ResponsePayload) -> Self {
        Self {
            request_id,
            payload,
        }
    }

    pub fn for_request(request: &Request, payload: ResponsePayload) -> Self {
        Self {
            request_id: request.request_id,
            payload,
        }
    }
}

impl ContractSerializer {
    pub fn new(buffer_pool: &Arc<BufferPool>) -> Self {
        Self {
            buffer_pool: buffer_pool.clone(),
        }
    }

    pub fn serialize_request(self: &Self, request: &Request) -> SerializeResult {
        match &request.payload {
            RequestPayload::NegotiateVersion(negotiate_version) => self.serialize_entity(
                negotiate_version,
                NEGOTIATE_VERSION_MESSAGE_TYPE_ID,
                request.request_id,
            ),
            RequestPayload::V1Publish(publish) => {
                self.serialize_entity(publish, V1_PUBLISH_MESSAGE_TYPE_ID, request.request_id)
            }
            RequestPayload::V1Consume(consumer) => {
                self.serialize_entity(consumer, V1_CONSUMER_MESSAGE_TYPE_ID, request.request_id)
            }
            RequestPayload::V1Ack(ack) => {
                self.serialize_entity(ack, V1_ACK_MESSAGE_TYPE_ID, request.request_id)
            }
            RequestPayload::V1Nack(nack) => {
                self.serialize_entity(nack, V1_NACK_MESSAGE_TYPE_ID, request.request_id)
            }
        }
    }

    pub fn serialize_response(self: &Self, response: &BrokerResponse) -> SerializeResult {
        match &response.payload {
            ResponsePayload::NegotiateVersion(negotiate_version) => self.serialize_entity(
                negotiate_version,
                NEGOTIATE_VERSION_MESSAGE_TYPE_ID,
                response.request_id,
            ),
            ResponsePayload::V1Publish(publish) => {
                self.serialize_entity(publish, V1_PUBLISH_MESSAGE_TYPE_ID, response.request_id)
            }
            ResponsePayload::V1Consume(consumer) => {
                self.serialize_entity(consumer, V1_CONSUMER_MESSAGE_TYPE_ID, response.request_id)
            }
            ResponsePayload::V1Ack(ack) => {
                self.serialize_entity(ack, V1_ACK_MESSAGE_TYPE_ID, response.request_id)
            }
            ResponsePayload::V1Nack(nack) => {
                self.serialize_entity(nack, V1_NACK_MESSAGE_TYPE_ID, response.request_id)
            }
        }
    }

    pub fn deserialize_request(self: &Self, buffer: Vec<u8>) -> DeserializeResult<Request> {
        let (message_type, request_id) = self.extract_metadata(&buffer);

        match message_type {
            NEGOTIATE_VERSION_MESSAGE_TYPE_ID => {
                match self.deserialize_entity::<v1::requests::NegotiateVersion>(buffer) {
                    Ok(negotiate_version) => Ok(Request {
                        request_id,
                        payload: RequestPayload::NegotiateVersion(negotiate_version),
                    }),
                    Err(err) => Err(err),
                }
            }
            V1_PUBLISH_MESSAGE_TYPE_ID => {
                match self.deserialize_entity::<v1::requests::Publish>(buffer) {
                    Ok(publish) => Ok(Request {
                        request_id,
                        payload: RequestPayload::V1Publish(publish),
                    }),
                    Err(err) => Err(err),
                }
            }
            V1_CONSUMER_MESSAGE_TYPE_ID => {
                match self.deserialize_entity::<v1::requests::Consume>(buffer) {
                    Ok(consumer) => Ok(Request {
                        request_id,
                        payload: RequestPayload::V1Consume(consumer),
                    }),
                    Err(err) => Err(err),
                }
            }
            V1_ACK_MESSAGE_TYPE_ID => match self.deserialize_entity::<v1::requests::Ack>(buffer) {
                Ok(ack) => Ok(Request {
                    request_id,
                    payload: RequestPayload::V1Ack(ack),
                }),
                Err(err) => Err(err),
            },
            V1_NACK_MESSAGE_TYPE_ID => {
                match self.deserialize_entity::<v1::requests::Nack>(buffer) {
                    Ok(nack) => Ok(Request {
                        request_id,
                        payload: RequestPayload::V1Nack(nack),
                    }),
                    Err(err) => Err(err),
                }
            }
            _ => panic!("Unsupported message type {message_type} in request"),
        }
    }

    pub fn deserialize_response(self: &Self, buffer: Vec<u8>) -> DeserializeResult<BrokerResponse> {
        let (message_type, request_id) = self.extract_metadata(&buffer);

        match message_type {
            NEGOTIATE_VERSION_MESSAGE_TYPE_ID =>
                match self.deserialize_entity::<v1::responses::Response<v1::responses::NegotiateVersionResult>>(buffer) {
                    Ok(response) => Ok(BrokerResponse{ request_id, payload: ResponsePayload::NegotiateVersion(response) }),
                    Err(err) => Err(err),
                }
            V1_PUBLISH_MESSAGE_TYPE_ID =>
                match self.deserialize_entity::<v1::responses::Response<v1::responses::PublishResult>>(buffer) {
                    Ok(response) => Ok(BrokerResponse{ request_id, payload: ResponsePayload::V1Publish(response) }),
                    Err(err) => Err(err),
                }
            V1_CONSUMER_MESSAGE_TYPE_ID =>
                match self.deserialize_entity::<v1::responses::Response<v1::responses::ConsumeResult>>(buffer) {
                    Ok(response) => Ok(BrokerResponse{ request_id, payload: ResponsePayload::V1Consume(response) }),
                    Err(err) => Err(err),
                }
            V1_ACK_MESSAGE_TYPE_ID =>
                match self.deserialize_entity::<v1::responses::Response<v1::responses::AckResult>>(buffer) {
                    Ok(response) => Ok(BrokerResponse{ request_id, payload: ResponsePayload::V1Ack(response) }),
                    Err(err) => Err(err),
                }
            V1_NACK_MESSAGE_TYPE_ID =>
                match self.deserialize_entity::<v1::responses::Response<v1::responses::NackResult>>(buffer) {
                    Ok(response) => Ok(BrokerResponse{ request_id, payload: ResponsePayload::V1Nack(response) }),
                    Err(err) => Err(err),
                }
            _ => panic!("Unsupported message type {message_type} in response")
        }
    }

    fn serialize_entity<T: Serialize>(
        self: &Self,
        entity: &T,
        message_type_id: MessageTypeId,
        request_id: RequestId,
    ) -> SerializeResult {
        let mut buffer = self.buffer_pool.get_with_capacity(0, BUFFER_CAPACITY);
        buffer.extend_from_slice(&message_type_id.to_le_bytes());
        buffer.extend_from_slice(&request_id.to_le_bytes());
        let mut serializer = Serializer::new(&mut buffer);
        match entity.serialize(&mut serializer) {
            Ok(_) => Ok(buffer),
            Err(err) => Err(SerializeError::Error {
                msg: format!("{err}"),
            }),
        }
    }

    fn extract_metadata(self: &Self, buffer: &Vec<u8>) -> (MessageTypeId, RequestId) {
        let message_type_id: MessageTypeId =
            MessageTypeId::from_le_bytes(buffer[0..MESSAGE_TYPE_SIZE].try_into().unwrap());
        let request_id: RequestId = RequestId::from_le_bytes(
            buffer[MESSAGE_TYPE_SIZE..MESSAGE_TYPE_SIZE + REQUEST_ID_SIZE]
                .try_into()
                .unwrap(),
        );
        (message_type_id, request_id)
    }

    fn deserialize_entity<'a, T>(self: &Self, buffer: Vec<u8>) -> DeserializeResult<T>
    where
        T: Deserialize<'a>,
    {
        let mut deserializer = Deserializer::new(&buffer[MESSAGE_TYPE_SIZE + REQUEST_ID_SIZE..]);
        let result = match Deserialize::deserialize(&mut deserializer) {
            Ok(entity) => DeserializeResult::Ok(entity),
            Err(err) => Err(DeserializeError::Error {
                msg: format!("{err:?}"),
            }),
        };
        self.buffer_pool.reuse(buffer);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_serialize_request() {
        let min_version = 4;
        let max_version = 10;
        let request = v1::requests::NegotiateVersion {
            min_version,
            max_version,
        };

        let mut buffer = Vec::new();
        buffer.resize(MESSAGE_TYPE_SIZE + REQUEST_ID_SIZE, 0);
        let mut serializer = Serializer::new(&mut buffer);
        request.serialize(&mut serializer).unwrap();
    }

    #[test]
    fn should_serialize_response() {
        let version = 5;
        let result = v1::responses::NegotiateVersionResult { version };

        let mut buffer = Vec::new();
        buffer.resize(MESSAGE_TYPE_SIZE + REQUEST_ID_SIZE, 0);
        let mut serializer = Serializer::new(&mut buffer);
        result.serialize(&mut serializer).unwrap();
    }

    #[test]
    fn roundtrip_version_negotiation_request() {
        let buffer_pool = BufferPool::new();
        let serializer = ContractSerializer::new(&Arc::new(buffer_pool));

        let request_id = 99;
        let min_version = 4;
        let max_version = 10;

        let original_payload = v1::requests::NegotiateVersion {
            min_version,
            max_version,
        };
        let original_request = Request {
            request_id,
            payload: RequestPayload::NegotiateVersion(original_payload),
        };

        let buffer = serializer.serialize_request(&original_request).unwrap();

        let deserialized_request = serializer.deserialize_request(buffer).unwrap();

        assert_eq!(original_request.request_id, deserialized_request.request_id);

        if let RequestPayload::NegotiateVersion(deserialized_payload) = deserialized_request.payload
        {
            assert_eq!(min_version, deserialized_payload.min_version);
            assert_eq!(max_version, deserialized_payload.max_version);
        } else {
            panic!("Wrong type of payload")
        }
    }

    #[test]
    fn roundtrip_version_negotiation_response() {
        let buffer_pool = BufferPool::new();
        let serializer = ContractSerializer::new(&Arc::new(buffer_pool));

        let request_id = 33;
        let version = 6;
        let outcome = v1::responses::RequestOutcome::Success;

        let original_payload = v1::responses::NegotiateVersionResult { version };
        let original_payload_response = v1::responses::Response {
            outcome,
            data: Some(original_payload),
        };
        let original_response = BrokerResponse {
            request_id,
            payload: ResponsePayload::NegotiateVersion(original_payload_response),
        };

        let buffer = serializer.serialize_response(&original_response).unwrap();

        let deserialized_response = serializer.deserialize_response(buffer).unwrap();

        assert_eq!(
            original_response.request_id,
            deserialized_response.request_id
        );

        if let ResponsePayload::NegotiateVersion(deserialized_payload) =
            deserialized_response.payload
        {
            if !matches!(
                deserialized_payload.outcome,
                v1::responses::RequestOutcome::Success
            ) {
                panic!()
            }
            if let Some(data) = deserialized_payload.data {
                assert_eq!(version, data.version);
            } else {
                panic!("No data")
            }
        } else {
            panic!("Wrong type of payload")
        }
    }
}
