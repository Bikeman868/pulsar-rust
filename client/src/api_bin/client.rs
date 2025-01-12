use super::{connection::Connection, contracts::PublishResult};
use log::{debug, info, warn};
use pulsar_rust_net::{
    bin_serialization::{
        ContractSerializer, DeserializeError, Request, RequestId, RequestPayload, ResponsePayload,
    },
    contracts::v1::{self, requests::NegotiateVersion, responses::RequestOutcome},
    data_types::{ContractVersionNumber, PartitionId, Timestamp, TopicId},
    sockets::buffer_pool::BufferPool,
};
use std::{
    collections::HashMap,
    sync::{
        mpsc::{RecvError, SendError, TryRecvError},
        Arc, Mutex,
    },
};
use uuid::Uuid;

pub(crate) type ClientMessage = Vec<u8>;

#[derive(Debug)]
pub enum ClientError {
    NotConnected,
    IncompatibleVersion,
    VersionNotSupported,
    SendError(SendError<Vec<u8>>),
    BadOutcome(RequestOutcome),
    NoData,
    IncorrectResponseType,
    DeserializeError(DeserializeError),
    RecvError(RecvError),
}

pub type ClientResult<T> = Result<T, ClientError>;

pub struct Client {
    authority: String,
    buffer_pool: Arc<BufferPool>,
    serializer: ContractSerializer,
    connection: Option<Connection>,
    version: Option<ContractVersionNumber>,
    next_request_id: Mutex<RequestId>,
}

impl Client {
    pub fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        info!("Client: Constructed for {authority}");

        Self {
            authority: String::from(authority),
            buffer_pool: buffer_pool.clone(),
            connection: None,
            serializer: ContractSerializer::new(&buffer_pool),
            version: None,
            next_request_id: Mutex::new(1),
        }
    }

    pub fn connect(self: &mut Self) {
        self.connection = Some(Connection::new(&self.buffer_pool, &self.authority));

        let payload = NegotiateVersion {
            min_version: 1,
            max_version: 1,
        };
        let request = Request {
            request_id: 0,
            payload: RequestPayload::NegotiateVersion(payload),
        };

        #[cfg(debug_assertions)]
        debug!("Client: Sending {:?}", request);

        let message = self.serializer.serialize_request(&request).unwrap();

        if let Err(e) = self.send(message) {
            panic!("Client: Error sending API version negotiation request: {e}");
        }

        match self.recv() {
            Ok(message) => match self.serializer.deserialize_response(message) {
                Ok(response) => {
                    #[cfg(debug_assertions)]
                    debug!("Client: Received {:?}", &response);
                    if let ResponsePayload::NegotiateVersion(version_response) = response.payload {
                        match version_response.outcome {
                            RequestOutcome::Success => {
                                if let Some(data) = version_response.data {
                                    #[cfg(debug_assertions)]
                                    debug!("Client: Negotiated API version {}", data.version);
                                    self.version = Some(data.version);
                                }
                            }
                            RequestOutcome::Warning(msg) => {
                                if let Some(data) = version_response.data {
                                    #[cfg(debug_assertions)]
                                    debug!("Client: Negotiated API version: {}", data.version);
                                    self.version = Some(data.version);
                                }
                                warn!("Client: Warning negotiating API version {}", msg)
                            }
                            RequestOutcome::NoData(msg) => {
                                panic!("Client: No data negotiating API version: {}", msg)
                            }
                            RequestOutcome::Error(msg) => {
                                panic!("Client: Error negotiating API version: {}", msg)
                            }
                        }
                    } else {
                        panic!("Client: Received wrong response type for negotiate version request")
                    }
                }
                Err(err) => match err {
                    DeserializeError::Error { msg } => panic!("Client: {}", msg),
                },
            },
            Err(err) => {
                panic!("Client: Error receiving API version negotiation response: {err}");
            }
        }
    }

    pub fn disconnect(self: &mut Self) {
        if let Some(connection) = self.connection.take() {
            connection.disconnect();
        }
    }

    pub fn is_connected(self: &Self) -> bool {
        self.connection.is_some()
    }

    pub fn publish(
        self: &Self,
        topic_id: TopicId,
        key: Option<String>,
        timestamp: Option<Timestamp>,
        attributes: HashMap<String, String>,
    ) -> ClientResult<PublishResult> {
        let request_id = self.get_next_request_id();
        let key: String = key.unwrap_or_else(|| Uuid::new_v4().to_string());

        match self.send_publish(request_id, topic_id, key, timestamp, attributes) {
            Ok(_) => match self.recv() {
                Ok(message) => match self.serializer.deserialize_response(message) {
                    Ok(response) => {
                        #[cfg(debug_assertions)]
                        debug!("Client: Received {:?}", &response);
                        if let ResponsePayload::V1Publish(publish_response) = response.payload {
                            if let RequestOutcome::Warning(ref msg) = publish_response.outcome {
                                warn!("Client: Warning publishing message {}", msg);
                            }
                            if let Some(data) = publish_response.data {
                                Ok(PublishResult::from(&data))
                            } else {
                                Err(ClientError::BadOutcome(publish_response.outcome))
                            }
                        } else {
                            Err(ClientError::IncorrectResponseType)
                        }
                    }
                    Err(err) => Err(ClientError::DeserializeError(err)),
                },
                Err(err) => Err(ClientError::RecvError(err)),
            },
            Err(err) => Err(err),
        }
    }

    fn get_next_request_id(self: &Self) -> RequestId {
        let mut next_request_id_lock = self.next_request_id.lock().unwrap();
        let request_id = *next_request_id_lock;
        *next_request_id_lock += 1;
        request_id
    }

    fn send_publish(
        self: &Self,
        request_id: RequestId,
        topic_id: TopicId,
        key: String,
        timestamp: Option<Timestamp>,
        attributes: HashMap<String, String>,
    ) -> ClientResult<()> {
        if self.connection.is_none() {
            return Err(ClientError::NotConnected);
        }
        if self.version.is_none() {
            return Err(ClientError::IncompatibleVersion);
        }
        let version = self.version.unwrap();

        let request = match version {
            1 => Request {
                request_id,
                payload: RequestPayload::V1Publish(v1::requests::Publish {
                    topic_id,
                    partition_id: self.get_partition_id(topic_id, &key),
                    key,
                    timestamp,
                    attributes,
                }),
            },
            _ => return Err(ClientError::VersionNotSupported),
        };

        #[cfg(debug_assertions)]
        debug!("Client: Sending {:?}", request);

        let message = self.serializer.serialize_request(&request).unwrap();

        if let Err(err) = self.send(message) {
            Err(ClientError::SendError(err))
        } else {
            Ok(())
        }
    }

    fn get_partition_id(self: &Self, _topic_id: TopicId, _key: &str) -> PartitionId {
        // TODO: hash message key to partition id
        1
    }

    fn try_recv(self: &Self) -> Result<ClientMessage, TryRecvError> {
        if let Some(connection) = &self.connection {
            connection.try_recv()
        } else {
            Err(TryRecvError::Disconnected)
        }
    }

    fn recv(self: &Self) -> Result<ClientMessage, RecvError> {
        if let Some(connection) = &self.connection {
            connection.recv()
        } else {
            Err(RecvError)
        }
    }

    fn send(&self, message: ClientMessage) -> Result<(), SendError<ClientMessage>> {
        if let Some(connection) = &self.connection {
            connection.send(message)
        } else {
            Err(SendError(message))
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.disconnect();
        info!("Client: Dropped");
    }
}
