use super::{
    connection::Connection,
    contracts::{AckResult, ClientMessage, ClientResult, ConsumeResult, NackResult, PublishResult},
    future_response::{FutureResponse, FutureResponseState},
};
use crate::api_bin::{
    async_receiver_thread::AsyncReceiverThread, contracts::ClientError,
    future_response::FutureHashMap,
};
use log::{debug, info};
use pulsar_rust_net::{
    bin_serialization::{
        ContractSerializer, DeserializeError, Request, RequestId, RequestPayload, ResponsePayload,
    },
    contracts::v1::{self, requests::NegotiateVersion, responses::RequestOutcome},
    data_types::{
        ConsumerId, ContractVersionNumber, MessageCount, PartitionId, SubscriptionId, Timestamp,
        TopicId,
    },
    sockets::buffer_pool::BufferPool,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{RecvError, SendError},
        Arc, Mutex,
    },
    thread,
};
use uuid::Uuid;

pub struct Client {
    authority: String,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    serializer: ContractSerializer,
    connection: Option<Connection>,
    version: Option<ContractVersionNumber>,
    next_request_id: Mutex<RequestId>,
    futures: Arc<Mutex<FutureHashMap>>,
}

impl Client {
    pub fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        info!("Client: Constructed for {authority}");

        Self {
            authority: String::from(authority),
            buffer_pool: buffer_pool.clone(),
            stop_signal: Arc::new(AtomicBool::new(false)),
            connection: None,
            serializer: ContractSerializer::new(&buffer_pool),
            version: None,
            next_request_id: Mutex::new(1),
            futures: Arc::new(Mutex::new(FutureHashMap::new())),
        }
    }

    pub fn connect(self: &mut Self) -> Result<(), String> {
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
                                if let Some(connection) = &mut self.connection {
                                    if let Some(receiver) = connection.take_receiver() {
                                        let thread = AsyncReceiverThread::new(
                                            &self.buffer_pool,
                                            &self.stop_signal,
                                            &self.futures,
                                            receiver,
                                        );
                                        thread::Builder::new()
                                            .name(String::from("async-rx"))
                                            .spawn(|| thread.run())
                                            .unwrap();
                                    }
                                }
                                Ok(())
                            }
                            RequestOutcome::Warning(msg) => {
                                if let Some(data) = version_response.data {
                                    #[cfg(debug_assertions)]
                                    debug!("Client: Negotiated API version: {}", data.version);
                                    self.version = Some(data.version);
                                }
                                Err(format!("Client: Warning negotiating API version {}", msg))
                            }
                            RequestOutcome::NoData(msg) => {
                                Err(format!("Client: No data negotiating API version: {}", msg))
                            }
                            RequestOutcome::Error(msg, _code) => {
                                Err(format!("Client: Error negotiating API version: {}", msg))
                            }
                        }
                    } else {
                        Err(format!(
                            "Client: Received wrong response type for negotiate version request"
                        ))
                    }
                }
                Err(err) => match err {
                    DeserializeError::Error { msg } => Err(format!("Client: {}", msg)),
                },
            },
            Err(err) => Err(format!(
                "Client: Error receiving API version negotiation response: {err}"
            )),
        }
    }

    pub fn disconnect(self: &mut Self) {
        self.stop_signal.store(true, Ordering::Relaxed);
        if let Some(connection) = self.connection.take() {
            connection.disconnect();
        }
    }

    pub fn is_connected(self: &Self) -> bool {
        self.connection.is_some()
    }

    /// Asynchronously publishes a message, returning a future that will complete when a response is received from the broker
    pub fn publish(
        self: &Self,
        topic_id: TopicId,
        key: Option<String>,
        timestamp: Option<Timestamp>,
        attributes: HashMap<String, String>,
    ) -> ClientResult<FutureResponse<PublishResult>> {
        let request_id = self.get_next_request_id();
        let key: String = key.unwrap_or_else(|| Uuid::new_v4().to_string());

        #[cfg(debug_assertions)]
        debug!("Client: Request {} publish with key {}", request_id, key);

        match self.send_publish(request_id, topic_id, &key, timestamp, attributes) {
            Ok(_) => {
                let state = Arc::new(Mutex::new(FutureResponseState::new()));
                let future = FutureResponse::new(&state);
                let mut futures = self.futures.lock().unwrap();
                futures.publish_futures.insert(request_id, state);
                Ok(future)
            }
            Err(err) => Err(err),
        }
    }

    /// Asynchronously consumes messages, returning a future that will complete
    /// when a response is received from the broker
    pub fn consume(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
        consumer_id: &Option<ConsumerId>,
        max_messages: MessageCount,
    ) -> ClientResult<FutureResponse<ConsumeResult>> {
        let request_id = self.get_next_request_id();
        match self.send_consume(
            request_id,
            topic_id,
            subscription_id,
            consumer_id,
            max_messages,
        ) {
            Ok(_) => {
                let state = Arc::new(Mutex::new(FutureResponseState::new()));
                let future = FutureResponse::new(&state);
                let mut futures = self.futures.lock().unwrap();
                futures.consume_futures.insert(request_id, state);
                Ok(future)
            }
            Err(err) => Err(err),
        }
    }

    /// Synchronously acknowledges a message, blocking until a response is received
    /// from the broker. This will cause the message to be deleted from the subscription
    pub fn ack(
        self: &Self,
        message_ref_key: &str,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> ClientResult<FutureResponse<AckResult>> {
        let request_id = self.get_next_request_id();
        match self.send_ack(request_id, message_ref_key, subscription_id, consumer_id) {
            Ok(_) => {
                let state = Arc::new(Mutex::new(FutureResponseState::new()));
                let future = FutureResponse::new(&state);
                let mut futures = self.futures.lock().unwrap();
                futures.ack_futures.insert(request_id, state);
                Ok(future)
            }
            Err(err) => Err(err),
        }
    }

    /// Synchronously negatively acknowledges a message, blocking until a response is received from the broker
    /// This will cause the message to be re-delivered
    pub fn nack(
        self: &Self,
        message_ref_key: &str,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> ClientResult<FutureResponse<NackResult>> {
        let request_id = self.get_next_request_id();
        match self.send_nack(request_id, message_ref_key, subscription_id, consumer_id) {
            Ok(_) => {
                let state = Arc::new(Mutex::new(FutureResponseState::new()));
                let future = FutureResponse::new(&state);
                let mut futures = self.futures.lock().unwrap();
                futures.nack_futures.insert(request_id, state);
                Ok(future)
            }
            Err(err) => Err(err),
        }
    }

    fn get_next_request_id(self: &Self) -> RequestId {
        let mut next_request_id = self.next_request_id.lock().unwrap();
        let request_id = *next_request_id;
        *next_request_id = if request_id == RequestId::MAX {
            0
        } else {
            request_id + 1
        };
        request_id
    }

    fn send_publish(
        self: &Self,
        request_id: RequestId,
        topic_id: TopicId,
        key: &str,
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
                    key: key.to_owned(),
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

    fn send_consume(
        self: &Self,
        request_id: RequestId,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
        consumer_id: &Option<ConsumerId>,
        max_messages: MessageCount,
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
                payload: RequestPayload::V1Consume(v1::requests::Consume {
                    topic_id,
                    subscription_id,
                    consumer_id: consumer_id.clone(),
                    max_messages,
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

    fn send_ack(
        self: &Self,
        request_id: RequestId,
        message_ref_key: &str,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
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
                payload: RequestPayload::V1Ack(v1::requests::Ack {
                    message_ref_key: message_ref_key.to_owned(),
                    subscription_id,
                    consumer_id,
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

    fn send_nack(
        self: &Self,
        request_id: RequestId,
        message_ref_key: &str,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
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
                payload: RequestPayload::V1Nack(v1::requests::Nack {
                    message_ref_key: message_ref_key.to_owned(),
                    subscription_id,
                    consumer_id,
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
