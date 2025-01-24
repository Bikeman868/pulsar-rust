use core::time::Duration;

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender, TryRecvError},
        Arc,
    },
    thread,
    time::Instant,
};

use super::server::ServerMessage;
use crate::App;
use log::{error, info, warn};
use pulsar_rust_net::{
    bin_serialization::{BrokerResponse, ContractSerializer, RequestPayload, ResponsePayload},
    contracts::v1::{self, responses::MessageRef},
    error_codes::{
        ERROR_CODE_BACKLOG_FULL, ERROR_CODE_GENERAL_FAILURE, ERROR_CODE_INCORRECT_NODE,
        ERROR_CODE_NO_COMPATIBLE_VERSION,
    },
    sockets::buffer_pool::BufferPool,
};

#[cfg(debug_assertions)]
use log::debug;

/// Receives requests from a mpsc channel, and processes the request to produce a reply,
/// postig the reply into another mpsc channel.
pub(crate) struct ProcessingThread {
    app: Arc<App>,
    stop_signal: Arc<AtomicBool>,
    serializer: ContractSerializer,
    sender: Arc<Sender<ServerMessage>>,
    receiver: Receiver<ServerMessage>,
    last_message_instant: Instant,
}

impl ProcessingThread {
    pub(crate) fn new(
        app: &Arc<App>,
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
        sender: &Arc<Sender<ServerMessage>>,
        receiver: Receiver<ServerMessage>,
    ) -> Self {
        Self {
            app: app.clone(),
            stop_signal: stop_signal.clone(),
            serializer: ContractSerializer::new(buffer_pool),
            sender: sender.clone(),
            receiver,
            last_message_instant: Instant::now(),
        }
    }

    pub(crate) fn run(mut self: Self) {
        info!("ProcessingThread: Started");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_process();
            self.sleep_if_idle();
        }
        info!("ProcessingThread: Stopped");
    }

    fn try_process(self: &mut Self) {
        match self.receiver.try_recv() {
            Ok(request_message) => {
                self.last_message_instant = Instant::now();
                #[cfg(debug_assertions)]
                debug!(
                    "ProcessingThread: processing request {request_message:?} from {}",
                    request_message.connection_id
                );

                match self.serializer.deserialize_request(request_message.body) {
                    Ok(request) => {
                        #[cfg(debug_assertions)]
                        debug!(
                            "ProcessingThread: Received {:?} from connection {}",
                            request, request_message.connection_id
                        );
                        let request_id = request.request_id;
                        let response_payload = match request.payload {
                            RequestPayload::NegotiateVersion(negotiate_version) => {
                                if negotiate_version.min_version == 1 {
                                    ResponsePayload::NegotiateVersion(
                                        v1::responses::Response::success(
                                            v1::responses::NegotiateVersionResult { version: 1 },
                                        ),
                                    )
                                } else {
                                    ResponsePayload::NegotiateVersion(
                                        v1::responses::Response::error(
                                            "Maximum supported API version is 1",
                                            ERROR_CODE_NO_COMPATIBLE_VERSION,
                                        ),
                                    )
                                }
                            }
                            RequestPayload::V1Publish(v1_publish) => {
                                let publish_message = v1_publish.into();
                                match self.app.pub_service.publish_message(publish_message) {
                                    Ok(message_ref) => {
                                        ResponsePayload::V1Publish(v1::responses::Response::success(
                                            v1::responses::PublishResult {
                                                message_ref: message_ref.into(),
                                            },
                                        ))
                                    }
                                    Err(err) => match err {
                                        crate::services::pub_service::PubError::Error(msg) =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error(&msg, ERROR_CODE_GENERAL_FAILURE)),
                                        crate::services::pub_service::PubError::TopicNotFound =>
                                            ResponsePayload::V1Publish(v1::responses::Response::warning("Unknown topic ID")),
                                        crate::services::pub_service::PubError::PartitionNotFound =>
                                            ResponsePayload::V1Publish(v1::responses::Response::warning("Unknown partition ID")),
                                        crate::services::pub_service::PubError::NodeNotFound =>
                                            ResponsePayload::V1Publish(v1::responses::Response::warning("Unknown node for this partition")),
                                        crate::services::pub_service::PubError::WrongNode(entity_ref) =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error(&format!("This node is not the owner of the partition, publish to {} instead", entity_ref.ip_address()), ERROR_CODE_INCORRECT_NODE)),
                                        crate::services::pub_service::PubError::BacklogCapacityExceeded =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error("Backlog capacity exceeded", ERROR_CODE_BACKLOG_FULL)),
                                        crate::services::pub_service::PubError::NoSubscribers =>
                                            ResponsePayload::V1Publish(v1::responses::Response::warning("No subscribers to this topic")),
                                    }
                                }
                            }
                            RequestPayload::V1Consume(v1_consume) => {
                                let topic_id = v1_consume.topic_id;
                                let subscription_id = v1_consume.subscription_id;
                                let consumer_id = v1_consume.consumer_id;
                                let max_messages = v1_consume.max_messages;
                                match self.app.sub_service.consume_max_messages(
                                    topic_id,
                                    subscription_id,
                                    consumer_id,
                                    max_messages,
                                ) {
                                    Ok(messages) => ResponsePayload::V1Consume(
                                        v1::responses::Response::success(
                                            v1::responses::ConsumeResult::from(&messages),
                                        ),
                                    ),
                                    Err(_) => {
                                        ResponsePayload::V1Consume(v1::responses::Response::error(
                                            "Failed to allocate consumer id",
                                            ERROR_CODE_GENERAL_FAILURE,
                                        ))
                                    }
                                }
                            }
                            RequestPayload::V1Ack(v1_ack) => {
                                let message_ack_key = v1_ack.message_ref_key;
                                let subscription_id = v1_ack.subscription_id;
                                let consumer_id = v1_ack.consumer_id;
                                match self.app.sub_service.ack(
                                    message_ack_key,
                                    subscription_id,
                                    consumer_id,
                                ) {
                                    Ok(success) => ResponsePayload::V1Ack(if success {
                                        v1::responses::Response::success(v1::responses::AckResult {
                                            success: true,
                                        })
                                    } else {
                                        v1::responses::Response::warning(
                                            "Message was already acknowledged",
                                        )
                                    }),
                                    Err(_) => {
                                        ResponsePayload::V1Ack(v1::responses::Response::error(
                                            "Failed to ack message",
                                            ERROR_CODE_GENERAL_FAILURE,
                                        ))
                                    }
                                }
                            }
                            RequestPayload::V1Nack(v1_nack) => {
                                let message_ref_key = v1_nack.message_ref_key;
                                let subscription_id = v1_nack.subscription_id;
                                let consumer_id = v1_nack.consumer_id;
                                match self.app.sub_service.nack(
                                    message_ref_key,
                                    subscription_id,
                                    consumer_id,
                                ) {
                                    Ok(success) => ResponsePayload::V1Nack(if success {
                                        v1::responses::Response::success(
                                            v1::responses::NackResult { success: true },
                                        )
                                    } else {
                                        v1::responses::Response::warning(
                                            "Message was already acknowledged",
                                        )
                                    }),
                                    Err(_) => {
                                        ResponsePayload::V1Nack(v1::responses::Response::error(
                                            "Failed to nack message",
                                            ERROR_CODE_GENERAL_FAILURE,
                                        ))
                                    }
                                }
                            }
                        };
                        let serialization_response =
                            BrokerResponse::new(request_id, response_payload);
                        let response_message = ServerMessage {
                            body: self
                                .serializer
                                .serialize_response(&serialization_response)
                                .expect(&format!(
                                    "Failed to serialize response to {} on {} connection",
                                    request_id, request_message.connection_id
                                )),
                            connection_id: request_message.connection_id,
                        };
                        if let Err(_) = self.sender.send(response_message) {
                            self.fatal(&format!(
                                "Failed to send response to {} from connection {}",
                                request_id, request_message.connection_id
                            ));
                        }
                    }
                    Err(err) => {
                        error!(
                            "Failed to deserialize request from connection {}. {:?}",
                            request_message.connection_id, err
                        );
                    }
                };
            }
            Err(e) => match e {
                TryRecvError::Empty => thread::sleep(Duration::from_millis(1)),
                TryRecvError::Disconnected => self.fatal("Receive channel disconnected"),
            },
        }
    }

    fn sleep_if_idle(self: &Self) {
        let idle_duration = self.last_message_instant.elapsed();
        if idle_duration > Duration::from_millis(50) {
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn fatal(self: &Self, msg: &str) {
        warn!("ProcessingThread: {}", msg);
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
