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
    bin_serialization::{ContractSerializer, RequestPayload, Response, ResponsePayload},
    contracts::v1::{self, responses::MessageRef},
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
                                            ResponsePayload::V1Publish(v1::responses::Response::error(&msg)),
                                        crate::services::pub_service::PubError::TopicNotFound =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error("Unknown topic ID")),
                                        crate::services::pub_service::PubError::PartitionNotFound =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error("Unknown partition ID")),
                                        crate::services::pub_service::PubError::NodeNotFound =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error("Unknown node for this partition")),
                                        crate::services::pub_service::PubError::WrongNode(entity_ref) =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error(&format!("This node is not the owner of the partition, publish to {} instead", entity_ref.ip_address()))),
                                        crate::services::pub_service::PubError::BacklogCapacityExceeded =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error("Backlog capacity exceeded")),
                                        crate::services::pub_service::PubError::NoSubscribers =>
                                            ResponsePayload::V1Publish(v1::responses::Response::error("No subscribers to this topic")),
                                    }
                                }
                            }
                            RequestPayload::V1Consumer(_consumer) => todo!(),
                            RequestPayload::V1Ack(_ack) => todo!(),
                            RequestPayload::V1Nack(_nack) => todo!(),
                        };
                        let serialization_response = Response::new(request_id, response_payload);
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
