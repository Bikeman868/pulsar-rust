use super::{
    contracts::PublishResult, 
    contracts::ConsumeResult, 
    contracts::AckResult, 
    contracts::NackResult, 
    future_response::FutureHashMap,
};
use crate::api_bin::contracts::ClientError;
use log::{debug, info, warn};
use pulsar_rust_net::{
    bin_serialization::{BrokerResponse, ContractSerializer, ResponsePayload},
    contracts::v1::responses::RequestOutcome,
    error_codes::ERROR_CODE_INCORRECT_NODE,
    sockets::buffer_pool::BufferPool,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, TryRecvError},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

pub(crate) struct AsyncReceiverThread {
    receiver: Receiver<Vec<u8>>,
    stop_signal: Arc<AtomicBool>,
    futures: Arc<Mutex<FutureHashMap>>,
    serializer: ContractSerializer,
    last_message_instant: Instant,
}

impl AsyncReceiverThread {
    pub(crate) fn new(
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
        futures: &Arc<Mutex<FutureHashMap>>,
        receiver: Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            stop_signal: stop_signal.clone(),
            futures: futures.clone(),
            serializer: ContractSerializer::new(&buffer_pool),
            receiver,
            last_message_instant: Instant::now(),
        }
    }

    pub(crate) fn run(mut self: Self) {
        info!("ClientReceiverThread: Started");

        while !self.stop_signal.load(Ordering::Relaxed) {
            if let Some(response) = self.try_receive() {
                self.complete_future(response);
                self.last_message_instant = Instant::now();
            }
            self.sleep_if_idle();
        }

        info!("ClientReceiverThread: Stopped");
    }

    fn try_receive(self: &mut Self) -> Option<BrokerResponse> {
        match self.receiver.try_recv() {
            Ok(buffer) => {
                match self.serializer.deserialize_response(buffer) {
                    Ok(response) => {
                        #[cfg(debug_assertions)]
                        debug!("ClientReceiverThread: Received {:?}", &response);
                        Some(response)
                    }
                    Err(err) => {
                        warn!("ClientReceiverThread: Failed to deserialize response from broker. {:?}", err);
                        None
                    }
                }
            }
            Err(err) => match err {
                TryRecvError::Empty => None,
                TryRecvError::Disconnected => {
                    info!("ClientReceiverThread: Receive channel disconnected");
                    self.stop_signal.store(true, Ordering::Relaxed);
                    None
                }
            },
        }
    }

    fn complete_future(self: &Self, response: BrokerResponse) {
        let request_id = response.request_id;
        match response.payload {
            ResponsePayload::V1Publish(response) => {
                let mut futures = self.futures.lock().unwrap();
                match futures.publish_futures.remove(&request_id) {
                    Some(state) => {
                        if let RequestOutcome::Warning(ref msg) = response.outcome {
                            warn!("ClientReceiverThread: Warning from broker publishing message {}", msg);
                        }
                        let result = if let Some(data) = response.data {
                            Ok(PublishResult::from(&data))
                        } else {
                            if let RequestOutcome::Error(msg, error_code) = response.outcome {
                                if error_code == ERROR_CODE_INCORRECT_NODE {
                                    Err(ClientError::IncorrectNode)
                                } else {
                                    Err(ClientError::Error(msg, error_code))
                                }
                            } else {
                                Err(ClientError::BadOutcome(response.outcome))
                            }
                        };
                        let mut state = state.lock().unwrap();
                        state.result = Some(result);
                        if let Some(waker) = state.waker.take() { waker.wake() }
                    }
                    None => warn!("ClientReceiverThread: Publish response received for request {request_id} but there is no corresponding publish future"),
                }
            }
            ResponsePayload::V1Consume(response) => {
                let mut futures = self.futures.lock().unwrap();
                match futures.consume_futures.remove(&request_id) {
                    Some(state) => {
                        if let RequestOutcome::Warning(ref msg) = response.outcome {
                            warn!("ClientReceiverThread: Warning from broker consuming messages {}", msg);
                        }
                        let result = if let Some(data) = response.data {
                            Ok(ConsumeResult::from(&data))
                        } else {
                            if let RequestOutcome::Error(msg, error_code) = response.outcome {
                                Err(ClientError::Error(msg, error_code))
                            } else {
                                Err(ClientError::BadOutcome(response.outcome))
                            }
                        };
                        let mut state = state.lock().unwrap();
                        state.result = Some(result);
                        if let Some(waker) = state.waker.take() { waker.wake() }
                    }
                    None => warn!("ClientReceiverThread: Consume response received for request {request_id} but there is no corresponding consume future"),
                }
            }
            ResponsePayload::V1Ack(response) => {
                let mut futures = self.futures.lock().unwrap();
                match futures.ack_futures.remove(&request_id) {
                    Some(state) => {
                        if let RequestOutcome::Warning(ref msg) = response.outcome {
                            warn!("ClientReceiverThread: Warning from broker ack {}", msg);
                        }
                        let result = if let Some(data) = response.data {
                            Ok(AckResult::from(&data))
                        } else {
                            if let RequestOutcome::Error(msg, error_code) = response.outcome {
                                Err(ClientError::Error(msg, error_code))
                            } else {
                                Err(ClientError::BadOutcome(response.outcome))
                            }
                        };
                        let mut state = state.lock().unwrap();
                        state.result = Some(result);
                        if let Some(waker) = state.waker.take() { waker.wake() }
                    }
                    None => warn!("ClientReceiverThread: Ack response received for request {request_id} but there is no corresponding ack future"),
                }
            }
            ResponsePayload::V1Nack(response) => {
                let mut futures = self.futures.lock().unwrap();
                match futures.nack_futures.remove(&request_id) {
                    Some(state) => {
                        if let RequestOutcome::Warning(ref msg) = response.outcome {
                            warn!("ClientReceiverThread: Warning from broker nack {}", msg);
                        }
                        let result = if let Some(data) = response.data {
                            Ok(NackResult::from(&data))
                        } else {
                            if let RequestOutcome::Error(msg, error_code) = response.outcome {
                                Err(ClientError::Error(msg, error_code))
                            } else {
                                Err(ClientError::BadOutcome(response.outcome))
                            }
                        };
                        let mut state = state.lock().unwrap();
                        state.result = Some(result);
                        if let Some(waker) = state.waker.take() { waker.wake() }
                    }
                    None => warn!("ClientReceiverThread: Nack response received for request {request_id} but there is no corresponding nack future"),
                }
            }
            _ => warn!("Received async response to non-async request"),
        }
    }

    fn sleep_if_idle(self: &Self) {
        let idle_duration = self.last_message_instant.elapsed();
        if idle_duration > Duration::from_millis(20) {
            thread::sleep(Duration::from_millis(2));
        }
    }
}
