use super::contracts::{AckResult, ClientResult, ConsumeResult, NackResult, PublishResult};
use pulsar_rust_net::bin_serialization::RequestId;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

pub(crate) struct FutureResponseState<T> {
    pub(crate) result: Option<ClientResult<T>>,
    pub(crate) waker: Option<Waker>,
}

pub struct FutureResponse<T> {
    state: Arc<Mutex<FutureResponseState<T>>>,
}

pub(crate) struct FutureHashMap {
    pub publish_futures: HashMap<RequestId, Arc<Mutex<FutureResponseState<PublishResult>>>>,
    pub consume_futures: HashMap<RequestId, Arc<Mutex<FutureResponseState<ConsumeResult>>>>,
    pub ack_futures: HashMap<RequestId, Arc<Mutex<FutureResponseState<AckResult>>>>,
    pub nack_futures: HashMap<RequestId, Arc<Mutex<FutureResponseState<NackResult>>>>,
}

impl<T> FutureResponseState<T> {
    pub(crate) fn new() -> Self {
        Self {
            result: None,
            waker: None,
        }
    }
}

impl<T> FutureResponse<T> {
    pub(crate) fn new(state: &Arc<Mutex<FutureResponseState<T>>>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl<T> Future for FutureResponse<T> {
    type Output = ClientResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();
        if state.result.is_some() {
            Poll::Ready(state.result.take().unwrap())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl FutureHashMap {
    pub(crate) fn new() -> Self {
        Self {
            publish_futures: HashMap::new(),
            consume_futures: HashMap::new(),
            ack_futures: HashMap::new(),
            nack_futures: HashMap::new(),
        }
    }
}
