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
use log::{info, warn};
use pulsar_rust_net::sockets::{
    buffer_pool::BufferPool,
    MessageLength,
};

#[cfg(debug_assertions)]
use log::debug;

/// Receives requests from a mpsc channel, and processes the request to produce a reply,
/// postig the reply into another mpsc channel.
pub(crate) struct ProcessingThread {
    _app: Arc<App>,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
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
            _app: app.clone(),
            buffer_pool: buffer_pool.clone(),
            stop_signal: stop_signal.clone(),
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
            Ok(request) => {
                self.last_message_instant = Instant::now();
                #[cfg(debug_assertions)]
                debug!("ProcessingThread: processing request {request:?}");
                // TODO: Deserialize messages, process them using App services and serialize responses
                let mut response = self.buffer_pool.get(request.body.len() as MessageLength);
                for i in 0..request.body.len() {
                    response[i] = request.body[i] ^ 0x7f
                }
                self.sender
                    .send(ServerMessage {
                        body: response,
                        connection_id: request.connection_id,
                    })
                    .ok();
                self.buffer_pool.reuse(request.body);
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
