use core::time::Duration;

use std::
    sync::{
        atomic::{
            AtomicBool, 
            Ordering
        }, mpsc::{Receiver, RecvTimeoutError, Sender}, Arc
    } 
;

use log::{warn, info, debug};
use pulsar_rust_net::sockets::{buffer_pool::{self, BufferPool}, MessageLength};
use super::server::ServerMessage;
use crate::App;


pub(crate) struct ProcessingThread {
    app: Arc<App>, 
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool<>>,
    sender: Arc<Sender<ServerMessage>>,
    receiver: Receiver<ServerMessage>,
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
            buffer_pool: buffer_pool.clone(),
            stop_signal: stop_signal.clone(),
            sender: sender.clone(),
            receiver,
        }
    }

    pub(crate) fn run(self: Self) {
        info!("Binary API processing thread starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            match self.receiver.recv_timeout(Duration::from_millis(50)) {
                Ok(request) => {
                    // debug!("ProcessingThread: processing request {request:?}");
                    // Echo request for now
                    let mut response = self.buffer_pool.get(request.body.len() as MessageLength);
                    for i in 0..request.body.len() { 
                        response[i] = request.body[i]
                    }
                    self.sender.send(ServerMessage { body: response, connection_id: request.connection_id }).ok();
                    self.buffer_pool.reuse(request.body);
                },
                Err(e) => match e {
                    RecvTimeoutError::Timeout => (),
                    RecvTimeoutError::Disconnected => warn!("ProcessingThread"),
                }
            }
        };
        info!("Binary API processing thread stopping");
    }
}
