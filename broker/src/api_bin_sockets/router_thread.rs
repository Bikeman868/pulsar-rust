use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, TryRecvError},
        Arc, RwLock,
    },
    thread,
    time::{Duration, Instant},
};

use super::{
    connection::Connection,
    server::{ConnectionId, ServerMessage},
};
use log::{debug, info, warn};

pub(crate) struct RouterThread {
    connections: Arc<RwLock<HashMap<ConnectionId, Connection>>>,
    receiver: Receiver<ServerMessage>,
    stop_signal: Arc<AtomicBool>,
    last_message_instant: Instant,
}

impl RouterThread {
    pub(crate) fn new(
        receiver: Receiver<ServerMessage>,
        stop_signal: &Arc<AtomicBool>,
        connections: &Arc<RwLock<HashMap<ConnectionId, Connection>>>,
    ) -> Self {
        Self {
            receiver,
            connections: connections.clone(),
            stop_signal: stop_signal.clone(),
            last_message_instant: Instant::now(),
        }
    }

    pub(crate) fn run(mut self: Self) {
        info!("Router: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_route();
            self.sleep_if_idle();
        }
        info!("Router: Stopping");
    }

    fn try_route(self: &mut Self) {
        match self.receiver.try_recv() {
            Ok(message) => {
                self.last_message_instant = Instant::now();
                let connection_id = message.connection_id;
                #[cfg(debug_assertions)]
                debug!("Router: Received message for connection {connection_id}");
                match self.connections.read().unwrap().get(&message.connection_id) {
                    Some(connection) => {
                        #[cfg(debug_assertions)]
                        debug!("Router: Sending message to connection {connection_id}");
                        match connection.sender.send(message) {
                            Ok(_) => {
                                #[cfg(debug_assertions)]
                                debug!("Router: Sent message to connection {connection_id}");
                            }
                            Err(e) => {
                                #[cfg(debug_assertions)]
                                debug!("Router: Error sending to connection {connection_id}: {e}");
                                self.connections.write().unwrap().remove(&connection_id);
                            }
                        }
                    }
                    None => {
                        warn!("Router: Message can't be routed, unknown connection id {connection_id}");
                    }
                }
            }
            Err(e) => match e {
                TryRecvError::Empty => {}
                TryRecvError::Disconnected => self.fatal(&format!("Receive channel disconnected")),
            },
        }
    }

    fn sleep_if_idle(self: &Self) {
        let idle_duration = self.last_message_instant.elapsed();
        if idle_duration > Duration::from_millis(50) {
            #[cfg(debug_assertions)]
            debug!("ProcessingThread: idle more tham 50ms");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn fatal(self: &Self, msg: &str) {
        warn!("Router: {}", msg);
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
