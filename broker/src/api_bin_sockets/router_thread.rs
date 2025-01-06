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
use log::{info, warn};

#[cfg(debug_assertions)]
use log::debug;

const IDLE_LIMIT_DURATION: Duration =  Duration::from_millis(50);
const IDLE_SLEEP_DURATION: Duration =  Duration::from_millis(10);

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
        info!("Router: Started");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_route();
            self.sleep_if_idle();
        }
        info!("Router: Stopped");
    }

    fn try_route(self: &mut Self) {
        match self.receiver.try_recv() {
            Ok(message) => {
                self.last_message_instant = Instant::now();
                let connection_id = message.connection_id;
                #[cfg(debug_assertions)]
                debug!("Router: Received response for connection {connection_id}");
                match self.connections.read().unwrap().get(&message.connection_id) {
                    Some(connection) => {
                        #[cfg(debug_assertions)]
                        debug!("Router: Sending response to connection {connection_id}");
                        match connection.send(message) {
                            Ok(_) => {
                                #[cfg(debug_assertions)]
                                debug!("Router: Sent response to connection {connection_id}");
                            }
                            Err(_e) => {
                                #[cfg(debug_assertions)]
                                debug!("Router: Error sending to connection {connection_id}: {_e}");
                                self.connections.write().unwrap().remove(&connection_id);
                            }
                        }
                    }
                    None => {
                        warn!("Router: Response can't be routed, unknown connection id {connection_id}");
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
        if idle_duration > IDLE_LIMIT_DURATION {
            thread::sleep(IDLE_SLEEP_DURATION);
        }
    }

    fn fatal(self: &Self, msg: &str) {
        warn!("Router: {}", msg);
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
