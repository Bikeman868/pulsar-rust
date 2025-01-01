use std::{
    collections::HashMap, sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::Receiver, 
        Arc, RwLock
    }
};

use log::{error, warn, info, debug};
use super::{
    connection::Connection, server::{ConnectionId, ServerMessage}
};

pub(crate) struct RouterThread {
    connections: Arc<RwLock<HashMap<ConnectionId, Connection>>>,
    receiver: Receiver<ServerMessage>,
    stop_signal: Arc<AtomicBool>,
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
        }
    }

    pub(crate) fn run(self: Self) {
        info!("Router: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            match self.receiver.recv() {
                Ok(message) => {
                    let connection_id = message.connection_id;
                    // debug!("Router: Received message for connection {connection_id}");
                    match self.connections.read().unwrap().get(&message.connection_id) {
                        Some(connection) => {
                            // debug!("Router: Sending message to connection {connection_id}");
                            match connection.sender.send(message) {
                                Ok(_) => {
                                    // debug!("Router: Sent message to connection {connection_id}");
                                }
                                Err(e) => {
                                    // debug!("Router: Error sending to connection {connection_id}: {e}");
                                    self.connections.write().unwrap().remove(&connection_id);
                                }
                            }
                        }
                        None => { warn!("Router: Message can't be routed, unknown connection id {connection_id}"); }
                    }
                }
                Err(e) => self.stop(&format!("Router: Error receiving message: {e}")),
            }
        }
        info!("Router: Stopping");
    }

    fn stop(self: &Self, msg: &str) {
        error!("{}", msg);
        // debug!("Router: Signalling thread to stop");
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
