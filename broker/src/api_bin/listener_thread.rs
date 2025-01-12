use std::{
    collections::HashMap,
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
        Arc, RwLock,
    },
    thread::{self},
};

use super::{
    connection::Connection,
    router_thread::RouterThread,
    server::{ConnectionId, ServerMessage},
};
use log::{info, warn};
use pulsar_rust_net::sockets::buffer_pool::BufferPool;

/// A thread that owns a Tcp listener, accepts connections to a listener and spawns a thread to handle
/// each client that connects.
pub(crate) struct ListenerThread {
    request_sender: Sender<ServerMessage>,
    listener: TcpListener,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    next_connection_id: ConnectionId,
    connections: Arc<RwLock<HashMap<ConnectionId, Connection>>>,
}

impl ListenerThread {
    pub(crate) fn new(
        response_receiver: Receiver<ServerMessage>,
        request_sender: Sender<ServerMessage>,
        listener: TcpListener,
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
    ) -> Self {
        let connections = Arc::new(RwLock::new(HashMap::new()));

        let router = RouterThread::new(response_receiver, stop_signal, &connections);
        thread::spawn(move || router.run());

        Self {
            request_sender,
            listener,
            buffer_pool: buffer_pool.clone(),
            stop_signal: stop_signal.clone(),
            next_connection_id: 1,
            connections,
        }
    }

    /// This method owns Self so that when this function exits the data will be dropped
    pub(crate) fn run(mut self: Self) {
        info!("ListenerThread: Started");

        while !self.stop_signal.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((stream, _address)) => self.handle_connection(stream),
                Err(e) => self.fatal(&format!("{e}")),
            }
        }

        self.connections
            .read()
            .unwrap()
            .values()
            .for_each(|connection| connection.stop());

        info!("ListenerThread: Stopped");
    }

    fn handle_connection(self: &mut Self, stream: TcpStream) {
        let connection_id = self.next_connection_id;
        self.next_connection_id += 1;

        info!("ListenerThread: A client connected. Id={connection_id}");
        let connection = Connection::new(
            &self.buffer_pool,
            &self.connections,
            self.request_sender.clone(),
            connection_id,
            stream,
        );

        self.connections
            .write()
            .unwrap()
            .insert(connection_id, connection);
    }

    fn fatal(self: &Self, msg: &str) {
        warn!("ListenerThread: {}", msg);
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
