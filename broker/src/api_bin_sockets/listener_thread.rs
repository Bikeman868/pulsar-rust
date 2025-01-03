use std::{
    collections::HashMap,
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Receiver, SendError, Sender},
        Arc, RwLock,
    },
    thread::{self},
};

use super::{
    connection::Connection,
    connection_thread::ConnectionThread,
    router_thread::RouterThread,
    server::{ConnectionId, ServerMessage},
};
use log::{error, info};
use pulsar_rust_net::sockets::buffer_pool::BufferPool;

/// A thread that owns a Tcp listener, accepts connections to a listener and spawns a thread to handle
/// each client that connects.
pub(crate) struct ListenerThread {
    sender: Sender<ServerMessage>,
    listener: TcpListener,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    next_connection_id: ConnectionId,
    connections: Arc<RwLock<HashMap<ConnectionId, Connection>>>,
}

impl ListenerThread {
    pub(crate) fn new(
        receiver: Receiver<ServerMessage>,
        sender: Sender<ServerMessage>,
        listener: TcpListener,
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
    ) -> Self {
        let connections = Arc::new(RwLock::new(HashMap::new()));

        let router = RouterThread::new(receiver, stop_signal, &connections);
        thread::spawn(move || router.run());

        Self {
            sender,
            listener,
            buffer_pool: buffer_pool.clone(),
            stop_signal: stop_signal.clone(),
            next_connection_id: 1,
            connections,
        }
    }

    /// This method owns Self so that when this function exits the data will be dropped
    pub(crate) fn run(mut self: Self) {
        info!("ListenerThread: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((stream, _address)) => {
                    stream.set_nonblocking(true).unwrap();
                    self.handle_connection(stream);
                }
                Err(e) => self.fatal(&format!("{e}")),
            }
        }
        info!("ListenerThread: Stopping");

        self.connections
            .read()
            .unwrap()
            .values()
            .for_each(|connection| connection.stop_signal.store(true, Ordering::Relaxed));
    }

    pub(super) fn send(&self, message: ServerMessage) -> Result<(), SendError<ServerMessage>> {
        self.sender.send(message)
    }

    fn handle_connection(self: &mut Self, stream: TcpStream) {
        let connection_id = self.next_connection_id;
        self.next_connection_id += 1;

        info!("ListenerThread: A client connected. id={connection_id}");

        let stop_signal = Arc::new(AtomicBool::new(false));
        let (tx_sender, tx_receiver) = channel::<ServerMessage>();

        let connection = Connection {
            connection_id,
            sender: tx_sender,
            stop_signal: stop_signal.clone(),
        };
        self.connections
            .write()
            .unwrap()
            .insert(connection_id, connection);

        let thread = ConnectionThread::new(
            tx_receiver,
            self.sender.clone(),
            stream,
            &self.buffer_pool,
            &stop_signal,
            &self.connections,
            connection_id,
        );
        thread::spawn(move || thread.run());
    }

    fn fatal(self: &Self, msg: &str) {
        error!("ListenerThread: {}", msg);
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
