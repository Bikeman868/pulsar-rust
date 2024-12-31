use std::{
    collections::HashMap, net::{TcpListener, TcpStream}, sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{channel, Receiver, SendError, Sender}, 
        Arc, RwLock
    }, thread::{
        self, 
    }
};

use pulsar_rust_net::sockets::buffer_pool::BufferPool;

use super::{
    server::{ConnectionId, ServerMessage}, 
    server_connection::ServerConnectionThread
};

/// A thread that listens for connections and spawns a thread to handle
/// each client that connects. Construct with new() then call run() in thread spawn closure
pub(crate) struct ServerListenerThread {
    sender: Sender<ServerMessage>, 
    listener: TcpListener,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    next_connection_id: ConnectionId,
    connections: Arc<RwLock<HashMap<ConnectionId, ServerConnection>>>,
}

struct ServerConnection {
    connection_id: ConnectionId,
    sender: Sender<ServerMessage>,
    stop_signal: Arc<AtomicBool>,
}

struct RouterThread {
    connections: Arc<RwLock<HashMap<ConnectionId, ServerConnection>>>,
    receiver: Receiver<ServerMessage>,
    stop_signal: Arc<AtomicBool>,
}

impl ServerListenerThread {
    pub(crate) fn new(
        receiver: Receiver<ServerMessage>, 
        sender: Sender<ServerMessage>,
        listener: TcpListener, 
        buffer_pool: &Arc<BufferPool>, 
        stop_signal: &Arc<AtomicBool>) -> Self {

        let connections = Arc::new(RwLock::new(HashMap::new()));

        let router = RouterThread::new(receiver, stop_signal, &connections);
        thread::spawn(move||router.run());

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
        dbg!("ServerListener: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((stream, _address)) => {
                    self.handle_connection(stream);
                }
                Err(e) => self.stop(&format!("ServerListener: {e}")),
            }
        }
        dbg!("ServerListener: Signalling connections to terminate");
        self.connections.read().unwrap().values().for_each(
            |con|con.stop_signal.store(true, Ordering::Relaxed)
        );
        dbg!("ServerListener: Stopping");
    }

    pub(crate) fn send(&self, message: ServerMessage) -> Result<(), SendError<ServerMessage>> {
        self.sender.send(message)
    }

    fn handle_connection(self: &mut Self, stream: TcpStream) {
        let connection_id = self.next_connection_id;
        self.next_connection_id += 1;

        dbg!(format!("ServerListener: A client connected. id={connection_id}"));

        let stop_signal = Arc::new(AtomicBool::new(false));
        let (tx_sender, tx_receiver) = channel::<ServerMessage>();

        let connection = ServerConnection {
            connection_id,
            sender: tx_sender,
            stop_signal: stop_signal.clone(),
        };
        self.connections.write().unwrap().insert(connection_id, connection);

        let thread = ServerConnectionThread::new(
            tx_receiver,
            self.sender.clone(),
            stream,
            &self.buffer_pool,
            &stop_signal,
            connection_id,
        );
        thread::spawn(move||thread.run());
    }

    fn stop(self: &Self, msg: &str) {
        dbg!(msg);
        dbg!("ServerListener: Signalling thread to stop");
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}

impl Drop for ServerConnection {
    fn drop(&mut self) {
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}

impl RouterThread {
    fn new(
        receiver: Receiver<ServerMessage>,
        stop_signal: &Arc<AtomicBool>,
        connections: &Arc<RwLock<HashMap<ConnectionId, ServerConnection>>>,
    ) -> Self {
        Self {
            receiver,
            connections: connections.clone(),
            stop_signal: stop_signal.clone(),
        }
    }

    fn run(self: Self) {
        dbg!("Router: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            match self.receiver.recv() {
                Ok(message) => {
                    let connection_id = message.connection_id;
                    dbg!(format!("Router: Received message for connection {connection_id}"));
                    match self.connections.read().unwrap().get(&message.connection_id) {
                        Some(connection) => {
                            dbg!(format!("Router: Sending message to connection {connection_id}"));
                            match connection.sender.send(message) {
                                Ok(_) => {
                                    dbg!(format!("Router: Sent message to connection {connection_id}"));
                                }
                                Err(e) => {
                                    dbg!(format!("Router: Error sending to connection {connection_id}: {e}"));
                                    self.connections.write().unwrap().remove(&connection_id);
                                }
                            }
                        }
                        None => { dbg!(format!("Router: Message can't be routed, unknown connection id {connection_id}")); }
                    }
                }
                Err(e) => self.stop(&format!("Router: Error receiving message: {e}")),
            }
        }
        dbg!("Router: Stopping");
    }

    fn stop(self: &Self, msg: &str) {
        dbg!(msg);
        dbg!("Router: Signalling thread to stop");
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
