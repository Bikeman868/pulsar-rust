use std::{
    net::TcpListener, 
    sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{channel, Receiver, RecvError, SendError, Sender, TryRecvError}, 
        Arc,
    }, 
    thread::{
        self, 
    }
};

use pulsar_rust_net::sockets::buffer_pool::BufferPool;

use crate::api_bin_sockets::server_listener::ServerListenerThread;

pub(crate) type ConnectionId = u32;

#[derive(Debug)]
pub(crate) struct ServerMessage {
    pub connection_id: ConnectionId,
    pub body: Vec<u8>,
}

pub(crate) struct Server {
    stop_signal: Arc<AtomicBool<>>,
    sender: Sender<ServerMessage>,
    receiver: Receiver<ServerMessage>,
}

impl Server {
    pub(crate) fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        dbg!(format!("Server: Listening on {authority}"));
        let listener = TcpListener::bind(authority).expect(&format!("Server: Failed to listen for connections on {authority}"));
        let stop_signal = Arc::new(AtomicBool::new(false));

        let (tx_sender, tx_receiver) = channel::<ServerMessage>();
        let (rx_sender, rx_receiver) = channel::<ServerMessage>();

        let thread = ServerListenerThread::new(tx_receiver, rx_sender, listener, &buffer_pool, &stop_signal);
        thread::spawn(move||thread.run());

        Self { stop_signal, sender: tx_sender, receiver: rx_receiver }
    }

    pub(crate) fn try_recv(self: &Self) -> Result<ServerMessage, TryRecvError> {
        self.receiver.try_recv()
    }

    pub(crate) fn recv(self: &Self) -> Result<ServerMessage, RecvError> {
        self.receiver.recv()
    }

    pub(crate) fn send(&self, message: ServerMessage) -> Result<(), SendError<ServerMessage>> {
        self.sender.send(message)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        dbg!(format!("Server: Signalling threads to stop"));
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
