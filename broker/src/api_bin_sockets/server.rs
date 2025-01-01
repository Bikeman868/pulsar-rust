use std::{
    net::{TcpListener, TcpStream}, 
    sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{channel, Receiver, RecvTimeoutError, Sender}, 
        Arc,
    }, 
    thread::{
        self, 
    }
};
use core::time::Duration;
use log::{info, debug};
use pulsar_rust_net::sockets::buffer_pool::BufferPool;
use crate::api_bin_sockets::listener_thread::ListenerThread;

pub(crate) type ConnectionId = u32;

#[derive(Debug)]
pub(crate) struct ServerMessage {
    pub connection_id: ConnectionId,
    pub body: Vec<u8>,
}

pub(crate) struct Server {
    stop_signal: Arc<AtomicBool<>>,
    sender: Arc<Sender<ServerMessage>>,
    receiver: Receiver<ServerMessage>,
    port: u16,
}

impl Server {
    pub(crate) fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        info!("Server: Listening on {authority}");
        let listener = TcpListener::bind(authority).expect(&format!("Server: Failed to listen for connections on {authority}"));
        let stop_signal = Arc::new(AtomicBool::new(false));
        let port = listener.local_addr().unwrap().port();

        let (tx_sender, tx_receiver) = channel::<ServerMessage>();
        let (rx_sender, rx_receiver) = channel::<ServerMessage>();

        let thread = ListenerThread::new(tx_receiver, rx_sender, listener, &buffer_pool, &stop_signal);
        thread::spawn(move||thread.run());

        Self { stop_signal, sender: Arc::new(tx_sender), receiver: rx_receiver, port }
    }

    
    pub(crate) fn recv_timeout(self: &Self, duration: Duration) -> Result<ServerMessage, RecvTimeoutError> {
        self.receiver.recv_timeout(duration)
    }

    pub(crate) fn sender(&self) -> Arc<Sender<ServerMessage>> {
        self.sender.clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        info!("Server: Signalling threads to stop");
        self.stop_signal.store(true, Ordering::Relaxed);

        // We need to initiate a connection to wake the listener thread
        let _ = TcpStream::connect(format!("127.0.0.1:{}", self.port));
    }
}
