use crate::api_bin::listener_thread::ListenerThread;
use log::info;
use pulsar_rust_net::sockets::buffer_pool::BufferPool;
use std::{
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc,
    },
    thread::{self},
};

pub(crate) type ConnectionId = u32;

#[derive(Debug)]
pub(crate) struct ServerMessage {
    pub connection_id: ConnectionId,
    pub body: Vec<u8>,
}

pub(crate) struct Server {
    stop_signal: Arc<AtomicBool>,
    sender: Arc<Sender<ServerMessage>>,
    receiver: Receiver<ServerMessage>,
    port: u16,
}

/// Binds to a local port and spawns a thread that will listen for client connections this endpoint.
/// When the server is dropped, the background listener thread is terminated.
/// Provides mpsc channels for request processing. Requests from clients are pushed into the rx channel and
/// application responses should be pushed into the tx channel. Individual clients are identified by a
/// connection id in each server message. It is important to copy the connection id into responses so that
/// they go to the right client.
impl Server {
    pub(crate) fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        let listener = TcpListener::bind(authority)
            .expect(&format!("Server: Failed to listen on {authority}"));
        info!("Server: Constructed for {authority}");
        let stop_signal = Arc::new(AtomicBool::new(false));
        let port = listener.local_addr().unwrap().port();

        let (tx_sender, tx_receiver) = channel::<ServerMessage>();
        let (rx_sender, rx_receiver) = channel::<ServerMessage>();

        let thread =
            ListenerThread::new(tx_receiver, rx_sender, listener, &buffer_pool, &stop_signal);
        thread::Builder::new()
            .name(String::from("bin-api-listener"))
            .spawn(move || thread.run())
            .unwrap();

        Self {
            stop_signal,
            sender: Arc::new(tx_sender),
            receiver: rx_receiver,
            port,
        }
    }

    pub(crate) fn try_recv(self: &Self) -> Result<ServerMessage, TryRecvError> {
        self.receiver.try_recv()
    }

    pub(crate) fn sender(&self) -> Arc<Sender<ServerMessage>> {
        self.sender.clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop_signal.store(true, Ordering::Relaxed);

        // We need to initiate a connection to wake the listener thread
        let _ = TcpStream::connect(format!("127.0.0.1:{}", self.port));
        info!("Server: Dropped");
    }
}
