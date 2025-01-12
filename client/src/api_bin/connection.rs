use log::{error, info};
use pulsar_rust_net::sockets::{buffer_pool::BufferPool, tcp_channel::TcpChannel};
use std::{
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Receiver, RecvError, SendError, Sender, TryRecvError},
        Arc,
    },
    time::Duration,
};

pub(crate) type ClientMessage = Vec<u8>;
const MAX_MESSAGE_LENGTH: usize = 512;

/// Opens a connection to a host endpoint and owns a thread that sends requests and
/// receives replies into mpsc channels
pub struct Connection {
    stop_signal: Arc<AtomicBool>,
    request_sender: Sender<ClientMessage>,
    response_receiver: Receiver<ClientMessage>,
    tcp_channel: TcpChannel,
}

impl Connection {
    pub fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        let stream = TcpStream::connect(&authority)
            .expect(&format!("Connection: Failed to connect to {}", authority));
        info!("Connection: Connected to {}", authority);

        stream.set_nonblocking(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(5)))
            .unwrap();

        let stop_signal = Arc::new(AtomicBool::new(false));
        let (request_sender, request_receiver) = channel::<ClientMessage>();
        let (response_sender, response_receiver) = channel::<ClientMessage>();

        let tcp_channel = TcpChannel::new(
            request_receiver,
            response_sender,
            stream,
            &buffer_pool,
            &stop_signal,
        );

        Self {
            stop_signal,
            request_sender,
            response_receiver,
            tcp_channel,
        }
    }

    // Stops the Tcp connection
    pub fn disconnect(self: Self) {
        self.stop_signal.store(true, Ordering::Relaxed);
        self.tcp_channel.stop();
    }

    /// Non-blocking call that returns a response from the host if there is one
    pub fn try_recv(self: &Self) -> Result<ClientMessage, TryRecvError> {
        self.response_receiver.try_recv()
    }

    /// Blocking call that waits until there is a response from the host
    pub fn recv(self: &Self) -> Result<ClientMessage, RecvError> {
        self.response_receiver.recv()
    }

    /// Non-blocking call that queues a message to send to the host
    pub fn send(&self, message: ClientMessage) -> Result<(), SendError<ClientMessage>> {
        if message.len() > MAX_MESSAGE_LENGTH {
            error!(
                "Connection: Message length {} exceeds maximum length of {}",
                message.len(),
                MAX_MESSAGE_LENGTH
            );
            Err(SendError(message))
        } else {
            self.request_sender.send(message)
        }
    }
}
