use log::{error, info};
use pulsar_rust_net::sockets::{buffer_pool::BufferPool, tcp_channel::TcpChannel};
use std::{
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Receiver, RecvError, SendError, Sender},
        Arc,
    },
    time::Duration,
};

use super::contracts::ClientMessage;

const MAX_MESSAGE_LENGTH: usize = 512;

/// Opens a connection to a host endpoint and owns a thread that sends requests and
/// receives replies into mpsc channels
pub struct Connection {
    stop_signal: Arc<AtomicBool>,
    request_sender: Sender<ClientMessage>,
    response_receiver: Option<Receiver<ClientMessage>>,
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
            response_receiver: Some(response_receiver),
            tcp_channel,
        }
    }

    // Stops the Tcp connection
    pub fn disconnect(self: Self) {
        self.stop_signal.store(true, Ordering::Relaxed);
        self.tcp_channel.stop();
    }

    /// Blocking call that waits until there is a response from the host
    pub fn recv(self: &Self) -> Result<ClientMessage, RecvError> {
        if let Some(receiver) = &self.response_receiver {
            receiver.recv()
        } else {
            Err(RecvError)
        }
    }

    /// Allows you to take over the receiving half of the connection. After
    /// calling this method, you can no longer use the recv function.
    pub fn take_receiver(self: &mut Self) -> Option<Receiver<Vec<u8>>> {
        self.response_receiver.take()
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
