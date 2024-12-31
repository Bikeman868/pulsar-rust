use std::{
    net::TcpStream, 
    sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{channel, Receiver, RecvError, SendError, Sender, TryRecvError}, 
        Arc
    }, 
    thread::{
        self, 
    }
};

use pulsar_rust_net::sockets::buffer_pool::BufferPool;
use crate::connection::client_connection::ClientConnectionThread;

pub(crate) type ClientMessage = Vec<u8>;

pub(crate) struct Client {
    stop_signal: Arc<AtomicBool<>>,
    sender: Sender<ClientMessage>,
    receiver: Receiver<ClientMessage>,
}

impl Client {
    pub fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        dbg!(format!("Client: Connecting to {authority}"));
        let stream = TcpStream::connect(authority).expect(&format!("Client: Failed to connect to {authority}"));
        let buffer_pool = buffer_pool.clone();
        let stop_signal = Arc::new(AtomicBool::new(false));

        let (tx_sender, tx_receiver) = channel::<ClientMessage>();
        let (rx_sender, rx_receiver) = channel::<ClientMessage>();

        let thread = ClientConnectionThread::new(
            tx_receiver,
            rx_sender,
            stream,
            &buffer_pool,
            &stop_signal,
        );
        thread::spawn(move||thread.run());

        Self { stop_signal, sender: tx_sender, receiver: rx_receiver }
    }

    pub fn try_recv(self: &Self) -> Result<ClientMessage, TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn recv(self: &Self) -> Result<ClientMessage, RecvError> {
        self.receiver.recv()
    }

    pub fn send(&self, message: ClientMessage) -> Result<(), SendError<ClientMessage>> {
        self.sender.send(message)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        dbg!(format!("Client: Signalling threads to stop"));
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
