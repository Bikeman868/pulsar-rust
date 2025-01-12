use log::{info, warn};
use std::{
    collections::HashMap,
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc, RwLock,
    },
    thread,
    time::{Duration, Instant},
};

use pulsar_rust_net::sockets::{buffer_pool::BufferPool, tcp_channel::TcpChannel};

use super::{
    connection::Connection,
    server::{ConnectionId, ServerMessage},
};

#[cfg(debug_assertions)]
use log::debug;

const IDLE_LIMIT_DURATION: Duration = Duration::from_millis(50);
const IDLE_SLEEP_DURATION: Duration = Duration::from_millis(10);

/// A thread that moves messages between a Tcp stream and a pair of channels
pub(crate) struct ConnectionThread {
    connection_id: ConnectionId,
    response_receiver: Receiver<ServerMessage>,
    request_sender: Sender<ServerMessage>,
    tcp_request_receiver: Receiver<Vec<u8>>,
    tcp_response_sender: Sender<Vec<u8>>,
    _tcp_channel: TcpChannel,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    connections: Arc<RwLock<HashMap<ConnectionId, Connection>>>,
    last_message_instant: Instant,
}

impl ConnectionThread {
    pub(super) fn new(
        receiver: Receiver<ServerMessage>,
        sender: Sender<ServerMessage>,
        stream: TcpStream,
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
        connections: &Arc<RwLock<HashMap<ConnectionId, Connection>>>,
        connection_id: ConnectionId,
    ) -> Self {
        let (tcp_response_sender, tcp_receiver) = channel();
        let (tcp_sender, tcp_request_receiver) = channel();

        stream.set_nonblocking(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(5)))
            .unwrap();

        let tcp_channel =
            TcpChannel::new(tcp_receiver, tcp_sender, stream, buffer_pool, stop_signal);

        Self {
            response_receiver: receiver,
            request_sender: sender,
            tcp_request_receiver,
            tcp_response_sender,
            _tcp_channel: tcp_channel,
            buffer_pool: buffer_pool.clone(),
            stop_signal: stop_signal.clone(),
            connections: connections.clone(),
            connection_id,
            last_message_instant: Instant::now(),
        }
    }

    pub(crate) fn run(mut self: Self) {
        info!("ConnectionThread: Started");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_send();
            self.try_receive();
            self.sleep_if_idle();
        }
        self.connections
            .write()
            .unwrap()
            .remove(&self.connection_id);
        info!("ConnectionThread: Stopped");
    }

    fn try_send(self: &mut Self) {
        match self.response_receiver.try_recv() {
            Ok(message) => {
                self.last_message_instant = Instant::now();
                #[cfg(debug_assertions)]
                debug!("ConnectionThread: Received response from channel: {message:?}");
                match self.tcp_response_sender.send(message.body) {
                    Ok(_) => (),
                    Err(err) => {
                        self.fatal(&"Tcp sender channel disconnected");
                        self.buffer_pool.reuse(err.0);
                    }
                }
            }
            Err(e) => match e {
                TryRecvError::Empty => {}
                TryRecvError::Disconnected => self.fatal(&"Response receiver channel disconnected"),
            },
        }
    }

    fn try_receive(self: &mut Self) {
        match self.tcp_request_receiver.try_recv() {
            Ok(message) => {
                self.last_message_instant = Instant::now();
                #[cfg(debug_assertions)]
                debug!("ConnectionThread: Received request from Tcp: {message:?}");
                match self.request_sender.send(ServerMessage {
                    body: message,
                    connection_id: self.connection_id,
                }) {
                    Ok(_) => (),
                    Err(err) => {
                        self.fatal(&"Request sender channel disconnected");
                        self.buffer_pool.reuse(err.0.body);
                    }
                }
            }
            Err(e) => match e {
                TryRecvError::Empty => {}
                TryRecvError::Disconnected => self.fatal(&"Tcp receiver channel disconnected"),
            },
        }
    }

    fn sleep_if_idle(self: &Self) {
        let idle_duration = self.last_message_instant.elapsed();
        if idle_duration > IDLE_LIMIT_DURATION {
            thread::sleep(IDLE_SLEEP_DURATION);
        }
    }

    fn fatal(self: &mut Self, msg: &str) {
        warn!("ConnectionThread: {}", msg);
        self.stop();
    }

    fn stop(self: &mut Self) {
        self.stop_signal.store(true, Ordering::Relaxed);
        self.last_message_instant = Instant::now();
    }
}
