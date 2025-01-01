use std::{
    net::TcpStream, 
    sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError}, 
        Arc
    }, time::Duration, 
};
use log::{error, info, debug};
use pulsar_rust_net::sockets::{
    buffer_pool::BufferPool, 
    connection::{
        send_tcp, 
        try_receive_tcp
    }
};

use super::client::ClientMessage;

// A thread that moves client messages beteen a Tcp stream and a pair of channels
pub(crate) struct ConnectionThread {
    receiver: Receiver<ClientMessage>, 
    sender: Sender<ClientMessage>, 
    stream: TcpStream, 
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
}

impl ConnectionThread {
    pub(crate) fn new(
        receiver: Receiver<ClientMessage>, 
        sender: Sender<ClientMessage>, 
        stream: TcpStream, 
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
    ) -> Self {
        Self { 
            receiver, 
            sender, 
            stream, 
            buffer_pool: buffer_pool.clone(), 
            stop_signal: stop_signal.clone(),
        }
    }

    pub(crate) fn run(mut self: Self) {
        info!("ConnectionThread: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_send();
            self.try_receive();
        }
        info!("ConnectionThread: Stopping");
    }

    fn try_send(self: &mut Self) {
        match self.receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(message) => {
                // debug!("ConnectionThread: Received message from channel: {message:?}");
                match send_tcp(message, &mut self.stream, &self.buffer_pool) {
                    Ok(_) => {
                        // debug!("ConnectionThread: Sent message to Tcp stream");
                    }
                    Err(e) => {
                        self.stop(&format!("ConnectionThread: Failed to send message to Tcp stream: {e:?}"));
                    }
                }
            }
            Err(e) => {
                match e {
                    RecvTimeoutError::Timeout => {}
                    RecvTimeoutError::Disconnected => self.stop(&"ConnectionThread: Receiver channel disconnected, stopping client connection"),
                }
            }
        }
    }

    fn try_receive(self: &mut Self) {
        match try_receive_tcp(&mut self.stream, &self.buffer_pool) {
            Ok(Some(buffer)) => {
                // debug!("ConnectionThread: Received message from Tcp stream: {buffer:?}");
                match self.sender.send(buffer) {
                    Ok(_) => { 
                        // debug!("ConnectionThread: Sent received message to channel");
                    }
                    Err(e) => self.stop(&format!("ConnectionThread: Failed to write received message to channel: {e}")),
                }
            }
            Ok(None) => (),
            Err(e) => self.stop(&format!("ConnectionThread: Failed read message from Tcp stream: {e:?}")),
        }
    }

    fn stop(self: &Self, msg: &str) {
        error!("{}", msg);
        // debug!("ConnectionThread: Signalling threads to stop");
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
