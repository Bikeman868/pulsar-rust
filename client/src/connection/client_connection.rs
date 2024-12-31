use std::{
    net::TcpStream, 
    sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{Receiver, Sender, TryRecvError}, 
        Arc
    }, 
};

use pulsar_rust_net::sockets::{
    buffer_pool::BufferPool, 
    connection::{
        send_tcp, 
        try_receive_tcp
    }
};

use super::client::ClientMessage;

// A thread that moves client messages beteen a Tcp stream and a pair of channels
pub(crate) struct ClientConnectionThread {
    receiver: Receiver<ClientMessage>, 
    sender: Sender<ClientMessage>, 
    stream: TcpStream, 
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
}

impl ClientConnectionThread {
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
        dbg!("ClientConnection: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_send();
            self.try_receive();
        }
        dbg!("ClientConnection: Stopping");
    }

    fn try_send(self: &mut Self) {
        match self.receiver.try_recv() {
            Ok(message) => {
                dbg!(format!("ClientConnection: Received message from channel: {message:?}"));
                match send_tcp(message, &mut self.stream, &self.buffer_pool) {
                    Ok(_) => {
                        dbg!(format!("ClientConnection: Sent message to Tcp stream"));
                    }
                    Err(e) => {
                        self.stop(&format!("ClientConnection: Failed to send message to Tcp stream: {e:?}"));
                    }
                }
            }
            Err(e) => {
                match e {
                    TryRecvError::Empty => {}
                    TryRecvError::Disconnected => self.stop(&"ClientConnection: Receiver channel disconnected, stopping client connection"),
                }
            }
        }
    }

    fn try_receive(self: &mut Self) {
        match try_receive_tcp(&mut self.stream, &self.buffer_pool) {
            Ok(Some(buffer)) => {
                dbg!(format!("ClientConnection: Received message from Tcp stream: {buffer:?}"));
                match self.sender.send(buffer) {
                    Ok(_) => { dbg!(format!("ClientConnection: Sent received message to channel")); }
                    Err(e) => self.stop(&format!("ClientConnection: Failed to write received message to channel: {e}")),
                }
            }
            Ok(None) => (),
            Err(e) => self.stop(&format!("ClientConnection: Failed read message from Tcp stream: {e:?}")),
        }
    }

    fn stop(self: &Self, msg: &str) {
        dbg!(msg);
        dbg!(format!("ClientConnection: Signalling threads to stop"));
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
