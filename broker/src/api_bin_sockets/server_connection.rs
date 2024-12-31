use std::{
    net::TcpStream, 
    sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{Receiver, Sender, TryRecvError}, 
        Arc
    }
};

use pulsar_rust_net::sockets::{
    buffer_pool::BufferPool, 
    connection::{
        send_tcp, 
        try_receive_tcp
    }
};

use super::server::{ConnectionId, ServerMessage};


/// A thread that moves server messages between a Tcp stream and a pair of channels
pub(crate) struct ServerConnectionThread {
    connection_id: ConnectionId,
    receiver: Receiver<ServerMessage>, 
    sender: Sender<ServerMessage>, 
    stream: TcpStream, 
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
}

impl ServerConnectionThread {
    pub(crate) fn new(
        receiver: Receiver<ServerMessage>, 
        sender: Sender<ServerMessage>, 
        stream: TcpStream, 
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
        connection_id: ConnectionId,
    ) -> Self {
        Self { 
            receiver, 
            sender, 
            stream, 
            buffer_pool: buffer_pool.clone(), 
            stop_signal: stop_signal.clone(), 
            connection_id
        }
    }

    pub(crate) fn run(mut self: Self) {
        dbg!("ServerConnection: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_send();
            self.try_receive();
        }
        dbg!("ServerConnection: Stopping");
    }

    fn try_send(self: &mut Self) {
        match self.receiver.try_recv() {
            Ok(message) => {
                dbg!(&format!("ServerConnection: Received message from channel: {message:?}"));
                match send_tcp(message.body, &mut self.stream, &self.buffer_pool) {
                    Ok(_) => {
                       dbg!(&format!("ServerConnection: Wrote message to Tcp stream"));
                    }
                    Err(e) => {
                        self.stop(&format!("ServerConnection: Failed to write message to Tcp stream: {e:?}"));
                    }
                }
            }
            Err(e) => {
                match e {
                    TryRecvError::Empty => {}
                    TryRecvError::Disconnected => self.stop(&"ServerConnection: Receiver channel disconnected"),
                }
            }
        }
    }

    fn try_receive(self: &mut Self) {
        match try_receive_tcp(&mut self.stream, &self.buffer_pool) {
            Ok(Some(buffer)) => {
                dbg!(format!("ServerConnection: Received message from Tcp stream: {buffer:?}"));
                match self.sender.send(ServerMessage { connection_id: self.connection_id, body: buffer }) {
                    Ok(_) => { dbg!(format!("ServerConnection: Posted message to channel")); }
                    Err(e) => self.stop(&format!("ServerConnection: Failed to post message to channel: {e}")),
                }
            }
            Ok(None) => (),
            Err(e) => self.stop(&format!("ServerConnection: Failed read message from Tcp stream: {e:?}")),
        }
    }

    fn stop(self: &Self, msg: &str) {
        dbg!(msg);
        dbg!("ServerConnection: Signalling thread to stop");
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
