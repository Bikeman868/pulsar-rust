use std::{
    net::{Shutdown, TcpStream}, sync::{
        atomic::{AtomicBool, Ordering}, 
        mpsc::{Receiver, Sender, TryRecvError}, 
        Arc
    }, thread, time::{Duration, Instant}
};
use log::{error, warn, info, debug};

use pulsar_rust_net::sockets::{
    buffer_pool::BufferPool, 
    connection::{
        send_tcp, 
        try_receive_tcp
    }
};

use super::server::{ConnectionId, ServerMessage};


/// A thread that moves server messages between a Tcp stream and a pair of channels
pub(crate) struct ConnectionThread {
    connection_id: ConnectionId,
    receiver: Receiver<ServerMessage>, 
    sender: Sender<ServerMessage>, 
    stream: TcpStream, 
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    created_instant: Instant,
    last_message_instant: Instant,
    max_idle: Duration,
    max_lifetime: Option<Duration>,
}

impl ConnectionThread {
    pub(super) fn new(
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
            connection_id,
            created_instant: Instant::now(),
            last_message_instant: Instant::now(),
            max_idle: Duration::from_secs(30),
            max_lifetime: None,
        }
    }

    pub(crate) fn run(mut self: Self) {
        info!("ConnectionThread: Starting");
        while !self.stop_signal.load(Ordering::Relaxed) {
            // debug!("ConnectionThread: try_send()");
            self.try_send();

            // debug!("ConnectionThread: try_receive()");
            self.try_receive();

            let idle_duration =  self.last_message_instant.elapsed();
            if idle_duration > Duration::from_millis(50) {
                // debug!("ConnectionThread: idle more tham 50ms");
                thread::sleep(Duration::from_millis(50));
                if let Some(max_lifetime) = self.max_lifetime { 
                    if self.created_instant.elapsed() > max_lifetime {
                        info!("ConnectionThread: Maximum lifetime exceeded");
                        self.stop_signal.store(true, Ordering::Relaxed);                
                    }
                }
                if idle_duration > self.max_idle {
                    info!("ConnectionThread: Idle for too long");
                    self.stop_signal.store(true, Ordering::Relaxed);                
                }
            }
        }
        info!("ConnectionThread: Stopping");
        // if let Err(err) = self.stream.shutdown(Shutdown::Both) {
        //     warn!("Error shutting down Tcp stream: {err}");
        // }
    }

    fn try_send(self: &mut Self) {
        match self.receiver.try_recv() {
            Ok(message) => {
                self.last_message_instant = Instant::now();
                // debug!("ConnectionThread: Received message from channel: {message:?}");
                match send_tcp(message.body, &mut self.stream, &self.buffer_pool) {
                    Ok(_) => {
                    //    debug!("ConnectionThread: Wrote message to Tcp stream");
                    }
                    Err(e) => {
                        self.stop(&format!("ConnectionThread: Failed to write message to Tcp stream: {e:?}"));
                    }
                }
            }
            Err(e) => {
                match e {
                    TryRecvError::Empty => { 
                        // debug!("ConnectionThread: Timeout waiting for message to send");
                    }
                    TryRecvError::Disconnected => self.stop(&"ConnectionThread: Receiver channel disconnected"),
                }
            }
        }
    }

    fn try_receive(self: &mut Self) {
        match try_receive_tcp(&mut self.stream, &self.buffer_pool) {
            Ok(Some(buffer)) => {
                self.last_message_instant = Instant::now();
                // debug!("ConnectionThread: Received message from Tcp stream: {buffer:?}");
                match self.sender.send(ServerMessage { connection_id: self.connection_id, body: buffer }) {
                    Ok(_) => { 
                        // debug!("ConnectionThread: Posted message to channel");
                    }
                    Err(e) => self.stop(&format!("ConnectionThread: Failed to post message to channel: {e}")),
                }
            }
            Ok(None) => {
                // debug!("ConnectionThread: No messages received from Tcp stream");
            }
            Err(e) => self.stop(&format!("ConnectionThread: Failed read message from Tcp stream: {e:?}")),
        }
    }

    fn stop(self: &Self, msg: &str) {
        error!("{}", msg);
        // debug!("ConnectionThread: Signalling thread to stop");
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
