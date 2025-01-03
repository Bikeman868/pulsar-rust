use log::{debug, error, info};
use pulsar_rust_net::sockets::{
    buffer_pool::BufferPool,
    connection::{send_tcp, try_receive_tcp},
};
use std::{
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender, TryRecvError},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use super::client::ClientMessage;

// A thread that moves client messages beteen a Tcp stream and a pair of channels
pub(crate) struct ConnectionThread {
    receiver: Receiver<ClientMessage>,
    sender: Sender<ClientMessage>,
    stream: TcpStream,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    last_message_instant: Instant,
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
            last_message_instant: Instant::now(),
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
        match self.receiver.try_recv() {
            Ok(message) => {
                self.last_message_instant = Instant::now();
                #[cfg(debug_assertions)]
                debug!("ConnectionThread: Received message from channel: {message:?}");
                match send_tcp(message, &mut self.stream, &self.buffer_pool) {
                    Ok(_) => {
                        #[cfg(debug_assertions)]
                        debug!("ConnectionThread: Sent message to Tcp stream");
                    }
                    Err(e) => {
                        self.stop(&format!(
                            "ConnectionThread: Failed to send message to Tcp stream: {e:?}"
                        ));
                    }
                }
            }
            Err(e) => match e {
                TryRecvError::Empty => {}
                TryRecvError::Disconnected => {
                    self.stop(&"ConnectionThread: Receiver channel disconnected")
                }
            },
        }
    }

    fn try_receive(self: &mut Self) {
        match try_receive_tcp(&mut self.stream, &self.buffer_pool) {
            Ok(Some(buffer)) => {
                #[cfg(debug_assertions)]
                debug!("ConnectionThread: Received message from Tcp stream: {buffer:?}");
                match self.sender.send(buffer) {
                    Ok(_) => {
                        #[cfg(debug_assertions)]
                        debug!("ConnectionThread: Sent received message to channel");
                    }
                    Err(e) => self.stop(&format!(
                        "ConnectionThread: Failed to write received message to channel: {e}"
                    )),
                }
            }
            Ok(None) => (),
            Err(e) => self.stop(&format!(
                "ConnectionThread: Failed read message from Tcp stream: {e:?}"
            )),
        }
    }

    fn sleep_if_idle(self: &Self) {
        let idle_duration = self.last_message_instant.elapsed();
        if idle_duration > Duration::from_millis(50) {
            #[cfg(debug_assertions)]
            debug!("ProcessingThread: idle more tham 50ms");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn stop(self: &Self, msg: &str) {
        error!("{}", msg);
        #[cfg(debug_assertions)]
        debug!("ConnectionThread: Signalling threads to stop");
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
