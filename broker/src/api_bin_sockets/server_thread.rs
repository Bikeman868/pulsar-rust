use core::time::Duration;

use std::{
    net::SocketAddrV4, 
    sync::{
        atomic::{
            AtomicBool, 
            Ordering
        }, mpsc::{channel, RecvTimeoutError, Sender}, Arc
    }, 
    thread::{
        self, available_parallelism
    }, 
};

use log::{warn, info};
use pulsar_rust_net::sockets::buffer_pool::BufferPool;
use crate::{App};

use super::{server::Server, ServerMessage, processing_thread::ProcessingThread};

pub(crate) struct ServerThread {
    stop_signal: Arc<AtomicBool<>>,
    app: Arc<App>, 
    addr: SocketAddrV4
}

impl ServerThread {
    pub(crate) fn new(
        stop_signal: &Arc<AtomicBool<>>,
        app: &Arc<App>, 
        addr: SocketAddrV4,
    )
     -> Self {
        Self{
            stop_signal: stop_signal.clone(),
            app: app.clone(),
            addr,
        }   
    }

    pub(crate) fn run(self: Self) {
        info!("Binary API server thread starting");

        let authority = format!("{}:{}", self.addr.ip(), self.addr.port());
        let buffer_pool = Arc::new(BufferPool::new());
        let server = Server::new(&buffer_pool, &authority);

        let cpus = available_parallelism().expect("Can't get number of CPUs").get();
        let mut processing_channels: Vec<Sender<ServerMessage>> = Vec::new();

        for _ in 0..cpus {
            let (sender, receiver) = channel::<ServerMessage>();
            processing_channels.push(sender);
            let processing_thread = ProcessingThread::new(
                &self.app,
                &buffer_pool,
                &self.stop_signal,
                &server.sender(),
                receiver,
            );
            thread::spawn(move||processing_thread.run());
        }

        let mut next_thread_index = 0usize;

        while !self.stop_signal.load(Ordering::Relaxed) {
            match server.recv_timeout(Duration::from_millis(50)) {
                Ok(message) => {
                    processing_channels[next_thread_index].send(message).ok();
                    next_thread_index = if next_thread_index == 0 { processing_channels.len() - 1 } else { next_thread_index - 1 };
                }
                Err(e) => {
                    match e {
                        RecvTimeoutError::Timeout => (),
                        RecvTimeoutError::Disconnected => warn!("ServerThread: Failed to receive request, channel is disconnected"),
                    }
                }
            }
        };

        info!("Binary API server thread stopping");
        drop(server);
    }
}
