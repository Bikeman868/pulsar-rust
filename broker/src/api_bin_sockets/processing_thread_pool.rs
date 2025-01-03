use core::time::Duration;

use std::{
    net::SocketAddrV4,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Sender, TryRecvError},
        Arc,
    },
    thread::{self, available_parallelism},
    time::Instant,
};

use crate::App;
use log::{debug, info, warn};
use pulsar_rust_net::sockets::buffer_pool::BufferPool;

use super::{
    processing_thread::ProcessingThread,
    server::{Server, ServerMessage},
};

pub(crate) struct ProcessingThreadPool {
    stop_signal: Arc<AtomicBool>,
    app: Arc<App>,
    addr: SocketAddrV4,
    authority: String,
    buffer_pool: Arc<BufferPool>,
    last_message_instant: Instant,
    next_thread_index: usize,
}

/// Owns a server, Receives requests from a server's rx channel and distributes them to a pool of
/// worker threads for processing. Worker threads reply directly to the server's tx channel.
impl ProcessingThreadPool {
    pub(crate) fn new(
        stop_signal: &Arc<AtomicBool>,
        buffer_pool: &Arc<BufferPool>,
        app: &Arc<App>,
        addr: SocketAddrV4,
    ) -> Self {
        Self {
            stop_signal: stop_signal.clone(),
            app: app.clone(),
            addr,
            authority: format!("{}:{}", addr.ip(), addr.port()),
            buffer_pool: buffer_pool.clone(),
            last_message_instant: Instant::now(),
            next_thread_index: 0,
        }
    }

    pub(crate) fn run(mut self: Self) {
        info!("ProcessingThreadPool: starting");

        let server = Server::new(&self.buffer_pool, &self.authority);
        let request_senders = self.create_threads(&server.sender());

        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_process(&server, &request_senders);
            self.sleep_if_idle();
        }

        info!("ProcessingThreadPool: stopping");
    }

    fn create_threads(
        self: &Self,
        response_sender: &Arc<Sender<ServerMessage>>,
    ) -> Vec<Sender<ServerMessage>> {
        let cpus = available_parallelism()
            .expect("ProcessingThreadPool: Can't get number of CPUs")
            .get();
        let mut request_senders: Vec<Sender<ServerMessage>> = Vec::with_capacity(cpus);

        for _ in 0..cpus {
            let (request_sender, request_receiver) = channel::<ServerMessage>();
            request_senders.push(request_sender);

            let processing_thread = ProcessingThread::new(
                &self.app,
                &self.buffer_pool,
                &self.stop_signal,
                response_sender,
                request_receiver,
            );
            thread::spawn(move || processing_thread.run());
        }

        request_senders
    }

    fn try_process(self: &mut Self, server: &Server, request_senders: &Vec<Sender<ServerMessage>>) {
        match server.try_recv() {
            Ok(message) => {
                self.last_message_instant = Instant::now();
                request_senders[self.next_thread_index].send(message).ok();
                self.next_thread_index = if self.next_thread_index == 0 {
                    request_senders.len() - 1
                } else {
                    self.next_thread_index - 1
                };
            }
            Err(e) => match e {
                TryRecvError::Empty => (),
                TryRecvError::Disconnected => self.fatal("Request channel disconnected"),
            },
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

    fn fatal(self: &Self, msg: &str) {
        warn!("ProcessingThreadPool: {}", msg);
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
