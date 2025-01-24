use std::{
    collections::HashMap,
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, SendError, Sender},
        Arc, RwLock,
    },
    thread,
};

use log::info;
use pulsar_rust_net::sockets::buffer_pool::BufferPool;

use super::{
    connection_thread::ConnectionThread,
    server::{ConnectionId, ServerMessage},
};

pub(crate) struct Connection {
    sender: Sender<ServerMessage>,
    stop_signal: Arc<AtomicBool>,
}

impl Connection {
    pub(crate) fn new(
        buffer_pool: &Arc<BufferPool>,
        connections: &Arc<RwLock<HashMap<ConnectionId, Connection>>>,
        request_sender: Sender<ServerMessage>,
        connection_id: ConnectionId,
        stream: TcpStream,
    ) -> Self {
        info!("Connection: Created {connection_id}");

        let stop_signal = Arc::new(AtomicBool::new(false));
        let (response_sender, response_receiver) = channel::<ServerMessage>();

        let thread = ConnectionThread::new(
            response_receiver,
            request_sender.clone(),
            stream,
            &buffer_pool,
            &stop_signal,
            &connections,
            connection_id,
        );
        thread::Builder::new()
            .name(String::from("bin-api-connection"))
            .spawn(move || thread.run())
            .unwrap();

        Self {
            sender: response_sender,
            stop_signal: stop_signal.clone(),
        }
    }

    pub(crate) fn stop(self: &Self) {
        self.stop_signal.store(true, Ordering::Relaxed);
    }

    pub(crate) fn send(
        self: &Self,
        message: ServerMessage,
    ) -> Result<(), SendError<ServerMessage>> {
        self.sender.send(message)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.stop();
        info!("Connection: Dropped");
    }
}
