use log::info;
use pulsar_rust_net::sockets::buffer_pool::BufferPool;
use std::
    sync::{
        mpsc::{RecvError, SendError, TryRecvError},
        Arc,
    }
;

use super::connection::Connection;

pub(crate) type ClientMessage = Vec<u8>;

pub struct Client {
    authority: String,
    buffer_pool: Arc<BufferPool>,
    connection: Option<Connection>,
}

impl Client {
    pub fn new(buffer_pool: &Arc<BufferPool>, authority: &str) -> Self {
        info!("Client: Constructed for {authority}");

        Self {
            authority: String::from(authority),
            buffer_pool: buffer_pool.clone(),
            connection: None,
        }
    }

    pub fn connect(self: &mut Self) {
        self.connection = Some(Connection::new(&self.buffer_pool, &self.authority));
    }

    pub fn disconnect(self: &mut Self) {
        if let Some(connection) = self.connection.take() {
            connection.disconnect();
        }
    }

    pub fn is_connected(self: &Self) -> bool { self.connection.is_some() }

    pub fn try_recv(self: &Self) -> Result<ClientMessage, TryRecvError> {
        if let Some(connection) = &self.connection {
            connection.try_recv()
        } else { 
            Err(TryRecvError::Disconnected)
        }
    }

    pub fn recv(self: &Self) -> Result<ClientMessage, RecvError> {
        if let Some(connection) = &self.connection {
            connection.recv()
        } else { 
            Err(RecvError)
        }
    }

    pub fn send(&self, message: ClientMessage) -> Result<(), SendError<ClientMessage>> {
        if let Some(connection) = &self.connection {
            connection.send(message)
        } else { 
            Err(SendError(message))
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.disconnect();
        info!("Client: Dropped");
    }
}
