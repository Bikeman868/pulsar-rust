use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::Sender,
    Arc,
};

use super::server::{ConnectionId, ServerMessage};

pub(crate) struct Connection {
    pub(super) connection_id: ConnectionId,
    pub(super) sender: Sender<ServerMessage>,
    pub(super) stop_signal: Arc<AtomicBool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}
