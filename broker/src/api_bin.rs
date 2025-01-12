use std::{
    net::SocketAddrV4,
    sync::Arc,
    thread::{self, JoinHandle},
};

use crate::App;
use log::info;
use processing_thread_pool::ProcessingThreadPool;
use pulsar_rust_net::sockets::buffer_pool::BufferPool;

mod connection;
mod connection_thread;
mod listener_thread;
mod processing_thread;
mod processing_thread_pool;
mod router_thread;
mod server;

pub fn serve(app: &Arc<App>, addr: SocketAddrV4) -> JoinHandle<()> {
    let buffer_pool = Arc::new(BufferPool::new());
    let server_thread = ProcessingThreadPool::new(&app.stop_signal, &buffer_pool, &app, addr);
    info!("Binary API listening on {addr}");
    thread::spawn(move || server_thread.run())
}
