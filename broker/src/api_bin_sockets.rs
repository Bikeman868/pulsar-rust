use core::time::Duration;

use std::{
    net::SocketAddrV4, 
    sync::{
        atomic::{
            AtomicBool, 
            Ordering
        }, mpsc::{channel, Receiver, RecvTimeoutError, Sender}, Arc
    }, 
    thread::{
        self, available_parallelism, JoinHandle
    }, 
};

use log::{warn, info, debug};
use pulsar_rust_net::sockets::{buffer_pool::BufferPool, MessageLength};
use server::{Server, ServerMessage};
use server_thread::ServerThread;
use crate::App;

mod server;
mod listener_thread;
mod router_thread;
mod connection_thread;
mod connection;
mod server_thread;
mod processing_thread;

pub fn serve(app: &Arc<App>, addr: SocketAddrV4) -> JoinHandle<()> {
    let server_thread = ServerThread::new(&app.stop_signal, &app, addr);
    info!("Binary API listening on {addr}");
    thread::spawn(move||server_thread.run())
}
