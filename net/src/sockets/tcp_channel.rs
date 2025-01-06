use super::{buffer_pool::BufferPool, MessageLength};
use log::{error, warn, info};
use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    sync::{atomic::{AtomicBool, Ordering}, mpsc::{Receiver, Sender, TryRecvError}, Arc}, thread::{self}, time::{Duration, Instant},
};

#[cfg(debug_assertions)]
use log::debug;

const IDLE_SLEEP_LIMIT: Duration = Duration::from_millis(50);
const IDLE_SLEEP_DURATION: Duration = Duration::from_millis(10);
const DISCONNECT_IDLE_TIME: Duration = Duration::from_secs(60);
const MESSAGE_LENGTH_SIZE: usize = size_of::<MessageLength>();
const MAX_MESSAGE_SIZE: usize = 4096;
const RECEIVE_BUFFER_SIZE: usize = MAX_MESSAGE_SIZE << 2;
const MAX_TX_RETRY_COUNT: usize = 5;
const TX_RETRY_INTERVAL: Duration = Duration::from_millis(10);

pub struct TcpChannel {
    stop_signal: Arc<AtomicBool>,
}

impl TcpChannel {
    pub fn new(
        receiver: Receiver<Vec<u8>>,
        sender: Sender<Vec<u8>>,
        stream: TcpStream,
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
    ) -> Self {
        info!("TcpChannel: Created");
        
        let thread = TcpThread::new(receiver, sender, stream, buffer_pool, stop_signal);
        thread::spawn(move || thread.run());

        Self {
            stop_signal: stop_signal.clone(),
        }
    }

    pub fn stop(self: &Self) {
        if !self.stop_signal.load(Ordering::Relaxed) {
            self.stop_signal.store(true, Ordering::Relaxed);
            info!("TcpChannel: Stopped");
        }
    }
}

impl Drop for TcpChannel {
    fn drop(&mut self) {
        self.stop();
        info!("TcpChannel: Dropped");
    }
}

struct TcpThread {
    stream: TcpStream,
    buffer_pool: Arc<BufferPool>,
    stop_signal: Arc<AtomicBool>,
    last_message_instant: Instant,

    channel_rx: Receiver<Vec<u8>>,
    channel_tx: Sender<Vec<u8>>,

    receive_buffer: [u8; RECEIVE_BUFFER_SIZE],
    receive_buffer_count: usize,
    consumed_count: usize,
}

impl TcpThread {
    fn new(
        receiver: Receiver<Vec<u8>>,
        sender: Sender<Vec<u8>>,
        stream: TcpStream,
        buffer_pool: &Arc<BufferPool>,
        stop_signal: &Arc<AtomicBool>,
    ) -> Self {
        Self {
            stream,
            buffer_pool: buffer_pool.clone(),
            stop_signal: stop_signal.clone(),
            last_message_instant: Instant::now(),

            channel_rx: receiver,
            channel_tx: sender,

            receive_buffer: [0u8; RECEIVE_BUFFER_SIZE],
            consumed_count: 0,
            receive_buffer_count: 0,
        }
    }

    fn run(mut self: Self) {
        info!("TcpThread: Started");
        while !self.stop_signal.load(Ordering::Relaxed) {
            self.try_send();
            self.try_receive();
            self.try_extract_received();
            self.stop_if_idle();
        }
        info!("TcpThread: Stopped");
    }

    fn stop(self: &mut Self) {
        self.stop_signal.store(true, Ordering::Relaxed);
        self.last_message_instant = Instant::now();
    }

    fn fatal(self: &mut Self, msg: &str) {
        info!("TcpThread: {}", msg);
        self.stop();
    }

    fn try_send(self: &mut Self) {
        let message = match self.channel_rx.try_recv() {
            Ok(message) => message,
            Err(e) => match e {
                TryRecvError::Empty => { return }
                TryRecvError::Disconnected => {
                    self.fatal(&"Channel receiver disconnected");
                    return
                }
            }
        };

        self.last_message_instant = Instant::now();
        
        let len = message.len();
        let length: MessageLength = len
            .try_into()
            .expect("TcpThread Tx: Message length must fit into MessageLength type");
        if len + MESSAGE_LENGTH_SIZE > MAX_MESSAGE_SIZE {
            error!("TcpThread Tx: {len} exceeds maximum message length and cannot be sent");
            self.buffer_pool.reuse(message);
            return;
        }

        let length_bytes = length.to_le_bytes();
        if self.send(&length_bytes) {
            let _ = self.send(&message[..]);
        }
        self.buffer_pool.reuse(message);
    }

    fn send(self: &mut Self, buf: &[u8]) -> bool {
        let mut retry_count = 0;
        loop {
            #[cfg(debug_assertions)]
            debug!("TcpThread Tx: Sending {buf:?}");

            match self.stream.write(buf) {
                Ok(_) => {
                    return true;
                }
                Err(e) => {
                    match e.kind(){
                        ErrorKind::ConnectionReset | 
                        ErrorKind::ConnectionAborted | 
                        ErrorKind::NotConnected => {
                            self.fatal("Tx stream closed by other party");
                            return false;
                        }
                        ErrorKind::WouldBlock => {
                        }
                        ErrorKind::TimedOut => {
                            warn!("TcpThread Tx: Timeout sending length: {e}");
                        },
                        ErrorKind::OutOfMemory => {
                            self.fatal("Out of memory sending length");
                            return false;
                        }
                        _ => {}
                    }
                }
            }
            retry_count += 1;
            if retry_count > MAX_TX_RETRY_COUNT {
                self.fatal("Retry count exceeded sending message to TcpStream");
                return false;
            } else {
                thread::sleep(TX_RETRY_INTERVAL);
            }
        }
    }

    fn try_receive(self: &mut Self) {
        match self.stream.read(&mut self.receive_buffer[self.receive_buffer_count..]) {
            Ok(byte_count) if byte_count == 0 => {}
            Ok(byte_count) => {
                #[cfg(debug_assertions)]
                debug!("TcpThread Rx: Received {byte_count} bytes");
                self.receive_buffer_count += byte_count;
            }
            Err(err) => {
                match err.kind() {
                    ErrorKind::ConnectionReset |
                    ErrorKind::ConnectionAborted |
                    ErrorKind::NotConnected => {
                        self.fatal("Rx stream closed by other party");
                    }
                    ErrorKind::WouldBlock => {
                    }
                    ErrorKind::TimedOut => {
                        warn!("TcpThread Rx: Timeout reading from TcpStream");
                    },
                    ErrorKind::OutOfMemory => {
                        self.fatal("Out of memory reading from TcpStream");
                    }
                    _ => {}
                }
            }
        }
    }

    fn try_extract_received(self: &mut Self) {
        loop {
            let residual_byte_count = self.receive_buffer_count - self.consumed_count;
            if residual_byte_count < MESSAGE_LENGTH_SIZE {
                break
            }

            #[cfg(debug_assertions)]
            debug!("TcpThread Rx: residual_byte_count:{residual_byte_count}. consumed_count:{} ", self.consumed_count);

            let length_start_index = self.consumed_count;
            let length_end_index = length_start_index + MESSAGE_LENGTH_SIZE;
            let length_bytes = self.receive_buffer[length_start_index..length_end_index].try_into().unwrap();
            let message_length = MessageLength::from_le_bytes(length_bytes);

            #[cfg(debug_assertions)]
            debug!("TcpThread Rx: Next message is {message_length} bytes");
            
            let entire_length = MESSAGE_LENGTH_SIZE + message_length as usize;
            if residual_byte_count < entire_length {
                break
            }

            let mut message = self.buffer_pool.get(message_length);
            let message_start_index = self.consumed_count + MESSAGE_LENGTH_SIZE;
            let message_end_index = message_start_index + (message_length as usize);
            message.copy_from_slice(&self.receive_buffer[message_start_index..message_end_index]);

            #[cfg(debug_assertions)]
            debug!("TcpThread Rx: Extracted message {message:?}");
    
            self.consumed_count += entire_length;

            match self.channel_tx.send(message) {
                Ok(_) => {
                    #[cfg(debug_assertions)]
                    debug!("TcpThread Rx: Posted message to channel");
                }
                Err(e) => {
                    self.fatal(&format!("Failed to post message to channel: {e}"));
                    return
                }
            }
        }

        let residual_byte_count = self.receive_buffer_count - self.consumed_count;
        if residual_byte_count == 0 {
            self.receive_buffer_count = 0;
            self.consumed_count = 0;
        } else {
            let space_remaining = RECEIVE_BUFFER_SIZE - self.receive_buffer_count;
            if space_remaining < MAX_MESSAGE_SIZE {
                #[cfg(debug_assertions)]
                debug!("TcpThread Rx: Making room in buffer. consumed:{} space:{}", self.consumed_count, space_remaining);
                self.receive_buffer.copy_within(self.consumed_count.., 0);
                self.receive_buffer_count -= self.consumed_count;
                self.consumed_count = 0;
            }
        }
    }

    fn stop_if_idle(self: &mut Self) {
        let idle_duration = self.last_message_instant.elapsed();
        if idle_duration > IDLE_SLEEP_LIMIT {
            if idle_duration > DISCONNECT_IDLE_TIME {
                info!("TcpThread: Idle for too long, disconnecting");
                self.stop();
            } else {
                thread::sleep(IDLE_SLEEP_DURATION);
            }
        }
    }
}
