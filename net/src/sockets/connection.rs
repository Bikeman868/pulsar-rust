use std::{
    io::{ErrorKind, Read, Write}, 
    net::TcpStream, sync::Arc, 
};
use log::{error, debug};
use super::{buffer_pool::BufferPool, MessageLength};

/// Sends a byte array over a Tcp stream
pub fn send_tcp(message: Vec<u8>, stream: &mut TcpStream, buffer_pool: &Arc<BufferPool>) -> std::io::Result<()> {
    let len = message.len();
    // debug!("Tcp: Sending length={len}");
    let length: MessageLength = len.try_into().expect("Tcp: Message length must fit into MessageLength type");
    let length = length.to_le_bytes();
    match stream.write(&length) {
        Ok(_) => {
            // debug!("Tcp: Sending message={message:?}");
            match stream.write(&message[..]) {
                Ok(_) => {
                    buffer_pool.reuse(message);
                    Ok(())
                }
                Err(e) => {
                    error!("Tcp: Error sending message: {e}");
                    Err(e)
                }
            }
        },
        Err(e) => {
            error!("Tcp: Error sending length: {e}");
            Err(e)
        }
    }
}

/// Receives a byte array from a Tcp stream
pub fn try_receive_tcp(stream: &mut TcpStream, buffer_pool: &Arc<BufferPool>) -> std::io::Result<Option<Vec<u8>>> {
    let mut length_buffer = MessageLength::default().to_ne_bytes();
    let length_byte_count = length_buffer.len();
    
    let bytes_available = match stream.peek(&mut length_buffer) {
        Ok(byte_count) => byte_count,
        Err(e) if e.kind() == ErrorKind::WouldBlock => {
            return Ok(None);
        }
        Err(e) => { return Err(e); }
    };

    if bytes_available < length_byte_count {
        // debug!("Tcp: Not enough bytes received for message length");
        return Ok(None); 
    }

    let message_length = MessageLength::from_le_bytes(length_buffer);
    let entire_length = length_byte_count + message_length as usize;
    // debug!("Tcp: Message is {message_length} bytes long");

    let mut buffer = buffer_pool.get(entire_length as MessageLength);
    let bytes_available = match stream.peek(&mut buffer) {
        Ok(byte_count) => byte_count,
        Err(e) if e.kind() == ErrorKind::WouldBlock => {
            buffer_pool.reuse(buffer);
            return Ok(None);
        }
        Err(e) => { 
            buffer_pool.reuse(buffer);
            return Err(e);
        }
    };
    buffer_pool.reuse(buffer);

    if bytes_available < entire_length { 
        // debug!("Tcp: Not enough bytes received for entire message");
        return Ok(None);
    }

    let mut message_buffer = buffer_pool.get(message_length);
    stream.read_exact(&mut length_buffer)?;
    stream.read_exact(&mut message_buffer)?;

    // debug!("Tcp: Received {message_buffer:?}");
    Ok(Some(message_buffer))
}
