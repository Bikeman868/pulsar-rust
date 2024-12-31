use std::{
    io::{Read, Write}, 
    net::TcpStream, sync::Arc, 
};

use super::{buffer_pool::BufferPool, MessageLength};

/// Sends a byte array over a Tcp stream
pub fn send_tcp(message: Vec<u8>, stream: &mut TcpStream, buffer_pool: &Arc<BufferPool>) -> std::io::Result<()> {
    let len = message.len();
    dbg!(format!("Tcp: Sending length={len}"));
    let length: MessageLength = len.try_into().expect("Tcp: Message length must fit into MessageLength type");
    let length = length.to_le_bytes();
    match stream.write(&length) {
        Ok(_) => {
            dbg!(format!("Tcp: Sending message={message:?}"));
            match stream.write(&message[..]) {
                Ok(_) => {
                    buffer_pool.reuse(message);
                    Ok(())
                }
                Err(e) => {
                    dbg!(format!("Tcp: Error sending message: {e}"));
                    Err(e)
                }
            }
        },
        Err(e) => {
            dbg!(format!("Tcp: Error sending length: {e}"));
            Err(e)
        }
    }
}

/// Receives a byte array from a Tcp stream
pub fn try_receive_tcp(stream: &mut TcpStream, buffer_pool: &Arc<BufferPool>) -> std::io::Result<Option<Vec<u8>>> {
    let mut length_buffer = MessageLength::default().to_ne_bytes();
    let bytes_available = stream.peek(&mut length_buffer)?;
    if bytes_available == length_buffer.len() {
        dbg!(format!("Tcp: Message length received"));
        stream.read_exact(&mut length_buffer)?;
        let length = MessageLength::from_le_bytes(length_buffer);
        dbg!(format!("Tcp: Message is {length} bytes long"));

        let mut buffer = buffer_pool.get(length);
        dbg!(format!("Tcp: Reading {length} bytes from Tcp stream"));
        match stream.read_exact( &mut buffer) {
            Ok(_) => {
                dbg!(format!("Tcp: Received {buffer:?}"));
                Ok(Some(buffer))
            },
            Err(e) => {
                dbg!(format!("Tcp: Error reading message: {e}"));
                Err(e)
            }
        }
    } else {
        Ok(None)
    }
}
