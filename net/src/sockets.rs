/*
Thin wrapper around sockets implemenation in the standard library.
Includes thread pooling and scheduling request handling
*/
pub mod buffer_pool;
pub mod tcp_channel;

pub type MessageLength = u16;
