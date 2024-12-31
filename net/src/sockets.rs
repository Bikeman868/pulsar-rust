/*
Thin wrapper around sockets implemenation in the standard library.
Includes thread pooling and scheduling request handling
*/
pub mod buffer_pool;
pub mod connection;
pub mod broker_to_broker;
pub mod client_to_broker;
pub mod broker_to_client;

pub(crate) type MessageLength = u16;
