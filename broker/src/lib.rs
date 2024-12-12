use std::sync::{atomic::{AtomicBool, AtomicU32}, Arc};

use persistence::PersistenceLayer;
use services::admin_service::AdminService;
use services::pub_service::PubService;
use services::sub_service::SubService;

/// Layer 1
/// Implemenations of various persistence schemes, for example in memory, file system or database
pub mod persistence;

/// Layer 2
/// Mechanisms for consistent updates on persisted data using entity versioning
pub mod data;

/// Layer 3
/// Defines the internal model of the state of the cluster
pub mod model;

/// Layer 4
/// Business logic surrounding publishing and subscribing
pub mod services;

/// Layer 5
/// Low performance http REST API for managing nodes, topics, partitions and subscriptions
pub mod api_http_warp;

/// Layer 5
/// High performance API streaming binary data to and from the client
pub mod api_bin_sockets;

/// Socket communications between nodes to keep the cluster in sync
pub mod node_sync;

/// Miscelaneous utility functions
pub mod utils;
pub mod collections;

/// Metrics and logging
pub mod observability;

pub struct App {
    pub stop_signal: Arc<AtomicBool>,
    pub request_count: Arc<AtomicU32>,
    pub peristence: Arc<PersistenceLayer>,
    pub pub_service: Arc<PubService>,
    pub sub_service: Arc<SubService>,
    pub admin_service: Arc<AdminService>,
}
