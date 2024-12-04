use std::sync::{atomic::AtomicU32, Arc};

use admin_service::AdminService;
use pub_service::PubService;
use sub_service::SubService;

/// Defines the internal data structures that are used to process messages
pub mod model;

/// Mechanisms for consistent updates on persisted data using entity versioning
pub mod data;

/// Implemenations of various persistence schemes, for example in memory, file system or database
pub mod persistence;

/// Business logic to support publishers
pub mod pub_service;

/// Business logic to support subscribers
pub mod sub_service;

/// Cluster administration functions such as managing topics, subscriptions and partitions
pub mod admin_service;

pub mod http_api_sockets;
/// Http REST API for managing nodes, topics, subscriptions etc
pub mod http_api_warp;

/// Socket communications between nodes to keep the cluster in sync
pub mod node_sync;

/// Miscelaneous utility functions
pub mod utils;

pub struct App {
    pub request_count: Arc<AtomicU32>,
    pub pub_service: Arc<PubService>,
    pub sub_service: Arc<SubService>,
    pub admin_service: Arc<AdminService>,
}
