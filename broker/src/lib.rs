#[macro_use]
extern crate lazy_static;

use observability::Metrics;
use persistence::PersistenceLayer;
use services::pub_service::PubService;
use services::sub_service::SubService;
use services::{admin_service::AdminService, stats_service::StatsService};
use std::sync::{atomic::AtomicBool, Arc};

/// This module is updated with a randomly generated build number automatically on each build
/// The build number is used as a version identifier
pub mod build_number;

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
pub mod api_http;

/// Layer 5
/// High performance API streaming binary data to and from the client
pub mod api_bin;

/// Socket communications between nodes to keep the cluster in sync
pub mod node_sync;

/// Miscelaneous
mod collections;
mod formatting;
mod utils;

/// Metrics and logging
pub mod observability;

pub struct App {
    pub stop_signal: Arc<AtomicBool>,
    pub metrics: Arc<Metrics>,
    pub peristence: Arc<PersistenceLayer>,
    pub pub_service: Arc<PubService>,
    pub sub_service: Arc<SubService>,
    pub admin_service: Arc<AdminService>,
    pub stats_service: Arc<StatsService>,
}
