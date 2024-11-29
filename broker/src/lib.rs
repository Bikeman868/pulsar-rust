/// Defines the internal data structures that are used to process messages
pub mod model;

/// Mechanisms for consistent updates on persisted data using entity versioning
pub mod data;

/// Implemenations of various persistence schemes, for example in memory, file system or database
pub mod persistence;

/// Business logic to support publishers
pub mod publishing;

/// Business logic to support subscribers
pub mod subscribing;

/// Http REST API for managing nodes, topics, subscriptions etc
pub mod api;

/// Socket communications between nodes to keep the cluster in sync
pub mod node_sync;

/// Miscelaneous utility functions
pub mod utils;
