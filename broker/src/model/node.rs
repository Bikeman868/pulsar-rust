use std::{
    collections::{hash_map, HashMap}, 
    sync::Arc
};
use pulsar_rust_net::data_types::{
    NodeId, PortNumber,
};
use crate::{
    data::{DataLayer, DataReadError},
    persistence::persisted_entities,
};

use super::RefreshStatus;

/// Represents a VM that is running the application and processing requests for pub/sub
#[derive(Debug)]
pub struct Node {
    persisted_data: persisted_entities::Node,
    refresh_status: RefreshStatus,
    node_id: NodeId,
    ip_address: String,
    admin_port: PortNumber,
    pubsub_port: PortNumber,
    sync_port: PortNumber,
}

impl Node {
    pub fn node_id(self: &Self) -> NodeId { self.node_id }
    pub fn ip_address(self: &Self) -> &String { &self.ip_address }
    pub fn admin_port(self: &Self) -> PortNumber { self.admin_port }
    pub fn pubsub_port(self: &Self) -> PortNumber { self.pubsub_port }
    pub fn sync_port(self: &Self) -> PortNumber { self.sync_port }

    pub fn new(data_layer: &Arc<DataLayer>, node_id: NodeId) -> Self {
        let node = data_layer.get_node(node_id).unwrap();
        let ip_address = node.ip_address.clone();
        let admin_port = node.admin_port;
        let pubsub_port = node.pubsub_port;
        let sync_port = node.sync_port;
        Self {
            node_id,
            persisted_data: node,
            refresh_status: RefreshStatus::Updated,
            ip_address,
            admin_port,
            pubsub_port,
            sync_port,
        }
    }

    pub fn refresh(self: &mut Self, data_layer: &DataLayer) {
        match data_layer.get_node(self.persisted_data.node_id) {
            Ok(node) => {
                self.persisted_data = node;
                self.refresh_status = RefreshStatus::Updated;
            },
            Err(err) => match err {
                DataReadError::NotFound { .. } => {
                    self.refresh_status = RefreshStatus::Deleted
                }
                _ => self.refresh_status = RefreshStatus::Stale
            },
        }
    }
}

/// Represents the list of nodes in the cluster
#[derive(Debug)]
pub struct NodeList {
    hash_by_node_id: HashMap<NodeId, Node>,
}

impl NodeList {
    pub fn new (nodes: impl Iterator<Item = Node>) -> Self{
        Self { hash_by_node_id: nodes.map(|n|(n.node_id, n)).collect() }
    }

    pub fn by_id(self: &Self, node_id: NodeId) -> Option<&Node> {
        Some(self.hash_by_node_id.get(&node_id)?)
    }

    pub fn iter(self: &Self) -> hash_map::Values<'_, NodeId, Node>  {
        self.hash_by_node_id.values()
    }
}
