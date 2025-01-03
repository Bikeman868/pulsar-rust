use crate::{
    data::{DataLayer, DataReadError},
    persistence::persisted_entities,
};
use pulsar_rust_net::data_types::{NodeId, PortNumber};
use std::sync::Arc;

use super::{Entity, EntityList, EntityRef, RefreshStatus};

/// Represents a VM that is running the application and processing requests for pub/sub
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Node {
    persisted_data: persisted_entities::Node,
    refresh_status: RefreshStatus,
    node_id: NodeId,
    ip_address: String,
    admin_port: PortNumber,
    pubsub_port: PortNumber,
    sync_port: PortNumber,
}

impl Entity<NodeId> for Node {
    fn key(self: &Self) -> NodeId {
        self.node_id
    }
}

pub type NodeRef = EntityRef<NodeId, Node>;
pub type NodeList = EntityList<NodeId, Node>;

impl Node {
    pub fn node_id(self: &Self) -> NodeId {
        self.node_id
    }
    pub fn ip_address(self: &Self) -> &String {
        &self.ip_address
    }
    pub fn admin_port(self: &Self) -> PortNumber {
        self.admin_port
    }
    pub fn pubsub_port(self: &Self) -> PortNumber {
        self.pubsub_port
    }
    pub fn sync_port(self: &Self) -> PortNumber {
        self.sync_port
    }

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
            }
            Err(err) => match err {
                DataReadError::NotFound { .. } => self.refresh_status = RefreshStatus::Deleted,
                _ => self.refresh_status = RefreshStatus::Stale,
            },
        }
    }
}
