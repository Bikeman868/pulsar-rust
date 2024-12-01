use std::{collections::HashMap, sync::Mutex};

use crate::{data::DataLayer, persistence::persisted_entities};

use super::{
    data_types::{NodeId, TopicId},
    RefreshStatus,
};

/// Represents all of the state information that is relevant to this node in the clueter.
/// Some of this data is stored persistently and can be refreshed when other nodes in the
/// cluster update the database. Other data is updated dynamically as messages are published
/// and consumed. These dynamic updates are logged as events that can be replayed to restore
/// the correct state after a node restart.
pub struct Cluster<'a> {
    data_layer: &'a DataLayer<'a>,
    refresh_status: RefreshStatus,
    refresh_lock: Mutex<bool>,
    persisted_data: persisted_entities::Cluster,
    nodes: HashMap<NodeId, super::Node>,
    topics: HashMap<TopicId, super::Topic>,
    my_node_id: NodeId,
}

impl<'a> Cluster<'a> {
    pub fn new(data_layer: &'a DataLayer, my_ip_address: &str) -> Self {
        let mut cluster = data_layer.get_cluster().unwrap();

        let my_node_id = match cluster
            .nodes
            .iter()
            .map(|&node_id| data_layer.get_node(node_id).unwrap())
            .find(|node| node.ip_address == my_ip_address)
        {
            Some(node) => node.node_id,
            None => {
                let node = data_layer.add_node(&my_ip_address).unwrap();
                cluster = data_layer.get_cluster().unwrap();
                node.node_id
            }
        };

        let nodes: HashMap<NodeId, super::Node> = cluster
            .nodes
            .iter()
            .map(|&node_id| (node_id, super::Node::new(data_layer, node_id)))
            .collect();

        let topics: HashMap<TopicId, super::Topic> = cluster
            .topics
            .iter()
            .map(|&topic_id| (topic_id, super::Topic::new(data_layer, topic_id)))
            .collect();

        Self {
            data_layer,
            refresh_status: RefreshStatus::Updated,
            refresh_lock: Mutex::new(false),
            persisted_data: cluster,
            nodes,
            topics,
            my_node_id,
        }
    }

    pub fn refresh(self: &mut Self) -> () {
        let refresh_lock = self.refresh_lock.lock().unwrap();

        match self.data_layer.get_cluster() {
            Ok(cluster) => {
                self.persisted_data = cluster;
                self.refresh_status = RefreshStatus::Updated;
            }
            Err(_) => {
                self.refresh_status = RefreshStatus::Stale;
            }
        };

        // TODO: Refresh nodes and topics removing ones that are in a deleted state
        // and creating new ones for ones that are new in the data. We can use the
        // original value of persisted_data to find changes.

        drop(refresh_lock);
    }

    pub fn my_node(self: &'a Self) -> &'a super::Node {
        self.nodes.get(&self.my_node_id).unwrap()
    }
}
