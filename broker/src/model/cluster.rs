use std::sync::{Arc, Mutex};
use pulsar_rust_net::data_types::{NodeId, PortNumber};
use crate::{data::DataLayer, persistence::persisted_entities};
use super::{NodeList, TopicList, RefreshStatus};

/// Represents all of the state information that is relevant to this node in the clueter.
/// Some of this data is stored persistently and can be refreshed when other nodes in the
/// cluster update the database. Other data is updated dynamically as messages are published
/// and consumed. These dynamic updates are logged as events that can be replayed to restore
/// the correct state after a node restart.
pub struct Cluster {
    data_layer: Arc<DataLayer>,
    refresh_lock: Mutex<bool>,
    refresh_status: RefreshStatus,
    persisted_data: persisted_entities::Cluster,
    pub nodes: Arc<NodeList>,
    pub topics: Arc<TopicList>,
    pub my_node_id: NodeId,
}

pub const DEFAULT_ADMIN_PORT: PortNumber = 8000;
pub const DEFAULT_PUBSUB_PORT: PortNumber = 8000;
pub const DEFAULT_SYNC_PORT: PortNumber = 8000;

impl Cluster {
    pub fn new(data_layer: Arc<DataLayer>, my_ip_address: &str) -> Self {
        let mut cluster = data_layer.get_cluster().unwrap();

        let my_node_id = match cluster
            .nodes
            .iter()
            .map(|&node_id| data_layer.get_node(node_id).unwrap())
            .find(|node| node.ip_address == my_ip_address)
        {
            Some(node) => node.node_id,
            None => {
                let node = data_layer.add_node(
                    &my_ip_address, 
                    DEFAULT_ADMIN_PORT, 
                    DEFAULT_PUBSUB_PORT,
                    DEFAULT_SYNC_PORT)
                .unwrap();
                cluster = data_layer.get_cluster().unwrap();
                node.node_id
            }
        };

        let nodes = Arc::new(
            NodeList::new(cluster.nodes.iter().map(|&node_id|super::Node::new(data_layer.clone(), node_id)))
        );
            
        let topics = Arc::new(
            TopicList::new(cluster.topics.iter().map(|&topic_id|super::Topic::new(data_layer.clone(), topic_id)))
        );
            
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

    pub fn my_node(self: &Self) -> &super::Node {
        self.nodes.by_id(self.my_node_id).unwrap()
    }
}
