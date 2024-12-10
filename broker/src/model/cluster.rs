use std::sync::{Arc, RwLock};
use pulsar_rust_net::data_types::{
    NodeId, PortNumber
};
use crate::{
    data::DataLayer, persistence::persisted_entities
};
use super::{
    node::{Node,NodeList}, 
    topic::{Topic,TopicList}, 
    RefreshStatus
};

#[derive(Debug)]
struct MutableState {
    refresh_status: RefreshStatus,
    cluster: persisted_entities::Cluster,
}

/// Represents all of the state information that is relevant to this node in the clueter.
/// Some of this data is stored persistently and can be refreshed when other nodes in the
/// cluster update the database. Other data is updated dynamically as messages are published
/// and consumed. These dynamic updates are logged as events that can be replayed to restore
/// the correct state after a node restart.
pub struct Cluster {
    data_layer: Arc<DataLayer>,
    mutable: RwLock<MutableState>,
    nodes: Arc<NodeList>,
    topics: Arc<TopicList>,
    my_node_id: NodeId,
}

pub const DEFAULT_ADMIN_PORT: PortNumber = 8000;
pub const DEFAULT_PUBSUB_PORT: PortNumber = 8001;
pub const DEFAULT_SYNC_PORT: PortNumber = 8002;

impl Cluster {
    pub fn nodes(self: &Self) -> &Arc<NodeList> { &self.nodes }
    pub fn topics(self: &Self) -> &Arc<TopicList> { &self.topics }
    pub fn my_node_id(self: &Self) -> NodeId { self.my_node_id }

    pub fn new(data_layer: &Arc<DataLayer>, my_ip_address: &str) -> Self {
        let mut cluster = data_layer.get_cluster().unwrap();

        let my_node_id = match cluster
            .node_ids
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
            NodeList::new(cluster.node_ids.iter().map(|&node_id|Node::new(data_layer, node_id)))
        );
            
        let topics = Arc::new(
            TopicList::new(cluster.topic_ids.iter().map(|&topic_id|Topic::new(data_layer, topic_id)))
        );
            
        Self {
            data_layer: Arc::clone(data_layer),
            mutable: RwLock::new(
                MutableState{
                    refresh_status: RefreshStatus::Updated,
                    cluster,
                }
            ),
            nodes,
            topics,
            my_node_id,
        }
    }

    pub fn refresh(self: &Self) -> () {
        let cluster_result = self.data_layer.get_cluster();

        let mutable_state: &mut MutableState = &mut *self.mutable.write().unwrap();

        match cluster_result {
            Ok(cluster) => {
                mutable_state.cluster = cluster;
                mutable_state.refresh_status = RefreshStatus::Updated;
            }
            Err(_) => {
                mutable_state.refresh_status = RefreshStatus::Stale;
            }
        };

        // TODO: Refresh nodes and topics removing ones that are in a deleted state
        // and creating new ones for ones that are new in the data. We can use the
        // original value of persisted_data to find changes.
    }

    pub fn my_node(self: &Self) -> &Node {
        self.nodes.by_id(self.my_node_id).unwrap()
    }
}
