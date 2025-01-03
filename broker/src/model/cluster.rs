use super::{
    node::{Node, NodeList, NodeRef},
    topic::{Topic, TopicList, TopicStats},
    EntityList, RefreshStatus,
};
use crate::{
    data::DataLayer,
    formatting::plain_text_builder::{PlainTextBuilder, ToPlainText},
    persistence::persisted_entities,
};
use pulsar_rust_net::data_types::{NodeId, PortNumber, TopicId};
use serde::Serialize;
use std::sync::{Arc, RwLock};

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Serialize)]
pub struct ClusterTopicStats {
    pub topic_id: TopicId,
    pub topic_stats: TopicStats,
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Serialize)]
pub struct ClusterStats {
    pub topics: Vec<ClusterTopicStats>,
}

impl ToPlainText for ClusterStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Cluster Stats", 0);
        builder.new_line();
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.indent();

        ClusterTopicStats::to_plain_text_header(builder);
        for topic in &self.topics {
            topic.to_plain_text(builder);
        }

        builder.outdent();
    }
}

impl ToPlainText for ClusterTopicStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Topic id", 10);
        TopicStats::to_plain_text_header(builder);
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.u32_left(self.topic_id, 10);
        self.topic_stats.to_plain_text(builder);
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
struct ClusterState {
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
    state: RwLock<ClusterState>,
    nodes: NodeList,
    topics: TopicList,
    my_node_id: NodeId,
}

pub const DEFAULT_ADMIN_PORT: PortNumber = 8000;
pub const DEFAULT_PUBSUB_PORT: PortNumber = 8001;
pub const DEFAULT_SYNC_PORT: PortNumber = 8002;

impl Cluster {
    pub fn nodes(self: &Self) -> &NodeList {
        &self.nodes
    }

    pub fn topics(self: &Self) -> &TopicList {
        &self.topics
    }

    pub fn my_node_id(self: &Self) -> NodeId {
        self.my_node_id
    }

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
                let node = data_layer
                    .add_node(
                        &my_ip_address,
                        DEFAULT_ADMIN_PORT,
                        DEFAULT_PUBSUB_PORT,
                        DEFAULT_SYNC_PORT,
                    )
                    .unwrap();
                cluster = data_layer.get_cluster().unwrap();
                node.node_id
            }
        };

        let nodes = EntityList::from_iter(
            cluster
                .node_ids
                .iter()
                .map(|&node_id| Node::new(data_layer, node_id)),
        );

        let topics = EntityList::from_iter(
            cluster
                .topic_ids
                .iter()
                .map(|&topic_id| Topic::new(data_layer, topic_id)),
        );

        Self {
            data_layer: Arc::clone(data_layer),
            state: RwLock::new(ClusterState {
                refresh_status: RefreshStatus::Updated,
                cluster,
            }),
            nodes,
            topics,
            my_node_id,
        }
    }

    pub fn refresh(self: &Self) -> () {
        let cluster_result = self.data_layer.get_cluster();

        let mutable_state: &mut ClusterState = &mut *self.state.write().unwrap();

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

    pub fn stats(self: &Self) -> ClusterStats {
        let topics = self
            .topics
            .values()
            .iter()
            .map(|topic| ClusterTopicStats {
                topic_id: topic.topic_id(),
                topic_stats: topic.stats(),
            })
            .collect();
        ClusterStats { topics }
    }

    pub fn my_node(self: &Self) -> NodeRef {
        self.nodes.get(&self.my_node_id).unwrap()
    }
}
