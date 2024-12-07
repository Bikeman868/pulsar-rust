/*
Note that the pulsar_rust_net crate always maps requests and responses to the latest version of the contract,
and this source file only has to map the latest version of the contract onto the internal model.
*/
use pulsar_rust_net::contracts::v1::responses;
use crate::model;

impl From<&model::Node> for responses::NodeSummary {
    fn from(node: &model::Node) -> Self {
        Self {
            node_id: node.node_id,
            ip_address: node.ip_address.clone(),
        }
    }
}

impl From<&model::Node> for responses::NodeDetail {
    fn from(node: &model::Node) -> Self {
        Self {
            node_id: node.node_id,
            ip_address: node.ip_address.clone(),
            admin_port: node.admin_port,
            pubsub_port: node.pubsub_port,
            sync_port: node.sync_port,
            catalogs: Vec::default(), // TODO
        }
    }
}

impl From<&model::Topic> for responses::TopicSummary {
    fn from(topic: &model::Topic) -> Self {
        Self {
            topic_id: topic.topic_id,
        }
    }
}

impl From<&model::Topic> for responses::TopicDetail {
    fn from(topic: &model::Topic) -> Self {
        Self {
            topic_id: topic.topic_id,
            partitions: Vec::default(), // TODO
        }
    }
}

impl From<&model::Partition> for responses::PartitionSummary {
    fn from(partition: &model::Partition) -> Self {
        Self {
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
        }
    }
}

impl From<&model::Partition> for responses::PartitionDetail {
    fn from(partition: &model::Partition) -> Self {
        Self {
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
            catalogs: Vec::default(), // TODO
        }
    }
}

impl From<&model::Catalog> for responses::CatalogSummary {
    fn from(catalog: &model::Catalog) -> Self {
        Self {
            topic_id: catalog.topic_id,
            partition_id: catalog.partition_id,
            catalog_id: catalog.catalog_id,
            node_id: catalog.node_id,
        }
    }
}

impl From<&model::Catalog> for responses::CatalogDetail {
    fn from(catalog: &model::Catalog) -> Self {
        Self {
            topic_id: catalog.topic_id,
            partition_id: catalog.partition_id,
            catalog_id: catalog.catalog_id,
            node_id: catalog.node_id,
        }
    }
}

impl From<&model::NodeList> for responses::NodeList {
    fn from(nodes: &model::NodeList) -> Self {
        Self {
            nodes: nodes.iter().map(|p| responses::NodeSummary::from(p)).collect(),
        }
    }
}

impl From<&model::TopicList> for responses::TopicList {
    fn from(topics: &model::TopicList) -> Self {
        Self {
            topics: topics.iter().map(|p| responses::TopicSummary::from(p)).collect(),
        }
    }
}

impl From<&model::PartitionList> for responses::PartitionList {
    fn from(partitions: &model::PartitionList) -> Self {
        Self {
            partitions: partitions.iter().map(|p| responses::PartitionSummary::from(p)).collect(),
        }
    }
}

impl From<&model::CatalogList> for responses::CatalogList {
    fn from(catalogs: &model::CatalogList) -> Self {
        Self {
            catalogs: catalogs.iter().map(|p| responses::CatalogSummary::from(p)).collect(),
        }
    }
}
