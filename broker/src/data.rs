use std::sync::Arc;

use serde::Deserialize;

use crate::{
    model::data_types::{CatalogId, NodeId, PartitionId, SubscriptionId, TopicId},
    persistence::{
        entity_persister::{DeleteError, LoadError, SaveError},
        persisted_entities::{Catalog, Cluster, Node, Partition, Subscription, Topic},
        Keyed, PersistenceLayer,
    },
};

#[derive(Debug, PartialEq)]
pub enum DataErr {
    Duplicate { msg: String },
    PersistenceFailure { msg: String },
    NotFound { msg: String },
}

pub type ReadResult<T> = Result<T, DataErr>;
pub type AddResult<T> = Result<T, DataErr>;
pub type UpdateResult<T> = Result<T, DataErr>;

pub struct DataLayer {
    cluster_name: String,
    persistence: Arc<PersistenceLayer>,
}

/// Provides functions for adding, retrieving, updating, and deleting entities that are persisted to the database.
///
/// If two threads (even ones running on different pods) simultaneously read-modify-write the same entity, then
/// one thread will succeed and the other will retry the whole read-modify-write operation. This guarantees that
/// all updates are persisted.
///
/// Ensures referential integrity between entities. For example adding or deleting a partition listt update the
/// partition list of the topic it belongs to. Also does cascading deletes, so deleting a topic will delete all of
/// the partitions and subscriptions, and for each partition, the catalogs will be deleted.
///
/// Note that this data layer is designed for storing configuration data only, and is not suitable for high throughput.
/// As originally implemented, we store topics, partitions etc and these things change very rarely. Do not add volatile
/// information to these entities and try to update them thousands of times per second, it won't work.
impl DataLayer {
    pub fn new(cluster_name: String, persistence: Arc<PersistenceLayer>) -> Self {
        Self {
            cluster_name,
            persistence,
        }
    }

    pub fn get_cluster(self: &Self) -> ReadResult<Cluster> {
        match self.persistence.load(&Cluster::key(&self.cluster_name)) {
            Ok(cluster) => Ok(cluster),
            Err(e) => match e {
                LoadError::Error { msg } => {
                    panic!("{} {}", msg, "getting the cluster record from the database")
                }
                LoadError::NotFound {
                    entity_type: _,
                    entity_key: _,
                } => {
                    let mut cluster =
                        Cluster::new(&self.cluster_name, Vec::new(), Vec::new(), 1, 1);
                    self.persistence
                        .save(&mut cluster)
                        .expect("create cluster record in the database");
                    Ok(cluster)
                }
            },
        }
    }

    pub fn get_node(self: &Self, node_id: NodeId) -> ReadResult<Node> {
        self.get_entity(&Node::key(node_id))
    }

    pub fn get_topic(self: &Self, topic_id: TopicId) -> ReadResult<Topic> {
        self.get_entity(&Topic::key(topic_id))
    }

    pub fn get_partition(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> ReadResult<Partition> {
        self.get_entity(&Partition::key(topic_id, partition_id))
    }

    pub fn get_catalog(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> ReadResult<Catalog> {
        self.get_entity(&Catalog::key(topic_id, partition_id, catalog_id))
    }

    pub fn get_subscription(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
    ) -> ReadResult<Subscription> {
        self.get_entity(&Subscription::key(topic_id, subscription_id))
    }

    pub fn get_nodes(self: &Self) -> ReadResult<Vec<Node>> {
        let cluster = match self.get_cluster() {
            Ok(c) => c,
            Err(e) => return ReadResult::Err(e),
        };

        let mut nodes: Vec<Node> = Vec::new();

        for node_id in cluster.nodes {
            match self.get_node(node_id) {
                Ok(node) => nodes.push(node),
                Err(e) => return Err(e),
            };
        }

        Ok(nodes)
    }

    pub fn get_topics(self: &Self) -> ReadResult<Vec<Topic>> {
        let cluster = match self.get_cluster() {
            Ok(c) => c,
            Err(e) => return ReadResult::Err(e),
        };

        let mut topics: Vec<Topic> = Vec::new();

        for topic_id in cluster.topics {
            match self.get_topic(topic_id) {
                Ok(topic) => topics.push(topic),
                Err(e) => return Err(e),
            };
        }

        Ok(topics)
    }

    pub fn get_partitions(self: &Self, topic: &Topic) -> ReadResult<Vec<Partition>> {
        let mut partitions: Vec<Partition> = Vec::new();

        for partition_id in &topic.partitions {
            match self.get_partition(topic.topic_id, *partition_id) {
                Ok(partition) => partitions.push(partition),
                Err(e) => return Err(e),
            };
        }

        Ok(partitions)
    }

    pub fn get_catalogs(self: &Self, partition: &Partition) -> ReadResult<Vec<Catalog>> {
        let mut catalogs: Vec<Catalog> = Vec::new();

        for catalog_id in &partition.catalogs {
            match self.get_catalog(partition.topic_id, partition.partition_id, *catalog_id) {
                Ok(catalog) => catalogs.push(catalog),
                Err(e) => return Err(e),
            };
        }

        Ok(catalogs)
    }

    pub fn get_subscriptions(self: &Self, topic: &Topic) -> ReadResult<Vec<Subscription>> {
        let mut subscriptions: Vec<Subscription> = Vec::new();

        for subscription_id in &topic.subscriptions {
            match self.get_subscription(topic.topic_id, *subscription_id) {
                Ok(subscription) => subscriptions.push(subscription),
                Err(e) => return Err(e),
            };
        }

        Ok(subscriptions)
    }

    pub fn add_node(self: &Self, ip_address: &str) -> AddResult<Node> {
        let mut node_id: NodeId = 0;

        self.update_cluster(|cluster| {
            node_id = cluster.next_node_id;
            cluster.next_node_id += 1;
            cluster.nodes.push(node_id);
        })?;

        let mut node = Node::new(node_id, ip_address);
        match self.persistence.save(&mut node) {
            Ok(_) => AddResult::Ok(node),
            Err(e) => match e {
                SaveError::Error { msg } => AddResult::Err(DataErr::PersistenceFailure {
                    msg: msg + " saving the new node",
                }),
                SaveError::VersionMissmatch => {
                    panic!("Impossible version mismatch saving new node record")
                }
            },
        }
    }

    pub fn add_topic(self: &Self, name: &str) -> AddResult<Topic> {
        let mut topic_id: TopicId = 0;

        self.update_cluster(|cluster| {
            topic_id = cluster.next_topic_id;
            cluster.next_topic_id += 1;
            cluster.topics.push(topic_id);
        })?;

        let mut topic = Topic::new(topic_id, name.to_owned(), Vec::new(), Vec::new(), 1, 1);
        match self.persistence.save(&mut topic) {
            Ok(_) => AddResult::Ok(topic),
            Err(e) => match e {
                SaveError::Error { msg } => {
                    return AddResult::Err(DataErr::PersistenceFailure {
                        msg: msg + " saving the new topic",
                    })
                }
                SaveError::VersionMissmatch => {
                    panic!("Impossible version mismatch saving new topic record")
                }
            },
        }
    }

    pub fn add_partition(self: &Self, topic_id: TopicId) -> AddResult<Partition> {
        let mut partition_id: PartitionId = 0;

        self.update_topic(topic_id, |topic| {
            partition_id = topic.next_partition_id;
            topic.next_partition_id += 1;
            topic.partitions.push(partition_id);
        })?;

        let mut partition = Partition::new(topic_id, partition_id, Vec::new(), 1);
        match self.persistence.save(&mut partition) {
            Ok(_) => AddResult::Ok(partition),
            Err(e) => match e {
                SaveError::Error { msg } => AddResult::Err(DataErr::PersistenceFailure {
                    msg: format!("{msg} saving the new partition in topic {topic_id}"),
                }),
                SaveError::VersionMissmatch => {
                    panic!("Impossible version mismatch saving new partition record")
                }
            },
        }
    }

    pub fn add_catalog(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        node_id: NodeId,
    ) -> AddResult<Catalog> {
        let mut catalog_id: CatalogId = 0;

        self.update_partition(topic_id, partition_id, |partition| {
            catalog_id = partition.next_catalog_id;
            partition.next_catalog_id += 1;
            partition.catalogs.push(catalog_id);
        })?;

        let mut catalog = Catalog::new(topic_id, partition_id, catalog_id, node_id);
        match self.persistence.save(&mut catalog) {
            Ok(_) => AddResult::Ok(catalog),
            Err(e) => match e {
                SaveError::Error { msg } => AddResult::Err(DataErr::PersistenceFailure {
                    msg: msg + " saving the new catalog",
                }),
                SaveError::VersionMissmatch => {
                    panic!("Impossible version mismatch saving new catalog record")
                }
            },
        }
    }

    pub fn add_subscription(self: &Self, topic_id: TopicId, name: &str) -> AddResult<Subscription> {
        let mut subscription_id: SubscriptionId = 0;

        self.update_topic(topic_id, |topic| {
            subscription_id = topic.next_subscription_id;
            topic.next_subscription_id += 1;
            topic.subscriptions.push(subscription_id);
        })?;

        let mut subscription = Subscription::new(topic_id, subscription_id, name.to_owned());
        match self.persistence.save(&mut subscription) {
            Ok(_) => AddResult::Ok(subscription),
            Err(e) => match e {
                SaveError::Error { msg } => AddResult::Err(DataErr::PersistenceFailure {
                    msg: msg + " saving the new subscription",
                }),
                SaveError::VersionMissmatch => {
                    panic!("Impossible version mismatch saving new subscription record")
                }
            },
        }
    }

    pub fn delete_node(self: &Self, node_id: NodeId) -> UpdateResult<()> {
        self.update_cluster(|cluster| cluster.nodes.retain(|&id| id != node_id))?;

        match self.persistence.delete(&Node::key(node_id)) {
            Ok(_) => UpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => UpdateResult::Err(DataErr::PersistenceFailure {
                    msg: format!("{msg} deleting node {node_id}"),
                }),
                DeleteError::NotFound { .. } => UpdateResult::Ok(()),
            },
        }
    }

    pub fn delete_topic(self: &Self, topic_id: TopicId) -> UpdateResult<()> {
        let topic = self.get_topic(topic_id)?;

        for subscription_id in topic.subscriptions {
            self.delete_subscription(topic_id, subscription_id)?
        }

        for partition_id in topic.partitions {
            self.delete_partition(topic_id, partition_id)?
        }

        self.update_cluster(|cluster| cluster.topics.retain(|&id| id != topic_id))?;

        match self.persistence.delete(&Topic::key(topic_id)) {
            Ok(_) => UpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => UpdateResult::Err(DataErr::PersistenceFailure {
                    msg: format!("{msg} deleting topic {topic_id}"),
                }),
                DeleteError::NotFound { .. } => UpdateResult::Ok(()),
            },
        }
    }

    pub fn delete_subscription(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
    ) -> UpdateResult<()> {
        self.update_topic(topic_id, |topic| {
            topic.subscriptions.retain(|&id| id != subscription_id)
        })?;

        match self
            .persistence
            .delete(&Subscription::key(topic_id, subscription_id))
        {
            Ok(_) => UpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => UpdateResult::Err(DataErr::PersistenceFailure {
                    msg: format!(
                        "{msg} deleting subscription {subscription_id} from topic {topic_id}"
                    ),
                }),
                DeleteError::NotFound { .. } => UpdateResult::Ok(()),
            },
        }
    }

    pub fn delete_partition(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> UpdateResult<()> {
        let partition = self.get_partition(topic_id, partition_id)?;

        for catalog_id in partition.catalogs {
            self.delete_catalog(topic_id, partition_id, catalog_id)?
        }

        self.update_topic(topic_id, |topic| {
            topic.partitions.retain(|&id| id != partition_id)
        })?;

        match self
            .persistence
            .delete(&Partition::key(topic_id, partition_id))
        {
            Ok(_) => UpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => UpdateResult::Err(DataErr::PersistenceFailure {
                    msg: format!("{msg} deleting partition {partition_id} from topic {topic_id}"),
                }),
                DeleteError::NotFound { .. } => UpdateResult::Ok(()),
            },
        }
    }

    pub fn delete_catalog(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> UpdateResult<()> {
        self.update_partition(topic_id, partition_id, |partition| {
            partition.catalogs.retain(|&id| id != catalog_id)
        })?;

        match self.persistence.delete(&Catalog::key(
            topic_id,
            partition_id,
            catalog_id,
        )) {
            Ok(_) => UpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => {
                    UpdateResult::Err(DataErr::PersistenceFailure {
                        msg: format!("{msg} deleting catalog {catalog_id} in partition {partition_id} from topic {topic_id}"),
                    })
                }
                DeleteError::NotFound { .. } => UpdateResult::Ok(()),
            },
        }
    }

    pub fn update_cluster<F>(self: &Self, mut update: F) -> UpdateResult<Cluster>
    where
        F: FnMut(&mut Cluster),
    {
        loop {
            let mut cluster = self.get_cluster()?;

            update(&mut cluster);

            match self.persistence.save(&mut cluster) {
                Ok(_) => return UpdateResult::Ok(cluster),
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return UpdateResult::Err(DataErr::PersistenceFailure {
                            msg: format!("{msg} updating the cluster"),
                        })
                    }
                    SaveError::VersionMissmatch => continue,
                },
            }
        }
    }

    pub fn update_topic<F>(self: &Self, topic_id: TopicId, mut update: F) -> UpdateResult<Topic>
    where
        F: FnMut(&mut Topic),
    {
        loop {
            let mut topic = self.get_topic(topic_id)?;

            update(&mut topic);

            match self.persistence.save(&mut topic) {
                Ok(_) => return UpdateResult::Ok(topic),
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return UpdateResult::Err(DataErr::PersistenceFailure {
                            msg: format!("{msg} updating topic {topic_id}"),
                        })
                    }
                    SaveError::VersionMissmatch => continue,
                },
            }
        }
    }

    pub fn update_node<F>(self: &Self, node_id: NodeId, mut update: F) -> UpdateResult<Node>
    where
        F: FnMut(&mut Node),
    {
        loop {
            let mut node = self.get_node(node_id)?;

            update(&mut node);

            match self.persistence.save(&mut node) {
                Ok(_) => return UpdateResult::Ok(node),
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return UpdateResult::Err(DataErr::PersistenceFailure {
                            msg: format!("{msg} updating node {node_id}"),
                        })
                    }
                    SaveError::VersionMissmatch => continue,
                },
            }
        }
    }

    pub fn update_partition<F>(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        mut update: F,
    ) -> UpdateResult<Partition>
    where
        F: FnMut(&mut Partition),
    {
        loop {
            let mut partition = self.get_partition(topic_id, partition_id)?;

            update(&mut partition);

            match self.persistence.save(&mut partition) {
                Ok(_) => return UpdateResult::Ok(partition),
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return UpdateResult::Err(DataErr::PersistenceFailure {
                            msg: format!(
                                "{msg} updating partition {partition_id} in topic {topic_id}"
                            ),
                        })
                    }
                    SaveError::VersionMissmatch => continue,
                },
            }
        }
    }

    pub fn update_catalog<F>(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
        mut update: F,
    ) -> UpdateResult<Catalog>
    where
        F: FnMut(&mut Catalog),
    {
        loop {
            let mut catalog = self.get_catalog(topic_id, partition_id, catalog_id)?;

            update(&mut catalog);

            match self.persistence.save(&mut catalog) {
                Ok(_) => return UpdateResult::Ok(catalog),
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return UpdateResult::Err(DataErr::PersistenceFailure {
                            msg: format!("{msg} updating catalog {catalog_id} in partition {partition_id} in topic {topic_id}"),
                        })
                    }
                    SaveError::VersionMissmatch => continue,
                },
            }
        }
    }

    fn get_entity<'b, T: Deserialize<'b>>(self: &Self, key: &impl Keyed) -> ReadResult<T> {
        match self.persistence.load::<T>(key) {
            Ok(entity) => ReadResult::Ok(entity),
            Err(e) => match e {
                LoadError::Error { msg } => ReadResult::Err(DataErr::PersistenceFailure { msg }),
                LoadError::NotFound {
                    entity_type,
                    entity_key,
                } => ReadResult::Err(DataErr::PersistenceFailure {
                    msg: format!(
                        "{} entity with id={} was not found",
                        entity_type, entity_key
                    ),
                }),
            },
        }
    }
}
