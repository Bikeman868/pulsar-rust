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
pub type UpdateResult = Result<(), DataErr>;

pub struct DataLayer<'a> {
    cluster_name: &'a str,
    persistence: &'a PersistenceLayer,
}

/// Provides functions for adding, retrieving, updating, and deleting entities that are persisted to the database.
/// If two threads (even ones running on different pods) simultaneously read-modify-write the same entity, then
/// one thread will succeed and the other will retry the whole read-modify-write operation. This guarantees that
/// all updates are persisted.
/// 
/// Note that this data layer is designed for storing configuration data only, and is not suitable for high throughput.
/// As originally implemented, we store topics, partitions etc and these things change very rarely. Do not add volatile
/// information to these entities and try to update them thousands of times per second, it won't work.
impl<'a> DataLayer<'a> {
    pub fn new(cluster_name: &'a str, persistence: &'a PersistenceLayer) -> Self {
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
        loop {
            let mut cluster = match self.get_cluster() {
                Ok(c) => c,
                Err(e) => return ReadResult::Err(e),
            };

            let node_id = cluster.next_node_id;
            cluster.next_node_id += 1;
            cluster.nodes.push(node_id);

            match self.persistence.save(&mut cluster) {
                Ok(_) => {
                    let mut node = Node::new(node_id, ip_address);
                    match self.persistence.save(&mut node) {
                        Ok(_) => return AddResult::Ok(node),
                        Err(e) => match e {
                            SaveError::Error { msg } => {
                                return AddResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " saving the new node",
                                })
                            }
                            SaveError::VersionMissmatch => (),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the cluster",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn add_topic(self: &Self, name: &str) -> AddResult<Topic> {
        loop {
            let mut cluster = match self.get_cluster() {
                Ok(c) => c,
                Err(e) => return ReadResult::Err(e),
            };

            let topic_id = cluster.next_topic_id;
            cluster.next_topic_id += 1;
            cluster.topics.push(topic_id);

            match self.persistence.save(&mut cluster) {
                Ok(_) => {
                    let mut topic =
                        Topic::new(topic_id, name.to_owned(), Vec::new(), Vec::new(), 1, 1);
                    match self.persistence.save(&mut topic) {
                        Ok(_) => return AddResult::Ok(topic),
                        Err(e) => match e {
                            SaveError::Error { msg } => {
                                return AddResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " saving the new topic",
                                })
                            }
                            SaveError::VersionMissmatch => (),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the cluster",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn add_partition(self: &Self, topic_id: TopicId) -> AddResult<Partition> {
        loop {
            let mut topic = match self.get_topic(topic_id) {
                Ok(t) => t,
                Err(e) => return AddResult::Err(e),
            };

            let partition_id = topic.next_partition_id;
            topic.next_partition_id += 1;
            topic.partitions.push(partition_id);

            match self.persistence.save(&mut topic) {
                Ok(_) => {
                    let mut partition = Partition::new(topic.topic_id, partition_id, Vec::new(), 1);
                    match self.persistence.save(&mut partition) {
                        Ok(_) => return AddResult::Ok(partition),
                        Err(e) => match e {
                            SaveError::Error { msg } => {
                                return AddResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " saving the new partition",
                                })
                            }
                            SaveError::VersionMissmatch => (),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the topic",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn add_catalog(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        node_id: NodeId,
    ) -> AddResult<Catalog> {
        loop {
            let mut partition = match self.get_partition(topic_id, partition_id) {
                Ok(t) => t,
                Err(e) => return AddResult::Err(e),
            };

            let catalog_id = partition.next_catalog_id;
            partition.next_catalog_id += 1;
            partition.catalogs.push(catalog_id);

            match self.persistence.save(&mut partition) {
                Ok(_) => {
                    let mut catalog = Catalog::new(
                        partition.topic_id,
                        partition.partition_id,
                        catalog_id,
                        node_id,
                    );
                    match self.persistence.save(&mut catalog) {
                        Ok(_) => return AddResult::Ok(catalog),
                        Err(e) => match e {
                            SaveError::Error { msg } => {
                                return AddResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " saving the new catalog",
                                })
                            }
                            SaveError::VersionMissmatch => (),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the partition",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn add_subscription(self: &Self, topic_id: TopicId, name: &str) -> AddResult<Subscription> {
        loop {
            let mut topic = match self.get_topic(topic_id) {
                Ok(t) => t,
                Err(e) => return AddResult::Err(e),
            };

            let subscription_id = topic.next_subscription_id;
            topic.next_subscription_id += 1;
            topic.subscriptions.push(subscription_id);

            match self.persistence.save(&mut topic) {
                Ok(_) => {
                    let mut subscription =
                        Subscription::new(topic.topic_id, subscription_id, name.to_owned());
                    match self.persistence.save(&mut subscription) {
                        Ok(_) => return AddResult::Ok(subscription),
                        Err(e) => match e {
                            SaveError::Error { msg } => {
                                return AddResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " saving the new subscription",
                                })
                            }
                            SaveError::VersionMissmatch => (),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the topic",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn delete_node(self: &Self, node_id: NodeId) -> UpdateResult {
        loop {
            let mut cluster = match self.get_cluster() {
                Ok(c) => c,
                Err(e) => return UpdateResult::Err(e),
            };

            cluster.nodes.retain(|&id| id != node_id);

            match self.persistence.save(&mut cluster) {
                Ok(_) => {
                    return match self.persistence.delete(&Node::key(node_id)) {
                        Ok(_) => UpdateResult::Ok(()),
                        Err(e) => match e {
                            DeleteError::Error { msg } => {
                                UpdateResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " deleting the node",
                                })
                            }
                            DeleteError::NotFound { .. } => UpdateResult::Ok(()),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the cluster",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn delete_topic(self: &Self, topic_id: TopicId) -> UpdateResult {
        loop {
            let topic = match self.get_topic(topic_id) {
                Ok(t) => t,
                Err(e) => return UpdateResult::Err(e),
            };

            for subscription_id in topic.subscriptions {
                match self.delete_subscription(topic_id, subscription_id) {
                    Ok(_) => (),
                    Err(e) => return UpdateResult::Err(e),
                }
            }

            for partition_id in topic.partitions {
                match self.delete_partition(topic_id, partition_id) {
                    Ok(_) => (),
                    Err(e) => return UpdateResult::Err(e),
                }
            }

            let mut cluster = match self.get_cluster() {
                Ok(c) => c,
                Err(e) => return UpdateResult::Err(e),
            };

            cluster.topics.retain(|&id| id != topic_id);

            match self.persistence.save(&mut cluster) {
                Ok(_) => {
                    return match self.persistence.delete(&Topic::key(topic_id)) {
                        Ok(_) => UpdateResult::Ok(()),
                        Err(e) => match e {
                            DeleteError::Error { msg } => {
                                UpdateResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " deleting the topic",
                                })
                            }
                            DeleteError::NotFound { .. } => UpdateResult::Ok(()),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the cluster",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn delete_subscription(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
    ) -> UpdateResult {
        loop {
            let mut topic = match self.get_topic(topic_id) {
                Ok(s) => s,
                Err(e) => return UpdateResult::Err(e),
            };

            topic.subscriptions.retain(|&id| id != subscription_id);

            match self.persistence.save(&mut topic) {
                Ok(_) => {
                    return match self
                        .persistence
                        .delete(&Subscription::key(topic_id, subscription_id))
                    {
                        Ok(_) => UpdateResult::Ok(()),
                        Err(e) => match e {
                            DeleteError::Error { msg } => {
                                UpdateResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " deleting the subscription",
                                })
                            }
                            DeleteError::NotFound { .. } => UpdateResult::Ok(()),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the topic",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn delete_partition(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> UpdateResult {
        loop {
            let partition = match self.get_partition(topic_id, partition_id) {
                Ok(t) => t,
                Err(e) => return UpdateResult::Err(e),
            };

            for catalog_id in partition.catalogs {
                match self.delete_catalog(topic_id, partition_id, catalog_id) {
                    Ok(_) => (),
                    Err(e) => return UpdateResult::Err(e),
                }
            }

            let mut topic = match self.get_topic(topic_id) {
                Ok(t) => t,
                Err(e) => return UpdateResult::Err(e),
            };

            topic.partitions.retain(|&id| id != partition_id);

            match self.persistence.save(&mut topic) {
                Ok(_) => {
                    return match self
                        .persistence
                        .delete(&Partition::key(topic_id, partition_id))
                    {
                        Ok(_) => UpdateResult::Ok(()),
                        Err(e) => match e {
                            DeleteError::Error { msg } => {
                                UpdateResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " deleting the partition",
                                })
                            }
                            DeleteError::NotFound { .. } => UpdateResult::Ok(()),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the topic",
                        })
                    }
                    SaveError::VersionMissmatch => (),
                },
            }
        }
    }

    pub fn delete_catalog(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        catalog_id: CatalogId,
    ) -> UpdateResult {
        loop {
            let mut partition = match self.get_partition(topic_id, partition_id) {
                Ok(p) => p,
                Err(e) => return UpdateResult::Err(e),
            };
            partition.catalogs.retain(|&id| id != catalog_id);

            match self.persistence.save(&mut partition) {
                Ok(_) => {
                    return match self.persistence.delete(&Catalog::key(
                        topic_id,
                        partition_id,
                        catalog_id,
                    )) {
                        Ok(_) => UpdateResult::Ok(()),
                        Err(e) => match e {
                            DeleteError::Error { msg } => {
                                UpdateResult::Err(DataErr::PersistenceFailure {
                                    msg: msg + " deleting the catalog",
                                })
                            }
                            DeleteError::NotFound { .. } => UpdateResult::Ok(()),
                        },
                    }
                }
                Err(e) => match e {
                    SaveError::Error { msg } => {
                        return AddResult::Err(DataErr::PersistenceFailure {
                            msg: msg + " updating the partition",
                        })
                    }
                    SaveError::VersionMissmatch => (),
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
