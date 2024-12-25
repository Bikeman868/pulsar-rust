use super::{
    DataAddError, DataAddResult, DataLayer, DataReadError, DataReadResult, DataUpdateError,
    DataUpdateResult,
};
use crate::persistence::{
    entity_persister::{DeleteError, SaveError},
    persisted_entities::Node,
};
use pulsar_rust_net::data_types::{NodeId, PortNumber};

impl DataLayer {
    pub fn get_node(self: &Self, node_id: NodeId) -> DataReadResult<Node> {
        self.get_entity(&Node::key(node_id))
    }

    pub fn get_nodes(self: &Self) -> DataReadResult<Vec<Node>> {
        let cluster = match self.get_cluster() {
            Ok(c) => c,
            Err(e) => return DataReadResult::Err(e),
        };

        let mut nodes: Vec<Node> = Vec::new();
        for node_id in cluster.node_ids {
            nodes.push(self.get_node(node_id)?);
        }
        Ok(nodes)
    }

    pub fn add_node(
        self: &Self,
        ip_address: &str,
        admin_port: PortNumber,
        pubsub_port: PortNumber,
        sync_port: PortNumber,
    ) -> DataAddResult<Node> {
        let mut node_id: NodeId = 0;

        if let Err(err) = self.update_cluster(|cluster| {
            node_id = cluster.next_node_id;
            cluster.next_node_id += 1;
            cluster.node_ids.push(node_id);
            true
        }) {
            return Err(DataAddError::PersistenceFailure {
                msg: (format!("Failed to update cluster. {:?}", err)),
            });
        }

        let mut node = Node::new(node_id, ip_address, admin_port, pubsub_port, sync_port);
        match self.persistence.save(&mut node) {
            Ok(_) => DataAddResult::Ok(node),
            Err(e) => match e {
                SaveError::Error { msg } => DataAddResult::Err(DataAddError::PersistenceFailure {
                    msg: msg + " saving the new node",
                }),
                SaveError::VersionMissmatch => {
                    panic!("Version mismatch saving new node record")
                }
                SaveError::Unmodified => {
                    panic!("Update closure always returns true")
                }
            },
        }
    }

    pub fn delete_node(self: &Self, node_id: NodeId) -> DataUpdateResult<()> {
        self.update_cluster(|cluster| {
            cluster.node_ids.retain(|&id| id != node_id);
            true
        })?;

        match self.persistence.delete(&Node::key(node_id)) {
            Ok(_) => DataUpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => {
                    DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                        msg: format!("{msg} deleting node {node_id}"),
                    })
                }
                DeleteError::NotFound { .. } => DataUpdateResult::Ok(()),
            },
        }
    }

    pub fn update_node<F>(self: &Self, node_id: NodeId, mut update: F) -> DataUpdateResult<Node>
    where
        F: FnMut(&mut Node) -> bool,
    {
        loop {
            let mut node = match self.get_node(node_id) {
                Ok(node) => node,
                Err(err) => {
                    return match err {
                        DataReadError::PersistenceFailure { msg } => {
                            Err(DataUpdateError::PersistenceFailure { msg })
                        }
                        DataReadError::NotFound => Err(DataUpdateError::NotFound),
                    }
                }
            };

            update(&mut node);

            match self.persistence.save(&mut node) {
                Ok(_) => return DataUpdateResult::Ok(node),
                Err(e) => match e {
                    SaveError::Unmodified => return DataUpdateResult::Ok(node),
                    SaveError::VersionMissmatch => continue,
                    SaveError::Error { msg } => {
                        return DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                            msg: format!("{msg} updating node {node_id}"),
                        })
                    }
                },
            }
        }
    }
}
