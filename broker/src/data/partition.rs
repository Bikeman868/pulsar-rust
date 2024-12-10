use pulsar_rust_net::data_types::{NodeId, PartitionId, TopicId};
use crate::persistence::{
    entity_persister::{DeleteError, SaveError},
    persisted_entities::{Topic, Partition},
};
use super::{DataAddError, DataAddResult, DataLayer, DataReadError, DataReadResult, DataUpdateError, DataUpdateResult};

impl DataLayer {
    pub fn get_partition(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> DataReadResult<Partition> {
        self.get_entity(&Partition::key(topic_id, partition_id))
    }

    pub fn get_partitions(self: &Self, topic: &Topic) -> DataReadResult<Vec<Partition>> {
        let mut partitions: Vec<Partition> = Vec::new();
        for partition_id in &topic.partition_ids {
            partitions.push(self.get_partition(topic.topic_id, *partition_id)?);
        }
        Ok(partitions)
    }

    pub fn add_partition(self: &Self, topic_id: TopicId, node_id: NodeId) -> DataAddResult<Partition> {
        let mut partition_id: PartitionId = 0;

        if let Err(err) = self.update_topic(topic_id, |topic| {
            partition_id = topic.next_partition_id;
            topic.next_partition_id += 1;
            topic.partition_ids.push(partition_id);
            true
        }) {
            return Err(DataAddError::PersistenceFailure { msg: (format!("Failed to update topic. {:?}", err)) });
        }

        let mut partition = Partition::new(topic_id, partition_id, Vec::new(), 1, node_id);
        match self.persistence.save(&mut partition) {
            Ok(_) => DataAddResult::Ok(partition),
            Err(e) => match e {
                SaveError::Error { msg } => DataAddResult::Err(DataAddError::PersistenceFailure {
                    msg: format!("{msg} saving the new partition in topic {topic_id}"),
                }),
                SaveError::VersionMissmatch => {
                    panic!("Version mismatch saving new partition record")
                }
                SaveError::Unmodified => {
                    panic!("Update closure always returns true")
                }
            },
        }
    }

    pub fn delete_partition(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> DataUpdateResult<()> {
        let partition = match self.get_partition(topic_id, partition_id) {
            Ok(partition) => partition,
            Err(err) => return match err {
                DataReadError::PersistenceFailure { msg } => Err(DataUpdateError::PersistenceFailure { msg }),
                DataReadError::NotFound => Err(DataUpdateError::NotFound),
            }
        };

        for ledger_id in partition.ledger_ids {
            self.delete_ledger(topic_id, partition_id, ledger_id)?
        }

        self.update_topic(topic_id, |topic| {
            topic.partition_ids.retain(|&id| id != partition_id);
            true
        })?;

        match self
            .persistence
            .delete(&Partition::key(topic_id, partition_id))
        {
            Ok(_) => DataUpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                    msg: format!("{msg} deleting partition {partition_id} from topic {topic_id}"),
                }),
                DeleteError::NotFound { .. } => DataUpdateResult::Ok(()),
            },
        }
    }

    pub fn update_partition<F>(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        mut update: F,
    ) -> DataUpdateResult<Partition>
    where
        F: FnMut(&mut Partition) -> bool,
    {
        loop {
            let mut partition = match self.get_partition(topic_id, partition_id) {
                Ok(partition) => partition,
                Err(err) => return match err {
                    DataReadError::PersistenceFailure { msg } => Err(DataUpdateError::PersistenceFailure { msg }),
                    DataReadError::NotFound => Err(DataUpdateError::NotFound),
                }
            };

            if !update(&mut partition) { return Err(DataUpdateError::Unmodified) }

            match self.persistence.save(&mut partition) {
                Ok(_) => return DataUpdateResult::Ok(partition),
                Err(e) => match e {
                    SaveError::Unmodified => return DataUpdateResult::Ok(partition),
                    SaveError::VersionMissmatch => continue,
                    SaveError::Error { msg } => {
                        return DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                            msg: format!(
                                "{msg} updating partition {partition_id} in topic {topic_id}"
                            ),
                        })
                    }
                },
            }
        }
    }
}