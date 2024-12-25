use super::{
    DataAddError, DataAddResult, DataLayer, DataReadError, DataReadResult, DataUpdateError,
    DataUpdateResult,
};
use crate::persistence::{
    entity_persister::{DeleteError, SaveError},
    persisted_entities::{Ledger, Partition},
};
use pulsar_rust_net::data_types::{LedgerId, NodeId, PartitionId, TopicId};

impl DataLayer {
    pub fn get_ledger(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
    ) -> DataReadResult<Ledger> {
        self.get_entity(&Ledger::key(topic_id, partition_id, ledger_id))
    }

    pub fn get_ledgers(self: &Self, partition: &Partition) -> DataReadResult<Vec<Ledger>> {
        let mut ledgers: Vec<Ledger> = Vec::new();
        for ledger_id in &partition.ledger_ids {
            ledgers.push(self.get_ledger(
                partition.topic_id,
                partition.partition_id,
                *ledger_id,
            )?);
        }
        Ok(ledgers)
    }

    pub fn get_last_ledger_id(self: &Self, partition: &Partition) -> Option<LedgerId> {
        match self.add_ledger_if_none(partition.topic_id, partition.partition_id) {
            Ok(ledger) => Some(ledger.ledger_id),
            Err(_) => None,
        }
    }

    pub fn add_ledger(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        node_id: NodeId,
    ) -> DataAddResult<Ledger> {
        let mut ledger_id: LedgerId = 0;

        if let Err(err) = self.update_partition(topic_id, partition_id, |partition| {
            ledger_id = partition.next_ledger_id;
            partition.next_ledger_id += 1;
            partition.ledger_ids.push(ledger_id);
            true
        }) {
            return Err(DataAddError::PersistenceFailure {
                msg: (format!("Failed to update partition. {:?}", err)),
            });
        }

        let mut ledger = Ledger::new(topic_id, partition_id, ledger_id, node_id);
        match self.persistence.save(&mut ledger) {
            Ok(_) => DataAddResult::Ok(ledger),
            Err(e) => match e {
                SaveError::Error { msg } => DataAddResult::Err(DataAddError::PersistenceFailure {
                    msg: msg + " saving the new ledger",
                }),
                SaveError::VersionMissmatch => {
                    panic!("Version mismatch saving new ledger record")
                }
                SaveError::Unmodified => {
                    panic!("Update closure always returns true")
                }
            },
        }
    }

    pub fn add_ledger_if_none(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> DataAddResult<Ledger> {
        let mut ledger_id: LedgerId = 0;
        let mut node_id: NodeId = 0;

        match self.update_partition(topic_id, partition_id, |partition| {
            // Save info from the partition
            ledger_id = partition.next_ledger_id - 1;
            node_id = partition.node_id;

            if partition.ledger_ids.iter().any(|id| *id == ledger_id) {
                // If this ledger already exists, then no update needed
                false
            } else {
                // Allocate the next ledger id within the partition
                ledger_id = partition.next_ledger_id;
                partition.next_ledger_id += 1;
                partition.ledger_ids.push(ledger_id);
                true
            }
        }) {
            Ok(_) => (), // Partition was updated with the new ledger id, we still need to create the ledger
            Err(err) => match err {
                DataUpdateError::Unmodified => {
                    match self.get_ledger(topic_id, partition_id, ledger_id) {
                        Ok(ledger) => return Ok(ledger),
                        Err(err) => {
                            return Err(DataAddError::PersistenceFailure {
                                msg: format!("{:?}", err),
                            })
                        }
                    }
                }
                err => {
                    return Err(DataAddError::PersistenceFailure {
                        msg: format!("{:?}", err),
                    })
                }
            },
        }

        // A new ledger id was allocated, so we need to create the ledger record
        let mut ledger = Ledger::new(topic_id, partition_id, ledger_id, node_id);
        match self.persistence.save(&mut ledger) {
            Ok(_) => DataAddResult::Ok(ledger),
            Err(e) => match e {
                SaveError::Error { msg } => DataAddResult::Err(DataAddError::PersistenceFailure {
                    msg: msg + " saving the new ledger",
                }),
                SaveError::VersionMissmatch => {
                    panic!("Version mismatch saving new ledger record")
                }
                SaveError::Unmodified => {
                    panic!("Update closure always returns true")
                }
            },
        }
    }

    pub fn update_ledger<F>(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
        mut update: F,
    ) -> DataUpdateResult<Ledger>
    where
        F: FnMut(&mut Ledger) -> bool,
    {
        loop {
            let mut ledger = match self.get_ledger(topic_id, partition_id, ledger_id) {
                Ok(ledger) => ledger,
                Err(err) => {
                    return match err {
                        DataReadError::PersistenceFailure { msg } => {
                            Err(DataUpdateError::PersistenceFailure { msg })
                        }
                        DataReadError::NotFound => Err(DataUpdateError::NotFound),
                    }
                }
            };

            update(&mut ledger);

            match self.persistence.save(&mut ledger) {
                Ok(_) => return DataUpdateResult::Ok(ledger),
                Err(e) => match e {
                    SaveError::Unmodified => return DataUpdateResult::Ok(ledger),
                    SaveError::VersionMissmatch => continue,
                    SaveError::Error { msg } => {
                        return DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                            msg: format!("{msg} updating ledger {ledger_id} in partition {partition_id} in topic {topic_id}"),
                        })
                    }
                },
            }
        }
    }

    pub fn delete_ledger(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
    ) -> DataUpdateResult<()> {
        self.update_partition(topic_id, partition_id, |partition| {
            partition.ledger_ids.retain(|&id| id != ledger_id);
            true
        })?;

        match self.persistence.delete(&Ledger::key(
            topic_id,
            partition_id,
            ledger_id,
        )) {
            Ok(_) => DataUpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => {
                    DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                        msg: format!("{msg} deleting ledger {ledger_id} in partition {partition_id} from topic {topic_id}"),
                    })
                }
                DeleteError::NotFound { .. } => DataUpdateResult::Ok(()),
            },
        }
    }
}
