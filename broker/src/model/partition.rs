use std::{
    collections::{hash_map, HashMap}, 
    sync::Arc
};
use pulsar_rust_net::data_types::{
    LedgerId, NodeId, PartitionId, TopicId,
};
use crate::data::{DataAddResult, DataLayer};
use super::ledger::{Ledger, LedgerList};

#[derive(Debug)]
pub struct Partition {
    current_ledger_id: LedgerId,
    node_id: NodeId,
    topic_id: TopicId,
    partition_id: PartitionId,
    ledgers: Arc<LedgerList>,
}

impl Partition {
    pub fn node_id(self: &Self) -> NodeId { self.node_id }
    pub fn topic_id(self: &Self) -> TopicId { self.topic_id }
    pub fn partition_id(self: &Self) -> PartitionId { self.partition_id }
    pub fn ledgers(self: &Self) -> &Arc<LedgerList> { &self.ledgers }

    pub fn new(data_layer: &Arc<DataLayer>, topic_id: TopicId, partition_id: PartitionId) -> Self {
        let partition = data_layer.get_partition(topic_id, partition_id).unwrap();
        let node_id = partition.node_id;

        if let Some(current_ledger_id) = data_layer.get_last_ledger_id(&partition) {

            let ledgers = Arc::new(LedgerList::new(partition
                .ledger_ids
                .iter()
                .map(|&ledger_id| Ledger::new(data_layer, topic_id, partition_id, ledger_id, 1))));

            Self {
                topic_id,
                partition_id,
                ledgers,
                current_ledger_id,
                node_id,
            }
        } else { panic!("No current ledger for partition {partition_id}") }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}

    pub fn current_ledger(self: &Self, node_id: NodeId) -> Option<&Ledger> { 
        let ledger = self.ledgers.by_id(self.current_ledger_id)?;
        if ledger.node_id() == node_id { Some(ledger) } else { None }
    }

    pub fn add_ledger(self: &Self, _data_layer: &Arc<DataLayer>, _node_id: NodeId) -> DataAddResult<&Ledger> {
        todo!("Add ledgers to a partition")
        // match data_layer.add_ledger(self.topic_id, self.partition_id, node_id) {
        //     Ok(ledger) => {
        //         let new_ledger = Ledger::new(data_layer, self.topic_id, self.partition_id, ledger.ledger_id, 1);
        //         Ok(&new_ledger)
        //     }
        //     Err(err) => Err(err),
        // }
    }

}

/// Represents the list of partitions for a topic
#[derive(Debug)]
pub struct PartitionList {
    hash_by_partition_id: HashMap<PartitionId, Partition>,
}

impl PartitionList {
    pub fn new (partitions: impl Iterator<Item = Partition>) -> Self{
        Self { hash_by_partition_id: partitions.map(|p|(p.partition_id, p)).collect() }
    }

    pub fn by_id(self: &Self, partition_id: PartitionId) -> Option<&Partition> {
        Some(self.hash_by_partition_id.get(&partition_id)?)
    }

    pub fn iter(self: &Self) -> hash_map::Values<'_, PartitionId, Partition>  {
        self.hash_by_partition_id.values()
    }
}
