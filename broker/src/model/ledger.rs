use std::{
    collections::{hash_map, HashMap}, 
    sync::{Arc, RwLock}
};
use pulsar_rust_net::data_types::{
    LedgerId, MessageId, NodeId, PartitionId, TopicId,
};
use crate::data::DataLayer;

#[derive(Debug)]
struct MutableState {
    next_message_id: MessageId,
}

#[derive(Debug)]
pub struct Ledger {
    mutable: RwLock<MutableState>,
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    node_id: NodeId,
}

impl Ledger {
    pub fn topic_id(self: &Self) -> TopicId { self.topic_id }
    pub fn partition_id(self: &Self) -> PartitionId { self.partition_id }
    pub fn ledger_id(self: &Self) -> LedgerId { self.ledger_id }
    pub fn node_id(self: &Self) -> NodeId { self.node_id }
    pub fn next_message_id(self: &Self) -> MessageId { self.mutable.read().unwrap().next_message_id }

    pub fn new(
        data_layer: &Arc<DataLayer>,
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
        next_message_id: MessageId,
    ) -> Self {
        let ledger = data_layer
            .get_ledger(topic_id, partition_id, ledger_id)
            .unwrap();

        let node_id = ledger.node_id;

        Self {
            mutable: RwLock::new(
                MutableState {
                    next_message_id
                }
            ),
            topic_id,
            partition_id,
            ledger_id,
            node_id,
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}

    pub fn allocate_message_id(self: &Self) -> Option<MessageId> {
        let mutable_state: &mut MutableState = &mut *self.mutable.write().unwrap();
        if mutable_state.next_message_id == 0 { return None }

        let id = mutable_state.next_message_id;
        mutable_state.next_message_id = if id == MessageId::MAX { 0 } else { id + 1 };

        Some(id)
    }
}

#[derive(Debug)]
pub struct LedgerList {
    hash_by_ledger_id: HashMap<LedgerId, Ledger>,
}

impl LedgerList {
    pub fn new (ledgers: impl Iterator<Item = Ledger>) -> Self{
        Self { hash_by_ledger_id: ledgers.map(|c|(c.ledger_id, c)).collect() }
    }

    pub fn by_id(self: &Self, ledger_id: LedgerId) -> Option<&Ledger> {
        Some(self.hash_by_ledger_id.get(&ledger_id)?)
    }

    pub fn iter(self: &Self) -> hash_map::Values<'_, LedgerId, Ledger>  {
        self.hash_by_ledger_id.values()
    }
}
