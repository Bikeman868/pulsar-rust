use std::{
    collections::{hash_map, HashMap}, 
    sync::{Arc, RwLock}
};
use pulsar_rust_net::data_types::{
    LedgerId, MessageId, NodeId, PartitionId, Timestamp, TopicId
};
use crate::{data::DataLayer, utils::now_epoc_millis};

use super::messages::Message;

#[derive(Debug)]
struct MutableState {
    next_message_id: MessageId,
    messages: HashMap<MessageId, Message>,
    last_update_timestamp: Timestamp,
}

#[derive(Debug)]
pub struct Ledger {
    mutable: RwLock<MutableState>,
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    node_id: NodeId,
    create_timestamp: Timestamp,
}

impl Ledger {
    pub fn topic_id(self: &Self) -> TopicId { self.topic_id }
    pub fn partition_id(self: &Self) -> PartitionId { self.partition_id }
    pub fn ledger_id(self: &Self) -> LedgerId { self.ledger_id }
    pub fn node_id(self: &Self) -> NodeId { self.node_id }
    pub fn next_message_id(self: &Self) -> MessageId { self.mutable.read().unwrap().next_message_id }
    pub fn create_timestamp(self: &Self) -> Timestamp { self.create_timestamp }
    pub fn message_count(self: &Self) -> usize { self.mutable.read().unwrap().messages.len() }
    pub fn last_update_timestamp(self: &Self) -> Timestamp { self.mutable.read().unwrap().last_update_timestamp }

    pub fn peek_message(self: &Self, message_id: MessageId) -> Option<Message> {
        Some(self.mutable.read().unwrap().messages.get(&message_id)?.clone())
    }

    pub fn all_message_ids(self: &Self) -> Vec<MessageId> {
        self.mutable.read().unwrap().messages.keys().map(|k|*k).collect()
    }

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
        let messages: HashMap<MessageId, Message> = HashMap::with_capacity(10000);
        let timestamp = now_epoc_millis();

        Self {
            mutable: RwLock::new(
                MutableState {
                    next_message_id,
                    messages,
                    last_update_timestamp: timestamp,
                }
            ),
            topic_id,
            partition_id,
            ledger_id,
            node_id,
            create_timestamp: timestamp,
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}

    pub fn allocate_message_id(self: &Self) -> Option<MessageId> {
        let mutable_state: &mut MutableState = &mut *self.mutable.write().unwrap();
        if mutable_state.next_message_id == 0 { return None }

        let id = mutable_state.next_message_id;
        mutable_state.next_message_id = if id == MessageId::MAX { 0 } else { id + 1 };
        mutable_state.last_update_timestamp = now_epoc_millis();

        Some(id)
    }

    pub fn publish_message(self: &Self, message: Message) {
        let mutable_state: &mut MutableState = &mut *self.mutable.write().unwrap();
        mutable_state.messages.insert(message.message_ref.message_id, message);
        mutable_state.last_update_timestamp = now_epoc_millis();
    }

    pub fn get_message(self: &Self, message_id: &MessageId) -> Option<Box<Message>> {
        let mutable_state = self.mutable.read().unwrap();
        Some(Box::new(Message::clone(mutable_state.messages.get(message_id)?)))
    }

    pub fn ack_message(self: &Self, message_id: &MessageId) {
        let mutable_state: &mut MutableState = &mut *self.mutable.write().unwrap();
        let messages = &mut mutable_state.messages;

        match messages.get_mut(message_id) {
            Some(message) => {
                message.ack_count += 1;
                if message.ack_count == message.subscriber_count {
                    messages.remove(message_id);
                }
                mutable_state.last_update_timestamp = now_epoc_millis();
            }
            // TODO: Log a warning, this should not happen because acked messages are removed from the subscription
            // and this struct should never see double acks.
            None => ()
        }
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
