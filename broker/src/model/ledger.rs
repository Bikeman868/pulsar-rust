use crate::{
    data::DataLayer,
    formatting::plain_text_builder::{PlainTextBuilder, ToPlainText},
    utils::now_epoc_millis,
};
use pulsar_rust_net::data_types::{LedgerId, MessageId, NodeId, PartitionId, Timestamp, TopicId};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::{messages::PublishedMessage, Entity, EntityList, EntityRef};

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Copy, Serialize)]
pub struct LedgerStats {
    pub message_count: usize,
    pub unacked_count: usize,
    pub next_message_id: MessageId,
    pub last_update_timestamp: Timestamp,
}

impl ToPlainText for LedgerStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Messages", 10);
        builder.str_left("Unacked", 10);
        builder.str_left("Next id", 10);
        builder.str_left("Last update", 15);
        builder.new_line();
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.usize_left(self.message_count, 10);
        builder.usize_left(self.unacked_count, 10);
        builder.u32_left(self.next_message_id, 15);
        builder.timestamp_left(self.last_update_timestamp, 10);
        builder.new_line();
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
struct LedgerState {
    messages: HashMap<MessageId, PublishedMessage>,
    stats: LedgerStats,
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Ledger {
    state: RwLock<LedgerState>,
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    node_id: NodeId,
    create_timestamp: Timestamp,
}

impl Entity<LedgerId> for Ledger {
    fn key(self: &Self) -> LedgerId {
        self.ledger_id
    }
}

pub type LedgerRef = EntityRef<LedgerId, Ledger>;
pub type LedgerList = EntityList<LedgerId, Ledger>;

impl Ledger {
    pub fn topic_id(self: &Self) -> TopicId {
        self.topic_id
    }
    pub fn partition_id(self: &Self) -> PartitionId {
        self.partition_id
    }
    pub fn ledger_id(self: &Self) -> LedgerId {
        self.ledger_id
    }
    pub fn node_id(self: &Self) -> NodeId {
        self.node_id
    }
    pub fn next_message_id(self: &Self) -> MessageId {
        self.state.read().unwrap().stats.next_message_id
    }
    pub fn create_timestamp(self: &Self) -> Timestamp {
        self.create_timestamp
    }
    pub fn message_count(self: &Self) -> usize {
        self.state.read().unwrap().stats.message_count
    }
    pub fn last_update_timestamp(self: &Self) -> Timestamp {
        self.state.read().unwrap().stats.last_update_timestamp
    }

    pub fn peek_message(self: &Self, message_id: MessageId) -> Option<PublishedMessage> {
        Some(
            self.state
                .read()
                .unwrap()
                .messages
                .get(&message_id)?
                .clone(),
        )
    }

    pub fn all_message_ids(self: &Self) -> Vec<MessageId> {
        self.state
            .read()
            .unwrap()
            .messages
            .keys()
            .map(|k| *k)
            .collect()
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
        let messages: HashMap<MessageId, PublishedMessage> = HashMap::with_capacity(10000);
        let timestamp = now_epoc_millis();

        let stats = LedgerStats {
            message_count: 0,
            unacked_count: 0,
            next_message_id,
            last_update_timestamp: timestamp,
        };

        Self {
            state: RwLock::new(LedgerState { messages, stats }),
            topic_id,
            partition_id,
            ledger_id,
            node_id,
            create_timestamp: timestamp,
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}

    pub fn stats(self: &Self) -> LedgerStats {
        self.state.read().unwrap().stats
    }

    pub fn allocate_message_id(self: &Self) -> Option<MessageId> {
        let state: &mut LedgerState = &mut *self.state.write().unwrap();
        if state.stats.next_message_id == 0 {
            return None;
        }

        let id = state.stats.next_message_id;
        state.stats.next_message_id = if id == MessageId::MAX { 0 } else { id + 1 };
        state.stats.last_update_timestamp = now_epoc_millis();

        Some(id)
    }

    pub fn publish_message(self: &Self, message: PublishedMessage) {
        let state: &mut LedgerState = &mut *self.state.write().unwrap();
        state.stats.message_count += 1;
        state.stats.unacked_count += message.subscriber_count;
        state.stats.last_update_timestamp = now_epoc_millis();
        state
            .messages
            .insert(message.message_ref.message_id, message);
    }

    pub fn get_message(self: &Self, message_id: &MessageId) -> Option<PublishedMessage> {
        let state = self.state.read().unwrap();
        Some(PublishedMessage::clone(state.messages.get(message_id)?))
    }

    pub fn ack(self: &Self, message_id: &MessageId) {
        let state: &mut LedgerState = &mut *self.state.write().unwrap();
        let messages = &mut state.messages;

        match messages.get_mut(message_id) {
            Some(message) => {
                message.ack_count += 1;
                state.stats.unacked_count -= 1;
                if message.ack_count == message.subscriber_count {
                    messages.remove(message_id);
                    state.stats.message_count -= 1;
                }
                state.stats.last_update_timestamp = now_epoc_millis();
            }
            // TODO: Log a warning, this should not happen because acked messages are removed from the subscription
            // and this struct should never see double acks.
            None => (),
        }
    }
}
