use super::{
    ledger::{Ledger, LedgerList, LedgerRef, LedgerStats},
    Entity, EntityList, EntityRef,
};
use crate::{
    data::{DataAddResult, DataLayer},
    formatting::plain_text_builder::{PlainTextBuilder, ToPlainText},
};
use pulsar_rust_net::data_types::{LedgerId, NodeId, PartitionId, TopicId};
use serde::Serialize;
use std::sync::Arc;

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Copy, Serialize)]
pub struct PartitionLedgerStats {
    pub ledger_id: LedgerId,
    pub ledger_stats: LedgerStats,
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Serialize)]
pub struct PartitionStats {
    pub node_id: NodeId,
    pub ledgers: Vec<PartitionLedgerStats>,
}

impl ToPlainText for PartitionStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Node id", 15);
        builder.new_line();
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.u16_left(self.node_id, 15);
        builder.new_line();

        builder.indent();
        if self.ledgers.len() > 0 {
            PartitionLedgerStats::to_plain_text_header(builder);
            for ledger in &self.ledgers {
                ledger.to_plain_text(builder);
            }
        }
        builder.outdent();
    }
}

impl ToPlainText for PartitionLedgerStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Ledger id", 10);
        LedgerStats::to_plain_text_header(builder);
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.u32_left(self.ledger_id, 10);
        self.ledger_stats.to_plain_text(builder);
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Partition {
    current_ledger_id: LedgerId,
    node_id: NodeId,
    topic_id: TopicId,
    partition_id: PartitionId,
    ledgers: LedgerList,
}

impl Entity<PartitionId> for Partition {
    fn key(self: &Self) -> PartitionId {
        self.partition_id
    }
}

pub type PartitionRef = EntityRef<PartitionId, Partition>;
pub type PartitionList = EntityList<PartitionId, Partition>;

impl Partition {
    pub fn node_id(self: &Self) -> NodeId {
        self.node_id
    }
    pub fn topic_id(self: &Self) -> TopicId {
        self.topic_id
    }
    pub fn partition_id(self: &Self) -> PartitionId {
        self.partition_id
    }
    pub fn ledgers(self: &Self) -> &LedgerList {
        &self.ledgers
    }

    pub fn new(data_layer: &Arc<DataLayer>, topic_id: TopicId, partition_id: PartitionId) -> Self {
        let partition = data_layer.get_partition(topic_id, partition_id).unwrap();
        let node_id = partition.node_id;

        if let Some(current_ledger_id) = data_layer.get_last_ledger_id(&partition) {
            let ledgers =
                EntityList::from_iter(partition.ledger_ids.iter().map(|&ledger_id| {
                    Ledger::new(data_layer, topic_id, partition_id, ledger_id, 1)
                }));

            Self {
                topic_id,
                partition_id,
                ledgers,
                current_ledger_id,
                node_id,
            }
        } else {
            panic!("No current ledger for partition {partition_id}")
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}

    pub fn stats(self: &Self) -> PartitionStats {
        let ledgers = self
            .ledgers
            .values()
            .iter()
            .map(|ledger_ref| PartitionLedgerStats {
                ledger_id: ledger_ref.ledger_id(),
                ledger_stats: ledger_ref.stats(),
            })
            .collect();
        PartitionStats {
            node_id: self.node_id,
            ledgers,
        }
    }

    pub fn current_ledger(self: &Self, node_id: NodeId) -> Option<LedgerRef> {
        let ledger = self.ledgers.get(&self.current_ledger_id)?;
        if ledger.node_id() == node_id {
            Some(ledger.clone())
        } else {
            None
        }
    }

    pub fn add_ledger(
        self: &Self,
        data_layer: &Arc<DataLayer>,
        node_id: NodeId,
    ) -> DataAddResult<LedgerRef> {
        match data_layer.add_ledger(self.topic_id, self.partition_id, node_id) {
            Ok(ledger) => {
                let ledger_ref = EntityRef::new(Ledger::new(
                    data_layer,
                    self.topic_id,
                    self.partition_id,
                    ledger.ledger_id,
                    1,
                ));
                self.ledgers.insert_ref(ledger_ref.clone());
                Ok(ledger_ref)
            }
            Err(err) => Err(err),
        }
    }
}
