/*
Provides read-only stats on the internal state of the broker.
*/

use crate::model::{
    cluster::{Cluster, ClusterStats},
    ledger::LedgerStats,
    partition::PartitionStats,
    topic::TopicStats,
};
use pulsar_rust_net::data_types::{LedgerId, PartitionId, TopicId};
use std::sync::Arc;

pub struct StatsService {
    cluster: Arc<Cluster>,
}

impl StatsService {
    pub fn new(cluster: &Arc<Cluster>) -> Self {
        Self {
            cluster: Arc::clone(cluster),
        }
    }

    pub fn ledger(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
        ledger_id: LedgerId,
    ) -> Option<LedgerStats> {
        Some(
            self.cluster
                .topics()
                .get(&topic_id)?
                .partitions()
                .get(&partition_id)?
                .ledgers()
                .get(&ledger_id)?
                .stats(),
        )
    }

    pub fn partition(
        self: &Self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<PartitionStats> {
        Some(
            self.cluster
                .topics()
                .get(&topic_id)?
                .partitions()
                .get(&partition_id)?
                .stats(),
        )
    }

    pub fn topic(self: &Self, topic_id: TopicId) -> Option<TopicStats> {
        Some(self.cluster.topics().get(&topic_id)?.stats())
    }

    pub fn cluster(self: &Self) -> Option<ClusterStats> {
        Some(self.cluster.stats())
    }
}
