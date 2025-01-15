/*
Version 1 data contracts for serializing request body
*/

use crate::data_types::{
    ConsumerId, ContractVersionNumber, MessageCount, PartitionId, SubscriptionId, Timestamp,
    TopicId,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Publish {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub key: String,
    pub timestamp: Option<Timestamp>,
    pub attributes: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Consume {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: Option<ConsumerId>,
    pub max_messages: MessageCount,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Ack {
    pub message_ref_key: String,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Nack {
    pub message_ref_key: String,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct NegotiateVersion {
    pub min_version: ContractVersionNumber,
    pub max_version: ContractVersionNumber,
}
