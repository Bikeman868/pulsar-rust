/*
Version 1 data contracts for serializing request body
*/

use crate::data_types::{ConsumerId, PartitionId, SubscriptionId, Timestamp, TopicId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub key: Option<String>,
    pub timestamp: Option<Timestamp>,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Consumer {
    pub topic_id: TopicId,
    pub subscription_id: SubscriptionId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ack {
    pub message_ack_key: String,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Serialize, Deserialize)]
pub struct Nack {
    pub message_ack_key: String,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}
