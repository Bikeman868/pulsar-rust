use serde::{Deserialize, Serialize};

use super::data_types::{ConsumerId, SubscriptionId};
use super::MessageRef;
use crate::persistence;

#[derive(Debug, Deserialize, Serialize)]
pub struct Ack {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Nack {
    pub message_ref: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Publish {
    pub message_ref: MessageRef,
}

impl persistence::Keyed for Ack {
    fn type_name() -> &'static str {
        "Ack"
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}

impl persistence::Keyed for Nack {
    fn type_name() -> &'static str {
        "Nack"
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}

impl persistence::Keyed for Publish {
    fn type_name() -> &'static str {
        "Publish"
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}
