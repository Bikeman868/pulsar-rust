use serde::{Serialize, Deserialize};

use crate::persistence;
use super::{data_types::SubscriptionId, data_types::ConsumerId, MessageRef};

#[derive(Debug, Deserialize, Serialize)]
pub struct Ack {
    pub id: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Nack {
    pub id: MessageRef,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Publish {
    pub id: MessageRef,
}

impl persistence::Keyed for Ack {
    fn type_name(self: &Self) -> &'static str { "Ack" }
    fn key(self: &Self) -> String { self.id.to_key() }
}

impl persistence::Keyed for Nack {
    fn type_name(self: &Self) -> &'static str { "Nack" }
    fn key(self: &Self) -> String { self.id.to_key() }
}

impl persistence::Keyed for Publish {
    fn type_name(self: &Self) -> &'static str { "Publish" }
    fn key(self: &Self) -> String { self.id.to_key() }
}
