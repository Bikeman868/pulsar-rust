use serde::{Deserialize, Serialize};

use crate::{
    model::{
        data_types::{ConsumerId, SubscriptionId},
        MessageRef,
    },
    persistence::Keyed,
};

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

impl Ack {
    pub fn new(
        message_ref: MessageRef,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> Self {
        Ack {
            message_ref,
            subscription_id,
            consumer_id,
        }
    }
}

impl Nack {
    pub fn new(
        message_ref: MessageRef,
        subscription_id: SubscriptionId,
        consumer_id: ConsumerId,
    ) -> Self {
        Nack {
            message_ref,
            subscription_id,
            consumer_id,
        }
    }
}

impl Publish {
    pub fn new(message_ref: MessageRef) -> Self {
        Publish { message_ref }
    }
}

impl Keyed for Ack {
    fn type_name(self: &Self) -> &'static str {
        "Ack"
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}

impl Keyed for Nack {
    fn type_name(self: &Self) -> &'static str {
        "Nack"
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}

impl Keyed for Publish {
    fn type_name(self: &Self) -> &'static str {
        "Publish"
    }
    fn key(self: &Self) -> String {
        self.message_ref.to_key()
    }
}
