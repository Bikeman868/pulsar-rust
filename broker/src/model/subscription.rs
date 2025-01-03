pub mod key_shared;
pub mod shared;

use crate::formatting::plain_text_builder::{PlainTextBuilder, ToPlainText};

use super::{messages::SubscribedMessage, Entity, EntityList, EntityRef};
use pulsar_rust_net::data_types::{ConsumerId, SubscriptionId, TopicId};
use serde::Serialize;

pub enum Subscription {
    Shared(shared::Subscription),
    KeyShared(key_shared::Subscription),
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone, Serialize)]
pub struct SubscriptionStats {
    queued_count: usize,
    unacked_count: usize,
    assigned_count: usize,
    affinity_count: usize,
}

impl ToPlainText for SubscriptionStats {
    fn to_plain_text_header(builder: &mut PlainTextBuilder) {
        builder.str_left("Queued", 10);
        builder.str_left("Unacked", 10);
        builder.str_left("Assigned", 10);
        builder.str_left("Affinity", 10);
        builder.new_line();
    }

    fn to_plain_text(self: &Self, builder: &mut PlainTextBuilder) {
        builder.usize_left(self.queued_count, 10);
        builder.usize_left(self.unacked_count, 10);
        builder.usize_left(self.assigned_count, 10);
        builder.usize_left(self.affinity_count, 10);
        builder.new_line();
    }
}

impl Entity<SubscriptionId> for Subscription {
    fn key(self: &Self) -> SubscriptionId {
        self.subscription_id()
    }
}

pub type SubscriptionRef = EntityRef<SubscriptionId, Subscription>;
pub type SubscriptionList = EntityList<SubscriptionId, Subscription>;

impl Subscription {
    pub fn topic_id(self: &Self) -> TopicId {
        match self {
            Subscription::Shared(subscription) => subscription.topic_id(),
            Subscription::KeyShared(subscription) => subscription.topic_id(),
        }
    }

    pub fn subscription_id(self: &Self) -> SubscriptionId {
        match self {
            Subscription::Shared(subscription) => subscription.subscription_id(),
            Subscription::KeyShared(subscription) => subscription.subscription_id(),
        }
    }

    pub fn name(self: &Self) -> String {
        match self {
            Subscription::Shared(subscription) => subscription.name(),
            Subscription::KeyShared(subscription) => subscription.name(),
        }
    }

    pub fn push(self: &Self, message: SubscribedMessage) {
        match self {
            Subscription::Shared(subscription) => subscription.push(message),
            Subscription::KeyShared(subscription) => subscription.push(message),
        }
    }

    pub fn pop(self: &Self, consumer_id: ConsumerId) -> Option<SubscribedMessage> {
        match self {
            Subscription::Shared(subscription) => subscription.pop(consumer_id),
            Subscription::KeyShared(subscription) => subscription.pop(consumer_id),
        }
    }

    pub fn connect_consumer(self: &Self) -> Option<ConsumerId> {
        match self {
            Subscription::Shared(subscription) => subscription.connect_consumer(),
            Subscription::KeyShared(subscription) => subscription.connect_consumer(),
        }
    }

    pub fn disconnect_consumer(self: &Self, consumer_id: ConsumerId) {
        match self {
            Subscription::Shared(subscription) => subscription.disconnect_consumer(consumer_id),
            Subscription::KeyShared(subscription) => subscription.disconnect_consumer(consumer_id),
        }
    }

    pub fn ack(self: &Self, consumer_id: ConsumerId, message_ref_key: &str) -> bool {
        match self {
            Subscription::Shared(subscription) => subscription.ack(consumer_id, message_ref_key),
            Subscription::KeyShared(subscription) => subscription.ack(consumer_id, message_ref_key),
        }
    }

    pub fn nack(self: &Self, consumer_id: ConsumerId, message_ref_key: &str) -> bool {
        match self {
            Subscription::Shared(subscription) => subscription.nack(consumer_id, message_ref_key),
            Subscription::KeyShared(subscription) => {
                subscription.nack(consumer_id, message_ref_key)
            }
        }
    }

    pub fn stats(self: &Self) -> SubscriptionStats {
        match self {
            Subscription::Shared(subscription) => subscription.stats(),
            Subscription::KeyShared(subscription) => subscription.stats(),
        }
    }
}
