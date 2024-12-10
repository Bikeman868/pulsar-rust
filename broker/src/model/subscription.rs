use std::sync::Arc;
use pulsar_rust_net::data_types::{
    SubscriptionId, TopicId,
};
use crate::data::DataLayer;

#[derive(Debug)]
pub struct Subscription {
}

impl Subscription {
    pub fn new(_data_layer: &DataLayer, _topic_id: TopicId, _subscription_id: SubscriptionId) -> Self {
        Self {
        }
    }

    pub fn refresh(self: &mut Self, _data_layer: &Arc<DataLayer>) {}
}
