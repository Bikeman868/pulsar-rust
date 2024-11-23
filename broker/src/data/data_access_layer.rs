use serde::Serialize;

use crate::model::database_entities::{Node, Partition, Subscription, Topic};
use crate::model::events::{Ack, Nack, Publish};
use crate::model::{self, data_types};
use crate::persistence::entities::{DeleteResult, EntityPersister, LoadResult, SaveResult};
use crate::persistence::events::{EventPersister, LogResult};
use crate::persistence::{
    build_entity_persister, build_event_persister, Keyed, PersistenceScheme, Versioned,
};

pub struct DataAccessLayer {
    event_persister: EventPersister,
    entity_persister: EntityPersister,
}

impl DataAccessLayer {
    pub fn new(event_persistence: PersistenceScheme, state_persistence: PersistenceScheme) -> Self {
        Self {
            event_persister: build_event_persister(event_persistence),
            entity_persister: build_entity_persister(state_persistence),
        }
    }

    pub fn load_node(entity_persister: &EntityPersister, key: &impl Keyed) -> LoadResult<Node> {
        entity_persister.load(Node::type_name(), key)
    }

    pub fn load_partition(self: &mut Self, key: &impl Keyed) -> LoadResult<Partition> {
        self.entity_persister.load(Partition::type_name(), key)
    }

    pub fn load_subscription(self: &mut Self, key: &impl Keyed) -> LoadResult<Subscription> {
        self.entity_persister.load(Subscription::type_name(), key)
    }

    pub fn load_topic(self: &mut Self, key: &impl Keyed) -> LoadResult<Topic> {
        self.entity_persister.load(Topic::type_name(), key)
    }

    pub fn save<T: Versioned + Keyed + Serialize>(self: &mut Self, entity: &mut T) -> SaveResult {
        self.entity_persister.save(entity)
    }

    pub fn delete_node(self: &mut Self, key: &impl Keyed) -> DeleteResult {
        self.entity_persister.delete(Node::type_name(), key)
    }

    pub fn delete_partition(self: &mut Self, key: &impl Keyed) -> DeleteResult {
        self.entity_persister.delete(Partition::type_name(), key)
    }

    pub fn delete_subscription(self: &mut Self, key: &impl Keyed) -> DeleteResult {
        self.entity_persister.delete(Subscription::type_name(), key)
    }

    pub fn delete_topic(self: &mut Self, key: &impl Keyed) -> DeleteResult {
        self.entity_persister.delete(Topic::type_name(), key)
    }

    pub fn log_publish(
        self: &mut Self,
        timestamp: &data_types::Timestamp,
        message_ref: &model::MessageRef,
    ) -> LogResult {
        let event = Publish {
            message_ref: *message_ref,
        };
        self.event_persister.log(&event, timestamp)
    }

    pub fn log_ack(
        self: &mut Self,
        timestamp: &data_types::Timestamp,
        message_ref: &model::MessageRef,
        subscription_id: &data_types::SubscriptionId,
        consumer_id: &data_types::ConsumerId,
    ) -> LogResult {
        let event = Ack {
            message_ref: *message_ref,
            subscription_id: *subscription_id,
            consumer_id: *consumer_id,
        };
        self.event_persister.log(&event, timestamp)
    }

    pub fn log_nack(
        self: &mut Self,
        timestamp: &data_types::Timestamp,
        message_ref: &model::MessageRef,
        subscription_id: &data_types::SubscriptionId,
        consumer_id: &data_types::ConsumerId,
    ) -> LogResult {
        let event = Nack {
            message_ref: *message_ref,
            subscription_id: *subscription_id,
            consumer_id: *consumer_id,
        };
        self.event_persister.log(&event, timestamp)
    }
}
