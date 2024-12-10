use pulsar_rust_net::data_types::{SubscriptionId, TopicId};
use crate::persistence::{
    entity_persister::{DeleteError, SaveError},
    persisted_entities::{Subscription, Topic},
};
use super::{DataAddError, DataAddResult, DataLayer, DataReadResult, DataUpdateError, DataUpdateResult};

impl DataLayer {
    pub fn get_subscription(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
    ) -> DataReadResult<Subscription> {
        self.get_entity(&Subscription::key(topic_id, subscription_id))
    }

    pub fn get_subscriptions(self: &Self, topic: &Topic) -> DataReadResult<Vec<Subscription>> {
        let mut subscriptions: Vec<Subscription> = Vec::new();
        for subscription_id in &topic.subscription_ids {
            subscriptions.push(self.get_subscription(topic.topic_id, *subscription_id)?);
        }
        Ok(subscriptions)
    }

    pub fn add_subscription(self: &Self, topic_id: TopicId, name: &str) -> DataAddResult<Subscription> {
        let mut subscription_id: SubscriptionId = 0;

        if let Err(err) = self.update_topic(topic_id, |topic| {
            subscription_id = topic.next_subscription_id;
            topic.next_subscription_id += 1;
            topic.subscription_ids.push(subscription_id);
            true
        }){
            return Err(DataAddError::PersistenceFailure { msg: (format!("Failed to update topic. {:?}", err)) });
        }

        let mut subscription = Subscription::new(topic_id, subscription_id, name.to_owned());
        match self.persistence.save(&mut subscription) {
            Ok(_) => DataAddResult::Ok(subscription),
            Err(e) => match e {
                SaveError::Error { msg } => DataAddResult::Err(DataAddError::PersistenceFailure {
                    msg: msg + " saving the new subscription",
                }),
                SaveError::VersionMissmatch => {
                    panic!("Version mismatch saving new subscription record")
                }
                SaveError::Unmodified => {
                    panic!("Update closure always returns true")
                }
            },
        }
    }

    pub fn delete_subscription(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
    ) -> DataUpdateResult<()> {
        self.update_topic(topic_id, |topic| {
            topic.subscription_ids.retain(|&id| id != subscription_id);
            true
        })?;

        match self
            .persistence
            .delete(&Subscription::key(topic_id, subscription_id))
        {
            Ok(_) => DataUpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                    msg: format!(
                        "{msg} deleting subscription {subscription_id} from topic {topic_id}"
                    ),
                }),
                DeleteError::NotFound { .. } => DataUpdateResult::Ok(()),
            },
        }
    }
}
