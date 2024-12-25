use super::*;
use crate::persistence::{
    entity_persister::{DeleteError, SaveError},
    persisted_entities::{Subscription, Topic},
};
use pulsar_rust_net::data_types::{SubscriptionId, TopicId};

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

    pub fn add_subscription(
        self: &Self,
        topic_id: TopicId,
        name: &str,
        has_key_affinity: bool,
    ) -> DataAddResult<Subscription> {
        let mut subscription_id: SubscriptionId = 0;

        if let Err(err) = self.update_topic(topic_id, |topic| {
            subscription_id = topic.next_subscription_id;
            topic.next_subscription_id += 1;
            topic.subscription_ids.push(subscription_id);
            true
        }) {
            return Err(DataAddError::PersistenceFailure {
                msg: (format!("Failed to update topic. {:?}", err)),
            });
        }

        let mut subscription = Subscription::new(
            topic_id,
            subscription_id,
            name.to_owned(),
            has_key_affinity,
            1,
        );
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
                DeleteError::Error { msg } => {
                    DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                        msg: format!(
                            "{msg} deleting subscription {subscription_id} from topic {topic_id}"
                        ),
                    })
                }
                DeleteError::NotFound { .. } => DataUpdateResult::Ok(()),
            },
        }
    }

    pub fn update_subscription<F>(
        self: &Self,
        topic_id: TopicId,
        subscription_id: SubscriptionId,
        mut update: F,
    ) -> DataUpdateResult<Subscription>
    where
        F: FnMut(&mut Subscription) -> bool,
    {
        loop {
            let mut subscription = match self.get_subscription(topic_id, subscription_id) {
                Ok(node) => node,
                Err(err) => {
                    return match err {
                        DataReadError::PersistenceFailure { msg } => {
                            Err(DataUpdateError::PersistenceFailure { msg })
                        }
                        DataReadError::NotFound => Err(DataUpdateError::NotFound),
                    }
                }
            };

            update(&mut subscription);

            match self.persistence.save(&mut subscription) {
                Ok(_) => return DataUpdateResult::Ok(subscription),
                Err(e) => match e {
                    SaveError::Unmodified => return DataUpdateResult::Ok(subscription),
                    SaveError::VersionMissmatch => continue,
                    SaveError::Error { msg } => {
                        return DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                            msg: format!(
                                "{msg} updating subscription {subscription_id} in topic {topic_id}"
                            ),
                        })
                    }
                },
            }
        }
    }
}
