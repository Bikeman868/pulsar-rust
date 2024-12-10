use pulsar_rust_net::data_types::TopicId;
use crate::persistence::{
    entity_persister::{DeleteError, SaveError},
    persisted_entities::Topic,
};
use super::{DataAddError, DataAddResult, DataLayer, DataReadError, DataReadResult, DataUpdateError, DataUpdateResult};

impl DataLayer {
    pub fn get_topic(self: &Self, topic_id: TopicId) -> DataReadResult<Topic> {
        self.get_entity(&Topic::key(topic_id))
    }

    pub fn get_topics(self: &Self) -> DataReadResult<Vec<Topic>> {
        let cluster = match self.get_cluster() {
            Ok(c) => c,
            Err(e) => return DataReadResult::Err(e),
        };

        let mut topics: Vec<Topic> = Vec::new();
        for topic_id in cluster.topic_ids {
            topics.push(self.get_topic(topic_id)?);
        }
        Ok(topics)
    }

    pub fn add_topic(self: &Self, name: &str) -> DataAddResult<Topic> {
        let mut topic_id: TopicId = 0;

        if let Err(err) = self.update_cluster(|cluster| {
            topic_id = cluster.next_topic_id;
            cluster.next_topic_id += 1;
            cluster.topic_ids.push(topic_id);
            true
        }) {
            return Err(DataAddError::PersistenceFailure { msg: (format!("Failed to update cluster. {:?}", err)) });
        }

        let mut topic = Topic::new(topic_id, name.to_owned(), Vec::new(), Vec::new(), 1, 1);
        match self.persistence.save(&mut topic) {
            Ok(_) => DataAddResult::Ok(topic),
            Err(e) => match e {
                SaveError::Error { msg } => {
                    return DataAddResult::Err(DataAddError::PersistenceFailure {
                        msg: msg + " saving the new topic",
                    })
                }
                SaveError::VersionMissmatch => {
                    panic!("Version mismatch saving new topic record")
                }
                SaveError::Unmodified => {
                    panic!("Update closure always returns true")
                }
            },
        }
    }

    pub fn delete_topic(self: &Self, topic_id: TopicId) -> DataUpdateResult<()> {
        let topic = match self.get_topic(topic_id) {
            Ok(topic) => topic,
            Err(err) => return match err {
                DataReadError::PersistenceFailure { msg } => Err(DataUpdateError::PersistenceFailure { msg }),
                DataReadError::NotFound => Err(DataUpdateError::NotFound),
            }
        };

        for subscription_id in topic.subscription_ids {
            self.delete_subscription(topic_id, subscription_id)?
        }

        for partition_id in topic.partition_ids {
            self.delete_partition(topic_id, partition_id)?
        }

        self.update_cluster(|cluster| {
            cluster.topic_ids.retain(|&id| id != topic_id);
            true
        })?;

        match self.persistence.delete(&Topic::key(topic_id)) {
            Ok(_) => DataUpdateResult::Ok(()),
            Err(e) => match e {
                DeleteError::Error { msg } => DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                    msg: format!("{msg} deleting topic {topic_id}"),
                }),
                DeleteError::NotFound { .. } => DataUpdateResult::Ok(()),
            },
        }
    }

    pub fn update_topic<F>(self: &Self, topic_id: TopicId, mut update: F) -> DataUpdateResult<Topic>
    where
        F: FnMut(&mut Topic) -> bool,
    {
        loop {
            let mut topic = match self.get_topic(topic_id) {
                Ok(topic) => topic,
                Err(err) => return match err {
                    DataReadError::PersistenceFailure { msg } => Err(DataUpdateError::PersistenceFailure { msg }),
                    DataReadError::NotFound => Err(DataUpdateError::NotFound),
                }
            };

            update(&mut topic);

            match self.persistence.save(&mut topic) {
                Ok(_) => return DataUpdateResult::Ok(topic),
                Err(e) => match e {
                    SaveError::Unmodified => return DataUpdateResult::Ok(topic),
                    SaveError::VersionMissmatch => continue,
                    SaveError::Error { msg } => {
                        return DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                            msg: format!("{msg} updating topic {topic_id}"),
                        })
                    }
                },
            }
        }
    }
}
