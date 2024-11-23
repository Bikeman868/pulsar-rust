use crate::persistence;
use crate::model::database_entities;

fn write_topic(state_persistence: &impl persistence::StatePersister, topic: &database_entities::Topic) 
-> Result<(), persistence::SaveError> {
    println!("Topic saved");
    Result::Ok(())
}

fn read_topic(state_persistence: &impl persistence::StatePersister, topic: &mut database_entities::Topic)
-> Result<(), persistence::LoadError> {
    println!("Topic loaded");
    Result::Ok(())
}

fn write_topic_list(state_persistence: &impl persistence::StatePersister)
-> Result<(), persistence::SaveError> {
    println!("Topic list saved");
    Result::Ok(())
}

fn read_topic_list(state_persistence: impl persistence::StatePersister)
-> Result<(), persistence::LoadError> {
    println!("Topic list loaded");
    Result::Ok(())
}
