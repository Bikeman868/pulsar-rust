use crate::persistence;
use crate::model;
use crate::model::data_types;

fn log_publish_event(
    event_persistence: impl persistence::EventPersister,
    timestamp: &data_types::Timestamp,
    message_ref: &model::MessageRef) {
    println!("Message publication logged {:?}", message_ref);
}

fn log_ack_event(
    event_persistence: impl persistence::EventPersister, 
    timestamp: &data_types::Timestamp,
    message_ref: &model::MessageRef) {
    println!("Message ack logged {:?}", message_ref);
}

fn log_nack_event(
    event_persistence: impl persistence::EventPersister,
    timestamp: &data_types::Timestamp,
    message_ref: &model::MessageRef) {
    println!("Message nack logged {:?}", message_ref);
}

fn delete_message_log(
    event_persistence: impl persistence::EventPersister, 
    timestamp: &data_types::Timestamp,
    message_ref: &model::MessageRef) {
    println!("Deleting transaction log for message {:?}", message_ref);
}
