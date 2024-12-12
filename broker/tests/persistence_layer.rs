use std::collections::HashMap;

use pulsar_rust_net::data_types::NodeId;
use pulsar_rust_broker::{
    model::messages::{Message, MessageRef},
    persistence::{
        entity_persister::{LoadError, LoadResult}, 
        event_logger::EventQueryOptions, 
        log_entries::{LogEntry, LoggedEvent}, 
        logged_events::{AckEvent, NackEvent, PublishEvent}, 
        persisted_entities::Node, 
        Keyed, 
        PersistenceLayer, 
        PersistenceScheme
    }
};

#[test]
fn should_persist_entities_in_memory() {
    let persistence =
        PersistenceLayer::new(PersistenceScheme::InMemory, PersistenceScheme::InMemory);

    let node_id: NodeId = 99;
    let node_key = Node::key(node_id);
    let mut saved_node = Node::new(node_id, "127.0.0.1", 8000, 8001, 8002);

    persistence.save(&mut saved_node).unwrap();
    assert_eq!(saved_node.version, 1);

    saved_node.ip_address = "10.2.45.6".to_owned();
    persistence.save(&mut saved_node).unwrap();
    assert_eq!(saved_node.version, 2);

    let loaded_node: Node = persistence.load(&node_key).unwrap();

    assert_eq!(saved_node.node_id, loaded_node.node_id);
    assert_eq!(saved_node.version, loaded_node.version);
    assert_eq!(saved_node.ip_address, loaded_node.ip_address);

    persistence.delete(&node_key).unwrap();

    let not_found_result: LoadResult<Node> = persistence.load(&node_key);
    assert_eq!(
        not_found_result,
        Err(LoadError::NotFound {
            entity_type: node_key.type_name().to_string(),
            entity_key: node_key.key()
        })
    );
}

#[test]
fn should_persist_events_in_memory() {
    let persistence =
        PersistenceLayer::new(PersistenceScheme::InMemory, PersistenceScheme::InMemory);

    let message_ref = MessageRef {
        topic_id: 1,
        partition_id: 16,
        ledger_id: 12,
        message_id: 544,
    };

    let message = Message {
        message_ref,
        key: "".to_owned(),
        timestamp: 8773873,
        published: 9839845,
        attributes: HashMap::new(),
        subscriber_count: 1,
        delivery_count: 0,
        ack_count: 0,
    };

    persistence
        .log_with_timestamp(&LoggedEvent::Publish(PublishEvent::new(&message)), 1)
        .unwrap();
    persistence
        .log_with_timestamp(&LoggedEvent::Ack(AckEvent::new(message_ref, 1, 10)), 2)
        .unwrap();
    persistence
        .log_with_timestamp(&LoggedEvent::Nack(NackEvent::new(message_ref, 2, 11)), 3)
        .unwrap();
    persistence
        .log_with_timestamp(&LoggedEvent::Nack(NackEvent::new(message_ref, 3, 12)), 4)
        .unwrap();

    let prefix =
        PersistenceLayer::build_partition_prefix(message_ref.topic_id, message_ref.partition_id);

    // Default ordering is descending - for log display
    let options = EventQueryOptions::default();
    let events: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix, &options)
        .collect();

    assert_eq!(events.len(), 4);

    assert_eq!(events[0].timestamp, 4);
    assert_eq!(events[0].type_name, "Nack");
    assert_eq!(events[0].key, "1:16:12:544");

    assert_eq!(events[1].timestamp, 3);
    assert_eq!(events[2].timestamp, 2);
    assert_eq!(events[3].timestamp, 1);

    // When replaying events to recover state. we want ascending order
    let options = EventQueryOptions::replay();
    let events: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix, &options)
        .collect();

    assert_eq!(events.len(), 4);

    assert_eq!(events[0].timestamp, 1);
    assert_eq!(events[0].type_name, "Pub");
    assert_eq!(events[0].key, "1:16:12:544");

    assert_eq!(events[1].timestamp, 2);
    assert_eq!(events[2].timestamp, 3);
    assert_eq!(events[3].timestamp, 4);

    let options = EventQueryOptions::range(1, 1);
    let events: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix, &options)
        .collect();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].timestamp, 3);
    assert_eq!(events[0].type_name, "Nack");
    assert_eq!(events[0].key, "1:16:12:544");
}

#[test]
fn should_selectively_delete_events() {
    let persistence =
        PersistenceLayer::new(PersistenceScheme::InMemory, PersistenceScheme::InMemory);

    for topic_id in 1..=3 {
        for partition_id in 1..=3 {
            for ledger_id in 1..=2 {
                for message_id in 1..=5 {
                    persistence
                        .log_event(&LoggedEvent::Publish(PublishEvent::new(&Message {
                            message_ref: MessageRef {
                                topic_id: topic_id,
                                partition_id: partition_id,
                                ledger_id: ledger_id,
                                message_id: message_id,
                            },
                            key: String::default(),
                            timestamp: 0,
                            published: 0,
                            attributes: HashMap::new(),
                            subscriber_count: 0,
                            delivery_count: 0,
                            ack_count: 0,
                            
                        })))
                        .unwrap();

                    persistence
                        .log_event(&LoggedEvent::Ack(AckEvent::new(
                            MessageRef {
                                topic_id: topic_id,
                                partition_id: partition_id,
                                ledger_id: ledger_id,
                                message_id: message_id,
                            },
                            1,
                            1,
                        )))
                        .unwrap();
                }
            }
        }
    }

    let prefix1 = PersistenceLayer::build_partition_prefix(1, 2);
    let prefix2 = PersistenceLayer::build_partition_prefix(1, 3);
    let prefix3 = PersistenceLayer::build_topic_prefix(1);
    let options = EventQueryOptions::default();

    let entries1: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix1, &options)
        .collect();
    let entries2: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix2, &options)
        .collect();
    let entries3: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix3, &options)
        .collect();

    assert_eq!(entries1.len(), 20);
    assert_eq!(entries2.len(), 20);
    assert_eq!(entries3.len(), 60);

    persistence.delete_events_for_message(1, 2, 1, 1).unwrap();

    let entries1: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix1, &options)
        .collect();
    let entries2: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix2, &options)
        .collect();
    let entries3: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix3, &options)
        .collect();

    assert_eq!(entries1.len(), 18);
    assert_eq!(entries2.len(), 20);
    assert_eq!(entries3.len(), 58);

    persistence.delete_events_for_partition(1, 2).unwrap();

    let entries1: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix1, &options)
        .collect();
    let entries2: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix2, &options)
        .collect();
    let entries3: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix3, &options)
        .collect();

    assert_eq!(entries1.len(), 0);
    assert_eq!(entries2.len(), 20);
    assert_eq!(entries3.len(), 40);
}
