use pulsar_rust_broker::{
    model::{data_types::NodeId, MessageRef},
    persistence::{
        entity_persister::{LoadError, LoadResult},
        event_logger::{EventQueryOptions, LogEntry},
        logged_events::{Ack, Nack, Publish},
        persisted_entities::Node,
        Keyed, PersistenceLayer, PersistenceScheme,
    },
};

#[test]
fn should_persist_entities_in_memory() {
    let persistence =
        PersistenceLayer::new(PersistenceScheme::InMemory, PersistenceScheme::InMemory);

    let node_id: NodeId = 99;
    let node_key = Node::key(node_id);
    let mut saved_node = Node::new(node_id, "127.0.0.1");

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
        catalog_id: 12,
        message_id: 544,
    };
    persistence
        .log_with_timestamp(&Publish::new(message_ref), 1)
        .unwrap();
    persistence
        .log_with_timestamp(&Ack::new(message_ref, 1, 10), 2)
        .unwrap();
    persistence
        .log_with_timestamp(&Nack::new(message_ref, 2, 11), 3)
        .unwrap();
    persistence
        .log_with_timestamp(&Nack::new(message_ref, 3, 12), 4)
        .unwrap();

    let prefix = message_ref.topic_id.to_string() + ":" + &message_ref.partition_id.to_string();

    let options1 = EventQueryOptions::default();
    let events1: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix, &options1)
        .collect();

    assert_eq!(events1.len(), 4);

    assert_eq!(events1[0].timestamp, 4);
    assert_eq!(events1[0].type_name, "Nack");
    assert_eq!(events1[0].key, "1:16:12:544");

    assert_eq!(events1[1].timestamp, 3);
    assert_eq!(events1[2].timestamp, 2);
    assert_eq!(events1[3].timestamp, 1);

    let options2 = EventQueryOptions::range(1, 1);
    let events2: Vec<LogEntry> = persistence
        .events_by_key_prefix(&prefix, &options2)
        .collect();

    assert_eq!(events2.len(), 1);
    assert_eq!(events2[0].timestamp, 3);
    assert_eq!(events2[0].type_name, "Nack");
    assert_eq!(events2[0].key, "1:16:12:544");
}
