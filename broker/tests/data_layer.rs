use std::sync::Arc;
use pulsar_rust_net::data_types::NodeId;
use pulsar_rust_broker::{
    data::{DataLayer, DataReadError},
    persistence::{
        entity_persister::{LoadError, LoadResult},
        persisted_entities::{Ledger, Partition, Subscription, Topic},
        PersistenceLayer, PersistenceScheme,
    },
};

#[test]
fn should_persist_data_in_memory() {
    let persistence = Arc::new(PersistenceLayer::new(
        PersistenceScheme::InMemory,
        PersistenceScheme::InMemory,
    ));
    let data_layer = DataLayer::new("local".to_owned(), &persistence);

    let saved_node = data_layer.add_node("127.0.0.1", 8000, 8001, 8002).unwrap();

    let saved_topic1 = data_layer.add_topic("topic1").unwrap();
    let saved_partition1 = data_layer.add_partition(saved_topic1.topic_id, saved_node.node_id).unwrap();
    data_layer
        .add_subscription(saved_topic1.topic_id, "subscription1")
        .unwrap();
    data_layer
        .add_subscription(saved_topic1.topic_id, "subscription2")
        .unwrap();
    data_layer
        .add_ledger(
            saved_partition1.topic_id,
            saved_partition1.partition_id,
            saved_node.node_id,
        )
        .unwrap();
    data_layer
        .add_ledger(
            saved_partition1.topic_id,
            saved_partition1.partition_id,
            saved_node.node_id,
        )
        .unwrap();

    let saved_topic2 = data_layer.add_topic("topic2").unwrap();
    let saved_partition2 = data_layer.add_partition(saved_topic2.topic_id, saved_node.node_id).unwrap();
    data_layer
        .add_subscription(saved_topic2.topic_id, "subscription1")
        .unwrap();
    data_layer
        .add_subscription(saved_topic2.topic_id, "subscription2")
        .unwrap();
    data_layer
        .add_ledger(
            saved_partition2.topic_id,
            saved_partition2.partition_id,
            saved_node.node_id,
        )
        .unwrap();
    data_layer
        .add_ledger(
            saved_partition2.topic_id,
            saved_partition2.partition_id,
            saved_node.node_id,
        )
        .unwrap();

    let cluster = data_layer.get_cluster().unwrap();
    assert_eq!(cluster.name, "local");
    assert_eq!(cluster.next_node_id, 2);
    assert_eq!(cluster.next_topic_id, 3);

    let topic1 = data_layer.get_topic(1).unwrap();
    assert_eq!(topic1.topic_id, 1);
    assert_eq!(topic1.name, "topic1");
    assert_eq!(topic1.next_partition_id, 2);
    assert_eq!(topic1.next_subscription_id, 3);

    let topic2 = data_layer.get_topic(2).unwrap();
    assert_eq!(topic2.topic_id, 2);
    assert_eq!(topic2.name, "topic2");
    assert_eq!(topic2.next_partition_id, 2);
    assert_eq!(topic2.next_subscription_id, 3);

    let topic1_partition1 = data_layer.get_partition(1, 1).unwrap();
    assert_eq!(topic1_partition1.topic_id, 1);
    assert_eq!(topic1_partition1.partition_id, 1);
    assert_eq!(topic1_partition1.next_ledger_id, 3);

    let topic2_partition1 = data_layer.get_partition(2, 1).unwrap();
    assert_eq!(topic2_partition1.topic_id, 2);
    assert_eq!(topic2_partition1.partition_id, 1);
    assert_eq!(topic2_partition1.next_ledger_id, 3);

    let topic1_subscription1 = data_layer.get_subscription(1, 1).unwrap();
    assert_eq!(topic1_subscription1.topic_id, 1);
    assert_eq!(topic1_subscription1.subscription_id, 1);

    let topic2_subscription1 = data_layer.get_subscription(2, 1).unwrap();
    assert_eq!(topic2_subscription1.topic_id, 2);
    assert_eq!(topic2_subscription1.subscription_id, 1);

    let topic1_partition1_ledger1 = data_layer.get_ledger(1, 1, 1).unwrap();
    assert_eq!(topic1_partition1_ledger1.topic_id, 1);
    assert_eq!(topic1_partition1_ledger1.partition_id, 1);
    assert_eq!(topic1_partition1_ledger1.ledger_id, 1);

    let topic1_partition1_ledger2 = data_layer.get_ledger(1, 1, 2).unwrap();
    assert_eq!(topic1_partition1_ledger2.topic_id, 1);
    assert_eq!(topic1_partition1_ledger2.partition_id, 1);
    assert_eq!(topic1_partition1_ledger2.ledger_id, 2);
}

#[test]
fn should_delete_nodes() {
    let persistence = Arc::new(PersistenceLayer::new(
        PersistenceScheme::InMemory,
        PersistenceScheme::InMemory,
    ));
    let data_layer = DataLayer::new("local".to_owned(), &persistence);

    let node1 = data_layer.add_node("10.0.22.1", 8000, 8001, 8002).unwrap();
    let node2 = data_layer.add_node("10.0.22.2", 8000, 8001, 8002).unwrap();
    let node3 = data_layer.add_node("10.0.22.3", 8000, 8001, 8002).unwrap();

    let cluster = data_layer.get_cluster().unwrap();
    assert_eq!(cluster.node_ids.len(), 3);
    assert_eq!(cluster.node_ids[0], node1.node_id);
    assert_eq!(cluster.node_ids[1], node2.node_id);
    assert_eq!(cluster.node_ids[2], node3.node_id);

    data_layer.delete_node(node2.node_id).unwrap();

    let cluster = data_layer.get_cluster().unwrap();
    assert_eq!(cluster.node_ids.len(), 2);
    assert_eq!(cluster.node_ids[0], node1.node_id);
    assert_eq!(cluster.node_ids[1], node3.node_id);

    let result = data_layer.get_node(node2.node_id);
    assert_eq!(
        result,
        Err(DataReadError::PersistenceFailure {
            msg: "Node entity with id=2 was not found".to_owned()
        })
    );
}

#[test]
fn should_cascade_delete_topic() {
    let persistence = Arc::new(PersistenceLayer::new(
        PersistenceScheme::InMemory,
        PersistenceScheme::InMemory,
    ));
    let data_layer = DataLayer::new("local".to_owned(), &persistence);

    let node_id: NodeId = 1;
    let topic1 = data_layer.add_topic("topic1").unwrap();
    let topic1_partition1 = data_layer.add_partition(topic1.topic_id, node_id).unwrap();
    data_layer
        .add_subscription(topic1.topic_id, "subscription1")
        .unwrap();
    data_layer
        .add_subscription(topic1.topic_id, "subscription2")
        .unwrap();
    data_layer
        .add_ledger(
            topic1_partition1.topic_id,
            topic1_partition1.partition_id,
            node_id,
        )
        .unwrap();
    data_layer
        .add_ledger(
            topic1_partition1.topic_id,
            topic1_partition1.partition_id,
            node_id,
        )
        .unwrap();

    let topic2 = data_layer.add_topic("topic2").unwrap();
    let topic2_partition2 = data_layer.add_partition(topic2.topic_id, node_id).unwrap();
    data_layer
        .add_subscription(topic2.topic_id, "subscription1")
        .unwrap();
    data_layer
        .add_subscription(topic2.topic_id, "subscription2")
        .unwrap();
    data_layer
        .add_ledger(
            topic2_partition2.topic_id,
            topic2_partition2.partition_id,
            node_id,
        )
        .unwrap();
    data_layer
        .add_ledger(
            topic2_partition2.topic_id,
            topic2_partition2.partition_id,
            node_id,
        )
        .unwrap();

    data_layer.delete_topic(topic1.topic_id).unwrap();

    data_layer.get_topic(2).unwrap();
    data_layer.get_partition(2, 1).unwrap();
    data_layer.get_subscription(2, 1).unwrap();
    data_layer.get_ledger(2, 1, 1).unwrap();

    let result: LoadResult<Ledger> = persistence.load(&Ledger::key(1, 1, 1));
    assert_eq!(
        result,
        Err(LoadError::NotFound {
            entity_type: "Ledger".to_owned(),
            entity_key: "1:1:1".to_owned()
        })
    );

    let result: LoadResult<Partition> = persistence.load(&Partition::key(1, 1));
    assert_eq!(
        result,
        Err(LoadError::NotFound {
            entity_type: "Partition".to_owned(),
            entity_key: "1:1".to_owned()
        })
    );

    let result: LoadResult<Subscription> = persistence.load(&Subscription::key(1, 1));
    assert_eq!(
        result,
        Err(LoadError::NotFound {
            entity_type: "Subscription".to_owned(),
            entity_key: "1:1".to_owned()
        })
    );

    let result: LoadResult<Topic> = persistence.load(&Topic::key(1));
    assert_eq!(
        result,
        Err(LoadError::NotFound {
            entity_type: "Topic".to_owned(),
            entity_key: "1".to_owned()
        })
    );
}
