use config::Config;
use std::{collections::HashMap, env, sync::Arc};

use pulsar_rust_broker::{
    data::DataLayer,
    persistence::{PersistenceLayer, PersistenceScheme},
};

fn main() {
    // Extract environment name from command line options
    let args: Vec<String> = env::args().collect();
    let environment: &'static str = match args.get(1) {
        Some(s) => s.clone().leak(),
        None => "dev",
    };
    let cluster_name: &'static str = match args.get(2) {
        Some(s) => s.clone().leak(),
        None => "local",
    };

    // Merge configuration sources for this environment
    let config = Config::builder()
        .add_source(config::File::with_name("Settings"))
        .add_source(config::File::with_name(
            &("Settings.".to_owned() + environment),
        ))
        .add_source(config::Environment::with_prefix("BROKER"))
        .build()
        .unwrap();

    // Deserialize application settings from the configuration
    let settings = config.try_deserialize::<HashMap<String, String>>().unwrap();

    // Build a persistence layer with the configured persistence scheme
    let event_persistence_scheme =
        PersistenceScheme::from_string(settings.get("persist-events").unwrap());
    let entity_persistence_scheme =
        PersistenceScheme::from_string(settings.get("persist-state").unwrap());
    let persistence_layer = Arc::new(PersistenceLayer::new(
        event_persistence_scheme,
        entity_persistence_scheme,
    ));

    // Build a data access layer on top of the persistence layer
    let data_layer = Arc::new(DataLayer::new(cluster_name, &persistence_layer));

    // If this is a debug build, then delete all of the data and build a a dev configuration
    #[cfg(debug_assertions)]
    {
        // Dalete all existing stored data
        persistence_layer.delete_all();

        // Create a development configuration in the database
        let node = data_layer.add_node("127.0.0.1").unwrap();
        let topic = data_layer.add_topic("my-topic").unwrap();
        let partition = data_layer.add_partition(topic.topic_id).unwrap();
        data_layer
            .add_subscription(topic.topic_id, "my-app")
            .unwrap();
        data_layer
            .add_catalog(partition.topic_id, partition.partition_id, node.node_id)
            .unwrap();
    }
}
