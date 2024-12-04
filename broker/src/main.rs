use config::Config;
use std::{
    collections::HashMap,
    env,
    net::Ipv4Addr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{task, time};

use pulsar_rust_broker::{
    admin_service::AdminService,
    data::DataLayer,
    http_api_sockets, http_api_warp,
    model::cluster::Cluster,
    persistence::{PersistenceLayer, PersistenceScheme},
    pub_service::PubService,
    sub_service::SubService,
    App,
};

#[tokio::main]
async fn main() {
    // Extract pod specific configuration from command line options
    let args: Vec<String> = env::args().collect();
    let environment: &'static str = match args.get(1) {
        Some(s) => s.clone().leak(),
        None => "dev",
    };
    let cluster_name: &'static str = match args.get(2) {
        Some(s) => s.clone().leak(),
        None => "local",
    };
    let ip_address: &'static str = match args.get(3) {
        Some(s) => s.clone().leak(),
        None => "127.0.0.1",
    };
    let port: &'static str = match args.get(4) {
        Some(s) => s.clone().leak(),
        None => "8000",
    };

    let ipv4 = Ipv4Addr::from_str(ip_address)
        .expect(&format!("Failed to parse {ip_address} as an IPv$ address"));
    let port: u16 = port
        .parse()
        .expect(&format!("Failed to parse {port} as a port number"));

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
    let data_layer = Arc::new(DataLayer::new(
        cluster_name.to_owned(),
        persistence_layer.clone(),
    ));

    // If this is a debug build, then delete all of the data and build a dev configuration
    #[cfg(debug_assertions)]
    {
        // Dalete all existing stored data
        persistence_layer.delete_all();

        // Create a development configuration in the database
        let node = data_layer.add_node(&ip_address).unwrap();
        let topic = data_layer.add_topic("my-topic").unwrap();
        let partition = data_layer.add_partition(topic.topic_id).unwrap();
        data_layer
            .add_subscription(topic.topic_id, "my-app")
            .unwrap();
        data_layer
            .add_catalog(partition.topic_id, partition.partition_id, node.node_id)
            .unwrap();
    }

    let cluster = Arc::new(Cluster::new(data_layer.clone(), ip_address));

    let app = Arc::new(App {
        request_count: Arc::new(AtomicU32::new(0)),
        pub_service: Arc::new(PubService::new(cluster.clone())),
        sub_service: Arc::new(SubService::new(cluster.clone())),
        admin_service: Arc::new(AdminService::new(cluster.clone())),
    });

    let signal = Arc::new(AtomicBool::new(false));
    task::spawn(print_request_count(app.clone(), signal.clone()));

    // http_api_warp::run(app.clone(), ipv4, port).await;
    http_api_sockets::run(app.clone(), ipv4, port);

    signal.store(true, Ordering::Relaxed);
}

async fn print_request_count(app: Arc<App>, signal: Arc<AtomicBool>) {
    while !signal.load(Ordering::Relaxed) {
        println!(
            "{} requests",
            app.clone().request_count.clone().swap(0, Ordering::Relaxed)
        );
        time::sleep(Duration::from_millis(1000)).await;
    }
}
