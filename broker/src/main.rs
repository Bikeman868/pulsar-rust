use config::Config;
use log::LevelFilter;
use pulsar_rust_broker::{
    api_bin_sockets, api_http_warp,
    data::DataLayer,
    model::cluster::Cluster,
    observability::Metrics,
    persistence::{PersistenceLayer, PersistenceScheme},
    services::{
        admin_service::AdminService, pub_service::PubService, stats_service::StatsService,
        sub_service::SubService,
    },
    App,
};
use std::{
    collections::HashMap,
    env,
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::task;

#[cfg(debug_assertions)]
use pulsar_rust_broker::model::cluster::{DEFAULT_ADMIN_PORT, DEFAULT_PUBSUB_PORT, DEFAULT_SYNC_PORT};

#[tokio::main]
async fn main() {
    let mut clog = colog::default_builder();
    clog.filter_level(LevelFilter::Info);
    #[cfg(debug_assertions)]
    clog.filter_level(LevelFilter::Debug);
    clog.init();

    // Extract pod specific configuration from command line options
    let args: Vec<String> = env::args().collect();

    // 1st command line arg is the name of the environment
    let environment: &'static str = match args.get(1) {
        Some(s) => s.clone().leak(),
        None => "dev",
    };

    // 2nd command line arg is the name of the cluster that this node belongs to
    let cluster_name: &'static str = match args.get(2) {
        Some(s) => s.clone().leak(),
        None => "local",
    };

    // 3rd command line arg is the IP address of the network interface to listen on
    let ip_address: &'static str = match args.get(3) {
        Some(s) => s.clone().leak(),
        None => "127.0.0.1",
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
    let data_layer = Arc::new(DataLayer::new(cluster_name.to_owned(), &persistence_layer));

    // If this is a debug build, then delete all of the data and build a dev configuration
    #[cfg(debug_assertions)]
    {
        // Dalete all existing stored data
        persistence_layer.delete_all();

        // Create a development configuration in the database
        let node = data_layer
            .add_node(
                &ip_address,
                DEFAULT_ADMIN_PORT,
                DEFAULT_PUBSUB_PORT,
                DEFAULT_SYNC_PORT,
            )
            .unwrap();
        let topic1 = data_layer.add_topic("topic-1").unwrap();
        let topic2 = data_layer.add_topic("topic-2").unwrap();
        let partition_1_1 = data_layer
            .add_partition(topic1.topic_id, node.node_id)
            .unwrap();
        let partition_1_2 = data_layer
            .add_partition(topic1.topic_id, node.node_id)
            .unwrap();
        let partition_1_3 = data_layer
            .add_partition(topic1.topic_id, node.node_id)
            .unwrap();
        let partition_2_1 = data_layer
            .add_partition(topic2.topic_id, node.node_id)
            .unwrap();
        let partition_2_2 = data_layer
            .add_partition(topic2.topic_id, node.node_id)
            .unwrap();
        let partition_2_3 = data_layer
            .add_partition(topic2.topic_id, node.node_id)
            .unwrap();
        data_layer
            .add_subscription(topic1.topic_id, "app-a", false)
            .unwrap();
        data_layer
            .add_subscription(topic1.topic_id, "app-b", true)
            .unwrap();
        data_layer
            .add_ledger(
                partition_1_1.topic_id,
                partition_1_1.partition_id,
                node.node_id,
            )
            .unwrap();
        data_layer
            .add_ledger(
                partition_1_2.topic_id,
                partition_1_2.partition_id,
                node.node_id,
            )
            .unwrap();
        data_layer
            .add_ledger(
                partition_1_3.topic_id,
                partition_1_3.partition_id,
                node.node_id,
            )
            .unwrap();
        data_layer
            .add_ledger(
                partition_2_1.topic_id,
                partition_2_1.partition_id,
                node.node_id,
            )
            .unwrap();
        data_layer
            .add_ledger(
                partition_2_2.topic_id,
                partition_2_2.partition_id,
                node.node_id,
            )
            .unwrap();
        data_layer
            .add_ledger(
                partition_2_3.topic_id,
                partition_2_3.partition_id,
                node.node_id,
            )
            .unwrap();
    }

    // Cluster is at the root of thr data model
    let cluster = Arc::new(Cluster::new(&data_layer, ip_address));

    // App is a container for the application singletons. Injecting App is much simpler than injecting dependnecies individually.
    // The application owns Arcs and the Arcs own the singeltons.
    let app = Arc::new(App {
        stop_signal: Arc::new(AtomicBool::new(false)),
        metrics: Arc::new(Metrics::new()),
        peristence: Arc::clone(&persistence_layer),
        pub_service: Arc::new(PubService::new(&persistence_layer, &cluster)),
        sub_service: Arc::new(SubService::new(&persistence_layer, &cluster)),
        admin_service: Arc::new(AdminService::new(&cluster)),
        stats_service: Arc::new(StatsService::new(&cluster)),
    });

    // Handle SIGTERM by setting the stop_signal boolean
    let stop_signal = app.stop_signal.clone();
    ctrlc::set_handler(move || stop_signal.store(true, Ordering::Relaxed)).unwrap();

    // Start sending metrics to StatsD
    task::spawn(send_metrics(Arc::clone(&app)));

    // Get endpoint configuration from DB
    let my_node = cluster.my_node();
    let ip_address = Ipv4Addr::from_str(&my_node.ip_address()).expect(&format!(
        "Failed to parse {} as an IPv4 address",
        my_node.ip_address()
    ));

    // Serve binary serialized requests over TCP/IP
    let admin_endpoint = SocketAddrV4::new(ip_address, my_node.pubsub_port());
    let api_bin_handle = api_bin_sockets::serve(&app, admin_endpoint);

    // Serve requests over http using warp and wait for it to terminate
    let admin_endpoint = SocketAddrV4::new(ip_address, my_node.admin_port());
    api_http_warp::serve(&app, admin_endpoint).await;

    // Wait for the bin api to terminate
    api_bin_handle.join().unwrap();
}

async fn send_metrics(app: Arc<App>) {
    app.metrics.run(&app.stop_signal).await;
}
