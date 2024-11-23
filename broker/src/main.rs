pub mod data;
pub mod model;
pub mod persistence;
pub mod publishing;
pub mod subscribing;
pub mod utils;

pub use config::Config;
use std::{collections::HashMap, env};

use crate::model::data_types::Timestamp;
use crate::model::database_entities::Node;
use crate::model::MessageRef;
use data::data_access_layer::DataAccessLayer;
use model::data_types::{ConsumerId, SubscriptionId};
use persistence::PersistenceScheme;
use utils::now_epoc_millis;

fn main() {
    let args: Vec<String> = env::args().collect();
    let environment: &'static str = match args.get(1) {
        Some(s) => s.clone().leak(),
        None => "dev",
    };

    let config = Config::builder()
        .add_source(config::File::with_name("Settings"))
        .add_source(config::File::with_name(
            &("Settings.".to_owned() + environment),
        ))
        .add_source(config::Environment::with_prefix("BROKER"))
        .build()
        .unwrap();

    let settings = config.try_deserialize::<HashMap<String, String>>().unwrap();
    let event_persistence_scheme = PersistenceScheme::from_string(settings.get("persist-events").unwrap());
    let state_persistence_scheme = PersistenceScheme::from_string(settings.get("persist-state").unwrap());

    let mut dal = DataAccessLayer::new(event_persistence_scheme, state_persistence_scheme);

    let mut node = Node::new(99, "127.0.0.1");
    dal.save(&mut node).unwrap();

    node.ip_address = "10.2.34.6".to_owned();
    dal.save(&mut node).unwrap();

    let message_ref = MessageRef {
        topic_id: 1,
        partition_id: 16,
        catalog_id: 644,
        message_id: 988887593,
    };

    let subscription_id: SubscriptionId = 782;
    let consumer_id: ConsumerId = 894;
    let timestamp: Timestamp = now_epoc_millis();

    dal.log_ack(&timestamp, &message_ref, &subscription_id, &consumer_id)
        .unwrap();
}
