pub mod data;
pub mod model;
pub mod persistence;
pub mod publishing;
pub mod subscribing;

use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::events::Ack;
use crate::model::MessageRef;

use crate::persistence::StatePersister;
use crate::model::data_types::Timestamp;
use crate::model::database_entities::Node;
use crate::persistence::{EventPersister, build_event_persister, build_state_persister};

fn main() {
    let event_persister = build_event_persister();
    let state_persister = build_state_persister();
 
    let node = Node {
        version: 1,
        id: 99,
        ip_address: "127.0.0.1".to_owned(),
    };
   
    state_persister.save(&node).unwrap();

    let message_ref = MessageRef { 
        partition_id: 16, 
        catalog_id: 876893,
        message_id: 98893,
    };

    let ack = Ack { 
        id: message_ref, 
        subscription_id: 782, 
        consumer_id: 897,
    };
    
    let timestamp: Timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    event_persister.log(&ack, &timestamp).unwrap();
}
