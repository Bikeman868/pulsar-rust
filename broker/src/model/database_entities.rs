use serde::{Deserialize, Serialize};

use super::data_types;
use crate::persistence;

#[derive(Debug, Deserialize, Serialize)]
pub struct Node {
    pub version: data_types::VersionNumber,
    pub id: data_types::NodeId,
    pub ip_address: String,
}

impl Node {
    pub fn new(id: data_types::NodeId, ip_address: &str) -> Self {
        Self { version: 0, id, ip_address: ip_address.to_owned() }
    }
}

impl persistence::Keyed for Node {
    fn type_name() -> &'static str {
        "Node"
    }
    fn key(self: &Self) -> String {
        self.id.to_string()
    }
}

impl persistence::Versioned for Node {
    fn version(self: &Self) -> data_types::VersionNumber {
        self.version
    }
    fn set_version(self: &mut Self, version: data_types::VersionNumber) {
        self.version = version
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Partition {
    pub version: data_types::VersionNumber,
    pub id: data_types::PartitionId,
}

impl persistence::Keyed for Partition {
    fn type_name() -> &'static str {
        "Partition"
    }
    fn key(self: &Self) -> String {
        self.id.to_string()
    }
}

impl persistence::Versioned for Partition {
    fn version(self: &Self) -> data_types::VersionNumber {
        self.version
    }
    fn set_version(self: &mut Self, version: data_types::VersionNumber) {
        self.version = version
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Subscription {
    pub version: data_types::VersionNumber,
    pub id: data_types::SubscriptionId,
}

impl persistence::Keyed for Subscription {
    fn type_name() -> &'static str {
        "Subscription"
    }
    fn key(self: &Self) -> String {
        self.id.to_string()
    }
}

impl persistence::Versioned for Subscription {
    fn version(self: &Self) -> data_types::VersionNumber {
        self.version
    }
    fn set_version(self: &mut Self, version: data_types::VersionNumber) {
        self.version = version
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Topic {
    pub version: data_types::VersionNumber,
    pub id: data_types::TopicId,
    pub partitions: Partition,
}

impl persistence::Keyed for Topic {
    fn type_name() -> &'static str {
        "Topic"
    }
    fn key(self: &Self) -> String {
        self.id.to_string()
    }
}

impl persistence::Versioned for Topic {
    fn version(self: &Self) -> data_types::VersionNumber {
        self.version
    }
    fn set_version(self: &mut Self, version: data_types::VersionNumber) {
        self.version = version
    }
}
