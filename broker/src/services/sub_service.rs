/*
Provides the functionallity needed to support message subscribers. Various APIs can
be layered on top of this service to expose this funtionallity to applicatins.
*/

use std::sync::Arc;

use crate::model::cluster::Cluster;

pub struct SubService {
    // cluster: Arc<Cluster>,
}

impl SubService {
    pub fn new(_cluster: &Arc<Cluster>) -> Self {
        Self { 
            // cluster: Arc::clone(cluster),
        }
    }
}
