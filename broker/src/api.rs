use warp::{Filter, Rejection, Reply};

mod requests {}

mod responses {
    use crate::model::data_types::NodeId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Node<'a> {
        pub id: NodeId,
        pub ip_address: &'a str,
    }
}

mod docs {
    use warp::{get, path, reply::html, Filter, Rejection, Reply};

    async fn root() -> Result<impl Reply, Rejection> {
        Ok(html("Root"))
    }

    pub fn routes() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        path::end().and(get()).and_then(root)
    }
}

mod admin {
    use super::responses::Node;
    use warp::{get, path, reply::json, Filter, Rejection, Reply};

    use crate::model::data_types::NodeId;

    async fn get_node(node_id: NodeId) -> Result<impl Reply, Rejection> {
        let node = Node {
            id: node_id,
            ip_address: "10.2.45.6",
        };
        Ok(json(&node))
    }

    pub fn routes() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        path! {"v1" / "admin" / "node" / NodeId}
            .and(get())
            .and_then(get_node)
    }
}

mod pubsub {
    use warp::{get, path, Filter, Rejection, Reply};

    async fn get_brokers(_topic_name: String) -> Result<impl Reply, Rejection> {
        Ok(warp::reply::html("Not yet implemented"))
    }

    pub fn routes() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        path! {"v1" / "pub" / "brokers" / String}
            .and(get())
            .and_then(get_brokers)
    }
}

pub fn routes() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    pubsub::routes().or(admin::routes()).or(docs::routes())
}
