use crate::App;
use std::{convert::Infallible, sync::Arc};
use warp::{Filter, Rejection, Reply};

mod requests {}

mod responses {
    use crate::model::data_types::NodeId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Node {
        pub id: NodeId,
        pub ip_address: String,
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
    use super::with_app;
    use crate::model::data_types::NodeId;
    use crate::App;
    use std::sync::Arc;
    use warp::{get, path, reply::json, Filter, Rejection, Reply};

    async fn get_node(node_id: NodeId, _app: Arc<App>) -> Result<impl Reply, Rejection> {
        let node = Node {
            id: node_id,
            ip_address: String::from("10.2.45.6"),
        };
        Ok(json(&node))
    }

    pub fn routes(app: Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        path!("v1" / "admin" / "node" / NodeId)
            .and(get())
            .and(with_app(app.clone()))
            .and_then(get_node)
    }
}

mod pubsub {
    use super::with_app;
    use crate::App;
    use std::sync::Arc;
    use warp::{get, path, reply::json, Filter, Rejection, Reply};

    async fn get_nodes(_topic_name: String, app: Arc<App>) -> Result<impl Reply, Rejection> {
        Ok(json(&app.cluster.my_node().persisted_data))
    }

    pub fn routes(app: Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        path!("v1" / "pub" / "nodes" / String)
            .and(get())
            .and(with_app(app.clone()))
            .and_then(get_nodes)
    }
}

fn with_app<'a>(app: Arc<App>) -> impl Filter<Extract = (Arc<App>,), Error = Infallible> + Clone {
    warp::any().map(move || app.clone())
}

pub fn routes(app: Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    pubsub::routes(app.clone())
        .or(admin::routes(app.clone()))
        .or(docs::routes())
}
