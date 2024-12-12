use std::sync::Arc;
use warp::{get, post, path, reply, body, Filter, Rejection, Reply};
use pulsar_rust_net::contracts::v1::{requests, responses};
use crate::{services::pub_service::PubError, App};
use super::with_app;

type TopicName = String;

async fn get_partitions_by_topic_name(
    topic_name: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    match app.pub_service.topic_by_name(&topic_name) {
        Some(topic) => {
            let mut map = responses::TopicPartitionMap::from(topic);
            let nodes = &*app.admin_service.all_nodes();
            map.nodes = nodes.iter().map(|node|responses::NodeDetail::from(node)).collect();
            Ok(reply::json(&map))
        },
        None => Err(warp::reject::not_found()),
    }
}

async fn publish_message(message: requests::Message, app: Arc<App>) ->  Result<impl Reply, Rejection> {
    match app.pub_service.publish_message(message.into()) {
        Ok(message_ref) => Ok(reply::json(&responses::PublishResult{
            result: responses::PostResult::success(),
            message_ref: Some(message_ref.into()),
        })),
        Err(err) => {
            let error = match err {
                PubError::Error(msg) => &msg.clone(),
                PubError::TopicNotFound => "No topic with this ID",
                PubError::PartitionNotFound => "No partition with this ID",
                PubError::NodeNotFound => "No node with this ID",
                PubError::WrongNode(node) => &format!("Wrong node for this partition. Publish to {} instead", node.ip_address()),
                PubError::BacklogCapacityExceeded => "The backlog storage is full",
            };
            Ok(reply::json(&responses::PublishResult{
                result: responses::PostResult::error(error),
                message_ref: None,
            }))
        }
    }
}

async fn ping(_app: Arc<App>) -> Result<impl Reply, Rejection> {
    Ok(reply::html("pong"))
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("v1" / "pub" / "ping")
        .and(get()).and(with_app(app))
        .and_then(ping)
    .or(path!("v1" / "pub" / "message")
        .and(post()).and(body::content_length_limit(512)).and(body::json()).and(with_app(app))
        .and_then(publish_message))
    .or(path!("v1" / "pub" / "partitions" / TopicName)
        .and(get()).and(with_app(app))
        .and_then(get_partitions_by_topic_name))
}
