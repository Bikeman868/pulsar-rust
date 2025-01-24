use super::with_app;
use crate::{observability::Metrics, services::pub_service::PubError, App};
use pulsar_rust_net::{
    contracts::v1::{
        requests,
        responses::{self, Response},
    },
    error_codes::{ERROR_CODE_BACKLOG_FULL, ERROR_CODE_GENERAL_FAILURE, ERROR_CODE_INCORRECT_NODE},
};
use std::sync::Arc;
use warp::{body, get, path, post, reply, Filter, Rejection, Reply};

type TopicName = String;

async fn get_partitions_by_topic_name(
    topic_name: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_PUB_MAPPING_COUNT);
    match app.pub_service.topic_by_name(&topic_name) {
        Some(topic) => {
            let mut map = responses::TopicPartitionMap::from(&topic);
            let nodes = &*app.admin_service.all_nodes();
            map.nodes = nodes
                .values()
                .iter()
                .map(|node| responses::NodeDetail::from(node))
                .collect();
            Ok(reply::json(&Response::success(map)))
        }
        None => Err(warp::reject::not_found()),
    }
}

async fn publish_message(
    message: requests::Publish,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_PUB_MESSAGE_COUNT);
    let response = match app.pub_service.publish_message(message.into()) {
        Ok(message_ref) => responses::Response::success(responses::PublishResult {
            message_ref: message_ref.into(),
        }),
        Err(err) => match err {
            PubError::Error(msg) => {
                responses::Response::error(&msg.clone(), ERROR_CODE_GENERAL_FAILURE)
            }
            PubError::TopicNotFound => responses::Response::warning("No topic with this ID"),
            PubError::PartitionNotFound => {
                responses::Response::warning("No partition with this ID")
            }
            PubError::NodeNotFound => responses::Response::warning("No node with this ID"),
            PubError::WrongNode(node) => responses::Response::error(
                &format!(
                    "Wrong node for this partition. Publish to {} instead",
                    node.ip_address()
                ),
                ERROR_CODE_INCORRECT_NODE,
            ),
            PubError::BacklogCapacityExceeded => {
                responses::Response::error("The backlog storage is full", ERROR_CODE_BACKLOG_FULL)
            }
            PubError::NoSubscribers => {
                responses::Response::warning("There are no active subscribers to this topic")
            }
        },
    };
    Ok(reply::json(&response))
}

async fn ping(app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_PUB_PING_COUNT);
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
