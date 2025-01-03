use super::with_app;
use crate::{observability::Metrics, services::sub_service::SubError, App};
use pulsar_rust_net::{
    contracts::v1::{requests, responses},
    data_types::{ConsumerId, SubscriptionId, TopicId},
};
use std::sync::Arc;
use warp::{body, get, path, post, reply, Filter, Rejection, Reply};

async fn get_message(
    topic_id: TopicId,
    subscription_id: SubscriptionId,
    consumer_id: ConsumerId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_MESSAGE_COUNT);
    let response = match app.sub_service.next_message(topic_id, subscription_id, consumer_id) {
        Ok((subscribed_message, published_message)) => {
            let message = responses::Message {
                message_ref: responses::MessageRef::from(&published_message.message_ref),
                message_key: published_message.key.clone(),
                message_ack_key: published_message.message_ref.to_key(),
                published: published_message.published,
                attributes: published_message.attributes,
                delivered: subscribed_message.delivered_timestamp.unwrap(),
                delivery_count: subscribed_message.delivery_count,
            };
            responses::Response::success(message)
        }
        Err(err) => {
            match err {
                SubError::Error(msg) => responses::Response::error(&msg),
                SubError::TopicNotFound => responses::Response::warning(&format!("No topic found with id {topic_id}")),
                SubError::SubscriptionNotFound => responses::Response::warning(&format!("No subscription found with id {subscription_id}")),
                SubError::PartitionNotFound => responses::Response::warning(&String::from("No partition found with this partition id. The partition may have been deleted")),
                SubError::LedgerNotFound => responses::Response::warning(&String::from("No ledger found found with this ledger id. The ledger may have been deleted")),
                SubError::MessageNotFound => responses::Response::warning(&String::from("No message found with this message id. The message may have been acked by all subscriptions")),
                SubError::NoneAvailable => responses::Response::no_data(&String::from("There are no more messages available at this time")),
            }
        }
    };
    Ok(reply::json(&response))
}

async fn get_nodes(app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_NODES_COUNT);
    Ok(reply::json(&responses::Response::success(
        responses::NodeList::from(app.sub_service.all_nodes()),
    )))
}

async fn get_topics(app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_TOPICS_COUNT);
    Ok(reply::json(&responses::Response::success(
        responses::TopicList::from(app.sub_service.all_topics()),
    )))
}

async fn allocate_consumer(
    body: requests::Consumer,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_CONSUMER_COUNT);
    let maybe_consumer_id = app
        .sub_service
        .allocate_consumer_id(body.topic_id, body.subscription_id);

    let response = if let Some(consumer_id) = maybe_consumer_id {
        responses::Response::success(responses::AllocateConsumerResult { consumer_id })
    } else {
        responses::Response::error(&format!(
            "Unable to allocate consumer id for topic {} and subscription {}",
            body.topic_id, body.subscription_id
        ))
    };
    Ok(reply::json(&response))
}

async fn ack_message(body: requests::Ack, app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_ACK_COUNT);
    let response =
        match app
            .sub_service
            .ack(body.message_ack_key, body.subscription_id, body.consumer_id)
        {
            Ok(found) => {
                if found {
                    responses::Response::success(responses::AckResult {})
                } else {
                    responses::Response::error(&String::from(
                        "No message found with this id, maybe this was acked already",
                    ))
                }
            }
            Err(err) => match err {
                SubError::Error(msg) => responses::Response::error(&msg),
                SubError::TopicNotFound => {
                    responses::Response::error(&String::from("No topic found with this id"))
                }
                SubError::SubscriptionNotFound => {
                    responses::Response::error(&String::from("No subscription found with this id"))
                }
                SubError::PartitionNotFound => {
                    responses::Response::error(&String::from("No partition found with this id"))
                }
                SubError::LedgerNotFound => {
                    responses::Response::error(&String::from("No ledger found with this id"))
                }
                SubError::MessageNotFound => {
                    responses::Response::error(&String::from("No message found with this id"))
                }
                SubError::NoneAvailable => {
                    responses::Response::error(&String::from("No data was available"))
                }
            },
        };
    Ok(reply::json(&response))
}

async fn nack_message(body: requests::Nack, app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_NACK_COUNT);
    let response =
        match app
            .sub_service
            .nack(body.message_ack_key, body.subscription_id, body.consumer_id)
        {
            Ok(found) => {
                if found {
                    responses::Response::success(responses::AckResult {})
                } else {
                    responses::Response::error(&String::from(
                        "No message found with this id, maybe this was acked already",
                    ))
                }
            }
            Err(err) => match err {
                SubError::Error(msg) => responses::Response::error(&msg),
                SubError::TopicNotFound => {
                    responses::Response::error(&String::from("No topic found with this id"))
                }
                SubError::SubscriptionNotFound => {
                    responses::Response::error(&String::from("No subscription found with this id"))
                }
                SubError::PartitionNotFound => {
                    responses::Response::error(&String::from("No partition found with this id"))
                }
                SubError::LedgerNotFound => {
                    responses::Response::error(&String::from("No ledger found with this id"))
                }
                SubError::MessageNotFound => {
                    responses::Response::error(&String::from("No message found with this id"))
                }
                SubError::NoneAvailable => {
                    responses::Response::error(&String::from("No data was available"))
                }
            },
        };
    Ok(reply::json(&response))
}

async fn ping(app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_PING_COUNT);
    Ok(reply::html("pong"))
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("v1" / "sub" / "ping")
        .and(get()).and(with_app(app))
        .and_then(ping)
    .or(path!("v1" / "sub" / "topic" / TopicId / "subscription" / SubscriptionId / "consumer" / ConsumerId / "message")
        .and(get()).and(with_app(app))
        .and_then(get_message))
    .or(path!("v1" / "sub" / "ack")
        .and(post()).and(body::content_length_limit(512)).and(body::json()).and(with_app(app))
        .and_then(ack_message))
    .or(path!("v1" / "sub" / "nack")
        .and(post()).and(body::content_length_limit(512)).and(body::json()).and(with_app(app))
        .and_then(nack_message))
    .or(path!("v1" / "sub" / "nodes")
        .and(get()).and(with_app(app))
        .and_then(get_nodes))
    .or(path!("v1" / "sub" / "topics")
        .and(get()).and(with_app(app))
        .and_then(get_topics))
    .or(path!("v1" / "sub" / "consumer")
        .and(post()).and(body::content_length_limit(512)).and(body::json()).and(with_app(app))
        .and_then(allocate_consumer))
}
