use super::with_app;
use crate::{observability::Metrics, services::sub_service::SubError, App};
use pulsar_rust_net::{
    contracts::v1::{requests, responses},
    data_types::{ConsumerId, SubscriptionId, TopicId},
    error_codes::ERROR_CODE_GENERAL_FAILURE,
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
        Ok(message) => {
            let message = responses::Message {
                message_ref: responses::MessageRef::from(&message.published_message.message_ref),
                message_key: message.published_message.key.clone(),
                message_ack_key: message.published_message.message_ref.to_key(),
                published: message.published_message.published,
                attributes: message.published_message.attributes,
                delivered: message.subscribed_message.delivered_timestamp.unwrap(),
                delivery_count: message.subscribed_message.delivery_count,
            };
            responses::Response::success(message)
        }
        Err(err) => {
            match err {
                SubError::Error(msg) => responses::Response::error(&msg, ERROR_CODE_GENERAL_FAILURE),
                SubError::TopicNotFound => responses::Response::warning(&format!("No topic found with id {topic_id}")),
                SubError::SubscriptionNotFound => responses::Response::warning(&format!("No subscription found with id {subscription_id}")),
                SubError::PartitionNotFound => responses::Response::warning(&String::from("No partition found with this partition id. The partition may have been deleted")),
                SubError::LedgerNotFound => responses::Response::warning(&String::from("No ledger found found with this ledger id. The ledger may have been deleted")),
                SubError::MessageNotFound => responses::Response::warning(&String::from("No message found with this message id. The message may have been acked by all subscriptions")),
                SubError::NoneAvailable => responses::Response::no_data(&String::from("There are no more messages available at this time")),
                SubError::FailedToAllocateConsumerId => responses::Response::warning("Failed allocate consumer id"),
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

async fn consume(body: requests::Consume, app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_CONSUME_COUNT);

    let response = match app.sub_service.consume_max_messages(
        body.topic_id,
        body.subscription_id,
        body.consumer_id,
        body.max_messages,
    ) {
        Ok(result) => responses::Response::success(responses::ConsumeResult::from(&result)),
        Err(err) => match err {
            SubError::Error(msg) => responses::Response::error(&msg, ERROR_CODE_GENERAL_FAILURE),
            SubError::TopicNotFound => responses::Response::warning("Topic not found"),
            SubError::SubscriptionNotFound => {
                responses::Response::warning("Subscription not found")
            }
            SubError::PartitionNotFound => responses::Response::warning("Partition not found"),
            SubError::LedgerNotFound => responses::Response::warning("Ledger not found"),
            SubError::MessageNotFound => {
                responses::Response::warning("Message not found in ledger")
            }
            SubError::NoneAvailable => responses::Response::no_data("No messages available"),
            SubError::FailedToAllocateConsumerId => responses::Response::error(
                "Failed to allocate consumer id",
                ERROR_CODE_GENERAL_FAILURE,
            ),
        },
    };
    Ok(reply::json(&response))
}

async fn ack_message(body: requests::Ack, app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_ACK_COUNT);
    let response =
        match app
            .sub_service
            .ack(body.message_ref_key, body.subscription_id, body.consumer_id)
        {
            Ok(found) => {
                if found {
                    responses::Response::success(responses::AckResult { success: true })
                } else {
                    responses::Response::warning(&String::from(
                        "No message found with this id, maybe this was acked already",
                    ))
                }
            }
            Err(err) => match err {
                SubError::Error(msg) => {
                    responses::Response::error(&msg, ERROR_CODE_GENERAL_FAILURE)
                }
                SubError::TopicNotFound => {
                    responses::Response::warning(&String::from("No topic found with this id"))
                }
                SubError::SubscriptionNotFound => responses::Response::warning(&String::from(
                    "No subscription found with this id",
                )),
                SubError::PartitionNotFound => {
                    responses::Response::warning(&String::from("No partition found with this id"))
                }
                SubError::LedgerNotFound => {
                    responses::Response::warning(&String::from("No ledger found with this id"))
                }
                SubError::MessageNotFound => {
                    responses::Response::warning(&String::from("No message found with this id"))
                }
                SubError::NoneAvailable => {
                    responses::Response::no_data(&String::from("No data was available"))
                }
                SubError::FailedToAllocateConsumerId => responses::Response::error(
                    &String::from("Failed to allocate consumer id"),
                    ERROR_CODE_GENERAL_FAILURE,
                ),
            },
        };
    Ok(reply::json(&response))
}

async fn nack_message(body: requests::Nack, app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.metrics.incr(Metrics::METRIC_HTTP_SUB_NACK_COUNT);
    let response =
        match app
            .sub_service
            .nack(body.message_ref_key, body.subscription_id, body.consumer_id)
        {
            Ok(found) => {
                if found {
                    responses::Response::success(responses::AckResult { success: true })
                } else {
                    responses::Response::warning(&String::from(
                        "No message found with this id, maybe this was acked already",
                    ))
                }
            }
            Err(err) => match err {
                SubError::Error(msg) => {
                    responses::Response::error(&msg, ERROR_CODE_GENERAL_FAILURE)
                }
                SubError::TopicNotFound => {
                    responses::Response::warning(&String::from("No topic found with this id"))
                }
                SubError::SubscriptionNotFound => responses::Response::warning(&String::from(
                    "No subscription found with this id",
                )),
                SubError::PartitionNotFound => {
                    responses::Response::warning(&String::from("No partition found with this id"))
                }
                SubError::LedgerNotFound => {
                    responses::Response::warning(&String::from("No ledger found with this id"))
                }
                SubError::MessageNotFound => {
                    responses::Response::warning(&String::from("No message found with this id"))
                }
                SubError::NoneAvailable => {
                    responses::Response::no_data(&String::from("No data was available"))
                }
                SubError::FailedToAllocateConsumerId => responses::Response::error(
                    &String::from("Failed to allocate consumer id"),
                    ERROR_CODE_GENERAL_FAILURE,
                ),
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
        .and_then(consume))
}
