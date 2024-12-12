use std::sync::Arc;
use warp::{get, path, reply, Filter, Rejection, Reply};
use pulsar_rust_net::{
    contracts::v1::responses::{
        LedgerDetail, LedgerList, Message, NodeDetail, NodeList, PartitionDetail, PartitionList, TopicDetail, TopicList
    }, 
    data_types::{
        LedgerId, MessageId, NodeId, PartitionId, TopicId
    }
};
use super::with_app;
use crate::App;

async fn get_node_by_id(node_id: NodeId, app: Arc<App>) -> Result<impl Reply, Rejection> {
    match app.admin_service.node_by_id(node_id) {
        Some(node) => Ok(reply::json(&NodeDetail::from(node))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_topic_by_id(topic_id: TopicId, app: Arc<App>) -> Result<impl Reply, Rejection> {
    match app.admin_service.topic_by_id(topic_id) {
        Some(topic) => Ok(reply::json(&TopicDetail::from(topic))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_partition_by_id(
    topic_id: TopicId,
    partition_id: PartitionId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    match app.admin_service.partition_by_id(topic_id, partition_id) {
        Some(partition) => Ok(reply::json(&PartitionDetail::from(partition))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_ledger_by_id(
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    match app
        .admin_service
        .ledger_by_id(topic_id, partition_id, ledger_id)
    {
        Some(ledger) => Ok(reply::json(&LedgerDetail::from(ledger))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_message_by_id(
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    message_id: MessageId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    match app
        .admin_service
        .ledger_by_id(topic_id, partition_id, ledger_id)
    {
        Some(ledger) => {
            match ledger.peek_message(message_id) {
                Some(message) => Ok(reply::json(&Message::from(&message))),
                None => Err(warp::reject::not_found()),
            }
        },
        None => Err(warp::reject::not_found()),
    }
}

async fn get_nodes(app: Arc<App>) -> Result<impl Reply, Rejection> {
    Ok(reply::json(&NodeList::from(app.admin_service.all_nodes().as_ref())))
}

async fn get_topics(app: Arc<App>) -> Result<impl Reply, Rejection> {
    Ok(reply::json(&TopicList::from(app.admin_service.all_topics().as_ref())))
}

async fn get_topic_partitions_by_id(
    topic_id: TopicId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    match app.admin_service.partitions_by_topic_id(topic_id) {
        Some(partitions) => Ok(reply::json(&PartitionList::from(partitions.as_ref()))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_partition_ledgers_by_id(
    topic_id: TopicId,
    partition_id: PartitionId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    match app.admin_service.ledgers_by_partition_id(topic_id, partition_id) {
        Some(ledgers) => Ok(reply::json(&LedgerList::from(ledgers.as_ref()))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_ledger_messages_by_id(
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    match app.admin_service.ledger_by_id(topic_id, partition_id, ledger_id) {
        Some(ledger) => Ok(reply::json(&ledger.all_message_ids())),
        None => Err(warp::reject::not_found()),
    }
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("v1" / "admin" / "nodes")
        .and(get()).and(with_app(app))
        .and_then(get_nodes)
    .or(path!("v1" / "admin" / "node" / NodeId)
        .and(get()).and(with_app(app))
        .and_then(get_node_by_id))
    .or(path!("v1" / "admin" / "topics")
        .and(get()).and(with_app(app))
        .and_then(get_topics))
    .or(path!("v1" / "admin" / "topic" / TopicId)
        .and(get()).and(with_app(app))
        .and_then(get_topic_by_id))
    .or(path!("v1" / "admin" / "topic" / TopicId / "partitions")
        .and(get()).and(with_app(app))
        .and_then(get_topic_partitions_by_id))
    .or(path!("v1" / "admin" / "topic" / TopicId / "partition" / PartitionId)
        .and(get()).and(with_app(app))
        .and_then(get_partition_by_id))
    .or(path!("v1" / "admin" / "topic" / TopicId / "partition" / PartitionId / "ledgers")
        .and(get()).and(with_app(app))
        .and_then(get_partition_ledgers_by_id))
    .or(path!("v1" / "admin" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId)
        .and(get()).and(with_app(app))
        .and_then(get_ledger_by_id))
    .or(path!("v1" / "admin" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId / "messageids")
        .and(get()).and(with_app(app))
        .and_then(get_ledger_messages_by_id))
    .or(path!("v1" / "admin" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId / "message" / MessageId)
        .and(get()).and(with_app(app))
        .and_then(get_message_by_id))
}
