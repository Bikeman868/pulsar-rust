use std::sync::Arc;
use serde::{Deserialize, Serialize};
use warp::{get, path, query, reply, Filter, Rejection, Reply};
use crate::{persistence::{event_logger::EventQueryOptions, PersistenceLayer}, App};
use pulsar_rust_net::{
    contracts::v1::responses::{LogEntry, LogEntrySummary}, data_types::{LedgerId, MessageId, PartitionId, TopicId}
};
use super::with_app;

#[derive(Serialize, Deserialize)]
struct LogParams {
    limit: Option<usize>,
    detailed: Option<bool>,
}

fn with_params() -> impl Filter<Extract = (LogParams,), Error = warp::Rejection> + Clone {
    query::<LogParams>()
}

fn get_options(params: LogParams) -> EventQueryOptions {
    EventQueryOptions{
        include_serialization: params.detailed.unwrap_or(false),
        descending: true,
        skip: 0,
        take: params.limit.unwrap_or(20),
    }
}

fn get_detailed_events(app: Arc<App>, prefix: String, options: EventQueryOptions) -> Vec<LogEntry> {
    app.peristence.events_by_key_prefix(&prefix, &options).map(|entry|LogEntry::from(&entry)).collect()
}

fn get_summary_events(app: Arc<App>, prefix: String, options: EventQueryOptions) -> Vec<LogEntrySummary> {
    app.peristence.events_by_key_prefix(&prefix, &options).map(|entry|LogEntrySummary::from(&entry)).collect()
}

fn get_events(app: Arc<App>, params: LogParams, prefix: String) -> Result<impl Reply, Rejection> {
    if params.detailed.unwrap_or(false) {
        let events = get_detailed_events(app, prefix, get_options(params));
        Ok(reply::json(&events))
    } else {
        let events = get_summary_events(app, prefix, get_options(params));
        Ok(reply::json(&events))
    }
}

async fn get_cluster_log(params: LogParams, app: Arc<App>) -> Result<impl Reply, Rejection> {
    get_events(app, params, String::default())
}

async fn get_topic_log(topic_id: TopicId, params: LogParams, app: Arc<App>) -> Result<impl Reply, Rejection> {
    get_events(app, params, PersistenceLayer::build_topic_prefix(topic_id))
}

async fn get_partition_log(topic_id: TopicId, partition_id: PartitionId, params: LogParams, app: Arc<App>) -> Result<impl Reply, Rejection> {
    get_events(app, params, PersistenceLayer::build_partition_prefix(topic_id, partition_id))
}

async fn get_ledger_log(topic_id: TopicId, partition_id: PartitionId, ledger_id: LedgerId, params: LogParams, app: Arc<App>) -> Result<impl Reply, Rejection> {
    get_events(app, params, PersistenceLayer::build_ledger_prefix(topic_id, partition_id, ledger_id))
}

async fn get_message_log(topic_id: TopicId, partition_id: PartitionId, ledger_id: LedgerId, message_id: MessageId, params: LogParams, app: Arc<App>) -> Result<impl Reply, Rejection> {
    get_events(app, params, PersistenceLayer::build_message_prefix(topic_id, partition_id, ledger_id, message_id))
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("v1" / "logs" )
        .and(get()).and(with_params()).and(with_app(app))
        .and_then(get_cluster_log)
    .or(path!("v1" / "logs" / "topic" / TopicId)
        .and(get()).and(with_params()).and(with_app(app))
        .and_then(get_topic_log))
    .or(path!("v1" / "logs" / "topic" / TopicId / "partition" / PartitionId)
        .and(get()).and(with_params()).and(with_app(app))
        .and_then(get_partition_log))
    .or(path!("v1" / "logs" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId)
        .and(get()).and(with_params()).and(with_app(app))
        .and_then(get_ledger_log))
    .or(path!("v1" / "logs" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId / "message" / MessageId)
        .and(get()).and(with_params()).and(with_app(app))
        .and_then(get_message_log))
}
