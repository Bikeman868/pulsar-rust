use super::with_app;
use crate::{
    formatting::html_builder::{HtmlBuilder, ToHtml},
    persistence::{event_logger::EventQueryOptions, PersistenceLayer},
    App,
};
use pulsar_rust_net::{
    contracts::v1::responses::{LogEntry, LogEntrySummary},
    data_types::{LedgerId, MessageId, PartitionId, TopicId},
    display::JoinableToString,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use warp::{
    get, header,
    http::Response,
    path, query,
    reply::{self},
    Filter, Rejection, Reply,
};

#[derive(Serialize, Deserialize)]
struct LogParams {
    limit: Option<usize>,
    detailed: Option<bool>,
    exact: Option<bool>,
}

fn with_params() -> impl Filter<Extract = (LogParams,), Error = warp::Rejection> + Clone {
    query::<LogParams>()
}

fn with_accept() -> impl Filter<Extract = (String,), Error = warp::Rejection> + Clone {
    const DEFAULT_ACCEPT: &str = "text/plain";
    header::optional::<String>("accept").map(|accept: Option<String>| {
        String::from(
            accept
                .unwrap_or(String::from(DEFAULT_ACCEPT))
                .split([',', ';'])
                .next()
                .unwrap_or(DEFAULT_ACCEPT),
        )
    })
}

fn get_options(params: LogParams, default_exact: bool) -> EventQueryOptions {
    EventQueryOptions {
        include_serialization: params.detailed.unwrap_or(false),
        descending: true,
        skip: 0,
        take: params.limit.unwrap_or(20),
        exact_match: params.exact.unwrap_or(default_exact),
    }
}

fn get_detailed_events(
    app: &Arc<App>,
    prefix: String,
    options: EventQueryOptions,
) -> Vec<LogEntry> {
    app.peristence
        .events_by_key_prefix(&prefix, &options)
        .map(|entry| LogEntry::from(&entry))
        .collect()
}

fn get_summary_events(
    app: &Arc<App>,
    prefix: String,
    options: EventQueryOptions,
) -> Vec<LogEntrySummary> {
    app.peristence
        .events_by_key_prefix(&prefix, &options)
        .map(|entry| LogEntrySummary::from(&entry))
        .collect()
}

fn get_detailed_response(
    app: &Arc<App>,
    params: LogParams,
    accept: String,
    prefix: String,
    default_exact: bool,
) -> warp::reply::Response {
    let events = get_detailed_events(app, prefix, get_options(params, default_exact));
    match accept.as_str() {
        "text/html" => {
            let writer = HtmlBuilder::new(events);

            writer.html(|w, _| {
                w.head("Log Entries", |w, _| {
                    w.css("/assets/css/main.css");
                    w.css("/assets/css/relaxed.css");
                    w.css("/assets/css/log-detail.css");
                });
                w.body("log-entries", |w, events| {
                    w.h1("page-title", "Event log detail");
                    events.to_html(&w);
                });
            });

            Response::builder()
                .header("Content-Type", &accept)
                .body(writer.build())
                .into_response()
        }
        "text/plain" => Response::builder()
            .header("Content-Type", &accept)
            .body(events.join("\n"))
            .into_response(),
        _ => reply::json(&events).into_response(),
    }
}

fn get_summary_response(
    app: &Arc<App>,
    params: LogParams,
    accept: String,
    prefix: String,
    default_exact: bool,
) -> warp::reply::Response {
    let events = get_summary_events(app, prefix, get_options(params, default_exact));
    match accept.as_str() {
        "text/html" => {
            let writer = HtmlBuilder::new(events);

            writer.html(|w, _| {
                w.head("Log Entries", |w, _| {
                    w.css("/assets/css/main.css");
                    w.css("/assets/css/compact.css");
                    w.css("/assets/css/log-summmary.css");
                });
                w.body("log-entries", |w, events| {
                    w.h1("page-title", "Event log summary");
                    events.to_html(&w);
                });
            });

            Response::builder()
                .header("Content-Type", &accept)
                .body(writer.build())
                .into_response()
        }
        "text/plain" => Response::builder()
            .header("Content-Type", &accept)
            .body(events.join("\n"))
            .into_response(),
        _ => reply::json(&events).into_response(),
    }
}

fn get_events(
    app: Arc<App>,
    params: LogParams,
    accept: String,
    prefix: String,
    default_exact: bool,
) -> Result<impl Reply, Rejection> {
    if params.detailed.unwrap_or(false) {
        Ok(get_detailed_response(
            &app,
            params,
            accept,
            prefix,
            default_exact,
        ))
    } else {
        Ok(get_summary_response(
            &app,
            params,
            accept,
            prefix,
            default_exact,
        ))
    }
}

async fn get_cluster_log(
    params: LogParams,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    get_events(app, params, accept, String::default(), false)
}

async fn get_topic_log(
    topic_id: TopicId,
    params: LogParams,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    get_events(
        app,
        params,
        accept,
        PersistenceLayer::build_topic_prefix(topic_id),
        false,
    )
}

async fn get_partition_log(
    topic_id: TopicId,
    partition_id: PartitionId,
    params: LogParams,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    get_events(
        app,
        params,
        accept,
        PersistenceLayer::build_partition_prefix(topic_id, partition_id),
        false,
    )
}

async fn get_ledger_log(
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    params: LogParams,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    get_events(
        app,
        params,
        accept,
        PersistenceLayer::build_ledger_prefix(topic_id, partition_id, ledger_id),
        false,
    )
}

async fn get_message_log(
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    message_id: MessageId,
    params: LogParams,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    get_events(
        app,
        params,
        accept,
        PersistenceLayer::build_message_prefix(topic_id, partition_id, ledger_id, message_id),
        true,
    )
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("v1" / "logs" )
        .and(get()).and(with_params()).and(with_accept()).and(with_app(app))
        .and_then(get_cluster_log)
    .or(path!("v1" / "logs" / "topic" / TopicId)
        .and(get()).and(with_params()).and(with_accept()).and(with_app(app))
        .and_then(get_topic_log))
    .or(path!("v1" / "logs" / "topic" / TopicId / "partition" / PartitionId)
        .and(get()).and(with_params()).and(with_accept()).and(with_app(app))
        .and_then(get_partition_log))
    .or(path!("v1" / "logs" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId)
        .and(get()).and(with_params()).and(with_accept()).and(with_app(app))
        .and_then(get_ledger_log))
    .or(path!("v1" / "logs" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId / "message" / MessageId)
        .and(get()).and(with_params()).and(with_accept()).and(with_app(app))
        .and_then(get_message_log))
}
