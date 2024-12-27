use super::with_app;
use crate::{
    formatting::plain_text_builder::{PlainTextBuilder, ToPlainText}, model::{
        cluster::ClusterStats, 
        ledger::LedgerStats, 
        partition::PartitionStats, 
        topic::TopicStats
    }, App
};
use pulsar_rust_net::data_types::{LedgerId, PartitionId, TopicId};
use std::sync::Arc;
use warp::{
    get, 
    header,
    path,
    http::Response,
    reply::{self},
    Filter, 
    Rejection, 
    Reply,
};

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

fn get_ledger_response(
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    accept: String,
    app: Arc<App>,
) -> warp::reply::Response {
    if let Some(stats) = app.stats_service.ledger(topic_id, partition_id, ledger_id) {
        match accept.as_str() {
            "application/json" => reply::json(&stats).into_response(),
            _ => {
                let mut builder = PlainTextBuilder::new();

                LedgerStats::to_plain_text_header(&mut builder);
                stats.to_plain_text(&mut builder);

                Response::builder()
                    .header("Content-Type", &accept)
                    .body(builder.build())
                    .into_response()
            }
        }
    } else {
        Response::builder().body("No stats available").into_response()
    }
}

fn get_partition_response(
    topic_id: TopicId,
    partition_id: PartitionId,
    accept: String,
    app: Arc<App>,
) -> warp::reply::Response {
    if let Some(stats) = app.stats_service.partition(topic_id, partition_id) {
        match accept.as_str() {
            "application/json" => reply::json(&stats).into_response(),
            _ => {
                let mut builder = PlainTextBuilder::new();

                PartitionStats::to_plain_text_header(&mut builder);
                stats.to_plain_text(&mut builder);
                
                Response::builder()
                    .header("Content-Type", &accept)
                    .body(builder.build())
                    .into_response()
            }
        }
    } else {
        Response::builder().body("No stats available").into_response()
    }
}

fn get_topic_response(
    topic_id: TopicId,
    accept: String,
    app: Arc<App>,
) -> warp::reply::Response {
    if let Some(stats) = app.stats_service.topic(topic_id) {
        match accept.as_str() {
            "application/json" => reply::json(&stats).into_response(),
            _ => {
                let mut builder = PlainTextBuilder::new();

                TopicStats::to_plain_text_header(&mut builder);
                stats.to_plain_text(&mut builder);
                
                Response::builder()
                    .header("Content-Type", &accept)
                    .body(builder.build())
                    .into_response()
            }
        }
    } else {
        Response::builder().body("No stats available").into_response()
    }
}

fn get_cluster_response(
    accept: String,
    app: Arc<App>,
) -> warp::reply::Response {
    if let Some(stats) = app.stats_service.cluster() {
        match accept.as_str() {
            "application/json" => reply::json(&stats).into_response(),
            _ => {
                let mut builder = PlainTextBuilder::new();

                ClusterStats::to_plain_text_header(&mut builder);
                stats.to_plain_text(&mut builder);

                Response::builder()
                    .header("Content-Type", &accept)
                    .body(builder.build())
                    .into_response()
            }
        }
    } else {
        Response::builder().body("No stats available").into_response()
    }
}

async fn get_ledger_stats(
    topic_id: TopicId,
    partition_id: PartitionId,
    ledger_id: LedgerId,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    Ok(get_ledger_response(topic_id, partition_id, ledger_id, accept, app))
}

async fn get_partition_stats(
    topic_id: TopicId,
    partition_id: PartitionId,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    Ok(get_partition_response(topic_id, partition_id, accept, app))
}

async fn get_topic_stats(
    topic_id: TopicId,
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    Ok(get_topic_response(topic_id, accept, app))
}

async fn get_cluster_stats(
    accept: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    Ok(get_cluster_response(accept, app))
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("stats" )
        .and(get()).and(with_accept()).and(with_app(app))
        .and_then(get_cluster_stats)
    .or(path!("stats" / "topic" / TopicId)
        .and(get()).and(with_accept()).and(with_app(app))
        .and_then(get_topic_stats))
    .or(path!("stats" / "topic" / TopicId / "partition" / PartitionId)
        .and(get()).and(with_accept()).and(with_app(app))
        .and_then(get_partition_stats))
    .or(path!("stats" / "topic" / TopicId / "partition" / PartitionId / "ledger" / LedgerId)
        .and(get()).and(with_accept()).and(with_app(app))
        .and_then(get_ledger_stats))
}
