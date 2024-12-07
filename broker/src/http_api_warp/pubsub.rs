use super::with_app;
use crate::App;
use std::sync::{atomic::Ordering, Arc};
use warp::{
    get, path,
    reply::{html, json},
    Filter, Rejection, Reply,
};
use pulsar_rust_net::contracts::v1::responses::PartitionList;

type TopicName = String;

async fn get_partitions_by_topic_name(
    topic_name: String,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    match app.pub_service.partitions_by_topic_name(&topic_name) {
        Some(partitions) => Ok(json(&PartitionList::from(&*partitions))),
        None => Err(warp::reject::not_found()),
    }
}

async fn ping(app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    Ok(html("pong"))
}

pub fn routes(app: Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("v1" / "pub" / "ping")
        .and(get())
        .and(with_app(app.clone()))
        .and_then(ping)
        .or(path!("v1" / "pub" / "partitions" / TopicName)
            .and(get())
            .and(with_app(app.clone()))
            .and_then(get_partitions_by_topic_name))
}
