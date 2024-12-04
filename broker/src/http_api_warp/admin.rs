use super::responses::{
    CatalogSummary, NodeList, NodeSummary, PartitionList, PartitionSummary, TopicList, TopicSummary,
};
use super::with_app;
use crate::model::data_types::{CatalogId, NodeId, PartitionId, TopicId};
use crate::App;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use warp::{get, path, reply::json, Filter, Rejection, Reply};

async fn get_node_by_id(node_id: NodeId, app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    match app.admin_service.node_by_id(node_id) {
        Some(node) => Ok(json(&NodeSummary::from(node))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_topic_by_id(topic_id: TopicId, app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    match app.admin_service.topic_by_id(topic_id) {
        Some(topic) => Ok(json(&TopicSummary::from(topic))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_partition_by_id(
    topic_id: TopicId,
    partition_id: PartitionId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    match app.admin_service.partition_by_id(topic_id, partition_id) {
        Some(partition) => Ok(json(&PartitionSummary::from(partition))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_catalog_by_id(
    topic_id: TopicId,
    partition_id: PartitionId,
    catalog_id: CatalogId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    match app
        .admin_service
        .catalog_by_id(topic_id, partition_id, catalog_id)
    {
        Some(catalog) => Ok(json(&CatalogSummary::from(catalog))),
        None => Err(warp::reject::not_found()),
    }
}

async fn get_nodes(app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    Ok(json(&NodeList::from(&app.admin_service.all_nodes())))
}

async fn get_topics(app: Arc<App>) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    Ok(json(&TopicList::from(&app.admin_service.all_topics())))
}

async fn get_topic_partitions_by_id(
    topic_id: TopicId,
    app: Arc<App>,
) -> Result<impl Reply, Rejection> {
    app.request_count.clone().fetch_add(1, Ordering::Relaxed);
    match app.admin_service.partitions_by_topic_id(topic_id) {
        Some(partitions) => Ok(json(&PartitionList::from(&partitions))),
        None => Err(warp::reject::not_found()),
    }
}

pub fn routes(app: Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("v1" / "admin" / "node" / NodeId)
        .and(get())
        .and(with_app(app.clone()))
        .and_then(get_node_by_id)
        .or(path!("v1" / "admin" / "topic" / TopicId)
            .and(get())
            .and(with_app(app.clone()))
            .and_then(get_topic_by_id))
        .or(
            path!("v1" / "admin" / "topic" / TopicId / "partition" / PartitionId)
                .and(get())
                .and(with_app(app.clone()))
                .and_then(get_partition_by_id),
        )
        .or(path!(
            "v1" / "admin" / "topic" / TopicId / "partition" / PartitionId / "catalog" / CatalogId
        )
        .and(get())
        .and(with_app(app.clone()))
        .and_then(get_catalog_by_id))
        .or(path!("v1" / "admin" / "nodes")
            .and(get())
            .and(with_app(app.clone()))
            .and_then(get_nodes))
        .or(path!("v1" / "admin" / "topics")
            .and(get())
            .and(with_app(app.clone()))
            .and_then(get_topics))
        .or(path!("v1" / "admin" / "topic" / TopicId / "partitions")
            .and(get())
            .and(with_app(app.clone()))
            .and_then(get_topic_partitions_by_id))
}
