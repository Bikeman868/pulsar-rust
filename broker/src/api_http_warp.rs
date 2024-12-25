/*
This module provides a REST API via http.
Note that this is nowhere near as fast as the internal API which streams binary serializations
over a persistent connection. This http based API is provided for compatibility with a wide
variaty of other tools and technologies. For high performance applications, use the Rust client.
*/

use crate::App;
use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddrV4,
    sync::{atomic::Ordering, Arc},
};
use warp::{Filter, Rejection, Reply};

mod admin; // CRUD operations on nodes, topics, subscriptions and partitions
mod assets; // Serving static assets like css files
mod docs; // Documentation pages
mod html; // Trait implementations to render html snippets
mod logs; // Query the transaction logs
mod stats; // Visibility into the internal state of the broker
mod publisher; // Http API for publishing messages
mod subscriber; // Http API for consuming messages

/// This warp filter injects the application context so that handlers can access services
fn with_app<'a>(app: &Arc<App>) -> impl Filter<Extract = (Arc<App>,), Error = Infallible> + Clone {
    let app = Arc::clone(app);
    warp::any().map(move || {
        let app = &app;
        Arc::clone(app)
    })
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
        publisher::routes(app)
    .or(subscriber::routes(app))
    .or(assets::routes(app))
    .or(admin::routes(app))
    .or(stats::routes(app))
    .or(logs::routes(app))
    .or(assets::routes(app))
    .or(docs::routes())
}

pub fn serve(app: &Arc<App>, addr: SocketAddrV4) -> impl Future<Output = ()> {
    let stop_signal = app.stop_signal.clone();

    let stop_future = async move {
        while !stop_signal.load(Ordering::Relaxed) {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    };

    let (_addr, server) = warp::serve(routes(app)).bind_with_graceful_shutdown(addr, stop_future);

    println!("Web service ready on {addr}");

    server
}
