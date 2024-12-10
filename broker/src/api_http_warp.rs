/*
This module provides a REST API via http.
Note that this is nowhere near as fast as the internal API which streams binary serializations
over a persistent connection. This http based API is provided for compatibility with a wide
variaty of other tools and technologies. For high performance applications, use the Rust client.
*/

use crate::App;
use std::{convert::Infallible, future::Future, net::SocketAddrV4, sync::{atomic::Ordering, Arc}};
use warp::{Filter, Rejection, Reply};

mod admin;
mod docs;
mod pubsub;

fn with_app<'a>(app: &Arc<App>) -> impl Filter<Extract = (Arc<App>,), Error = Infallible> + Clone {
    let app = Arc::clone(app);
    warp::any().map(move || {
        let app = &app;
        app.request_count.fetch_add(1, Ordering::Relaxed);
        Arc::clone(app)
    })
}

pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    pubsub::routes(app)
        .or(admin::routes(app))
        .or(docs::routes())
}

pub fn serve(app: &Arc<App>, addr: SocketAddrV4) -> impl Future<Output = ()> {
    let stop_signal = app.stop_signal.clone();

    let stop_future = async move {
        while !stop_signal.load(Ordering::Relaxed) {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    };
    
    let (_addr, server) = warp::serve(routes(app))
        .bind_with_graceful_shutdown(addr, stop_future);
    
    server
}
