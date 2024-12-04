/*
This module provides a REST API via http.
Note that this is nowhere near as fast as the internal API which streams binary serializations
over a persistent connection. This http based API is provided for compatibility with a wide
variaty of other tools and technologies. For high performance applications, use the Rust client.
*/

use crate::App;
use std::{convert::Infallible, net::Ipv4Addr, sync::Arc};
use warp::{Filter, Rejection, Reply};

mod admin;
mod docs;
mod pubsub;
mod requests;
mod responses;

fn with_app<'a>(app: Arc<App>) -> impl Filter<Extract = (Arc<App>,), Error = Infallible> + Clone {
    warp::any().map(move || app.clone())
}

pub fn routes(app: Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    pubsub::routes(app.clone())
        .or(admin::routes(app.clone()))
        .or(docs::routes())
}

pub async fn run(app: Arc<App>, ipv4: Ipv4Addr, port: u16) {
    warp::serve(routes(app.clone())).run((ipv4, port)).await
}
