use std::{fs, path::PathBuf, sync::Arc};
use warp::{get, path, reply, Filter, Rejection, Reply, http::{header, StatusCode, Response}};
use crate::App;
use super::with_app;

async fn css(name: String, _app: Arc<App>) -> Result<impl Reply, Rejection> {
    if name.contains(|c| c == '\\' || c == ':') {
        Ok(Response::builder()
           .status(StatusCode::BAD_REQUEST)
           .body(String::from("Bad request"))
           .unwrap())
    } else {
        let mut path = PathBuf::from("./assets/css/");
        path.push(name);

        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(_) => return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(String::from("Not found"))
            .unwrap()),
        };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/css")
            .header(header::CACHE_CONTROL, "max-age=31536000, immutable")
            .body(content)
            .unwrap())
    }
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("assets" / "css" / String)
        .and(get()).and(with_app(app))
        .and_then(css)
}
