use warp::{get, path, reply::html, Filter, Rejection, Reply};

async fn root() -> Result<impl Reply, Rejection> {
    Ok(html("Root"))
}

#[rustfmt::skip]
pub fn routes() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path::end().and(get()).and_then(root)
}
