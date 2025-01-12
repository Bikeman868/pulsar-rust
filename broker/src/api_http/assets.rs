use super::with_app;
use crate::App;
use regex::Regex;
use std::{fs, path::PathBuf, sync::Arc};
use warp::{
    get,
    http::{header, Response, StatusCode},
    path, Filter, Rejection, Reply,
};

const CSS_FILENAME_REGEX: &str = r"^[a-z\-]+\.css$";

async fn css(name: String, _app: Arc<App>) -> Result<impl Reply, Rejection> {
    lazy_static! {
        static ref REGEX: Regex = Regex::new(CSS_FILENAME_REGEX).unwrap();
    }

    if REGEX.is_match(&name) {
        let mut path = PathBuf::from("./assets/css/");
        path.push(name);

        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(_) => {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(String::from("Not found"))
                    .unwrap())
            }
        };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/css")
            .header(header::CACHE_CONTROL, "max-age=31536000, immutable")
            .body(content)
            .unwrap())
    } else {
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(String::from("Bad request"))
            .unwrap())
    }
}

#[rustfmt::skip]
pub fn routes(app: &Arc<App>) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("assets" / "css" / String)
        .and(get()).and(with_app(app))
        .and_then(css)
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;

    #[test]
    pub fn filename_regex_tests() {
        let regex = Regex::new(CSS_FILENAME_REGEX).unwrap();
        assert_eq!(true, regex.is_match("main.css"));
        assert_eq!(false, regex.is_match("Main.css"));
        assert_eq!(true, regex.is_match("log-detail.css"));
        assert_eq!(false, regex.is_match("main.html"));
        assert_eq!(false, regex.is_match("../main.css"));
    }
}
