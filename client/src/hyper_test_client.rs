use core::fmt;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http_body_util::Empty;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::task;

pub async fn run_test() -> super::Result<()> {
    let url = "http://localhost:8000/v1/admin/nodes".parse::<hyper::Uri>()?;
    let concurrency = 50;
    let repeat_count = 100;

    let authority = url.authority().expect("Failed to parse authority from URL");

    let addr = Arc::new(String::from(authority.as_str()));
    let req = Arc::new(
        Request::builder()
            .method("GET")
            .uri(url.path())
            .header(hyper::header::HOST, authority.as_str())
            .header(hyper::header::CONNECTION, "close")
            .body(Empty::<Bytes>::new())?,
    );

    // Warm up the connection
    fetch(addr.clone(), req.clone()).await?;

    let start = Instant::now();

    let mut tasks = Vec::new();

    for _ in 0..concurrency {
        tasks.push(task::spawn(fetch(addr.clone(), req.clone())));
    }

    for _ in 1..repeat_count {
        for _ in 0..concurrency {
            let task = tasks.remove(0);
            let _ = task.await?;
            tasks.push(task::spawn(fetch(addr.clone(), req.clone())));
        }
    }

    for task in tasks {
        let _ = task.await?;
    }

    let elapsed = start.elapsed();
    println!(
        "Elapsed: {:.2?} GET {} {} times with {} concurrency",
        elapsed,
        url,
        concurrency * repeat_count,
        concurrency
    );
    println!(
        "Average {} req/sec",
        1000000 * concurrency * repeat_count / elapsed.as_micros()
    );

    Ok(())
}

async fn fetch(addr: Arc<String>, req: Arc<Request<Empty<Bytes>>>) -> super::Result<()> {
    let stream = TcpStream::connect(&*addr).await?;
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
    });

    let res = sender.send_request((*req).clone()).await?;

    match res.status() {
        StatusCode::OK => Ok(()),
        _ => Err(Box::new(HttpError::new(res.status()))),
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
struct HttpError {
    pub status: StatusCode,
}

impl HttpError {
    pub fn new(status: StatusCode) -> Self {
        Self { status }
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Http status code {}", self.status)
    }
}

impl Error for HttpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
    fn description(&self) -> &str {
        ""
    }
    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}
