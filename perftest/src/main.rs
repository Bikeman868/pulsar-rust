mod async_test_client;
mod socket_test_client;
mod sync_test_client;

use log::LevelFilter;
use std::env;

#[tokio::main]
async fn main() {
    let mut clog = colog::default_builder();

    #[cfg(debug_assertions)]
    clog.filter_level(LevelFilter::Debug);
    
    #[cfg(not(debug_assertions))]
    clog.filter_level(LevelFilter::Warn);

    clog.init();

    let args: Vec<String> = env::args().collect();
    match args.get(1) {
        Some(s) if s == "socket" => socket_test_client::run_test(),
        Some(s) if s == "async" => async_test_client::run_test().await,
        Some(s) if s == "sync" => sync_test_client::run_test(),
        Some(_) | None => {
            println!("Pass 'socket', 'async' or 'sync' on the command line");
        }
    }
}
