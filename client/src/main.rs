mod hyper_test_client;
mod socket_test_client;
mod net_test_client;

use std::{env, error::Error};

use log::LevelFilter;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    let mut clog = colog::default_builder();
    clog.filter_level(LevelFilter::Info);
    clog.init();

    let args: Vec<String> = env::args().collect();
    match args.get(1) {
        Some(s) if s == "socket" => socket_test_client::run_test().await,
        Some(s) if s == "hyper" => hyper_test_client::run_test().await,
        Some(s) if s == "net" => net_test_client::run_test().await,
        Some(_) | None => {
            println!("Pass 'socket', 'hyper', or 'net' on the command line");
            Ok(())
        }
    }
}
