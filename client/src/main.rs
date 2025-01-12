mod net_test_client;
mod socket_test_client;

use std::{env, error::Error};

use log::LevelFilter;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn main() {
    let mut clog = colog::default_builder();
    clog.filter_level(LevelFilter::Info);
    #[cfg(debug_assertions)]
    clog.filter_level(LevelFilter::Debug);
    clog.init();

    let args: Vec<String> = env::args().collect();
    match args.get(1) {
        Some(s) if s == "socket" => socket_test_client::run_test(),
        Some(s) if s == "net" => net_test_client::run_test(),
        Some(_) | None => {
            println!("Pass 'socket', 'hyper', or 'net' on the command line");
        }
    }
}
