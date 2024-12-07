//mod hyper_test_client;
mod socket_test_client;

use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    socket_test_client::run_test().await
    // hyper_test_client::run_test().await
}
