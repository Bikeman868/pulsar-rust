use log::error;
use pulsar_rust_client::api_bin_sockets::client::Client;
use pulsar_rust_net::sockets::{buffer_pool::BufferPool, MessageLength};
use std::{
    sync::Arc,
    thread::{self, available_parallelism},
    time::Instant,
};

pub async fn run_test() -> super::Result<()> {
    let authority = "localhost:8001";
    let repeat_count: usize = 10000;
    let concurrency = available_parallelism()
        .expect("Can't get the number of CPUs")
        .get();

    let buffer_pool = Arc::new(BufferPool::new());

    let start = Instant::now();

    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency * 2);

    for _ in 0..concurrency {
        let buffer_pool = buffer_pool.clone();
        threads.push(thread::spawn(move || {
            let client = Client::new(&buffer_pool, authority);
            let message_size = 30 as MessageLength;

            let start_producing = Instant::now();

            for _ in 0..repeat_count {
                let mut message = buffer_pool.get(message_size);
                for i in 0..message_size {
                    message[i as usize] = i as u8;
                }

                if let Err(e) = client.send(message) {
                    error!("{e}");
                }
            }

            println!(
                "Elapsed: {:.2?} producing {} messages. Throughput {} req/sec",
                start_producing.elapsed(),
                repeat_count,
                (repeat_count as f32 / (start_producing.elapsed().as_micros() as f32 / 1000000.0))
                    .floor()
            );

            let start_consumingg = Instant::now();

            for _ in 0..repeat_count {
                match client.recv() {
                    Ok(message) => {
                        buffer_pool.reuse(message);
                    }
                    Err(e) => {
                        error!("{e}");
                    }
                }
            }

            println!(
                "Elapsed: {:.2?} consuming {} messages. Throughput {} req/sec",
                start_consumingg.elapsed(),
                repeat_count,
                (repeat_count as f32 / (start_consumingg.elapsed().as_micros() as f32 / 1000000.0))
                    .floor()
            );
        }))
    }

    for thread in threads {
        thread.join().expect("Failed to join worker thread");
    }

    let elapsed = start.elapsed();
    println!(
        "Elapsed: {:.2?} posting {} messages {} concurrency",
        elapsed,
        concurrency * repeat_count,
        concurrency
    );
    println!(
        "Average throughput {} req/sec",
        ((concurrency * repeat_count) as f32 / (elapsed.as_micros() as f32 / 1000000.0)).floor()
    );
    println!(
        "Average latency {} µs",
        ((elapsed.as_micros() as f32) / ((concurrency * repeat_count) as f32)).floor()
    );

    Ok(())
}

struct TestThread {}

impl TestThread {
    fn run(self: Self) {}
}
