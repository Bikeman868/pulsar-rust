use pulsar_rust_client::api_bin_sockets::client::Client;
use pulsar_rust_net::sockets::{buffer_pool::BufferPool, MessageLength};
use std::{
    sync::{Arc, Barrier, RwLock},
    thread::{self, available_parallelism},
    time::Instant,
};

pub async fn run_test() -> super::Result<()> {
    let authority = "localhost:8001";
    let repeat_count: usize = 10000;
    let concurrency = available_parallelism().expect("Can't get the number of CPUs").get();

    let buffer_pool = Arc::new(BufferPool::new());

    let start = Arc::new(RwLock::new(Instant::now()));
    let barrier = Arc::new(Barrier::new(concurrency));

    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency);
    let message_size = 10 as MessageLength;

    for _ in 0..concurrency {
        let buffer_pool = buffer_pool.clone();
        let start = start.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            let mut client = Client::new(&buffer_pool, authority);
            client.connect();
            barrier.wait();
            *start.write().unwrap() = Instant::now();

            for _ in 0..repeat_count {
                let mut message = buffer_pool.get(message_size);
                for i in 0..message_size {
                    message[i as usize] = i as u8;
                }

                if let Err(e) = client.send(message) {
                    panic!("TestClient: {e}");
                }
            }

            for _ in 0..repeat_count {
                match client.recv() {
                    Ok(message) => {
                        buffer_pool.reuse(message);
                    }
                    Err(e) => {
                        panic!("TestClient: {e}");
                    }
                }
            }
        }))
    }

    for thread in threads {
        thread.join().expect("Failed to join worker thread");
    }

    let elapsed = start.read().unwrap().elapsed();
    println!(
        "Elapsed: {:.2?} posting {} messages {} concurrency",
        elapsed,
        concurrency * repeat_count,
        concurrency
    );
    println!(
        "Average throughput {} req/sec",
        thousands(&((concurrency * repeat_count) as f32 / (elapsed.as_micros() as f32 / 1000000.0)).floor().to_string())
    );
    println!(
        "Average latency {} µs",
        ((elapsed.as_micros() as f32) / ((concurrency * repeat_count) as f32)).floor()
    );

    Ok(())
}

fn thousands(number: &str) -> String {
    number
    .as_bytes()
    .rchunks(3)
    .rev()
    .map(std::str::from_utf8)
    .collect::<Result<Vec<&str>, _>>()
    .unwrap()
    .join(",")
}