use std::{
    sync::Arc, 
    thread::available_parallelism, 
    time::Instant
};
use log::{error,info};
use pulsar_rust_net::sockets::{buffer_pool::BufferPool, MessageLength};
use pulsar_rust_client::api_bin_sockets::client::Client;

pub async fn run_test() -> super::Result<()> {
    let authority = "localhost:8001";
    let repeat_count: usize = 1000;
    let concurrency = available_parallelism()
        .expect("Can't get the number of CPUs")
        .get();

    let buffer_pool = Arc::new(BufferPool::new());
    let client = Client::new(&buffer_pool, authority);

    let start = Instant::now();

    // let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency);
    // for index in 0..concurrency {
    //     let thread_id = index + 1;
    //     let request = build_publish_request(&authority);
    //     let worker = thread::Builder::new().name(format!("client-{}", thread_id));
    //     threads.push(
    //         worker
    //             .spawn(move || send_request(authority, repeat_count, request.as_bytes(), thread_id))
    //             .unwrap(),
    //     );
    // }

    let message_size = 30 as MessageLength;
    for _ in 0..repeat_count {
        let mut message = buffer_pool.get(message_size);
        for i in 1..=message_size {
            message.push(i as u8);
        }
        
        if let Err(e) = client.send(message) { 
            error!("{e}");
        }

        match client.recv() {
            Ok(message) => {
                buffer_pool.reuse(message);
            }
            Err(e) => { 
                error!("{e}");
            }
        }
    }

    // for thread in threads {
    //     thread.join().expect("Failed to join worker thread");
    // }

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

struct TestThread {

}

impl TestThread {
    fn run(self: Self) {

    }
}