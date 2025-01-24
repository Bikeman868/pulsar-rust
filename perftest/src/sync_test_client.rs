use pulsar_rust_client::{blocking::Client, BufferPool, SubscriptionId, TopicId};
#[cfg(not(debug_assertions))]
use std::thread::available_parallelism;
use std::{
    collections::HashMap,
    sync::{Arc, Barrier, RwLock},
    thread::{self},
    time::Instant,
};

pub fn run_test() {
    let authority = "localhost:8001";

    #[cfg(debug_assertions)]
    let repeat_count: usize = 3;

    #[cfg(debug_assertions)]
    let concurrency = 1;

    #[cfg(not(debug_assertions))]
    let repeat_count: usize = 100;
    
    #[cfg(not(debug_assertions))]
    let concurrency = available_parallelism()
        .expect("Can't get the number of CPUs")
        .get();
    let concurrency = 1;

    let buffer_pool = Arc::new(BufferPool::new());

    let start = Arc::new(RwLock::new(Instant::now()));
    let barrier = Arc::new(Barrier::new(concurrency));

    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let buffer_pool = buffer_pool.clone();
        let start = start.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            let mut client = Client::new(&buffer_pool, authority);
            client.connect().unwrap();

            // Wait for all threads to have a connection before strting the timer
            barrier.wait();
            *start.write().unwrap() = Instant::now();

            for _ in 0..repeat_count {
                // Publish a message
                let topic_id: TopicId = 1;
                let mut attributes = HashMap::new();
                attributes.insert(String::from("order_number"), String::from("ABC123"));
                match client.publish(topic_id, None, None, attributes) {
                    Ok(publish_result) => {
                        #[cfg(debug_assertions)]
                        println!("Published {:?}", publish_result.message_ref.message_id);

                        /*
                        // Consume a message
                        let subscription_id: SubscriptionId = 1;
                        let consume_result = client.consume(topic_id, subscription_id, None, 1).unwrap();

                        #[cfg(debug_assertions)]
                        println!("Allocated consumer id: {}", consume_result.consumer_id);

                        // Ack the message
                        for message in consume_result.messages {
                            #[cfg(debug_assertions)]
                            println!("{:?}", message);

                            let _ = client.ack(
                                &message.message_ref_key,
                                subscription_id,
                                consume_result.consumer_id,
                            );
                        }
                        */
                    }
                    Err(_) => (),
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
        "Average throughput {} messages/sec",
        thousands(
            &((concurrency * repeat_count) as f32 / (elapsed.as_micros() as f32 / 1000000.0))
                .floor()
                .to_string()
        )
    );
    println!(
        "Average latency {} µs",
        thousands(
            &((elapsed.as_micros() as f32) / (repeat_count as f32))
                .floor()
                .to_string()
        )
    );
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
