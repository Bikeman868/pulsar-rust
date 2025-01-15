use pulsar_rust_client::api_bin::client::Client;
use pulsar_rust_net::{
    data_types::{SubscriptionId, TopicId},
    sockets::buffer_pool::BufferPool,
};
use std::{
    collections::HashMap,
    sync::{Arc, Barrier, RwLock},
    thread::{self, available_parallelism},
    time::Instant,
};

pub fn run_test() {
    let authority = "localhost:8001";

    #[cfg(debug_assertions)]
    let repeat_count: usize = 3;
    #[cfg(debug_assertions)]
    let concurrency = 1;

    #[cfg(not(debug_assertions))]
    let repeat_count: usize = 10000;
    #[cfg(not(debug_assertions))]
    let concurrency = available_parallelism()
        .expect("Can't get the number of CPUs")
        .get();

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
            client.connect();

            // Wait for all threads to have a connection before strting the timer
            barrier.wait();
            *start.write().unwrap() = Instant::now();

            // Publish a message
            let topic_id: TopicId = 1;
            let mut attributes = HashMap::new();
            attributes.insert(String::from("order_number"), String::from("ABC123"));
            let publish_result = client.publish(topic_id, None, None, attributes).unwrap();

            #[cfg(debug_assertions)]
            println!("Published {:?}", publish_result.message_ref);

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

            // for _ in 0..repeat_count {
            //     request_id += 1;

            //     let payload = NegotiateVersion{ min_version: 1, max_version: 1 };
            //     let request = Request{
            //         request_id,
            //         payload: RequestPayload::NegotiateVersion(payload),
            //     };

            //     #[cfg(debug_assertions)]
            //     debug!("TestClient: Sending {:?}", request);

            //     let message = serializer.serialize_request(&request).unwrap();

            //     if let Err(e) = client.send(message) {
            //         panic!("TestClient: {e}");
            //     }
            // }

            // for _ in 0..repeat_count {
            //     match client.recv() {
            //         Ok(message) => {
            //             match serializer.deserialize_response(message) {
            //                 Ok(response) => {
            //                     #[cfg(debug_assertions)]
            //                     debug!("TestClient: Received {:?}", response);
            //                 },
            //                 Err(err) => match err {
            //                     DeserializeError::Error { msg } => panic!("TestClient: {}", msg),
            //                 }
            //             }
            //         }
            //         Err(err) => { panic!("TestClient: {err}"); }
            //     }
            // }
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
        thousands(
            &((concurrency * repeat_count) as f32 / (elapsed.as_micros() as f32 / 1000000.0))
                .floor()
                .to_string()
        )
    );
    println!(
        "Average latency {} µs",
        ((elapsed.as_micros() as f32) / ((concurrency * repeat_count) as f32)).floor()
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
