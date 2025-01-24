use pulsar_rust_client::{
    contracts::{ClientError, ConsumeResult, PublishResult}, non_blocking::{Client, FutureResponse}, BufferPool, SubscriptionId, TopicId
};
use std::{
    collections::{HashMap, VecDeque}, 
    sync::Arc, 
    thread::{self, available_parallelism}, 
    time::{Duration, Instant}
};
use tokio::{runtime::Handle, task::JoinHandle};

pub async fn run_test() {
    let authority = "localhost:8001";

    #[cfg(debug_assertions)]
    let repeat_count: usize = 3;

    #[cfg(not(debug_assertions))]
    let concurrency = available_parallelism()
        .expect("Can't get the number of CPUs")
        .get();

    #[cfg(not(debug_assertions))]
    let repeat_count: usize = 1000 * concurrency;

    let buffer_pool = Arc::new(BufferPool::new());
    let mut publish_tasks: VecDeque<JoinHandle<Result<PublishResult, ClientError>>> =
        VecDeque::new();

    let mut client = Client::new(&buffer_pool, authority);
    client.connect().unwrap();

    let topic_id: TopicId = 1;
    let subscription_id: SubscriptionId = 1;
    
    // Start a thread that will consume messages and Ack them
    let consumer_buffer_pool = buffer_pool.clone();
    let consumer_runtime_handle = Handle::current();
    let consumer_thread = thread::spawn(move || {
        let mut client = Client::new(&consumer_buffer_pool, authority);
        client.connect().unwrap();
        
        let mut remaining_messages = repeat_count;
        let mut consumer_id = None;
        let mut consume_futures: VecDeque<FutureResponse<ConsumeResult>> = VecDeque::new();

        for _ in 0..20 {
            consume_futures.push_back(client.consume(topic_id, subscription_id, &consumer_id, 50).unwrap());
        }

        while remaining_messages > 0 {
            let future = consume_futures.pop_front().unwrap();
            consume_futures.push_back(client.consume(topic_id, subscription_id, &consumer_id, 50).unwrap());
            let result = consumer_runtime_handle.block_on(future);
            match result {
                Ok(consume_result) => {
                    consumer_id = Some(consume_result.consumer_id);
                    remaining_messages = remaining_messages - consume_result.messages.len();
                    for message in consume_result.messages {
                        client.ack(&message.message_ref_key, subscription_id, consume_result.consumer_id).unwrap();
                    }
                }
                Err(err) => println!("{err:?}"),
            }
        }
    });

    let start = Instant::now();

    // Publish messages and gather futures
    for i in 0..repeat_count {
        let mut attributes = HashMap::new();
        attributes.insert(
            String::from("order_number"),
            String::from("ORDER_") + &(i + 1).to_string(),
        );

        let future = client.publish(topic_id, None, None, attributes).unwrap();
        let handle = tokio::spawn(future);

        publish_tasks.push_back(handle);
    }

    println!("Elapsed: {:.2?} publishing messages", start.elapsed());

    // Wait for publish futures to complete
    let mut counter = 0;
    while publish_tasks.len() > 0 {
        let handle = publish_tasks.pop_front().unwrap();
        if handle.is_finished() {
            match handle.await {
                Ok(result) => {
                    match result {
                        Ok(publish_result) => {
                            #[cfg(debug_assertions)]
                            println!("Message {} published successfully", publish_result.message_ref.message_id)
                        }
                        Err(_err) => {
                            #[cfg(debug_assertions)]
                            println!("Failed to publish: {_err:?}");
                        }
                    }
                }
                Err(err) => println!("Failed to await future: {err:?}"),
            }
        } else {
            publish_tasks.push_back(handle);
        }
        if counter == 500 {
            counter = 0;
            thread::sleep(Duration::from_millis(1));
        } else {
            counter = counter + 1;
        }
    }

    println!("Elapsed: {:.2?} receiving publish acknowledgements", start.elapsed());

    // Wait for consumer thread to terminate
    consumer_thread.join().unwrap();

    let elapsed = start.elapsed();
    println!("Elapsed: {:.2?} publishing, consuming and acking {} messages", elapsed, repeat_count,);
    println!(
        "Average throughput {} messages/sec",
        thousands(
            &(repeat_count as f32 / (elapsed.as_micros() as f32 / 1000000.0))
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
