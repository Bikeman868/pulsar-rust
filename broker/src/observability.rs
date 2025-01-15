use statsd::Client;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::time;

pub struct Metrics {
    client: Mutex<Client>,
    counts: Mutex<HashMap<String, f64>>,
}

impl Metrics {
    pub const METRIC_HTTP_PUB_MESSAGE_COUNT: &str = "http.request.pub.message.count";
    pub const METRIC_HTTP_PUB_MAPPING_COUNT: &str = "http.request.pub.mapping.count";
    pub const METRIC_HTTP_PUB_PING_COUNT: &str = "http.request.pub.ping.count";

    pub const METRIC_HTTP_SUB_MESSAGE_COUNT: &str = "http.request.sub.message.count";
    pub const METRIC_HTTP_SUB_CONSUME_COUNT: &str = "http.request.sub.consume.count";
    pub const METRIC_HTTP_SUB_NODES_COUNT: &str = "http.request.sub.nodes.count";
    pub const METRIC_HTTP_SUB_TOPICS_COUNT: &str = "http.request.sub.topics.count";
    pub const METRIC_HTTP_SUB_ACK_COUNT: &str = "http.request.sub.ack.count";
    pub const METRIC_HTTP_SUB_NACK_COUNT: &str = "http.request.sub.nack.count";
    pub const METRIC_HTTP_SUB_PING_COUNT: &str = "http.request.sub.ping.count";

    pub const METRIC_HTTP_ADMIN_COUNT: &str = "http.request.admin.count";

    pub fn new() -> Self {
        let client = statsd::Client::new("127.0.0.1:8125", "pulsar").unwrap();
        let counts = HashMap::with_capacity(200);

        Self {
            client: Mutex::new(client),
            counts: Mutex::new(counts),
        }
    }

    pub fn incr(self: &Self, metric: &str) {
        let metric = String::from(metric);
        let mut counts = self.counts.lock().unwrap();
        *counts.entry(metric).or_insert(0.0) += 1.0;
    }

    pub fn decr(self: &Self, metric: &str) {
        let metric = String::from(metric);
        let mut counts = self.counts.lock().unwrap();
        *counts.entry(metric).or_insert(0.0) += 1.0;
    }

    pub fn count(self: &Self, metric: &str, count: f64) {
        let metric = String::from(metric);
        let mut counts = self.counts.lock().unwrap();
        *counts.entry(metric).or_insert(0.0) += count;
    }

    // pub fn guage(self: &Self, metric: &str, value: f64) {
    //     let mut pipeline = self.pipeline.lock().unwrap();
    //     pipeline.gauge(metric, value);
    // }

    // pub fn timer(self: &Self, metric: &str, elapsed_millis: f64) {
    //     let mut pipeline = self.pipeline.lock().unwrap();
    //     pipeline.timer(metric, elapsed_millis);
    // }

    pub async fn run(self: &Self, stop_signal: &Arc<AtomicBool>) {
        let stop_signal = stop_signal.clone();
        while !stop_signal.load(Ordering::Relaxed) {
            time::sleep(Duration::from_millis(1000)).await;

            let client = self.client.lock().unwrap();
            let mut counts = self.counts.lock().unwrap();

            let mut pipeline = client.pipeline();
            for (metric, count) in counts.iter() {
                pipeline.count(metric, *count);
            }

            pipeline.send(&client);
            counts.clear();
        }
    }
}
