use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpStream,
    thread::{self, available_parallelism},
    time::Instant,
};

pub fn run_test() {
    let authority = "localhost:8000";
    let repeat_count: usize = 10000;
    let concurrency = available_parallelism()
        .expect("Can't get the number of CPUs")
        .get();

    let start = Instant::now();

    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency * 2);
    for index in 0..concurrency {
        let thread_id = index + 1;
        let request = build_publish_request(&authority);
        let worker = thread::Builder::new().name(format!("client-{}", thread_id));
        threads.push(
            worker
                .spawn(move || send_request(authority, repeat_count, request.as_bytes(), thread_id))
                .unwrap(),
        );
    }

    for index in 0..concurrency {
        let thread_id = concurrency + index + 1;
        let request = build_get_message_request(&authority);
        let worker = thread::Builder::new().name(format!("client-{}", thread_id));
        threads.push(
            worker
                .spawn(move || send_request(authority, repeat_count, request.as_bytes(), thread_id))
                .unwrap(),
        );
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
        "Average throughput {} messages/sec",
        thousands(&((concurrency * repeat_count) as f32 / (elapsed.as_micros() as f32 / 1000000.0)).floor().to_string())
    );
    println!(
        "Average latency {} µs",
        ((elapsed.as_micros() as f32) / ((concurrency * repeat_count) as f32)).floor()
    );
}

fn build_publish_request(authority: &str) -> String {
    let path = "/v1/pub/message";
    let q = "\"";
    let topic_id = rand::random::<u16>() % 2 + 1;
    let partition_id = rand::random::<u16>() % 3 + 1;
    let id: u16 = rand::random();
    let body = format!("{{ {q}topic_id{q}:{topic_id}, {q}partition_id{q}:{partition_id}, {q}attributes{q}:{{{q}message-id{q}:{q}{id}{q} }} }}");
    let len = body.len();
    let headers = format!("Host: {authority}\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: {len}\r\n");
    format!("POST {path} HTTP/1.1\r\n{headers}\r\n{body}")
}

fn build_get_message_request(authority: &str) -> String {
    let topic_id = rand::random::<u16>() % 2 + 1;
    let path = format!("/v1/sub/topic/{topic_id}/subscription/1/consumer/1/message");
    let headers = format!("Host: {authority}\r\nConnection: keep-alive\r\n");
    format!("GET {path} HTTP/1.1\r\n{headers}\r\n")
}

fn _build_ack_request(authority: &str) -> String {
    let path = "/v1/sub/ack";
    let q = "\"";
    let topic_id = rand::random::<u16>() % 2 + 1;
    let partition_id = rand::random::<u16>() % 3 + 1;
    let id: u16 = rand::random();
    let body = format!("{{ {q}topic_id{q}:{topic_id}, {q}partition_id{q}:{partition_id}, {q}attributes{q}:{{{q}message-id{q}:{q}{id}{q} }} }}");
    let len = body.len();
    let headers = format!("Host: {authority}\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: {len}\r\n");
    format!("POST {path} HTTP/1.1\r\n{headers}\r\n{body}")
}

fn send_request(authority: &str, count: usize, request: &[u8], _thread_id: usize) {
    let stream =
        TcpStream::connect(authority).expect(&format!("Failed to connect to {}", authority));

    let mut response_lines = BufReader::new(&stream).lines().map(|result| match result {
        Ok(line) => line,
        Err(err) => {
            println!("{}", err);
            String::default()
        }
    });

    let mut request_writer = BufWriter::new(&stream);

    for _ in 0..count {
        request_writer
            .write_all(&request)
            .expect("Failed to write request to stream");
        request_writer
            .flush()
            .expect("Failed to flush request stream");

        let _ = extract_response(&mut response_lines);
        // println!("{:?}", extract_response(&mut response_lines));
    }
}

fn extract_response(lines: &mut impl Iterator<Item = String>) -> Vec<String> {
    let mut result = Vec::new();
    let mut _content_length: u32 = 0;
    loop {
        match lines.next() {
            Some(line) if line.len() > 0 => {
                if line.starts_with("content-length: ") {
                    _content_length = line[16..].parse().unwrap();
                }
                result.push(line);
            }
            // Some(_) if content_length > 0 => {
            //     match lines.next() {
            //         Some(line) => result.push(line),
            //         None => { return result }
            //     }
            // }
            _ => {
                return result;
            }
        }
    }
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
