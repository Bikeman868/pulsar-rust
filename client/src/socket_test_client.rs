use std::{
    io::{BufRead, BufReader, BufWriter, Write}, 
    net::TcpStream, 
    thread::{self, available_parallelism}, 
    time::Instant,
};

pub async fn run_test() -> super::Result<()> {
    let authority = "localhost:8000";
    let path = "/v1/admin/nodes";
    let repeat_count: usize = 100000;
    let concurrency = available_parallelism().expect("Can't get the number of CPUs").get();

    let request_text = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n", path, authority);

    let start = Instant::now();

    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency);
    for index in 0..concurrency {
        let thread_id = index + 1;
        let request = request_text.clone();
        let worker = thread::Builder::new()
            .name(format!("client-{}", thread_id));
        threads.push(worker.spawn(move || make_request(authority, repeat_count, request.as_bytes(), thread_id)).unwrap());
    }
    for thread in threads {
        thread.join().expect("Failed to join worker thread");
    }

    let elapsed = start.elapsed();
    println!(
        "Elapsed: {:.2?} GET {}{} {} times with {} concurrency",
        elapsed,
        authority,
        path,
        concurrency * repeat_count,
        concurrency
    );
    println!(
        "Average {} req/sec",
        ((concurrency * repeat_count) as f32 / (elapsed.as_micros() as f32 / 1000000.0)).floor()
    );

    Ok(())
}

fn make_request(authority: &str, count: usize, request: &[u8], _thread_id: usize) {
    let stream = TcpStream::connect(authority).expect(&format!("Failed to connect to {}", authority));

    let mut response_lines = BufReader::new(&stream)
        .lines()
        .map(|result| match result {
            Ok(line) => line,
            Err(err) => {
                println!("{}", err);
                String::default()
            },
        });

    let mut request_writer = BufWriter::new(&stream);

    for _ in 0..count {
        request_writer.write_all(&request).expect("Failed to write request to stream");
        request_writer.flush().expect("Failed to flush request stream");

        let _ = extract_response(&mut response_lines);
    }
}

fn extract_response(lines: &mut impl Iterator<Item = String>) -> Vec<String> {
    let mut result = Vec::new();
    loop {
        match lines.next() {
            Some(line) if line.len() > 0 => { result.push(line); },
            _ => { return result; }
        }
    }
}
