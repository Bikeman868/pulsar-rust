use std::{
    io::{
        self, prelude::*, BufReader, BufWriter
    }, 
    net::{Ipv4Addr, TcpListener, TcpStream}, 
    sync::{atomic::{AtomicBool, Ordering}, mpsc, Arc}, 
    thread::{self, available_parallelism}, time::Duration
};
use crate::App;

pub fn run(app: Arc<App>, ipv4: Ipv4Addr, port: u16, stop_signal: Arc<AtomicBool>) {
    let concurrency = available_parallelism().expect("Can't get number of CPUs").get();
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency);
    let mut senders: Vec<mpsc::Sender<Arc<TcpStream>>> = Vec::with_capacity(concurrency);

    let authority = format!("{}:{}", ipv4, port);
    let listener = TcpListener::bind(authority).expect(&format!("Failed to bind to port {}", port));

    // Non-blocking mode simply does not work!!
    // listener.set_nonblocking(true).expect("Can't set TcpListener to non-blocking");

    for index in 0..concurrency {
        let (sender, receiver) = mpsc::channel();
        senders.push(sender);

        let app = app.clone();
        let stop_signal = stop_signal.clone();
        threads.push(thread::spawn(move || process_connections(app, index + 1, receiver, stop_signal)))
    }

    let mut thread_index = 0;
    let mut incomming = listener.incoming();
    while !stop_signal.clone().load(Ordering::Relaxed) {
        match incomming.next() {
            Some(result) => {
                let stream = match result { 
                    Ok(stream) => Arc::new(stream), 
                    Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => { continue; },
                    Err(err) => {
                        println!("Error from TcpListener incomming next. {}", err);
                        break;
                    },
                };
                if let Err(err) = senders[thread_index].send(stream.clone()) {
                    send_service_unavailable(stream.clone());
                    println!("Failed to queue request to worker thread. {}", err);
                    break;
                }
                thread_index = (thread_index + 1) % concurrency;
            },
            None => { }, // TcpListener never returns None
        }
    }

    println!("Waiting for worker threads to terminate");

    for thread in threads {
        if let Err(e) = thread.join() {
            println!("Thread join failed {:?}", e);
        }
    }

    println!("All worker threads have terminated");
}

fn process_connections(app: Arc<App>, thread_id: usize, receiver: mpsc::Receiver<Arc<TcpStream>>, stop_signal: Arc<AtomicBool>) {
    while !stop_signal.clone().load(Ordering::Relaxed) {
        match receiver.try_recv() {
            Ok(stream) => handle_connection(app.clone(), thread_id, stream, stop_signal.clone()),
            Err(err) => match err {
                mpsc::TryRecvError::Empty => {},
                mpsc::TryRecvError::Disconnected => {
                    println!("Worker thread {} terminating because channel is disconncted", thread_id);
                    return
                },
            },
        }
    }
    println!("Worker thread {} terminating because application is exiting", thread_id);
}

fn send_service_unavailable(stream: Arc<TcpStream>) {
    println!("Sending service unavailable response");
    let response = "HTTP/1.1 503 Service Unavailable\r\n\r\n".as_bytes();
    let mut response_writer = BufWriter::new(&*stream);
    let _ = response_writer.write_all(response);
}

fn handle_connection(app: Arc<App>, thread_id: usize, stream: Arc<TcpStream>, stop_signal: Arc<AtomicBool>) {
    let stream = stream.clone();
    let mut request_lines = BufReader::new(&*stream)
        .lines()
        .map(|result| match result {
            Ok(line) => line,
            Err(err) => {
                println!("Worker thread {} detected connection closed by client", thread_id);
                String::default()
            },
        });

    let mut response_writer = BufWriter::new(&*stream);

    let mut keep_alive = true;
    while keep_alive {
        keep_alive = false; // This is the default in HTTP/1.0
        
        let http_request = extract_request(&mut request_lines);

        if http_request.len() == 0 {
            println!("Worker thread {} closing connection because client closed their end", thread_id);
            return;
        } else {
            let is_processing = !stop_signal.clone().load(Ordering::Relaxed);
            for line in http_request {
                if is_processing {
                    let line_lower = line.to_lowercase();
                    if line_lower.contains("http/1.1") || line_lower == "connection: keep-alive" { keep_alive = true }
                    else if line_lower == "connection: close" { keep_alive = false; }
                }
            }
        }
        
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".as_bytes();
        match response_writer.write_all(response) {
            Ok(_) => { 
                match response_writer.flush(){
                    Ok(_) => (),
                    Err(err) => { 
                        println!("Worker thread {} failed to flush response stream. {}", thread_id, err);
                        return; 
                    }
                }
            },
            Err(err) => { 
                println!("Worker thread {} failed to write to response stream. {}", thread_id, err);
                return; 
            }
        };

        app.clone().request_count.fetch_add(1, Ordering::Relaxed);
    }
    println!("Worker thread {} closing the connection", thread_id);
}

fn extract_request(lines: &mut impl Iterator<Item = String>) -> Vec<String> {
    let mut result = Vec::new();
    loop {
        match lines.next() {
            Some(line) if line.len() > 0 => { result.push(line); },
            _ => { return result; }
        }
    }
}
