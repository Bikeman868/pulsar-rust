use crate::App;
use std::{
    io::{prelude::*, BufReader},
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::{atomic::Ordering, mpsc, Arc},
    thread::{self, available_parallelism, Thread},
};

pub fn run(app: Arc<App>, ipv4: Ipv4Addr, port: u16) {
    let authority = format!("{}:{}", ipv4, port);
    let listener = TcpListener::bind(authority).unwrap();

    let concurrency = available_parallelism().unwrap().get();
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(concurrency);
    let mut senders: Vec<mpsc::Sender<TcpStream>> = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let (sender, receiver) = mpsc::channel();
        senders.push(sender);

        let app = app.clone();
        threads.push(thread::spawn(move || process_connections(app, receiver)))
    }

    let mut thread_index = 0;
    for stream in listener.incoming() {
        senders[thread_index].send(stream.unwrap()).unwrap();
        thread_index = (thread_index + 1) % concurrency;
    }
}

fn process_connections(app: Arc<App>, receiver: mpsc::Receiver<TcpStream>) {
    loop {
        let stream = receiver.recv().unwrap();
        handle_connection(app.clone(), stream);
    }
}

fn handle_connection(app: Arc<App>, mut stream: TcpStream) {
    let buf_reader = BufReader::new(&stream);
    let _http_request: Vec<_> = buf_reader
        .lines()
        .map(|result| result.unwrap())
        .take_while(|line| !line.is_empty())
        .collect();

    let response = "HTTP/1.1 200 OK\r\n\r\n";
    stream.write_all(response.as_bytes()).unwrap();

    app.clone()
        .request_count
        .clone()
        .fetch_add(1, Ordering::Relaxed);
}
