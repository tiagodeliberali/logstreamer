use std::collections::VecDeque;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

const C_KEY: u8 = 99;
const P_KEY: u8 = 112;
const Q_KEY: u8 = 113;

fn main() {
    let queue: VecDeque<String> = VecDeque::new();
    let queue = Arc::new(Mutex::new(queue));

    let listener = match TcpListener::bind("127.0.0.1:8080") {
        Ok(listener) => listener,
        Err(err) => panic!("Failed to bind address\r\n{}", err),
    };

    for stream in listener.incoming() {
        let cloned_queue = queue.clone();
        match stream {
            Ok(valid_stream) => {
                thread::spawn(move || {
                    handle_connection(valid_stream, cloned_queue);
                });
            }
            Err(err) => println!("Failed to process current stream\n{}", err),
        };
    }
}

fn handle_connection(mut stream: TcpStream, queue: Arc<Mutex<VecDeque<String>>>) {
    loop {
        let mut buffer = [0; 512];
        let size = match stream.read(&mut buffer) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to read stream\n{}", err);
                return;
            }
        };

        match &buffer[0] {
            &P_KEY => store_data(&stream, &buffer, size, queue.clone()),
            &C_KEY => read_data(&stream, queue.clone()),
            &Q_KEY => return,
            _ => continue,
        }
    }
}

fn store_data(
    mut stream: &TcpStream,
    buffer: &[u8; 512],
    size: usize,
    queue: Arc<Mutex<VecDeque<String>>>,
) {
    let content = String::from_utf8_lossy(&buffer[1..size]).to_string();
    // println!("[APPEND] {}", content);
    queue.lock().unwrap().push_front(content);
    stream.write_all(b"ok\r\n").unwrap();
}

fn read_data(mut stream: &TcpStream, queue: Arc<Mutex<VecDeque<String>>>) {
    if let Some(content) = queue.lock().unwrap().pop_back() {
        stream.write_all(content.as_bytes()).unwrap();
    } else {
        stream.write_all("empty\r\n".as_bytes()).unwrap();
    }
}
