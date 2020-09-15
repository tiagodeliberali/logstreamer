use std::collections::BTreeMap;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

const C_KEY: u8 = 99;
const P_KEY: u8 = 112;
const Q_KEY: u8 = 113;

fn main() {
    let queue: BTreeMap<usize, String> = BTreeMap::new();
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

fn handle_connection(mut stream: TcpStream, queue: Arc<Mutex<BTreeMap<usize, String>>>) {
    loop {
        let mut buffer = [0; 512];
        let size = match stream.read(&mut buffer) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to read stream\r\n{}", err);
                return;
            }
        };

        let content = String::from_utf8_lossy(&buffer[1..size]).to_string();

        match &buffer[0] {
            &P_KEY => store_data(&stream, content, queue.clone()),
            &C_KEY => read_data(&stream, content, queue.clone()),
            &Q_KEY => return,
            _ => continue,
        }
    }
}

fn store_data(mut stream: &TcpStream, content: String, queue: Arc<Mutex<BTreeMap<usize, String>>>) {
    println!("[APPEND] {}", content);
    let offset;
    {
        let mut locked_queue = queue.lock().unwrap();
        offset = locked_queue.len();
        locked_queue.insert(offset, content);
    }
    stream
        .write_all(format!("ok [offset: {}]\r\n", offset).as_bytes())
        .unwrap();
}

fn read_data(mut stream: &TcpStream, content: String, queue: Arc<Mutex<BTreeMap<usize, String>>>) {
    let offset = content
        .replace("\r", "")
        .replace("\n", "")
        .parse::<usize>()
        .unwrap_or(0);
    for (key, value) in queue.lock().unwrap().range(offset..) {
        stream
            .write_all(format!("{}: {}", key, value).as_bytes())
            .unwrap();
    }
}
