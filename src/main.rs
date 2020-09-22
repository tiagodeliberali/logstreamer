use logstreamer::{Action, ActionMessage, Response, ResponseMessage};
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

struct Storage {
    queue: Mutex<Vec<String>>,
}

impl Storage {
    fn new() -> Storage {
        Storage {
            queue: Mutex::new(Vec::new()),
        }
    }

    fn add_content(&self, content: String) -> u32 {
        let mut locked_queue = self.queue.lock().unwrap();
        locked_queue.push(content);
        (locked_queue.len() - 1) as u32
    }
}

fn main() {
    let storage = Arc::new(Storage::new());

    let listener = match TcpListener::bind("127.0.0.1:8080") {
        Ok(listener) => listener,
        Err(err) => panic!("Failed to bind address\r\n{}", err),
    };

    for stream in listener.incoming() {
        let cloned_storage = storage.clone();
        match stream {
            Ok(valid_stream) => {
                thread::spawn(move || {
                    handle_connection(valid_stream, cloned_storage);
                });
            }
            Err(err) => println!("Failed to process current stream\n{}", err),
        };
    }
}

fn handle_connection(mut stream: TcpStream, storage: Arc<Storage>) {
    loop {
        let mut buffer = [0; 512];
        let _ = match stream.read(&mut buffer) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to read stream\r\n{}", err);
                return;
            }
        };

        let message = ActionMessage::parse(&buffer);

        let response_list = match message.action {
            Action::Produce(content) => store_data(content, storage.clone()),
            Action::Consume(offset, limit) => read_data(offset, limit, storage.clone()),
            Action::Quit => return,
            Action::Invalid => vec![ResponseMessage::new_empty()],
        };

        let mut response_content: Vec<u8> = Vec::new();
        for response in response_list {
            response_content.extend(response.as_vec());
        }

        if response_content.is_empty() {
            response_content.extend(ResponseMessage::new_empty().as_vec());
        }

        stream.write_all(&response_content[..]).unwrap();
        stream.flush().unwrap();
    }
}

fn store_data(content: String, storage: Arc<Storage>) -> Vec<ResponseMessage> {
    let offset = storage.add_content(content);
    vec![ResponseMessage::new(Response::Offset(offset))]
}

fn read_data(offset: u32, limit: u32, storage: Arc<Storage>) -> Vec<ResponseMessage> {
    let mut content_list = Vec::new();
    let locked_queue = storage.queue.lock().unwrap();

    if locked_queue.is_empty() {
        return content_list;
    }

    let range_end = usize::min((offset + limit) as usize, locked_queue.len());
    let range_start = usize::min(offset as usize, range_end - 1);
    let mut position = offset as u32;

    for value in locked_queue[range_start..range_end].iter() {
        content_list.push(ResponseMessage::new(Response::Content(
            position,
            value.clone(),
        )));
        position += 1;
    }

    content_list
}
