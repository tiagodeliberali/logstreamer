use logstreamer::{Action, ActionMessage, Broker, ResponseMessage};
use std::io::prelude::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn main() {
    let broker = Arc::new(Broker::new());

    let listener = match TcpListener::bind("127.0.0.1:8080") {
        Ok(listener) => listener,
        Err(err) => panic!("Failed to bind address\r\n{}", err),
    };

    for stream in listener.incoming() {
        let cloned_broker = broker.clone();
        match stream {
            Ok(valid_stream) => {
                thread::spawn(move || {
                    handle_connection(valid_stream, cloned_broker);
                });
            }
            Err(err) => println!("Failed to process current stream\n{}", err),
        };
    }
}

fn handle_connection(mut stream: TcpStream, broker: Arc<Broker>) {
    loop {
        let mut buffer = [0; 1024];
        let _ = match stream.read(&mut buffer) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to read stream\r\n{}", err);
                return;
            }
        };

        let message = ActionMessage::parse(&buffer);

        let response_list = match message.action {
            Action::Produce(topic, content) => broker.store_data(topic, content),
            Action::Consume(topic, offset, limit) => broker.read_data(topic, offset, limit),
            Action::CreateTopic(topic, partition_number) => {
                broker.add_topic(topic, partition_number)
            }
            Action::InitializeController(_broker_list) => Vec::new(),
            Action::InitializeBroker(_broker_list) => Vec::new(),
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
