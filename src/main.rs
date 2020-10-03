use logstreamer::{Action, ActionMessage, Broker, ResponseMessage};
use std::env;
use std::io::prelude::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn main() {
    let args: Vec<String> = env::args().collect();
    let broker_address = match args.get(1) {
        Some(value) => value.into(),
        None => String::from("127.0.0.1:8080"),
    };
    
    let broker = Arc::new(Broker::new());

    let listener = match TcpListener::bind(&broker_address) {
        Ok(listener) => listener,
        Err(err) => panic!("Failed to bind address\r\n{}", err),
    };
    println!("Started server at {}", &broker_address);

    let cloned_broker = broker.clone();
    thread::spawn(move || loop {
        cloned_broker.loop_failure_detector();
    });

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
            Action::InitializeController(brokers) => broker.init_controller(brokers),
            Action::InitializeBroker(id, brokers) => broker.init_broker(id, brokers),
            Action::IamAlive(id) => broker.receive_signal(id),
            Action::Invalid => Vec::new(),
            Action::Quit => {
                stream
                    .write_all(&ResponseMessage::new_empty().as_vec()[..])
                    .unwrap();
                stream.flush().unwrap();
                return;
            }
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
