use logstreamer::{Action, ActionMessage, Response, ResponseMessage};
use std::io::prelude::*;
use std::net::TcpStream;
use std::thread;
use std::time::Instant;

fn main() {
    let consumer = thread::spawn(move || {
        let start = Instant::now();
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();
        let mut i = 0;

        let message = ActionMessage::new(Action::Consume, String::from("consumer"));
        let message_content = &message.as_vec()[..];

        loop {
            stream.write_all(message_content).unwrap();
            stream.flush().unwrap();
            let mut buffer = [0; 512];
            let size = match stream.read(&mut buffer) {
                Ok(value) => value,
                Err(err) => {
                    println!("Failed to read stream\n{}", err);
                    continue;
                }
            };

            let response_list = ResponseMessage::parse(&buffer);
            let response = response_list.first().unwrap();

            if let Response::Content(offset, content) = &response.response {
                if *offset == 1_999_999 {
                    println!("CONSUMER MESSAGE FOUND {}", content);
                    break;
                }
            }

            if i % 50_000 == 0 {
                print!(
                    "CONSUMED MESSAGE: {} WITH VALUE VALUE: {}",
                    i,
                    String::from_utf8_lossy(&buffer[..size])
                );
            }
            i += 1;
        }

        let duration = start.elapsed();

        println!("DURATION CONSUMER: {:?}", duration);
    });

    let producer = thread::spawn(move || {
        let start = Instant::now();
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();

        for i in 0..2_000_000 {
            let message = ActionMessage::new(
                Action::Produce(format!("nice message {}", i)),
                String::new(),
            );
            stream.write_all(&message.as_vec()[..]).unwrap();
            stream.flush().unwrap();
            let mut buffer = [0; 512];
            let _ = match stream.read(&mut buffer) {
                Ok(value) => value,
                Err(err) => {
                    println!("Failed to read stream\n{}", err);
                    continue;
                }
            };

            if i % 50_000 == 0 {
                println!("PRODUCED MESSAGE: {}", i);
            }
        }
        let duration = start.elapsed();

        println!("DURATION PRODUCER: {:?}", duration);
    });

    consumer.join().unwrap();
    producer.join().unwrap();
}
