use logstreamer::{Action, ActionMessage, Response, ResponseMessage};
use std::io::prelude::*;
use std::net::TcpStream;
use std::thread;
use std::time::Instant;

fn send_message(stream: &mut TcpStream, message: ActionMessage) -> Vec<ResponseMessage> {
    stream.write_all(&message.as_vec()[..]).unwrap();
    stream.flush().unwrap();

    let mut buffer = [0; 1024];
    let _ = match &stream.read(&mut buffer) {
        Ok(value) => value,
        Err(err) => {
            println!("Failed to read stream\n{}", err);
            return vec![ResponseMessage::new_empty()];
        }
    };

    ResponseMessage::parse(&buffer)
}

fn main() {
    let mut consumers = Vec::new();
    for consumer_id in 0..1 {
        let consumer = thread::spawn(move || {
            thread::sleep_ms(500 / (consumer_id + 1));
            let start = Instant::now();
            let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();
            let mut i = 0;
            let mut offset_found = false;
            let consumer_name = format!("consumer_{}", consumer_id);

            let response_list = send_message(
                &mut stream,
                ActionMessage::new(Action::GetOffset, consumer_name.clone()),
            );

            let mut current_offset = 0;
            if let Response::Offset(value) = response_list.first().unwrap().response {
                println!("CONSUMER INITIAL OFFSET {}", value);
                current_offset = value;
            }

            while !offset_found {
                let response_list = send_message(
                    &mut stream,
                    ActionMessage::new(Action::Consume(current_offset, 1), consumer_name.clone()),
                );

                let mut last_offset = 0;
                for response in response_list {
                    if let Response::Content(offset, content) = &response.response {
                        last_offset = *offset;
                        if *offset == 1_999_999 {
                            println!("CONSUMER MESSAGE FOUND {}", content);
                            offset_found = true;
                        } else if i % 50_000 == 0 {
                            println!("CONSUMED MESSAGE: {} WITH VALUE VALUE: {}", offset, content);
                            let _ = send_message(
                                &mut stream,
                                ActionMessage::new(
                                    Action::CommitOffset(last_offset),
                                    consumer_name.clone(),
                                ),
                            );
                        }
                    }
                    i += 1;
                }
                current_offset = last_offset + 1;
            }

            let duration = start.elapsed();

            let _ = send_message(
                &mut stream,
                ActionMessage::new(Action::Quit, String::from("consumer")),
            );

            println!("DURATION CONSUMER: {:?}", duration);
        });
        consumers.push(consumer);
    }

    let producer = thread::spawn(move || {
        let start = Instant::now();
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();

        for i in 0..2_000_000 {
            let message = ActionMessage::new(
                Action::Produce(format!("nice message {}", i)),
                String::new(),
            );
            let _ = send_message(&mut stream, message);

            if i % 50_000 == 0 {
                println!("PRODUCED MESSAGE: {}", i);
            }
        }
        let duration = start.elapsed();

        let _ = send_message(&mut stream, ActionMessage::new(Action::Quit, String::new()));

        println!("DURATION PRODUCER: {:?}", duration);
    });

    for consumer in consumers {
        consumer.join().unwrap();
    }
    producer.join().unwrap();
}
