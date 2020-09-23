use logstreamer::{Action, ActionMessage, Response, ResponseMessage};
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;

fn to_clean_string(input: &[u8]) -> String {
    String::from_utf8_lossy(&input)
        .to_string()
        .replace("\r", "")
        .replace("\n", "")
}

fn send_message(stream: &mut TcpStream, message: ActionMessage) -> Vec<ResponseMessage> {
    stream.write_all(&message.as_vec()[..]).unwrap();
    stream.flush().unwrap();

    let mut buffer = [0; 512];
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
    let mut exit = false;
    let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();

    println!("logstreamer client");
    while !exit {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let action = input.as_bytes()[0];

        let message = match action {
            // c - consume
            99 => ActionMessage::new(
                Action::Consume(
                    String::from("topic"),
                    0,
                    to_clean_string(&input.as_bytes()[1..5])
                        .parse::<u32>()
                        .unwrap(),
                    to_clean_string(&input.as_bytes()[5..9])
                        .parse::<u32>()
                        .unwrap(),
                ),
                to_clean_string(&input.as_bytes()[9..]),
            ),
            // p - produce
            112 => ActionMessage::new(
                Action::Produce(
                    String::from("topic"),
                    0,
                    to_clean_string(&input.as_bytes()[1..]),
                ),
                String::new(),
            ),
            // n - new topic
            110 => ActionMessage::new(Action::CreateTopic(String::from("topic"), 1), String::new()),
            // q - quit
            113 => {
                exit = true;
                ActionMessage::new(Action::Quit, String::new())
            }
            _ => ActionMessage::new(Action::Invalid, String::new()),
        };

        let response_list = send_message(&mut stream, message);

        for response in response_list {
            match response.response {
                Response::Empty => println!("[empty]"),
                Response::Content(offset, value) => println!("[content: {}] {}", offset, value),
                Response::Offset(value) => println!("[offset] {}", value),
                Response::Error => println!("[error]"),
            }
        }
    }
}
