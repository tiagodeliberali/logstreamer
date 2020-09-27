use logstreamer::{Action, ActionMessage, Client, Content, OffsetValue, Response, TopicAddress};
use std::io;

fn to_clean_string(input: &[u8]) -> String {
    String::from_utf8_lossy(&input)
        .to_string()
        .replace("\r", "")
        .replace("\n", "")
}

fn main() {
    let mut exit = false;
    let mut client = Client::new(String::from("127.0.0.1:8080"));

    println!("logstreamer client");
    while !exit {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let action = input.as_bytes()[0];

        let message = match action {
            // i
            105 => ActionMessage::new(
                Action::InitializeController(vec![
                    String::from("127.0.0.1:8080"),
                    String::from("127.0.0.1:8081"),
                    String::from("127.0.0.1:8082"),
                ]),
                String::new(),
            ),
            // c - consume
            99 => ActionMessage::new(
                Action::Consume(
                    TopicAddress::new(String::from("topic"), 0),
                    OffsetValue(
                        to_clean_string(&input.as_bytes()[1..5])
                            .parse::<u32>()
                            .unwrap(),
                    ),
                    to_clean_string(&input.as_bytes()[5..9])
                        .parse::<u32>()
                        .unwrap(),
                ),
                to_clean_string(&input.as_bytes()[9..]),
            ),
            // p - produce
            112 => ActionMessage::new(
                Action::Produce(
                    TopicAddress::new(String::from("topic"), 0),
                    vec![Content::new(to_clean_string(&input.as_bytes()[1..]))],
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

        let response_list = client.send_message(message);

        for response in response_list {
            match response.response {
                Response::Empty => println!("[empty]"),
                Response::Content(offset, value) => {
                    println!("[content: {}] {}", offset.0, value.value)
                }
                Response::Offset(value) => println!("[offset] {}", value.0),
                Response::Error => println!("[error]"),
            }
        }
    }
}
