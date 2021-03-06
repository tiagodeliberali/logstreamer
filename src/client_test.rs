use logstreamer::{Action, ActionMessage, Client, Content, OffsetValue, Response, TopicAddress};
use std::thread;
use std::time::Duration;
use std::time::Instant;

const NUMBER_OF_PRODUCERS: u32 = 10;
const NUMBER_OF_CONSUMERS: u32 = 10;
const CONSUMER_LIMIT: u32 = 30;

fn main() {
    let mut producers = Vec::new();
    for producer_id in 0..NUMBER_OF_PRODUCERS {
        producers.push(thread::spawn(move || {
            let start = Instant::now();

            let mut client = Client::new(String::from("127.0.0.1:8080"));

            let create_topic_message = ActionMessage::new(
                Action::CreateTopic(String::from("topic"), NUMBER_OF_PRODUCERS),
                String::new(),
            );
            let _ = client.send_message(create_topic_message);

            let mut content_list = Vec::new();
            for i in 0..=2_000_000 {
                if i != 0 && i % 30 == 0 {
                    let message = ActionMessage::new(
                        Action::Produce(
                            TopicAddress::new(String::from("topic"), producer_id),
                            content_list.clone(),
                        ),
                        String::new(),
                    );
                    let _ = client.send_message(message);
                    content_list.clear();
                }
                content_list.push(Content::new(format!("nice message {}", i)));

                if i % 400_000 == 0 {
                    println!("PRODUCED MESSAGE {}: {}", producer_id, i);
                }
            }
            let duration = start.elapsed();

            let _ = client.send_message(ActionMessage::new(Action::Quit, String::new()));

            println!("DURATION PRODUCER: {:?}", duration);
        }));
    }

    let mut consumers = Vec::new();
    for consumer_id in 0..NUMBER_OF_CONSUMERS {
        let consumer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(500u64 / (consumer_id as u64 + 1)));
            let start = Instant::now();
            let mut client = Client::new(String::from("127.0.0.1:8080"));
            let mut i = 0;
            let mut offset_found = false;
            let consumer_name = format!("consumer_{}", consumer_id);
            let mut current_offset = 0;
            let mut consumed_messages = 0;

            while !offset_found {
                let response_list = client.send_message(ActionMessage::new(
                    Action::Consume(
                        TopicAddress::new(String::from("topic"), consumer_id),
                        OffsetValue(current_offset),
                        CONSUMER_LIMIT,
                    ),
                    consumer_name.clone(),
                ));

                let mut last_offset = 0;
                for response in response_list {
                    if let Response::Content(offset, content) = &response.response {
                        last_offset = offset.0;
                        consumed_messages += 1;
                        if last_offset == 1_999_999 {
                            println!("CONSUMER MESSAGE FOUND {}: {}", consumer_id, content.value);
                            offset_found = true;
                        } else if i % 400_000 == 0 {
                            println!(
                                "CONSUMED MESSAGE (total {}) {}: {} WITH VALUE VALUE: {}",
                                consumed_messages, consumer_id, offset.0, content.value
                            );
                        }
                    }
                    i += 1;
                }
                current_offset = last_offset + 1;
            }

            let duration = start.elapsed();

            let _ = client.send_message(ActionMessage::new(Action::Quit, String::from("consumer")));

            println!(
                "DURATION CONSUMER (total: {}): {:?}",
                consumed_messages, duration
            );
        });
        consumers.push(consumer);
    }

    for consumer in consumers {
        consumer.join().unwrap();
    }
    for producer in producers {
        producer.join().unwrap();
    }
}
