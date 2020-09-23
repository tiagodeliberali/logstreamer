use logstreamer::{Action, ActionMessage, Cluster, OffsetValue, Response, ResponseMessage};
use logstreamer::{Content, TopicAddress};
use std::io::prelude::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn main() {
    let cluster = Arc::new(Cluster::new());

    let listener = match TcpListener::bind("127.0.0.1:8080") {
        Ok(listener) => listener,
        Err(err) => panic!("Failed to bind address\r\n{}", err),
    };

    for stream in listener.incoming() {
        let cloned_cluster = cluster.clone();
        match stream {
            Ok(valid_stream) => {
                thread::spawn(move || {
                    handle_connection(valid_stream, cloned_cluster);
                });
            }
            Err(err) => println!("Failed to process current stream\n{}", err),
        };
    }
}

fn handle_connection(mut stream: TcpStream, cluster: Arc<Cluster>) {
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
            Action::Produce(topic, content) => store_data(topic, content, cluster.clone()),
            Action::Consume(topic, offset, limit) => {
                read_data(topic, offset, limit, cluster.clone())
            }
            Action::CreateTopic(topic, partition_number) => {
                add_topic(topic, partition_number, cluster.clone())
            }
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

fn store_data(
    topic: TopicAddress,
    content: Content,
    cluster: Arc<Cluster>,
) -> Vec<ResponseMessage> {
    match cluster.add_content(topic, content) {
        Some(offset) => vec![ResponseMessage::new(Response::Offset(offset))],
        None => vec![ResponseMessage::new(Response::Error)],
    }
}

fn read_data(
    topic: TopicAddress,
    offset: OffsetValue,
    limit: u32,
    cluster: Arc<Cluster>,
) -> Vec<ResponseMessage> {
    let mut content_list = Vec::new();

    match cluster.get_partition(topic) {
        Some(partition) => {
            let locked_partition = partition.queue.lock().unwrap();

            if locked_partition.is_empty() {
                return content_list;
            }

            let range_end = usize::min((offset.0 + limit) as usize, locked_partition.len());
            let range_start = usize::min(offset.0 as usize, range_end - 1);
            let mut position = offset.0 as u32;

            for value in locked_partition[range_start..range_end].iter() {
                content_list.push(ResponseMessage::new(Response::Content(
                    OffsetValue(position),
                    value.clone(),
                )));
                position += 1;
            }
            content_list
        }
        None => vec![ResponseMessage::new(Response::Error)],
    }
}

fn add_topic(topic: String, partition_number: u32, cluster: Arc<Cluster>) -> Vec<ResponseMessage> {
    cluster.add_topic(topic, partition_number as usize);
    vec![]
}
