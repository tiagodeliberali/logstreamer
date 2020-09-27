use crate::communication::{Action, ActionMessage, Response, ResponseMessage};
use crate::core::{Content, OffsetValue, TopicAddress};
use crate::storage::Cluster;
use std::io::prelude::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use std::sync::Mutex;

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub fn new(broker: String) -> Client {
        Client {
            stream: TcpStream::connect(broker).unwrap(),
        }
    }

    pub fn send_message(&mut self, message: ActionMessage) -> Vec<ResponseMessage> {
        self.stream.write_all(&message.as_vec()[..]).unwrap();
        self.stream.flush().unwrap();

        let mut buffer = [0; 1024];
        let _ = match &self.stream.read(&mut buffer) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to read stream\n{}", err);
                return vec![ResponseMessage::new_empty()];
            }
        };

        ResponseMessage::parse(&buffer)
    }
}

struct FailureDetector {
    id: u32,
    trusted: u32,
    received: bool,
    brokers: Vec<String>,
    durations: Vec<Duration>,
}

impl FailureDetector {
    pub fn new(id: u32, brokers: Vec<String>) -> FailureDetector {
        let durations = vec![Duration::from_secs(5); brokers.len()];

        FailureDetector {
            id,
            trusted: 0,
            received: true,
            brokers: Vec::new(),
            durations,
        }
    }

    pub fn run_loop(&mut self) {
        if self.trusted == self.id {
            thread::sleep(Duration::from_secs(5));
            self.send_messages();
        } else if self.trusted < self.id {
            thread::sleep(*self.durations.get(self.trusted as usize).unwrap());
            self.check_received_message();
        }
    }

    fn send_messages(&mut self) {
        for broker in self.brokers[(self.id as usize + 1)..].iter() {
            let mut client = Client::new(broker.clone());
            client.send_message(ActionMessage::new(Action::IamAlive(self.id), String::new()));
            client.send_message(ActionMessage::new(Action::Quit, String::new()));
        }
    }

    fn check_received_message(&mut self) {
        if self.received {
            self.received = false;
        } else {
            self.trusted += 1;
        }
    }

    pub fn receive_signal(&mut self, id: u32) {
        if id == self.trusted {
            self.received = true;
        } else if id < self.trusted {
            let duration = self.durations.get_mut(id as usize).unwrap();
            *duration = Duration::from_secs(duration.as_secs() + 5);
            self.trusted = id;
            self.received = true;
        }
    }
}

#[derive(Default)]
pub struct Broker {
    cluster: Cluster,
    failure_detector: Mutex<Option<FailureDetector>>,
}

impl Broker {
    pub fn new() -> Broker {
        let cluster = Cluster::new();
        let failure_detector = Mutex::new(None);
        Broker {
            cluster,
            failure_detector,
        }
    }

    pub fn init_broker(&self, id: u32, brokers: Vec<String>) {
        let failure_detector = FailureDetector::new(id, brokers);
        self.failure_detector.lock().unwrap().replace(failure_detector);
    }

    pub fn receive_signal(&self, id: u32) {
        let mut locked_failure_detector = self.failure_detector.lock().unwrap();
        let optional_failure_detector = locked_failure_detector.as_mut();
        if let Some(failure_detector) = optional_failure_detector {
            failure_detector.receive_signal(id);
        }
    }

    pub fn loop_failure_detector(&self) {
        let mut locked_failure_detector = self.failure_detector.lock().unwrap();
        let optional_failure_detector = locked_failure_detector.as_mut();
        if let Some(failure_detector) = optional_failure_detector {
            failure_detector.run_loop();
        }
    }

    pub fn store_data(&self, topic: TopicAddress, content: Vec<Content>) -> Vec<ResponseMessage> {
        match self.cluster.add_content(topic, content) {
            Some(offset) => vec![ResponseMessage::new(Response::Offset(offset))],
            None => vec![ResponseMessage::new(Response::Error)],
        }
    }

    pub fn read_data(
        &self,
        topic: TopicAddress,
        offset: OffsetValue,
        limit: u32,
    ) -> Vec<ResponseMessage> {
        let mut content_list = Vec::new();

        match self.cluster.get_partition(topic) {
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

    pub fn add_topic(&self, topic: String, partition_number: u32) -> Vec<ResponseMessage> {
        self.cluster.add_topic(topic, partition_number as usize);
        vec![]
    }
}

pub struct Controller {}
