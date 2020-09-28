use crate::core::{Content, OffsetValue, TopicAddress};

struct Buffer<'a> {
    position: usize,
    buffer: &'a [u8],
}

impl<'a> Buffer<'a> {
    fn new(buffer: &[u8]) -> Buffer {
        Buffer {
            position: 0,
            buffer,
        }
    }
    fn read_u8(&mut self) -> u8 {
        let data = self.buffer[self.position];
        self.position += 1;
        data
    }

    fn read_string(&mut self) -> String {
        let string_size = self.read_u32() as usize;
        let data =
            String::from_utf8_lossy(&self.buffer[(self.position)..(self.position + string_size)])
                .to_string();
        self.position += string_size;
        data
    }

    fn read_u32(&mut self) -> u32 {
        let data: [u8; 4] = [
            self.buffer[self.position],
            self.buffer[self.position + 1],
            self.buffer[self.position + 2],
            self.buffer[self.position + 3],
        ];
        self.position += 4;
        u32::from_be_bytes(data)
    }

    fn finished_read(&self) -> bool {
        self.position >= self.buffer.len()
    }
}

fn write_string(content: &mut Vec<u8>, value: &String) {
    write_u32(content, value.len() as u32);
    content.extend_from_slice(value.as_bytes());
}

fn write_u32(content: &mut Vec<u8>, value: u32) {
    content.extend_from_slice(&value.to_be_bytes());
}

pub enum Action {
    Produce(TopicAddress, Vec<Content>),
    Consume(TopicAddress, OffsetValue, u32),
    CreateTopic(String, u32),
    InitializeController(Vec<String>),
    InitializeBroker(u32, Vec<String>),
    IamAlive(u32),
    Quit,
    Invalid,
}

pub struct ActionMessage {
    pub action: Action,
    pub consumer_id: String,
}

impl ActionMessage {
    pub fn new(action: Action, consumer_id: String) -> ActionMessage {
        ActionMessage {
            action,
            consumer_id,
        }
    }

    pub fn parse(buffer: &[u8]) -> ActionMessage {
        let mut data = Buffer::new(buffer);

        let action = match data.read_u8() {
            1 => {
                let topic = TopicAddress::new(data.read_string(), data.read_u32());
                let content_length = data.read_u32();
                let mut content_list = Vec::new();
                for _ in 0..content_length {
                    content_list.push(Content::new(data.read_string()))
                }
                Action::Produce(topic, content_list)
            }
            2 => {
                let topic = TopicAddress::new(data.read_string(), data.read_u32());
                let offset = OffsetValue(data.read_u32());
                let limit = data.read_u32();
                Action::Consume(topic, offset, limit)
            }
            3 => {
                let topic = data.read_string();
                let partition = data.read_u32();
                Action::CreateTopic(topic, partition)
            }
            4 => {
                let mut broker_list = Vec::new();
                let size = data.read_u32() as usize;
                for _ in 0..size {
                    broker_list.push(data.read_string());
                }
                Action::InitializeController(broker_list)
            }
            5 => {
                let mut broker_list = Vec::new();
                let broker_id = data.read_u32();
                let size = data.read_u32() as usize;
                for _ in 0..size {
                    let broker = data.read_string();
                    broker_list.push(broker);
                }
                Action::InitializeBroker(broker_id, broker_list)
            }
            6 => {
                let id = data.read_u32();
                Action::IamAlive(id)
            }
            99 => Action::Quit,
            _ => Action::Invalid,
        };

        let consumer_id = data.read_string();

        ActionMessage {
            action,
            consumer_id,
        }
    }

    pub fn as_vec(&self) -> Vec<u8> {
        let mut content_vec: Vec<u8> = Vec::new();

        match &self.action {
            Action::Produce(topic, content_list) => {
                content_vec.push(1);
                write_string(&mut content_vec, &topic.name);
                write_u32(&mut content_vec, topic.partition);
                write_u32(&mut content_vec, content_list.len() as u32);
                for content in content_list {
                    write_string(&mut content_vec, &content.value);
                }
            }
            Action::Consume(topic, offset, limit) => {
                content_vec.push(2);
                write_string(&mut content_vec, &topic.name);
                write_u32(&mut content_vec, topic.partition);
                write_u32(&mut content_vec, offset.0);
                write_u32(&mut content_vec, *limit);
            }
            Action::CreateTopic(topic, partition) => {
                content_vec.push(3);
                write_string(&mut content_vec, &topic);
                write_u32(&mut content_vec, *partition);
            }
            Action::InitializeController(broker_list) => {
                content_vec.push(4);
                write_u32(&mut content_vec, broker_list.len() as u32);
                for broker in broker_list {
                    write_string(&mut content_vec, broker);
                }
            }
            Action::InitializeBroker(broker_id, broker_list) => {
                content_vec.push(5);
                write_u32(&mut content_vec, *broker_id);
                write_u32(&mut content_vec, broker_list.len() as u32);
                for broker in broker_list {
                    write_string(&mut content_vec, broker);
                }
            }
            Action::IamAlive(id) => {
                content_vec.push(6);
                write_u32(&mut content_vec, *id);
            }
            Action::Quit => content_vec.push(99),
            Action::Invalid => content_vec.push(0),
        }

        write_string(&mut content_vec, &self.consumer_id);

        content_vec
    }
}

pub enum Response {
    Empty,
    Offset(OffsetValue),
    Content(OffsetValue, Content),
    AskTheController(String),
    Error,
}

pub struct ResponseMessage {
    pub response: Response,
}

impl ResponseMessage {
    pub fn new(response: Response) -> ResponseMessage {
        ResponseMessage { response }
    }

    pub fn new_empty() -> ResponseMessage {
        ResponseMessage {
            response: Response::Empty,
        }
    }

    pub fn parse(buffer: &[u8]) -> Vec<ResponseMessage> {
        let mut result_list = Vec::new();
        let mut data = Buffer::new(buffer);
        let mut read_all = false;

        while !read_all {
            let response = match data.read_u8() {
                1 => {
                    let offset = OffsetValue(data.read_u32());
                    let content = Content::new(data.read_string());
                    Response::Content(offset, content)
                }
                2 => {
                    let offset = OffsetValue(data.read_u32());
                    Response::Offset(offset)
                }
                3 => Response::Error,
                4 => {
                    let broker = data.read_string();
                    Response::AskTheController(broker)
                }
                _ => {
                    read_all = true;
                    Response::Empty
                }
            };

            result_list.push(ResponseMessage { response });
            read_all = read_all || data.finished_read();
        }

        result_list
    }

    pub fn as_vec(&self) -> Vec<u8> {
        let mut content_vec = Vec::new();

        match &self.response {
            Response::Empty => content_vec.push(0),
            Response::Content(offset, content) => {
                content_vec.push(1);
                write_u32(&mut content_vec, offset.0);
                write_string(&mut content_vec, &content.value);
            }
            Response::Offset(offset) => {
                content_vec.push(2);
                write_u32(&mut content_vec, offset.0);
            }
            Response::Error => content_vec.push(3),
            Response::AskTheController(broker_id) => {
                content_vec.push(4);
                write_string(&mut content_vec, &broker_id);
            }
        }

        content_vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_invalid_action() {
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(Action::Invalid, consumer_id.clone());

        let message_as_vec = message.as_vec();
        let message = ActionMessage::parse(&message_as_vec[..]);

        assert!(matches!(message.action, Action::Invalid));
        assert_eq!(message.consumer_id, consumer_id);
    }

    #[test]
    fn should_convert_quit_action() {
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(Action::Quit, consumer_id.clone());

        let message_as_vec = message.as_vec();
        let message = ActionMessage::parse(&message_as_vec[..]);

        assert!(matches!(message.action, Action::Quit));
        assert_eq!(message.consumer_id, consumer_id);
    }

    #[test]
    fn should_convert_consume_action() {
        let topic = TopicAddress::new(String::from("topic"), 1);
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(
            Action::Consume(topic, OffsetValue(3), 10),
            consumer_id.clone(),
        );

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::Consume(parsed_topic, offset, limit) = message.action {
            assert_eq!(parsed_topic.name, "topic");
            assert_eq!(parsed_topic.partition, 1);
            assert_eq!(offset.0, 3);
            assert_eq!(limit, 10);
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, consumer_id);
    }

    #[test]
    fn should_convert_produce_action() {
        let consumer_id = String::from("consumer_id");
        let topic = TopicAddress::new(String::from("topic"), 1);
        let content = vec![Content::new(String::from("Message Content"))];

        let message = ActionMessage::new(Action::Produce(topic, content), consumer_id.clone());

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::Produce(parsed_topic, content) = message.action {
            assert_eq!(parsed_topic.name, "topic");
            assert_eq!(parsed_topic.partition, 1);
            assert_eq!(content.len(), 1);
            assert_eq!(content.first().unwrap().value, "Message Content");
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, consumer_id);
    }

    #[test]
    fn should_convert_produce_multiple_content_action() {
        let consumer_id = String::from("consumer_id");
        let topic = TopicAddress::new(String::from("topic"), 1);
        let content = vec![
            Content::new(String::from("Message Content")),
            Content::new(String::from("Message other")),
            Content::new(String::from("Message final")),
        ];

        let message = ActionMessage::new(Action::Produce(topic, content), consumer_id.clone());

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::Produce(parsed_topic, content) = message.action {
            assert_eq!(parsed_topic.name, "topic");
            assert_eq!(parsed_topic.partition, 1);
            assert_eq!(content.len(), 3);
            assert_eq!(content.get(0).unwrap().value, "Message Content");
            assert_eq!(content.get(1).unwrap().value, "Message other");
            assert_eq!(content.get(2).unwrap().value, "Message final");
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, consumer_id);
    }

    #[test]
    fn should_convert_create_topic_action() {
        let consumer_id = String::from("consumer_id");
        let topic = String::from("topic");

        let message =
            ActionMessage::new(Action::CreateTopic(topic.clone(), 1), consumer_id.clone());

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::CreateTopic(parsed_topic, partition) = message.action {
            assert_eq!(parsed_topic, topic);
            assert_eq!(partition, 1);
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, consumer_id);
    }

    #[test]
    fn shoyd_convert_initialize_controller_action() {
        let broker_list = vec![String::from("broker1"), String::from("broker2")];

        let message = ActionMessage::new(
            Action::InitializeController(broker_list),
            String::from("consumer_id"),
        );

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::InitializeController(list) = message.action {
            assert_eq!(2, list.len());
            assert_eq!(list.get(0).unwrap(), "broker1");
            assert_eq!(list.get(1).unwrap(), "broker2");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn shoyd_convert_iamalive_action() {
        let message = ActionMessage::new(Action::IamAlive(10), String::from("consumer_id"));

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::IamAlive(id) = message.action {
            assert_eq!(id, 10);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn shoyd_convert_initialize_broker_action() {
        let broker_list = vec![String::from("broker1"), String::from("broker2")];

        let message = ActionMessage::new(
            Action::InitializeBroker(5, broker_list),
            String::from("consumer_id"),
        );

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::InitializeBroker(id, list) = message.action {
            assert_eq!(5, id);
            assert_eq!(2, list.len());
            assert_eq!(list.get(0).unwrap(), "broker1");
            assert_eq!(list.get(1).unwrap(), "broker2");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_convert_empty_response() {
        let message = ResponseMessage::new(Response::Empty);

        let parsed_message = message.as_vec();
        let message = ResponseMessage::parse(&parsed_message[..]);
        let message = message.first().unwrap();

        assert!(matches!(message.response, Response::Empty));
    }

    #[test]
    fn should_convert_error_response() {
        let message = ResponseMessage::new(Response::Error);

        let parsed_message = message.as_vec();
        let message = ResponseMessage::parse(&parsed_message[..]);
        let message = message.first().unwrap();

        assert!(matches!(message.response, Response::Error));
    }

    #[test]
    fn should_convert_content_response() {
        let content = Content::new(String::from("nice content"));
        let message = ResponseMessage::new(Response::Content(OffsetValue(100), content));

        let parsed_message = message.as_vec();
        let message = ResponseMessage::parse(&parsed_message[..]);
        let message = message.first().unwrap();

        if let Response::Content(offset, content) = &message.response {
            assert_eq!(offset.0, 100);
            assert_eq!(content.value, "nice content");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_convert_offset_response() {
        let message = ResponseMessage::new(Response::Offset(OffsetValue(100)));

        let parsed_message = message.as_vec();
        let message = ResponseMessage::parse(&parsed_message[..]);
        let message = message.first().unwrap();

        if let Response::Offset(value) = &message.response {
            assert_eq!(value.0, 100);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_convert_askthecontrolller_response() {
        let message =
            ResponseMessage::new(Response::AskTheController(String::from("localhost:8080")));

        let parsed_message = message.as_vec();
        let message = ResponseMessage::parse(&parsed_message[..]);
        let message = message.first().unwrap();

        if let Response::AskTheController(value) = &message.response {
            assert_eq!(value, "localhost:8080");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_parse_mixed_response() {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(
                OffsetValue(100),
                Content::new(String::from("nice content")),
            ))
            .as_vec(),
        );

        bytes.extend_from_slice(&ResponseMessage::new(Response::Offset(OffsetValue(101))).as_vec());

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(
                OffsetValue(102),
                Content::new(String::from("last content")),
            ))
            .as_vec(),
        );

        let message_list = ResponseMessage::parse(&bytes[..]);

        assert_eq!(message_list.len(), 3);

        let message = message_list.get(0).unwrap();
        if let Response::Content(offset, content) = &message.response {
            assert_eq!(offset.0, 100);
            assert_eq!(content.value, "nice content");
        } else {
            assert!(false);
        }

        let message = message_list.get(1).unwrap();
        if let Response::Offset(offset) = &message.response {
            assert_eq!(offset.0, 101);
        } else {
            assert!(false);
        }

        let message = message_list.get(2).unwrap();
        if let Response::Content(offset, content) = &message.response {
            assert_eq!(offset.0, 102);
            assert_eq!(content.value, "last content");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_parse_multiple_content_responses() {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(
                OffsetValue(100),
                Content::new(String::from("nice content")),
            ))
            .as_vec(),
        );

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(
                OffsetValue(101),
                Content::new(String::from("other content")),
            ))
            .as_vec(),
        );

        bytes.extend_from_slice(&ResponseMessage::new(Response::Error).as_vec());

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(
                OffsetValue(102),
                Content::new(String::from("last content")),
            ))
            .as_vec(),
        );

        let message_list = ResponseMessage::parse(&bytes[..]);

        assert_eq!(message_list.len(), 4);

        let message = message_list.first().unwrap();
        if let Response::Content(offset, content) = &message.response {
            assert_eq!(offset.0, 100);
            assert_eq!(content.value, "nice content");
        } else {
            assert!(false);
        }

        let message = message_list.get(1).unwrap();
        if let Response::Content(offset, content) = &message.response {
            assert_eq!(offset.0, 101);
            assert_eq!(content.value, "other content");
        } else {
            assert!(false);
        }

        let message = message_list.get(2).unwrap();
        assert!(matches!(message.response, Response::Error));

        let message = message_list.get(3).unwrap();
        if let Response::Content(offset, content) = &message.response {
            assert_eq!(offset.0, 102);
            assert_eq!(content.value, "last content");
        } else {
            assert!(false);
        }
    }
}
