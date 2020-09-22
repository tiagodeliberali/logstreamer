fn read_string(buffer: &[u8], position: usize) -> (String, usize) {
    let (string_size, size) = read_u32(buffer, position);
    (
        String::from_utf8_lossy(
            &buffer[(position + size)..(position + size + string_size as usize)],
        )
        .to_string(),
        string_size as usize + size,
    )
}

fn read_u32(buffer: &[u8], position: usize) -> (u32, usize) {
    let data: [u8; 4] = [
        buffer[position],
        buffer[position + 1],
        buffer[position + 2],
        buffer[position + 3],
    ];
    (u32::from_be_bytes(data), 4)
}

fn write_string(content: &mut Vec<u8>, value: String) {
    write_u32(content, value.len() as u32);
    content.extend_from_slice(value.as_bytes());
}

fn write_u32(content: &mut Vec<u8>, value: u32) {
    content.extend_from_slice(&value.to_be_bytes());
}

pub enum Action {
    Produce(String, u32, String),
    Consume(String, u32, u32, u32),
    CreateTopic(String, u32),
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
        let mut consumer_id_position = 1 as usize;

        let action = match &buffer[0] {
            1 => {
                let (topic, topic_size) = read_string(buffer, consumer_id_position);
                consumer_id_position += topic_size;

                let (partition, partition_size) = read_u32(buffer, consumer_id_position);
                consumer_id_position += partition_size;

                let (content, content_size) = read_string(buffer, consumer_id_position);
                consumer_id_position += content_size;

                Action::Produce(topic, partition, content)
            }
            2 => {
                let (topic, topic_size) = read_string(buffer, consumer_id_position);
                consumer_id_position += topic_size;

                let (partition, partition_size) = read_u32(buffer, consumer_id_position);
                consumer_id_position += partition_size;

                let (offset, offset_size) = read_u32(buffer, consumer_id_position);
                consumer_id_position += offset_size;

                let (limit, limit_size) = read_u32(buffer, consumer_id_position);
                consumer_id_position += limit_size;

                Action::Consume(topic, partition, offset, limit)
            }
            3 => {
                let (topic, topic_size) = read_string(buffer, consumer_id_position);
                consumer_id_position += topic_size;

                let (partition, partition_size) = read_u32(buffer, consumer_id_position);
                consumer_id_position += partition_size;

                Action::CreateTopic(topic, partition)
            }
            4 => Action::Quit,
            _ => Action::Invalid,
        };

        let (consumer_id, _) = read_string(buffer, consumer_id_position);

        ActionMessage {
            action,
            consumer_id,
        }
    }

    pub fn as_vec(&self) -> Vec<u8> {
        let mut content: Vec<u8> = Vec::new();

        match &self.action {
            Action::Produce(topic, partition, value) => {
                content.push(1);
                write_string(&mut content, topic.clone());
                write_u32(&mut content, *partition);
                write_string(&mut content, value.clone());
            }
            Action::Consume(topic, partition, offset, limit) => {
                content.push(2);
                write_string(&mut content, topic.clone());
                write_u32(&mut content, *partition);
                write_u32(&mut content, *offset);
                write_u32(&mut content, *limit);
            }
            Action::CreateTopic(topic, partition) => {
                content.push(3);
                write_string(&mut content, topic.clone());
                write_u32(&mut content, *partition);
            }
            Action::Quit => content.push(4),
            Action::Invalid => content.push(0),
        }

        write_string(&mut content, self.consumer_id.clone());

        content
    }
}

pub enum Response {
    Empty,
    Offset(u32),
    Content(u32, String),
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
        let mut position = 0 as usize;
        let mut read_all = false;
        let max_position = buffer.len();

        while !read_all {
            let response = match &buffer[position] {
                1 => {
                    position += 1;
                    let (offset, offset_size) = read_u32(buffer, position);
                    let (content, content_size) = read_string(buffer, position + offset_size);
                    position += offset_size + content_size;
                    Response::Content(offset, content)
                }
                2 => {
                    position += 1;
                    let (offset, offset_size) = read_u32(buffer, position);
                    position += offset_size;
                    Response::Offset(offset)
                }
                3 => {
                    position += 1;
                    Response::Error
                }
                _ => {
                    read_all = true;
                    Response::Empty
                }
            };

            result_list.push(ResponseMessage { response });
            read_all = read_all || position >= max_position;
        }

        result_list
    }

    pub fn as_vec(&self) -> Vec<u8> {
        let mut content = Vec::new();

        match &self.response {
            Response::Empty => content.push(0),
            Response::Content(offset, value) => {
                content.push(1);
                write_u32(&mut content, *offset);
                write_string(&mut content, value.clone());
            }
            Response::Offset(offset) => {
                content.push(2);
                write_u32(&mut content, *offset);
            }
            Response::Error => content.push(3),
        }

        content
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
        let topic = String::from("topic");
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(
            Action::Consume(topic.clone(), 1, 3, 10),
            consumer_id.clone(),
        );

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::Consume(parsed_topic, partition, offset, limit) = message.action {
            assert_eq!(parsed_topic, topic);
            assert_eq!(partition, 1);
            assert_eq!(offset, 3);
            assert_eq!(limit, 10);
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, consumer_id);
    }

    #[test]
    fn should_convert_produce_action() {
        let consumer_id = String::from("consumer_id");
        let topic = String::from("topic");
        let content = String::from("Message Content");

        let message = ActionMessage::new(
            Action::Produce(topic.clone(), 1, content),
            consumer_id.clone(),
        );

        let parsed_message = message.as_vec();
        let message = ActionMessage::parse(&parsed_message[..]);

        if let Action::Produce(parsed_topic, partition, value) = message.action {
            assert_eq!(parsed_topic, topic);
            assert_eq!(partition, 1);
            assert_eq!(value, "Message Content");
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
        let content = String::from("nice content");
        let message = ResponseMessage::new(Response::Content(100, content.clone()));

        let parsed_message = message.as_vec();
        let message = ResponseMessage::parse(&parsed_message[..]);
        let message = message.first().unwrap();

        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 100);
            assert_eq!(value, &content);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_convert_offset_response() {
        let message = ResponseMessage::new(Response::Offset(100));

        let parsed_message = message.as_vec();
        let message = ResponseMessage::parse(&parsed_message[..]);
        let message = message.first().unwrap();

        if let Response::Offset(value) = message.response {
            assert_eq!(value, 100);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_parse_mixed_response() {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(100, String::from("nice content"))).as_vec(),
        );

        bytes.extend_from_slice(&ResponseMessage::new(Response::Offset(101)).as_vec());

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(102, String::from("last content"))).as_vec(),
        );

        let message_list = ResponseMessage::parse(&bytes[..]);

        assert_eq!(message_list.len(), 3);

        let message = message_list.get(0).unwrap();
        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 100);
            assert_eq!(value, "nice content");
        } else {
            assert!(false);
        }

        let message = message_list.get(1).unwrap();
        if let Response::Offset(offset) = &message.response {
            assert_eq!(*offset, 101);
        } else {
            assert!(false);
        }

        let message = message_list.get(2).unwrap();
        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 102);
            assert_eq!(value, "last content");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_parse_multiple_content_responses() {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(100, String::from("nice content"))).as_vec(),
        );

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(101, String::from("other content"))).as_vec(),
        );

        bytes.extend_from_slice(&ResponseMessage::new(Response::Error).as_vec());

        bytes.extend_from_slice(
            &ResponseMessage::new(Response::Content(102, String::from("last content"))).as_vec(),
        );

        let message_list = ResponseMessage::parse(&bytes[..]);

        assert_eq!(message_list.len(), 4);

        let message = message_list.first().unwrap();
        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 100);
            assert_eq!(value, "nice content");
        } else {
            assert!(false);
        }

        let message = message_list.get(1).unwrap();
        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 101);
            assert_eq!(value, "other content");
        } else {
            assert!(false);
        }

        let message = message_list.get(2).unwrap();
        assert!(matches!(message.response, Response::Error));

        let message = message_list.get(3).unwrap();
        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 102);
            assert_eq!(value, "last content");
        } else {
            assert!(false);
        }
    }
}
