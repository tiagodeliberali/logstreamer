fn read_string(buffer: &[u8], position: usize) -> (String, usize) {
    let size = buffer[position] as usize;
    (
        String::from_utf8_lossy(&buffer[(position + 1)..(position + 1 + size)]).to_string(),
        size,
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
    content.push(value.len() as u8);
    content.extend_from_slice(value.as_bytes());
}

fn write_u32(content: &mut Vec<u8>, value: u32) {
    content.extend_from_slice(&value.to_be_bytes());
}

pub enum Action {
    Produce(String),
    Consume(u32),
    CommitOffset(u32),
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
                let (content, size) = read_string(buffer, 1);
                consumer_id_position += 1 + size;
                Action::Produce(content)
            }
            2 => {
                let (limit, size) = read_u32(buffer, 1);
                consumer_id_position += size;
                Action::Consume(limit)
            }
            3 => {
                let (offset, size) = read_u32(buffer, 1);
                consumer_id_position += size;
                Action::CommitOffset(offset)
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
            Action::Produce(value) => {
                content.push(1);
                write_string(&mut content, value.clone());
            }
            Action::Consume(limit) => {
                content.push(2);
                write_u32(&mut content, *limit);
            }
            Action::CommitOffset(value) => {
                content.push(3);
                write_u32(&mut content, *value);
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
                    position += offset_size + content_size + 1;
                    Response::Content(offset, content)
                }
                2 => {
                    position += 1;
                    let (offset, offset_size) = read_u32(buffer, position);
                    position += offset_size;
                    Response::Offset(offset)
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
        }

        content
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_invalid_action_to_vec() {
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(Action::Invalid, consumer_id.clone());

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 0); // action id
        assert_eq!(parsed_message[1], 11); // consumer_id length
        assert_eq!(String::from_utf8_lossy(&parsed_message[2..13]), consumer_id);
    }

    #[test]
    fn should_parse_invalid_action() {
        let mut bytes = vec![
            0,  // action id
            11, // consumer_id length
        ];
        bytes.extend_from_slice(b"consumer_id");

        let message = ActionMessage::parse(&bytes[..]);

        assert!(matches!(message.action, Action::Invalid));
        assert_eq!(message.consumer_id, "consumer_id");
    }

    #[test]
    fn should_convert_quit_action_to_vec() {
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(Action::Quit, consumer_id.clone());

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 4); // action id
        assert_eq!(parsed_message[1], 11); // consumer_id length
        assert_eq!(String::from_utf8_lossy(&parsed_message[2..13]), consumer_id);
    }

    #[test]
    fn should_parse_quit_action() {
        let mut bytes = vec![
            4,  // action id
            11, // consumer_id length
        ];
        bytes.extend_from_slice(b"consumer_id");

        let message = ActionMessage::parse(&bytes[..]);

        assert!(matches!(message.action, Action::Quit));
        assert_eq!(message.consumer_id, "consumer_id");
    }

    #[test]
    fn should_convert_consume_action_to_vec() {
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(Action::Consume(10), consumer_id.clone());

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 2); // action id
        assert_eq!(parsed_message[4], 10); // consumer_id length
        assert_eq!(parsed_message[5], 11); // consumer_id length
        assert_eq!(String::from_utf8_lossy(&parsed_message[6..17]), consumer_id);
    }

    #[test]
    fn should_parse_consume_action() {
        let mut bytes = vec![
            2, // action id
            0, 0, 0, 10, // limit
            11, // consumer_id length
        ];
        bytes.extend_from_slice(b"consumer_id");

        let message = ActionMessage::parse(&bytes[..]);

        if let Action::Consume(limit) = message.action {
            assert_eq!(limit, 10);
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, "consumer_id");
    }

    #[test]
    fn should_convert_produce_action_to_vec() {
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(
            Action::Produce(String::from("Message Content")),
            consumer_id.clone(),
        );

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 1); // action id
        assert_eq!(parsed_message[1], 15); // consumer_id length
        assert_eq!(
            String::from_utf8_lossy(&parsed_message[2..17]),
            "Message Content"
        );
        assert_eq!(parsed_message[17], 11); // consumer_id length
        assert_eq!(
            String::from_utf8_lossy(&parsed_message[18..29]),
            consumer_id
        );
    }

    #[test]
    fn should_parse_produce_action() {
        let mut bytes = vec![
            1,  // action id
            15, // content length
        ];
        bytes.extend_from_slice(b"Message Content");
        bytes.push(11);
        bytes.extend_from_slice(b"consumer_id");

        let message = ActionMessage::parse(&bytes[..]);

        if let Action::Produce(value) = message.action {
            assert_eq!(value, "Message Content");
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, "consumer_id");
    }

    #[test]
    fn should_convert_commit_offset_action_to_vec() {
        let consumer_id = String::from("consumer_id");
        let message = ActionMessage::new(Action::CommitOffset(100), consumer_id.clone());

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 3); // action id
        assert_eq!(parsed_message[4], 100); // offset (4 bytes)
        assert_eq!(parsed_message[5], 11); // consumer_id length
        assert_eq!(String::from_utf8_lossy(&parsed_message[6..17]), consumer_id);
    }

    #[test]
    fn should_parse_commit_offset_action() {
        let mut bytes = vec![
            3, // action id
            0, 0, 0, 100, // offset (4 bytes)
            11,  // content length
        ];
        bytes.extend_from_slice(b"consumer_id");

        let message = ActionMessage::parse(&bytes[..]);

        if let Action::CommitOffset(value) = message.action {
            assert_eq!(value, 100);
        } else {
            assert!(false);
        }

        assert_eq!(message.consumer_id, "consumer_id");
    }

    #[test]
    fn should_convert_empty_response_to_vec() {
        let message = ResponseMessage::new(Response::Empty);

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 0); // action id
    }

    #[test]
    fn should_parse_empty_response() {
        let bytes = vec![
            0, // action id
        ];

        let message = ResponseMessage::parse(&bytes[..]);
        let message = message.first().unwrap();

        assert!(matches!(message.response, Response::Empty));
    }

    #[test]
    fn should_convert_content_response_to_vec() {
        let message = ResponseMessage::new(Response::Content(100, String::from("nice content")));

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 1); // action id
        assert_eq!(parsed_message[4], 100); // action id
        assert_eq!(parsed_message[5], 12); // content length
        assert_eq!(
            String::from_utf8_lossy(&parsed_message[6..18]),
            "nice content"
        );
    }

    #[test]
    fn should_parse_consume_response() {
        let mut bytes = vec![
            1, // action id
            0, 0, 0, 100, // offset (4 bytes)
            12,  // content length
        ];
        bytes.extend_from_slice(b"nice content");

        let message = ResponseMessage::parse(&bytes[..]);
        let message = message.first().unwrap();

        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 100);
            assert_eq!(value, "nice content");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_convert_offset_response_to_vec() {
        let message = ResponseMessage::new(Response::Offset(100));

        let parsed_message = message.as_vec();

        assert_eq!(parsed_message[0], 2); // action id
        assert_eq!(parsed_message[4], 100); // offset
    }

    #[test]
    fn should_parse_offset_response() {
        let bytes = vec![
            2, // action id
            0, 0, 0, 100, // offset (4 bytes)
        ];

        let message = ResponseMessage::parse(&bytes[..]);
        let message = message.first().unwrap();

        if let Response::Offset(value) = message.response {
            assert_eq!(value, 100);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn should_parse_multiple_consumes_response() {
        let mut bytes = vec![
            1, // action id
            0, 0, 0, 100, // offset (4 bytes)
            12,  // content length
        ];
        bytes.extend_from_slice(b"nice content");

        bytes.extend_from_slice(&[
            2, // action id
            0, 0, 0, 101, // offset (4 bytes)
        ]);

        bytes.extend_from_slice(&[
            1, // action id
            0, 0, 0, 102, // offset (4 bytes)
            12,  // content length
        ]);
        bytes.extend_from_slice(b"last content");

        let message_list = ResponseMessage::parse(&bytes[..]);

        assert_eq!(message_list.len(), 3);

        let message = message_list.first().unwrap();
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
    fn should_parse_mixed_responses() {
        let mut bytes = vec![
            1, // action id
            0, 0, 0, 100, // offset (4 bytes)
            12,  // content length
        ];
        bytes.extend_from_slice(b"nice content");

        bytes.extend_from_slice(&[
            1, // action id
            0, 0, 0, 101, // offset (4 bytes)
            13,  // content length
        ]);
        bytes.extend_from_slice(b"other content");

        bytes.extend_from_slice(&[
            1, // action id
            0, 0, 0, 102, // offset (4 bytes)
            12,  // content length
        ]);
        bytes.extend_from_slice(b"last content");

        let message_list = ResponseMessage::parse(&bytes[..]);

        assert_eq!(message_list.len(), 3);

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
        if let Response::Content(offset, value) = &message.response {
            assert_eq!(*offset, 102);
            assert_eq!(value, "last content");
        } else {
            assert!(false);
        }
    }
}
