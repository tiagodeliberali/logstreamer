pub struct TopicAddress {
    pub name: String,
    pub partition: u32,
}

impl TopicAddress {
    pub fn new(name: String, partition: u32) -> TopicAddress {
        TopicAddress { name, partition }
    }
}

#[derive(Clone)]
pub struct Content {
    pub value: String,
}

impl Content {
    pub fn new(value: String) -> Content {
        Content { value }
    }
}
