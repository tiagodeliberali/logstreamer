use crate::core::{Content, OffsetValue, TopicAddress};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

pub struct Cluster {
    topics: RwLock<HashMap<String, Vec<Arc<Partition>>>>,
}

impl Cluster {
    pub fn new() -> Cluster {
        Cluster {
            topics: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_topic(&self, topic_name: String, partition_number: usize) {
        let mut partitions = Vec::with_capacity(partition_number);
        for _ in 0..partition_number {
            partitions.push(Arc::new(Partition::new()));
        }
        self.topics.write().unwrap().insert(topic_name, partitions);
    }

    pub fn get_partition(&self, topic: TopicAddress) -> Option<Arc<Partition>> {
        let topics = self.topics.read().unwrap();
        match topics.get(&topic.name) {
            Some(partition_list) => {
                let partition = partition_list.get(topic.partition as usize).unwrap();
                Some(partition.clone())
            }
            None => None,
        }
    }

    pub fn add_content(&self, topic: TopicAddress, content: Content) -> Option<OffsetValue> {
        let topics = self.topics.read().unwrap();
        match topics.get(&topic.name) {
            Some(partition_list) => {
                let partition = partition_list.get(topic.partition as usize).unwrap();
                let offset = partition.add_content(content);
                Some(offset)
            }
            None => None,
        }
    }
}

pub struct Partition {
    pub queue: Mutex<Vec<Content>>,
}

impl Partition {
    pub fn new() -> Partition {
        Partition {
            queue: Mutex::new(Vec::new()),
        }
    }

    pub fn add_content(&self, content: Content) -> OffsetValue {
        let mut locked_queue = self.queue.lock().unwrap();
        locked_queue.push(content);
        OffsetValue(locked_queue.len() as u32 - 1)
    }
}
