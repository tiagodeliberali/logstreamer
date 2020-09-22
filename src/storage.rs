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

    pub fn get_partition(&self, topic: String, partition: u32) -> Option<Arc<Partition>> {
        let topics = self.topics.read().unwrap();
        match topics.get(&topic) {
            Some(partition_list) => {
                let partition = partition_list.get(partition as usize).unwrap();
                Some(partition.clone())
            }
            None => None,
        }
    }

    pub fn add_content(&self, topic: String, partition: u32, content: String) -> Option<u32> {
        let topics = self.topics.read().unwrap();
        match topics.get(&topic) {
            Some(partition_list) => {
                let partition = partition_list.get(partition as usize).unwrap();
                let offset = partition.add_content(content);
                Some(offset as u32)
            }
            None => None,
        }
    }
}

pub struct Partition {
    pub queue: Mutex<Vec<String>>,
}

impl Partition {
    pub fn new() -> Partition {
        Partition {
            queue: Mutex::new(Vec::new()),
        }
    }

    pub fn add_content(&self, content: String) -> u32 {
        let mut locked_queue = self.queue.lock().unwrap();
        locked_queue.push(content);
        (locked_queue.len() - 1) as u32
    }
}
