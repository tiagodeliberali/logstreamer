use crate::communication::{Response, ResponseMessage};
use crate::core::{Content, OffsetValue, TopicAddress};
use crate::storage::Cluster;

pub struct Broker {
    cluster: Cluster,
}

impl Broker {
    pub fn new() -> Broker {
        let cluster = Cluster::new();
        Broker { cluster }
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
