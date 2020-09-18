use std::io::prelude::*;
use std::net::TcpStream;
use std::thread;
use std::time::Instant;

fn main() {
    let consumer = thread::spawn(move || {
        let last_value = b"nice message 1999999";
        let start = Instant::now();
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();
        let mut i = 0;

        loop {
            stream.write_all("c\r\n".as_bytes()).unwrap();
            stream.flush().unwrap();
            let mut buffer = [0; 512];
            let size = match stream.read(&mut buffer) {
                Ok(value) => value,
                Err(err) => {
                    println!("Failed to read stream\n{}", err);
                    continue;
                }
            };

            if buffer.starts_with(last_value) {
                println!(
                    "CONSUMER MESSAGE FOUND {}",
                    String::from_utf8_lossy(&buffer[..size])
                );
                break;
            }

            if i % 50_000 == 0 {
                print!(
                    "CONSUMED MESSAGE: {} WITH VALUE VALUE: {}",
                    i,
                    String::from_utf8_lossy(&buffer[..size])
                );
            }
            i += 1;
        }

        let duration = start.elapsed();

        println!("DURATION CONSUMER: {:?}", duration);
    });

    let producer = thread::spawn(move || {
        let start = Instant::now();
        let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();

        for i in 0..2_000_000 {
            stream
                .write_all(format!("pnice message {}\r\n", i).as_bytes())
                .unwrap();
            stream.flush().unwrap();
            let mut buffer = [0; 512];
            let _ = match stream.read(&mut buffer) {
                Ok(value) => value,
                Err(err) => {
                    println!("Failed to read stream\n{}", err);
                    continue;
                }
            };

            if i % 50_000 == 0 {
                println!("PRODUCED MESSAGE: {}", i);
            }
        }
        let duration = start.elapsed();

        println!("DURATION PRODUCER: {:?}", duration);
    });

    consumer.join().unwrap();
    producer.join().unwrap();
}
