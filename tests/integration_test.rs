use ::codecrafters_redis::redis_cli;
use std::net::TcpStream;
use std::io::{Write, Read};
use std::process::exit;
use std::thread;

#[test]
fn test_ping() {
    // define args of this test
    let args = vec!["redis-cli".to_owned(), "--port".to_owned(), "6380".to_owned()];
    // start the server 
    let _ = thread::spawn(|| { redis_cli(args.into_iter()); });
    
    // connect to server 
    let mut stream = TcpStream::connect("localhost:6380").unwrap();

    if stream.write_all("*1\r\n$4\r\nPING\r\n".as_bytes()).is_err() {
        println!("couldnt send PING to the server!, exiting.");
        exit(1);
    }

    // Create a buffer to receive data
    let mut buffer = vec![0; 1024]; 
    // Read data from the server into the buffer
    let bytes_read = stream.read(&mut buffer).unwrap(); 
   
    assert!(&buffer[0..bytes_read] == "+PONG\r\n".as_bytes()); 
}

