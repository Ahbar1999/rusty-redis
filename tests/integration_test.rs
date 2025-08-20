use ::codecrafters_redis::redis_cli;
use std::fs::File;
use std::net::TcpStream;
use std::io::{BufReader, Read, Write};
use std::process::exit;
use std::thread;
use serde::Deserialize;

const SERVER_PORT: &str = "6380";

#[derive(Debug, Deserialize)]
pub struct Test {
    pub seq_no: usize, 
    pub input: String,
    pub output: String,
    pub about: String,
}


#[test]
fn test_ping() {
    // define args of this test
    let args = vec!["redis-cli".to_owned(), "--port".to_owned(), SERVER_PORT.to_owned()];
    // start the server 
    let _ = thread::spawn(|| { redis_cli(args.into_iter()); });
    // connect to server 
    let mut stream = TcpStream::connect("localhost:6380").unwrap();

    if stream.write_all("*1\r\n$4\r\nPING\r\n".as_bytes()).is_err() {
        println!("couldn't PING the server!, exiting.");
        assert!(false);
        exit(1);
    }

    let mut buffer = vec![0; 1024]; 
    let bytes_read = stream.read(&mut buffer).unwrap(); 
   
    assert!(&buffer[0..bytes_read] == "+PONG\r\n".as_bytes()); 
}

#[test]
fn test_acl_auth() {
    let args = vec!["redis-cli".to_owned(), "--port".to_owned(), SERVER_PORT.to_owned()]; 
    let _ = thread::spawn(|| { redis_cli(args.into_iter()); });
    let mut stream = TcpStream::connect("localhost:6380").unwrap();
    let mut buffer = vec![0; 1024]; 

    let mut test_file_path = std::env::current_dir().unwrap();
    test_file_path.push("tests/config/redis-acl-tests_sequential.csv");
    // println!("{}", test_file_path.to_string_lossy());
    let tests_file = File::open(test_file_path).unwrap();

    let mut rdr = csv::Reader::from_reader(BufReader::new(tests_file));

    for result in rdr.deserialize() {
        buffer.clear(); 
        let test: Test = result.unwrap();
        println!("{:?}", test);
        if stream.write_all(test.input.as_bytes()).is_err() {
            println!("failed to transmit input for test: {:?}", test);
            exit(1);
        }  

        let bytes_read = stream.read(&mut buffer).unwrap(); 
    
       assert!(&buffer[0..bytes_read] == test.output.as_bytes());
    }  
}

