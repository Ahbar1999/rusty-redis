#![allow(unused_imports)]
use std::{io::{BufRead, BufReader, Read, Write}, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let mut input_buf = String::from("");
    let mut reader; 
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("conn recvd!");
                reader = BufReader::new(&_stream);
                reader.read_to_string(&mut input_buf).unwrap();
                // _stream.read_to_string(&mut input_buf).unwrap();
                println!("{:?}", &input_buf);
                for line in input_buf.split('\n') {
                    if line == "PING" {
                        _stream.write(b"+PONG\r\n").unwrap();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
