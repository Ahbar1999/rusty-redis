#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let mut input_buf = String::from("");
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                _stream.read_to_string(&mut input_buf).unwrap();
                println!("{}", &input_buf);
                for cmd in input_buf.split('\n') {
                    if cmd == "PING" {
                        _stream.write_all(b"+PONG\r\n").unwrap();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
