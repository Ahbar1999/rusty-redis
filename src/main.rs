#![allow(unused_imports)]
use std::{io::{BufRead, BufReader, Read, Write}, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let mut input_buf: Vec<u8> = vec![0; 1024]; 
    let mut bytes_rx =0;

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                loop {
                    // READ one PING, SEND one PONG
                    bytes_rx = _stream.read(&mut input_buf).unwrap();
                    if bytes_rx == 0 {
                        break;
                    }
                    _stream.write(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
