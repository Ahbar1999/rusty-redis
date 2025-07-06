#![allow(unused_imports)]
use std::io::{BufRead, BufReader, Read, Write, ErrorKind};
use tokio::net::{TcpStream, TcpListener};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, _)  = listener.accept().await.unwrap();
        // this spawns a tokio "asyncrhonous green thread" in the same program's thread
        tokio::spawn(async move {
            conn(stream).await;
        });
    }
}

async fn conn(_stream: TcpStream) {

    let mut input_buf: Vec<u8> = vec![0; 1024]; 
    loop {
        // READ one PING, SEND one PONG
        _stream.readable().await.unwrap();
        match _stream.try_read(&mut input_buf) {
            Ok(bytes_rx) => {
                if bytes_rx == 0 {
                    break;
                }   // break out of connection if no more commands to read

                // respond to th command
                _stream.writable().await.unwrap();

                _stream.try_write(b"+PONG\r\n").unwrap();
                // if you want error handling, do it the following way
                // match _stream.try_write(b"+PONG\r\n") {
                //     Ok(bytes_tx) => {
                //         // bytes_tx < 0 ? do some error handling
                //         continue;
                //     } 
                //     Err(e) => {
                //         println!("{:?}", e);
                //     }
                // }
            } 
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }
}
