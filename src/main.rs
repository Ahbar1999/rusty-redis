use std::{fmt::format, io::ErrorKind, mem::uninitialized, vec};
// use bytes::{Bytes, BytesMut};
use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}};

const DELIM: u8 = b'\r';

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, _)  = listener.accept().await.unwrap();
        // this spawns a tokio "asyncrhonous green thread" 
        tokio::spawn(async move {
            conn(stream).await;
        });
    }
}

async fn conn(mut _stream: TcpStream) { // represents an incoming connection

    let mut input_buf: Vec<u8> = vec![0; 1024]; 
    loop {
        input_buf.fill(0);
        _stream.readable().await.unwrap();
        
        // let bytes_rx = _stream.try_read(&mut input_buf).unwrap();    // refactored to a single if
        // let bytes_rx = _stream.try_read(&mut input_buf).unwrap();
        match _stream.try_read(&mut input_buf) {
            Ok(bytes_rx) => {
                if bytes_rx == 0 {
                    break;
                }
            },
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                continue;
            },
            Err(e) => {
                println!("{}", e);
            }
        }
        // we know that its a string (for now)
        // let (sz, out) = parse_string(1, &input_buf);
        // respond to th command; currently without any error handling
        _stream.writable().await.unwrap();
        // let data = input_buf[0..bytes_rx]; 
        let result = parse(0, &input_buf);
        for mut res in result {
            if res == "ECHO" {
                continue;
            }
            if res == "PING" {
                res = String::from("PONG");
            }
            let output =format!("${}\r\n{}\r\n", res.len(), res);
            // let output = String::from("$") + stringify!(sz) + "\r\n" + &out + "\r\n";  
            // match 

            println!("sending: {}", output);
            _stream.write_all(output.as_bytes()).await.unwrap(); 
            // {
                // Ok(bytes_tx) => {
                //    println!("{} bytes sent", bytes_tx); 
                // },
                // Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                //     continue;
                // },
                // Err(e) => {
                //     println!("{:?}", e);
                // },

            // }
        }
        // _stream.try_write(b"+PONG\r\n").unwrap();
        /* 
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
        */
    }
}

// struct BufSlice(usize, usize);
// enum RESPType {
//     Int(BufSlice), 
//     String(BufSlice),
//     Boolean(BufSlice), 
//     Double(BufSlice), 
//     Array(Vec<RESPType>),
//     Null(BufSlice)
// }

fn parse(ptr: usize, buf: &[u8]) -> Vec<String> {
    // print!("{:?}", buf);
    // for b in buf {
    //     print!("{} ", *b as char);
    // }
    // println!("");

    match buf[ptr] {
        b'$' => {
            let (_, s) = parse_string(ptr, buf);
            vec![s]
        },
        b'*' => parse_array(ptr, buf),
        _ => unimplemented!()
    }
}

fn parse_array(ptr: usize, buf: &[u8]) -> Vec<String> {
    let mut size =0;
    let mut i = ptr + 1;
    while buf[i] != DELIM {
        size = 10 * size + (buf[i] as char).to_digit(10).unwrap() as usize;
        i += 1;
    }
    i += 2;
    println!("size of array: {}", size);

    let mut result:Vec<String> = vec![];
    for _ in 0..size {
        match buf[i] as char {
            '$' => {
                let (new_ptr, out) = parse_string(i, buf);
                i = new_ptr;
                result.push(out);
            }
            _ => {
                unimplemented!()
            }
        }
    } 

    result
}

fn parse_string(ptr: usize, buf: &[u8]) -> (usize, String) {
    let mut size =0 as usize;
    let mut i = ptr + 1; // ptr points to string identifier: '$'
    while buf[i] != DELIM {
        size = 10 * size + (buf[i] as char).to_digit(10).unwrap() as usize;
        i += 1;
    }
    i += 2; // skip both \r and \n
    
    // println!("size of string: {}", size);
    // skip "ECHO" 
    // while buf[i] !=  DELIM {
    //     i += 1;
    // }
    // i += 2;

    let mut result = String::new();

    for j in 0..size {
        result.push(buf[j as usize + i] as char);
    }

    i += size;
    i += 2;

    println!("string parsed: {}", result);
    return (i, result); 
}

// return (current index, (start, end)) 
// fn parse(buf:  &BytesMut) -> (usize, BufSlice)  {
//     if buf.is_empty() {
//         unimplemented!();
//     } 

//     match &buf[ptr] {
//         b'*' => {
//             // Array
//             let mut size = 0;
//             while buf[ptr] != DELIM {
//                 size = 10 * size + i32::from(buf[ptr]);
//                 ptr += 1;
//             }
//             ptr += 1;

//             // we know size number of elements are there in this array 
//             for i in 0..size {
//                 // this might return a string, an int, or another array
//                 let res = parse(ptr, buf); 
//             }

//             unimplemented!(); 
//         } 
//         b'$' => {
//             parse_string(ptr, )
//         }
//         _ => {
//             unimplemented!();
//         }
//         unimplemented!();
//     }


// 