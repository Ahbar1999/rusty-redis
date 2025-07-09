use std::{collections::HashMap, fmt::format, hash::Hash, io::ErrorKind, mem::uninitialized, time::SystemTime, vec};
// use bytes::{Bytes, BytesMut};
use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}};

const DELIM: u8 = b'\r';
const SKIP_LEN: usize = 2;
const DIR: &str = "/tmp/redis-files/";

// #[derive(Debug, Hash]
// struct RKey<'a> {
//     key: &'a str,
//     timeout: usize
// }

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
    let mut storage: HashMap<String, (String, SystemTime, usize)> = HashMap::new();
    let args: Vec<String> = std::env::args().collect();
    let mut dir = "UNSET";
    let mut dbfilename = "UNSET";

    if args.len() > 4 {
        dir = &args[2];
        dbfilename = &args[4];
    }

    // println!("{:?}", args);
    let mut input_buf: Vec<u8> = vec![0; 1024]; 
    loop {
        input_buf.fill(0);
        _stream.readable().await.unwrap();
        
        match _stream.try_read(&mut input_buf) {
            Ok(bytes_rx) => {
                if bytes_rx == 0 {
                    break;
                }
            },
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // this error handling in necessary otherwise it would necessarily block
                continue;
            },
            Err(e) => {
                println!("{}", e);
            }
        }

        // we know that its a string (for now)
        // respond to th command; currently without any error handling
        // _stream.writable().await.unwrap();
        // let data = input_buf[0..bytes_rx]; 
        let mut output: String = String::from("");
        let mut args = parse(0, &input_buf);  // these are basically commands, at one point we will have to parse commands with their parameters, they could be int, boolean etc.   
        args[0] = args[0].to_uppercase();

        match args[0].as_str() {
            "ECHO" => {
                output = encode_bulk(&args[1]);
            },
            "PING" => {
                output = encode_bulk("PONG");
            },
            "SET" => {
                let mut timeout: usize = usize::MAX;    // inf
                if args.len() > 3 { // input validation is not being performed
                    // SET foo bar px milliseconds
                    timeout = args[4].parse().unwrap();
                }
                cmd_set(&args[1], &args[2], SystemTime::now(),timeout, &mut storage);
                output = response_ok();
            },
            "GET" => {
                match cmd_get(&args[1], &mut storage) {
                    Some(s) => {
                        output = encode_bulk(s);
                    },
                    None => {
                        output = encode_bulk(&output);
                    }
                }
            },
            "CONFIG" => {
                match args[2].as_str() {
                    "dir" => {
                        output = format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", dir.len(), dir);
                    },
                    "dbfilename" => {
                        output = format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n", dbfilename.len(), dbfilename);
                    }
                    ,
                    _ => {
                        unimplemented!();
                    }
                }
            },
            _ => {
                unimplemented!();
            }
        } 
        // if args[0] == "ECHO" {
        //     output = encode_bulk(&args[1]);
        // }
        // if args[0] == "PING" {
        //     output = encode_bulk("PONG");
        //     // args[0] = String::from("PONG");
        // }
        // if args[0].to_uppercase() == "SET" {
        //     let mut timeout: usize = usize::MAX;    // inf
        //     if args.len() > 3 { // input validation is not being performed
        //         // SET foo bar px milliseconds
        //         timeout = args[4].parse().unwrap();
        //     }
        //     cmd_set(&args[1], &args[2], SystemTime::now(),timeout, &mut storage);
        //     output = response_ok();
        // }
        // if args[0].to_uppercase() == "GET" {
        //     match cmd_get(&args[1], &mut storage) {
        //         Some(s) => {
        //             output = encode_bulk(s);
        //         },
        //         None => {
        //             output = encode_bulk(&output);
        //         }
        //     }
        // }

        // let output =format!("${}\r\n{}\r\n", res.len(), res);
        // let output = String::from("$") + stringify!(sz) + "\r\n" + &out + "\r\n";  
        // match 

        println!("sending: {}", output);
        _stream.write_all(output.as_bytes()).await.unwrap();
        // if you wanna use try_write() you will need to manually send data piece by piece by tracking bytes sent 
        // match _stream.try_write(output.as_bytes()) {
        //     Ok(_) => {
        //         continue; 
        //     },
        //     Err(e) if e.kind() == ErrorKind::WouldBlock => {
        //         continue;
        //     },
        //     Err(e) => {
        //         println!("{}", e);
        //         break;
        //     }
        // } 
        // {
    }
}


fn get_dir() -> String {
    format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", DIR.len(), DIR)
}

// returns +OK\r\n
fn response_ok() -> String {
    String::from("+OK\r\n")
}

fn cmd_set(key: &String, val: &String, val2: SystemTime, val3: usize,  storage: &mut HashMap<String, (String, SystemTime, usize)>) {
    println!("setting {} to {} for {:?}ms", key, val, val2);
    storage.insert(key.clone(), (val.clone(), val2, val3));
}

// first correct use of lifetimes ???? 
// i used to pray for times like these 
fn cmd_get<'a>(key: &String, storage: &'a mut HashMap<String, (String, SystemTime, usize)>) -> Option<&'a String> {
    println!("searching for {:?}", key);

    // rewrap for our purposes
    match storage.get(key) {
        Some((res, t, exp)) => {
            match t.elapsed() {
                Ok(t_elapsed) => {
                    // println!("time elapsed: {:?}", t_elapsed);
                    if t_elapsed.as_millis() > *exp as u128 {
                        // we could add deletion but we leave it for now
                        // as it would require us to return concrete strings not slices
                        // because after deletion slices wont be available to return hence the compilation error
                        // storage.remove_entry(key);
                        None
                    } else {
                        Some (res)
                    }
                },
                Err(e) => {
                    println!("{}", e);
                    None
                }
            }
        },
        None => {
            println!("did not find key");
            None
        }
    }
}

fn encode_bulk(s: &str) -> String {
    if s.is_empty() {
        // return NULL string
        return format!("$-1\r\n");
    }

    let data = String::from(s);
    
    format!("${}\r\n{}\r\n", data.len(), data)
}

// parse a single command
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
        b'*' => {
            let (_, v) = parse_array(ptr, buf);
            v
        }
        _ => unimplemented!()
    }
}

// you might have to call this recursively
fn parse_array(ptr: usize, buf: &[u8]) -> (usize, Vec<String>) {
    let mut size =0;
    let mut i = ptr + 1;
    while buf[i] != DELIM {
        size = 10 * size + (buf[i] as char).to_digit(10).unwrap() as usize;
        i += 1;
    }
    i += SKIP_LEN;
    // println!("size of array: {}", size);

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

    (i, result)
}

fn parse_string(ptr: usize, buf: &[u8]) -> (usize, String) {
    let mut size =0 as usize;
    let mut i = ptr + 1; // ptr points to string identifier: '$'
    while buf[i] != DELIM {
        size = 10 * size + (buf[i] as char).to_digit(10).unwrap() as usize;
        i += 1;
    }
    i += SKIP_LEN; // skip both \r and \n

    let mut result = String::new();

    for j in 0..size {
        result.push(buf[j as usize + i] as char);
    }

    i += size;
    i += SKIP_LEN;

    println!("string parsed: {}", result);
    return (i, result); 
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
