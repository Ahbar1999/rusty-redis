use std::{collections::HashMap, fmt::format, fs::create_dir, hash::Hash, io::{stdout, BufWriter, ErrorKind, Write}, mem::uninitialized, ops::BitAnd, slice, time::{Duration, SystemTime, UNIX_EPOCH}, vec};
use bytes::{BufMut, BytesMut};
// use bytes::{Bytes, BytesMut};
use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use crc64::crc64;

const DELIM: u8 = b'\r';
const SKIP_LEN: usize = 2;
const _RDB_METADATA_SECTION_FLAG_: u8 = 0xFA;
const _RDB_DATA_SECTION_FLAG_: u8 = 0xFE;
const _RDB_END_: u8 = 0xFF;
const _RDB_TIMESTAMP_MS_FLAG: u8 = 0xFC;
const _RDB_TIMESTAMP_S_FLAG: u8 = 0xFC;

// const RDB_METADATA: &str = "FA\n";   // no metadata subsection for not 
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
    let mut storage: HashMap<String, (String, SystemTime, Option<usize>)> = HashMap::new();
    let params: Vec<String> = std::env::args().collect();
    let mut dir = String::from("dir/");
    let mut dbfilename = String::from("dump.rdb");

    if params.len() > 4 {
        dir = params[2].clone() + "/";
        dbfilename = params[4].clone();
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
                let mut timeout = Option::None;    // inf
                if args.len() > 3 { // input validation is not being performed
                    // SET foo bar px milliseconds
                    timeout = Option::Some(args[4].parse().unwrap());
                }
                cmd_set(&args[1], &args[2], SystemTime::now(),timeout, &mut storage);
                // let _ = cmd_save(&storage, &dir, &dbfilename).await;  // for testing
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
                        // because we are adding an extra forward slash to join paths, we need to return the original string
                        output = format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", dir.len() - 1, dir[0..(dir.len()-1)].to_string());
                        dir = output.clone();
                    },
                    "dbfilename" => {
                        output = format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n", dbfilename.len(), dbfilename);
                        dbfilename = output.clone();
                    }
                    ,
                    _ => {
                        unimplemented!();
                    }
                }
            },
            "SAVE" => {
                output = cmd_save(&storage, &dir, &dbfilename).await;
            },
            "KEYS" => {
                output = cmd_keys(&dir, &dbfilename).await;
            },
            _ => {
                unimplemented!();
            }
        } 

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

async fn cmd_keys(dir: &String, dbfilename: &String) -> String {
    let mut matched_keys: Vec<String> = vec![];
    // read the rdb file
    // read the keys, match them against some given pattern
    // we could simply search this in the storage map but i wanna do it the right way
    let path =dir.clone() + dbfilename.as_str();
    println!("reading from file {}", &path);
    let file = File::open(&path).await;
    let mut buf: Vec<u8> = vec![];
    match file {
        Ok(mut f) => {
            f.read_to_end(&mut buf).await.unwrap();
        },
        Err(ref e) if e.kind() == ErrorKind::NotFound => {
            // db file does not exist so no data found
            return encode_array(&matched_keys);
        },
        Err(e) => {
            panic!("{}", e);
        }
    }

    // print the hex dump of the file
    println!("{}", buf.iter().map(|b| format!("{:02X} ", b)).collect::<String>());

    let mut key: String = String::new();
    let mut i = 9; // skip header bytes
    let mut mask =0;

    while buf[i] == _RDB_METADATA_SECTION_FLAG_ {
        i += 1;                     // skip the flag(current)
        // parse key value
        {   // key value pairs depending on data type inferred from encoding 
            for _ in 0..2 { // once for key, once for value
                mask = buf[i].bitand(3 << 6);
                if mask == 0 {
                    i += 1 + buf[i] as usize;     // skip size byte + next bytes of string(which are give by lower 6 bits)
                } else {    // mask == 1100.00
                    // string contains an integer
                    i += 1; // size byte (current)
                    if buf[i] & 3 == 0 {
                        i += 1;             // 8 bit integer follows
                    } else if buf[i] & 3 == 1 {
                        i += 2;             // 16 bit integer follows (in little endian)
                    } else if buf[i] & 3 == 2 {
                        i += 4;             // 32 bit integer follows
                    }
                }
            }
        }
        // i += 1 + buf[i] as usize;   // skip the metadata key
        // i += 1 + buf[i] as usize;   // skip the metadata value  
    }

    while buf[i] == _RDB_DATA_SECTION_FLAG_ {
        i += 1;     // DB section flag(current)
        i += 1;     // DB index
        i += 1;     // storage info section flag
        i += 1;     // untimed k,v pairs
        i += 1;     // timed k,v pairs
    }

    while i < buf.len() {
        key = String::new();
        // 0xFF marks end of the db file section
        if buf[i] == _RDB_END_ {
            break;
        }

        // i += 1;     // skip flag byte
        if buf[i] == _RDB_TIMESTAMP_MS_FLAG || buf[i] == _RDB_TIMESTAMP_S_FLAG { // if timestamp flag present
            i += 1;     // skip flag byte(current)
            i += 4;     // skip timestamp(next 4 bytes, if timestamp was stored in secs)
        }
        i += 1;     // skip value type byte

        // if k is an integer then you need to parse it like above  
        // buf[i] contains size encoded string size
        for &ch in &buf[(i + 1)..(i + 1 + buf[i] as usize)] {
            key.push(char::from(ch));
        }

        // currently we return all the keys because we are matching against * pattern 
        matched_keys.push(key);

        // skip key size byte + key length bytes 
        i += 1 + buf[i] as usize;    // skip key bytes
        i += 1 + buf[i] as usize;    // skip value bytes
    }

    encode_array(&matched_keys)
}

async fn cmd_save(storage: &HashMap<String, (String, SystemTime, Option<usize>)>, dir: &String, dbfilename: &String) -> String {
    // create new directory 
    tokio::fs::create_dir_all(&dir).await.unwrap();
    println!("{} directory created", &dir);
    // create new dbfile with "dbfilename" in the dir directory 
    let path = dir.clone() + dbfilename.as_str();

    println!("creating file {}", &path); 
    let mut out = tokio::fs::File::create(&path).await.unwrap();
    let mut out_bytes: Vec<u8> = vec![];
    for &b in b"REDIS0011" { // +9 bytes
        out_bytes.put_u8(b);
    }

    out_bytes.put_u8(0xFE); // flag for start of db section; +2B 
    out_bytes.put_u8(0);        // index of db; +1B
    out_bytes.put_u8(0xFB); // flag for size of hash table; +2B

    // one byte: first two bits are 0 => next 6 bits represent the size 
    // count of total k, v pairs
    // todo!("following two sizes need to be different!");
    out_bytes.put_u8(storage.len() as u8);   // +1B
    // out.write_all(&((storage.len() << 2) as u8).to_le_bytes()).await.unwrap(); // size encoded table size value 
    // count of timed k, v pairs same as above;
    out_bytes.put_u8(storage.len() as u8);   // +1B
    // out.write_all(&((storage.len() << 2) as u8).to_le_bytes()).await.unwrap();

    // while reading the file we can skip bytes until here
    for (k, (value, timestamp, exp)) in storage.iter() {
        if let Some(n) = exp {
            // timestamp flag
            // timstamp bytes(4)
            out_bytes.put_u8(0xFD); // +1B
            out_bytes.put_f32(timestamp.duration_since(UNIX_EPOCH).ok().unwrap().saturating_add(Duration::from_millis(*n as u64)).as_secs_f32());
        }
        // out_bytes.put_f32(new_ts.as_secs_f32());    // +4B

        // value type byte
        out_bytes.put_u8(0);    // +1B
        // a single byte repsenting the size of the following string
        // key size + bytes
        out_bytes.put_u8(k.len() as u8);
        for &b in k.as_bytes() {
            out_bytes.put_u8(b);
        }

        // value size + bytes
        out_bytes.put_u8(value.len() as u8);
        for &b in value.as_bytes() {
            out_bytes.put_u8(b);
        }
    }                // end section
    out_bytes.put_u8(_RDB_END_); // 1 Byte flag
    // finally put the checksum
    let checksum = crc64(0, &out_bytes);
    out_bytes.put_u64(checksum);
    out.write_all(&out_bytes).await.unwrap();

    response_ok()
}

// returns +OK\r\n
fn response_ok() -> String {
    String::from("+OK\r\n")
}

fn cmd_set(key: &String, val: &String, val2: SystemTime, val3: Option<usize>,  storage: &mut HashMap<String, (String, SystemTime, Option<usize>)>) {
    println!("setting {} to {} for {:?}ms", key, val, val2);
    storage.insert(key.clone(), (val.clone(), val2, val3));
}

// first correct use of lifetimes ???? 
// i used to pray for times like these 
fn cmd_get<'a>(key: &String, storage: &'a mut HashMap<String, (String, SystemTime, Option<usize>)>) -> Option<&'a String> {
    println!("searching for {:?}", key);

    // rewrap for our purposes
    match storage.get(key) {
        Some((res, t, exp)) => {
            match t.elapsed() {
                Ok(t_elapsed) => {
                    match exp {
                        None => Some(res),  // if no expiry 
                        Some(n) => {
                            if t_elapsed.as_millis() > *n as u128 {
                                // we could add deletion but we leave it for now
                                // as it would require us to return concrete strings not slices
                                // because after deletion slices wont be available to return hence the compilation error
                                // storage.remove_entry(key);
                                None
                            } else {
                                Some (res)
                            }
                        }
                    }
                    // println!("time elapsed: {:?}", t_elapsed);
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

fn encode_array(vals: &Vec<String>) -> String {
    let mut output = format!("*{}\r\n", vals.len());
    let sentinel  = "$";
    for v in vals {
        output += &sentinel;
        output += v.len().to_string().as_str();
        output += "\r\n";
        output += v;
        output += "\r\n"; 
    }

    output
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
        _ => {
            vec![]
        }
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
// 52 45 44 49 53 30 30 31 31 
// FA
// 09 72 65 64 69 73 2D 76 65 72
// 05 37 2E 32 2E 30 
// FA
// 0A 72 65 64 69 73 2D 62 69 74 73 
// C0 40 
// FE 00 
// FB 
// 01 
// 00 
// 00 
// 0A 73 74 72 61 77 62 65 72 72 79 06 6F 72 61 6E 67 65 FF 7A 4C 72 69 90 72 69 07