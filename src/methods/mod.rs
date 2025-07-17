pub mod methods {
    // this module contains all the redist command methods

    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::{collections::HashMap, ops::BitAnd, time::{Duration, SystemTime, UNIX_EPOCH}, vec};
    use bytes::BufMut;
    use tokio::net::TcpStream;
    use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt}, sync::Mutex};
    use crc64::crc64;
    use crate::utils::utils::*;

    pub async fn cmd_fullresync(_: &Args) -> Vec<u8> {
        // in future we would wanna read contents from the file on disk and return it
        // right now the server expects us to hard code contents of such a file   
        encode_file(_EMPTY_RDB_FILE_.as_bytes()) 
    }

    pub fn encode_file(contents: &[u8]) -> Vec<u8> {
        let raw_bytes = hex::decode(contents).unwrap(); 
        let mut res= Vec::<u8>::new(); 
        res.push(b'$');
        for b in raw_bytes.len().to_string().as_bytes() {
            res.push(*b);
        }
        res.push(b'\r');
        res.push(b'\n');

        for b in raw_bytes {
            res.push(b);
        }

        res
    }

    pub async fn cmd_psync(config_args: &Args) -> String {
        encode_simple(&vec!["FULLRESYNC", format!("{}", config_args.master_replid).as_str(), format!("{}", config_args.master_repl_offset).as_str()])
    } 

    pub fn encode_simple(vals: &Vec<&str>) -> String {
        let mut result = vec![];
        for &v in vals {
            result.push(v);
        }

        let mut s = result.join(" ");
        s.push_str("\r\n");
        s = "+".to_owned() + &s;

        s
    }

    pub async fn connect_to_master(addr: &str, socket: &str) -> TcpStream {
        println!("slave connecting to master on:{}", socket);
        TcpStream::connect(format!("{}:{}", addr, socket)).await.unwrap() 
    }

    pub async fn cmd_info(config_args: &Args) -> String {
        let mut res = String::new();
        res.push_str(&format!("role:{}", if config_args.replicaof.starts_with("None") {"master"} else {"slave"}).as_str());
        if config_args.replicaof.starts_with("None") {
            res.push_str(&format!("\nmaster_replid:{}", config_args.master_replid));
            res.push_str(&format!("\nmaster_repl_offset:{}", config_args.master_repl_offset));
        }  
        
        encode_bulk(&res)
    }

    pub async fn cmd_config(query: &String, config_args: &Args) -> String {
        match query.as_str() {
            "dir" => {
                format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", &config_args.dir.len(), &config_args.dir)
            },
            "dbfilename" => {
                format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n", config_args.dbfilename.len(), &config_args.dbfilename)
            },
            _ => {
                unimplemented!("Unexpected config parameter requested: {}", query);
            }
        }
    }

    pub async fn cmd_keys(dbfilename: &String, storage_ref: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>) -> String {
        let mut matched_keys: Vec<String> = vec![];

        cmd_sync(dbfilename, storage_ref.clone()).await;
        let storage = storage_ref.lock().await;
        if !storage.is_empty() {
            for (key, (_, exp_ts)) in storage.iter() {
                // add code for pattern matching keys in the future
                if let Some(time) = exp_ts {
                    if SystemTime::now() <= *time {  // if not expired
                        matched_keys.push(key.clone());
                    }
                } else {    // or if no expiry
                    matched_keys.push(key.clone());
                }
            }
        }

        encode_array(&matched_keys)
    }

    pub async fn cmd_sync(dbfilepath: &String, storage_ref: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>) {
        // read the rdb file
        // read the keys, match them against some given pattern
        // we could simply search this in the storage map but i wanna do it the right way
        // let path =dir.clone() + dbfilename.as_str();
        if dbfilepath.starts_with("UNSET") {
            return;
        }

        let mut storage = storage_ref.lock().await;

        println!("reading from file {}", &dbfilepath);
        let file = File::open(&dbfilepath).await;
        let mut buf: Vec<u8> = vec![];
        match file {
            Ok(mut f) => {
                f.read_to_end(&mut buf).await.unwrap();
            },
            Err(ref e) if e.kind() == ErrorKind::NotFound => {
                // db file does not exist so no data found
                return;
                // encode_array(&matched_keys);
            },
            Err(e) => {
                panic!("{}", e);
            }
        }

        // print the hex dump of the file
        println!("hex dump of rdb file");
        println!("{}", buf.iter().map(|b| format!("{:02X} ", b)).collect::<String>());

        // let mut key: String;
        // let mut value: String;
        // let mut ts: Option<SystemTime> = None;

        let mut i = 9; // skip header bytes
        let mut mask;

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

        // println!("parsed metadata");
        while buf[i] == _RDB_DATA_SECTION_FLAG_ {
            i += 1;     // DB section flag(current)
            i += 1;     // DB index
            i += 1;     // storage info section flag
            i += 1;     // total k,v pairs
            i += 1;     // timed k,v pairs
        }

        // println!("parsed db metadata");
        while i < buf.len() {
            let mut new_kv = StorageKV{
                key: String::from(""), 
                value: String::from(""), 
                exp_ts: None
            }; 
            // key = String::new();
            // value = String::new();
            // ts = None;

            // 0xFF marks end of the db file section
            if buf[i] == _RDB_END_ {
                break;
            }

            // if timestamp flag present
            if buf[i] == _RDB_TIMESTAMP_S_FLAG { 
                i += 1;     // skip flag byte(current)
                let mut ts_bytes: [u8; 4] = [0x0, 0x0, 0x0, 0x0];
                for j in 0..4 {
                    ts_bytes[j] = buf[i + j];
                }
                new_kv.exp_ts = Some(UNIX_EPOCH + Duration::from_secs_f32(u32::from_le_bytes(ts_bytes) as f32));
                i += 4;     // skip timestamp(next 4 bytes, if timestamp was stored in secs)
            } else if buf[i] == _RDB_TIMESTAMP_MS_FLAG {
                i += 1;     // skip flag byte(current)
                let mut ts_bytes: [u8; 8] = [0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0];
                for j in 0..8 {
                    ts_bytes[j] = buf[i + j];
                }
                new_kv.exp_ts = Some(UNIX_EPOCH + Duration::from_millis(u64::from_le_bytes(ts_bytes)));
                i += 8;     // skip timestamp(next 8 bytes, if timestamp was stored in msecs)
            }
            i += 1;     // skip value type byte
            // println!("parsed timing info");
            // if k is an integer then you need to parse it like above  
            // buf[i] contains size encoded string size
            for &ch in &buf[(i + 1)..(i + 1 + buf[i] as usize)] {
                new_kv.key.push(char::from(ch));
            }
            // println!("parsed key");

            i += 1 + buf[i] as usize;    // skip key bytes
            // currently we return all the keys because we are matching against * pattern 
            // matched_keys.push(key);
            // skip key size byte + key length bytes

            // currently the values are also prefix encoded strings so this works  
            for &ch in &buf[(i + 1)..(i + 1 + buf[i] as usize)] {
                new_kv.value.push(char::from(ch));
            }
            i += 1 + buf[i] as usize;    // skip value bytes

            // println!("parsed value");
            storage.insert(new_kv.key.clone(), (new_kv.value.clone(), new_kv.exp_ts.clone()));
            println!("record inserted: {:?}", new_kv);
        }
    }

    pub async fn cmd_save(storage_ref: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>, dbfilepath: &String) -> String {
        let storage = storage_ref.lock().await;
        // assumes the directory structure already exists
        println!("creating file {}", &dbfilepath); 
        let mut out = tokio::fs::File::create(&dbfilepath).await.unwrap();
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
        // count of timed k, v pairs same as above;
        out_bytes.put_u8(storage.len() as u8);   // +1B
        // out.write_all(&((storage.len() << 2) as u8).to_le_bytes()).await.unwrap();

        // while reading the file we can skip bytes until here
        for (k, (value, timestamp)) in storage.iter() {
            if let Some(ts) = timestamp {
                // timestamp flag
                // timstamp bytes(4)
                out_bytes.put_u8(_RDB_TIMESTAMP_MS_FLAG); // +1B
                out_bytes.put_u64(ts.duration_since(UNIX_EPOCH).ok().unwrap().as_millis() as u64);   // +8B; always store in ms
            } 
            // else no timestamp flag and dat a for this k, v pair

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
    pub fn response_ok() -> String {
        String::from("+OK\r\n")
    }

    // change this signature to take StorageKV struct
    pub async fn cmd_set(cmd_args: &Vec<String>, storage_ref: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>) -> String {
        let mut new_kv = StorageKV {
            key: cmd_args[1].clone(),
            value: cmd_args[2].clone(),
            exp_ts: None,
        };

        if cmd_args.len() > 3 { // input validation is not being performed
            // SET foo bar px milliseconds
            let n = cmd_args[4].parse().unwrap();
            new_kv.exp_ts = SystemTime::now().checked_add(Duration::from_millis(n));
        }
        println!("insert new record: {:?}", new_kv);
        storage_ref.lock().await.insert(new_kv.key, (new_kv.value, new_kv.exp_ts));

        response_ok()
    }

    // first correct use of lifetimes ???? 
    // i used to pray for times like these 
    pub async fn cmd_get<'a>(key: &String, dbfilepath: &String, storage_ref: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>) -> String {
        // syncing db
        println!("syncing db.."); 
        cmd_sync(dbfilepath, storage_ref.clone()).await;
        // println!("searching for {:?}", key);

        let storage = storage_ref.lock().await; 
        // rewrap for our purposes
        let result = match storage.get(key) {
            Some((res, ts)) => {
                match ts {
                    Some(n) => {
                        if *n < SystemTime::now() {
                            None
                        } else {
                            Some (res)
                        }
                    },
                    None => Some(res),  // if no expiry 
                }
            },
            None => {
                println!("did not find key");
                None
            }
        };

        match result {
            Some(s) => {
                encode_bulk(s)
            },
            None =>{
                encode_bulk("")
            }
        }
    }

    pub fn cmd_get_ack(bytes_offset: usize) -> String {
        encode_array(&vec!["REPLCONF".to_owned(), "ACK".to_owned(), bytes_offset.to_string()])
    }

    /*
    async fn cmd_get_key_rdb() -> Option<RDBValue> {
        unimplemented!()
    }
    */
}


