pub mod methods {
    // this module contains all the redist command methods

    use core::panic;
    use std::collections::VecDeque;
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::{collections::HashMap, ops::BitAnd, time::{Duration, SystemTime, UNIX_EPOCH}, vec};
    use bytes::BufMut;
    use hex::encode;
    use tokio::net::TcpStream;
    use tokio::select;
    use tokio::sync::broadcast;
    use tokio::time::{interval, sleep};
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
        // res.push(raw_bytes.len() as u8);
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

    pub async fn cmd_keys(dbfilename: &String, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {
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

        encode_array(&matched_keys, true)
    }

    pub async fn cmd_sync(dbfilepath: &String, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) {
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
        // println!("hex dump of rdb file");
        // println!("{}", buf.iter().map(|b| format!("{:02X} ", b)).collect::<String>());
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
                value: RDBValue::String(String::from("")),
                //  { value: String::from(""), value_type: RDBValueType::String }, 
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

            let mut new_value = String::new(); 
            // currently the values are also prefix encoded strings so this works  
            for &ch in &buf[(i + 1)..(i + 1 + buf[i] as usize)] {
                // new_kv.value.value.push(char::from(ch));
                new_value.push(char::from(ch));
            }
            i += 1 + buf[i] as usize;    // skip value bytes

            new_kv.value = RDBValue::String(new_value);
            // println!("parsed value");
            storage.insert(new_kv.key.clone(), (new_kv.value.clone(), new_kv.exp_ts.clone()));
            println!("record inserted: {:?}", new_kv);
        }
    }

    pub async fn cmd_save(
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>, 
        dbfilepath: &String) -> String {
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
            let value_data = match value {
                RDBValue::String(data) => {
                    data
                },
                _ => {
                    unimplemented!("RDB file parsing is implemented for string data type only.")
                }
            };
            out_bytes.put_u8(value_data.len() as u8);
            for &b in value_data.as_bytes() {
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
    pub async fn cmd_set(cmd_args: &Vec<String>, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {
        let mut new_kv = StorageKV {
            key: cmd_args[1].clone(),
            value: RDBValue::String(cmd_args[2].clone()),
            //, value_type: RDBValueType::String },
            exp_ts: None,
        };

        if cmd_args.len() > 3 { // input validation is not being performed
            // SET foo bar px milliseconds
            let n = cmd_args[4].parse().unwrap();
            new_kv.exp_ts = SystemTime::now().checked_add(Duration::from_millis(n));
        }
        // println!("insert new record: {:?}", new_kv);
        storage_ref.lock().await.insert(new_kv.key, (new_kv.value, new_kv.exp_ts));

        response_ok()
    }

    // first correct use of lifetimes ???? 
    // i used to pray for times like these 
    pub async fn cmd_get<'a>(key: &String, dbfilepath: &String, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> Option<RDBValue> {
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
            Some(rdb_val) => {
                Some(rdb_val.clone())
            },
            None => {
                None
            }
        } 
    }

    pub fn cmd_get_ack(bytes_offset: usize) -> String {
        encode_array(&vec!["REPLCONF".to_owned(), "ACK".to_owned(), bytes_offset.to_string()], true)
    }

    pub async fn cmd_wait(max_ack: usize, 
            max_wait: usize, 
            glob_config: Arc<Mutex<GlobConfig>>,
            target_bytes: usize) -> String {
        let t = SystemTime::now(); 
        // let max_ack: usize = cmd_args[1].parse().unwrap();  // maximum clients needed to ack 
        let timeout  = Duration::from_millis(max_wait as u64);

        let mut res = 0;
        // use REPLCONF GETACK to get information about bytes processed from replicas
        let mut ticker = interval(Duration::from_millis((max_wait / 10) as u64));

        while SystemTime::now().duration_since(t).unwrap() <= timeout {
             
            let mut acks: usize = 0;
            for (_, replica_info) in &glob_config.lock().await.replicas {
                // println!("replica: {}, bytes_rx: {}", port, replica_info.bytes_rx);
                if replica_info.bytes_rx >= target_bytes {
                    acks += 1;
                } 
            }

            if acks >= max_ack {
                res= acks;
                break;
            }

            res = acks;

            ticker.tick().await;
        }

        encode_int(res)
    }

    pub async fn cmd_xadd(
        cmd_args: &Vec<String>, 
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        tx: broadcast::Sender<Vec<u8>>) -> String {
        let mut new_kv = StorageKV {
            key: cmd_args[1].clone(),
            value: RDBValue::Stream(vec![StreamEntry{
                id: {
                    // '*' part will be replaced with usize::MAX
                    if cmd_args[2] == "*" {
                        (usize::MAX, usize::MAX)
                    }  else {
                        let id_parts = cmd_args[2].split_once('-').unwrap();
                        if id_parts.1 == "*" {
                            (usize::from_str_radix(id_parts.0, 10).unwrap(), usize::MAX)
                        } else {
                            (usize::from_str_radix(id_parts.0, 10).unwrap(), usize::from_str_radix(id_parts.1, 10).unwrap())
                        }
                    } 
                },
                value: {
                    let mut kv_pairs = vec![];
                    for i in 3..cmd_args.len() -1 {
                        kv_pairs.push((cmd_args[i].clone(), cmd_args[i + 1].clone()));
                    }
                    kv_pairs
                },
            }]), 
            exp_ts: None,
        };

        // stream data doesnt support time yet 
        // if cmd_args.len() > 3 { // input validation is not being performed
        //     // SET foo bar px milliseconds
        //     let n = cmd_args[4].parse().unwrap();
        //     new_kv.exp_ts = SystemTime::now().checked_add(Duration::from_millis(n));
        // }
        // println!("inserting into stream: {:?}", new_kv);
        // id validation
        match &new_kv.value {
            RDBValue::Stream(new_value_vec) => {
                if new_value_vec[0].id == (0, 0) {
                    return redis_err(_ERROR_STREAM_NULL_ID_);
                }
            },
            _ => {
                panic!("invalid data type found in cmdxadd");
            } 
        }

        let mut db_data= storage_ref.lock().await;
        let result;

        match db_data.get_mut(&new_kv.key) {
            Some((value, _)) => {
                match value {
                    RDBValue::Stream(value_vec) => {
                        match new_kv.value {
                            RDBValue::Stream(mut new_value_vec) => {
                                let prev_id = value_vec.iter().next_back().unwrap().id;
                                if  prev_id >= new_value_vec[0].id {
                                    return redis_err(_ERROR_STREAM_GEQ_ID_EXISTS_);
                                }
                                if new_value_vec[0].id.0 == usize::MAX {
                                    new_value_vec[0].id.0 = prev_id.0 + 1;
                                }
                                if new_value_vec[0].id.1 == usize::MAX {
                                    if prev_id.0 == new_value_vec[0].id.0 { // if first part matches with previous element's id 
                                        new_value_vec[0].id.1 = prev_id.1 + 1;
                                    } else {
                                        new_value_vec[0].id.1 = 0 + (new_value_vec[0].id.0 == 0) as usize;
                                    }
                                }
                                value_vec.push(new_value_vec[0].to_owned());

                                result = format!("{}-{}", new_value_vec[0].id.0, new_value_vec[0].id.1);
                            },
                            _ => {
                                unimplemented!("inconsistent data types in xadd");
                            }
                        }
                    },
                    _ => {
                        unimplemented!("found string data in a stream cmd_xadd");
                    }
                }
            },
            None => {
                match new_kv.value {
                    RDBValue::Stream(ref mut new_value_vec) => {
                        if new_value_vec[0].id.0 == usize::MAX {
                            new_value_vec[0].id.0 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as usize;
                        }
                        if new_value_vec[0].id.1 == usize::MAX {
                            if new_value_vec[0].id.0 > 0 {
                                new_value_vec[0].id.1 = 0;
                            } else {
                                // if first part is 0 then second part must start from 1
                                new_value_vec[0].id.1 = 1;
                            }
                        }
                        
                        result = format!("{}-{}", &new_value_vec[0].id.0, &new_value_vec[0].id.1);
                        db_data.insert(
                            new_kv.key,
                            (new_kv.value, None));

                    },
                    _ => {
                        unimplemented!("invalid state int cmd_xadd()");
                    }
                };
                
            }
        }

        tx.send(_EVENT_DB_UPDATED_.as_bytes().to_vec().to_owned()).unwrap();
        encode_bulk(&result)
    }

    pub async fn cmd_xrange(
        cmd_args: &Vec<String>, 
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {

        let key = cmd_args[1].as_str();
        let mut id_start = (0, 0);
        if cmd_args[2].find("-").is_none() {
            id_start.0 = cmd_args[2].as_str().parse().unwrap();
        }  else {
            let id_parts = cmd_args[2].split_once('-').unwrap();
            // when start id is just "-" id_start just defaults to (0, 0) 
            id_start = (id_parts.0.parse().unwrap_or_default(), id_parts.1.parse().unwrap_or_default());
        }

        let mut id_end = (0, usize::MAX); 
        if cmd_args[3] == "+" {
            id_end = (usize::MAX, usize::MAX);
        } else if cmd_args[3].find("-").is_none() {
            id_end.0 = cmd_args[3].as_str().parse().unwrap();
        } else { 
            let id_parts = cmd_args[3].split_once('-').unwrap();
            id_end = (id_parts.0.parse().unwrap(), id_parts.1.parse().unwrap());
        }

        let _db = storage_ref.lock().await;

        let mut result: Vec<String> = vec![];
        match _db.get(key) {
            Some((stream_kvs, _)) => {
                match stream_kvs {
                    RDBValue::Stream(stream_data) => {
                        for entry in stream_data {
                            if id_start <= entry.id && entry.id <= id_end {
                                result.push(entry.serialize());
                            }  
                        } 
                    },
                    _ => {
                        panic!("type mismatch in xrange");
                    }
                }
            },
            None => {
                ()
            }
        } 
        
        encode_array(&result, false)
    }

    pub async fn cmd_xread(
        cmd_args: &Vec<String>, 
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        mut rx:  broadcast::Receiver<Vec<u8>>) -> String {

        let mut final_result = vec![];      // accumulated reuslts
        let mut result: Vec<String> = vec![];           // result of one stream
        let mut start = 2;
        let mut state: HashMap<String, (usize, usize)> = HashMap::new();

        if cmd_args[1] == "block" {
            // save the state before blocking
            {
                let _db = storage_ref.lock().await;
                for k  in _db.keys() {
                    match &_db.get(k).unwrap().0 {
                        RDBValue::Stream(entries) => {
                            // save stream latest entries
                            state.insert(k.clone(), entries.iter().next_back().unwrap().id.clone());
                        },
                        _ => {
                            continue;
                        }
                    }
                }
            }
            start += 2;
            let sleep_duration = Duration::from_millis(cmd_args[2].parse().unwrap());
            if sleep_duration.as_millis() > 0 {
                sleep(sleep_duration).await;
            } else {
                // wait for new entry in the db
                loop {
                    select! {
                        data = rx.recv() => {
                            if let Ok(msg) = data {
                                if msg == _EVENT_DB_UPDATED_.as_bytes() {
                                    // move on
                                    break;
                                } else {
                                    // continue to listen
                                }
                            }
                        }
                    }
                }
            }
        }

        let mid = (cmd_args.len() - 1 - start + 1) / 2;

        for i in start..(start + mid) {
            result.clear();
            let key = cmd_args[i].as_str();
            
            let mut id_start = (0, 0);

            if cmd_args[i + mid] == "$" {
                if let Some(id) = state.get(key) {
                    id_start = *id;
                }
            } else if cmd_args[i + mid].find("-").is_none() {
                id_start.0 = cmd_args[i + mid].as_str().parse().unwrap();
            }  else {
                let id_parts = cmd_args[i + mid].split_once('-').unwrap();
                // when start id is just "-" id_start just defaults to (0, 0) 
                id_start = (id_parts.0.parse().unwrap_or_default(), id_parts.1.parse().unwrap_or_default());
            }    
            let id_end = (usize::MAX, usize::MAX); 
    
            let _db = storage_ref.lock().await;
    
            let mut result: Vec<String> = vec![];
            match _db.get(key) {
                Some((stream_kvs, _)) => {
                    match stream_kvs {
                        RDBValue::Stream(stream_data) => {
                            for entry in stream_data {
                                if id_start < entry.id && entry.id <= id_end {
                                    result.push(entry.serialize());
                                }  
                            } 
                        },
                        _ => {
                            panic!("type mismatch in xrange");
                        }
                    }
                },
                None => {
                    panic!("key entry not found in cmd_xread()"); 
                }
            }
            if !result.is_empty() {
                final_result.push(encode_array(&vec![encode_bulk(key), encode_array(&result, false)], false)); 
            }
        } 

        if final_result.is_empty() {
            return encode_bulk("");
        }    
        encode_array(&final_result, false)
    }

    pub async fn cmd_incr(cmd_args: &Vec<String>, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {

        let mut _db =storage_ref.lock().await;
        let mut result = "0".to_owned();
        
        if let Some((rdb_value, _)) = _db.get_mut(&cmd_args[1]) {
            match rdb_value {
                RDBValue::String(s) => {
                    if let Ok(num) = s.parse::<usize>() {
                        *s = (num + 1).to_string();
                        result = s.clone();
                    } else {
                        return redis_err(_ERROR_INCR_NOT_AN_INT_); 
                    }
                },
                _ => {
                    unimplemented!("incr only implemented for RDBValue::String");
                }
            }
        } else {
            let new_kv = StorageKV {
                key: cmd_args[1].clone(),
                value: RDBValue::String(1.to_string()),
                exp_ts: None
            };
            _db.insert(new_kv.key, (new_kv.value, new_kv.exp_ts));
            result = 1.to_string();
        }

        encode_int(result.parse().unwrap())
    }

    pub async fn cmd_list_push(
        cmd_args: &Vec<String>,
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        push_back: bool) -> String {

        let key = &cmd_args[1];
        let mut _db = storage_ref.lock().await;

        if _db.get_mut(key).is_none() {
            _db.insert(key.clone(), (RDBValue::List(VecDeque::new()), None));
        } 

        let (rdb_value, _) = _db.get_mut(key).unwrap(); 

        match rdb_value {
            RDBValue::List(v) => {
                for i in 2..cmd_args.len() {
                    if push_back {
                        v.push_back(cmd_args[i].clone());
                    } else {
                        v.push_front(cmd_args[i].clone());
                    }
                }

                return encode_int(v.len());
            },
            _ => {
                panic!("invalid data type in cmd_list_push()");
            }
        }
    }

    pub async fn cmd_lrange(
        cmd_args: &Vec<String>,
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {
            
        let key = &cmd_args[1];
        let mut l: isize = cmd_args[2].parse().unwrap();
        let mut r: isize = cmd_args[3].parse().unwrap();

        let _db = storage_ref.lock().await;
        let mut result = vec![];

        if let Some((rdb_val, _)) =  _db.get(key) {
            match rdb_val {
                RDBValue::List(v) => {
                    let size = v.len() as isize;
                    if l < 0 && l < -(size - 1) {
                        l = 0;
                    }
                    if r < 0 && r < -(size - 1) {
                        r = 0;
                    } 

                    if l < 0 {
                        l = (l + size) % size;
                    }
                    if r < 0 {
                        r = (r + size) % size;
                    }

                    if l > r {
                        return encode_array(&vec![], true);
                    }

                    for j in l..std::cmp::min(r + 1, size) {
                        result.push(v[j as usize].clone());
                    }         
                },
                _ => { 
                    panic!("mismatched data types");
                }
            }
        } 

        encode_array(&result, true)
    }

    pub async fn cmd_llen(cmd_args: &Vec<String>, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {

        if let Some((rdb_val, _)) = storage_ref.lock().await.get(&cmd_args[1]) {
            match rdb_val {
                RDBValue::List(v) => {
                    return encode_int(v.len());
                },
                _ => {
                    panic!("invalid data type for this key in cmd_llen()");
                }
            }
        }

        return encode_int(0);
    } 

    pub async fn cmd_lpop(
        cmd_args: &Vec<String>,
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {

        let mut result = vec![];
        if let Some((rdb_val, _)) = storage_ref.lock().await.get_mut(&cmd_args[1]) {
            match rdb_val {
                RDBValue::List(v) => {
                    let mut remove_count: usize = 1; 
                    if cmd_args.len() > 2 {
                        remove_count = cmd_args[2].parse().unwrap();
                    }

                    while let Some(val) = v.pop_front() {
                        result.push(val);
                        remove_count -= 1;
                        if remove_count == 0 {
                            break;
                        } 
                    }

                    return encode_array(&result, true);
                },
                _ => {
                    panic!("invalid data type for this key in cmd_lpop()");
                }
            }
        } else {
            return encode_bulk("none"); 
        }
    }

    pub async fn cmd_exec(
        cmds: &Vec<(usize, Vec<String>)>, 
        config_args: &mut Args,
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        tx: broadcast::Sender<Vec<u8>>,
        glob_config: Arc<Mutex<GlobConfig>>) -> Vec<Vec<u8>> {

        let dbfilepath = "".to_owned() + &config_args.dir + "/" + &config_args.dbfilename;
        let mut output = vec![];

        // bytes_rx represents the number of bytes of commands that came after handshake sequence 
        for (bytes_rx, cmd_args) in cmds {
            println!("exec: {:?}", cmd_args);
            let responses = match cmd_args[0].to_uppercase().as_str() {
                "ECHO" => {
                    vec![encode_bulk(&cmd_args[1]).as_bytes().to_owned()]
                },
                "PING" => {
                    if config_args.replicaof.starts_with("None") {  // if this server instance is a master, part of handshake   
                        vec![encode_simple(&vec!["PONG"]).as_bytes().to_owned()] 
                    } else {    // if its a replica, dont send back any response
                        config_args.bytes_rx += bytes_rx;
                        // glob_config.lock().await.replica_writes += bytes_rx;
                        vec![]
                    }
                },
                "SET" => {
                    // replica and master both account of these bytes
                    // println!("added {:?} bytes to {:?}", &cmd_args, config_args);
                    config_args.bytes_rx += bytes_rx;
                    
                    if config_args.replicaof.starts_with("None") {  // if this server is a master
                        tx.send(encode_array(&cmd_args, true).as_bytes().to_vec()).unwrap();  // send replication
                    }
                    
                    let mut response = vec![cmd_set(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()];

                    // if a replica then dont send any response since write commands only come from the master
                    if !config_args.replicaof.starts_with("None") {
                        response.pop();
                    } 

                    response
                },
                "GET" => {
                    let result = cmd_get(&cmd_args[1], &dbfilepath, storage_ref.clone()).await;
                    match result {
                        Some(rdb_value) => {
                            match rdb_value {
                                RDBValue::String(s) => {
                                    vec![encode_bulk(s.as_str()).as_bytes().to_owned()] 
                                },
                                _ => {
                                    unimplemented!("cmd_get not implemented for keys that store stream data type");
                                }
                            }
                        },
                        None => {
                            vec![encode_bulk("").as_bytes().to_owned()] 
                        }
                    }
                },
                "CONFIG" => {
                    vec![cmd_config(&cmd_args[2], &config_args).await.as_bytes().to_owned()]
                },
                "SAVE" => {
                    vec![cmd_save(storage_ref.clone(), &dbfilepath).await.as_bytes().to_owned()]
                },
                "KEYS" => {
                    vec![cmd_keys(&dbfilepath, storage_ref.clone()).await.as_bytes().to_owned()]
                },
                "INFO" => {
                    vec![cmd_info(&config_args).await.as_bytes().to_owned()]
                },
                "REPLCONF" => {
                    if cmd_args[1] == "GETACK"{    // return number of bytes processed by this replica
                        // config_args.write_bytes_rx += bytes_rx; // replconf is also a write a command  
                        // println!("added {:?} bytes to {:?}", cmd_args, config_args);
                        if config_args.bytes_rx > 0 {
                            config_args.bytes_rx += bytes_rx;
                        }
                        vec![cmd_get_ack(config_args.bytes_rx).as_bytes().to_owned()]
                    } else if cmd_args[1] == "ACK" {
                        // this message was sent by replica to (this instance) master
                        // save these bytes for this replica
                        // you need to get this replica's port that its listening on, it passed that port when it connected 
                        // save that port in the config_args of this connection
                        // then key it into the global_args.replicas and increment byte_rx there
                        
                        // add to: bytes recvd by the replica sending the ack 
                        println!("recvd ack from: {}", &config_args.other_port);
                        glob_config.lock().await.replicas.get_mut(&config_args.other_port).unwrap().bytes_rx += cmd_args[2].parse::<usize>().unwrap(); 
                        vec![]
                    } else {
                        // port sharing by replica to master, this assumes that this command is always sent on the correct connection
                        if cmd_args[1] == "listening-port" {
                            config_args.replica_conn = true;
                            config_args.other_port = cmd_args[2].parse().unwrap();
                            glob_config.lock().await.replicas.insert(cmd_args[2].parse().unwrap(), ReplicaInfo{bytes_rx: 0});
                        }
                        vec![encode_simple(&vec!["OK"]).as_bytes().to_owned()]
                    }
                },
                "PSYNC" => {
                    println!("pysnc() {:?}", config_args);
                    vec![cmd_psync(&config_args).await.as_bytes().to_owned(), cmd_fullresync(&config_args).await] 
                },
                "WAIT" => {
                    // save the byte of all the commands processed before this WAIT command
                    let target_bytes;
                    {
                        // target_bytes = glob_config.lock().await.replica_writes;
                        target_bytes = config_args.bytes_rx;    // bytes received by master
                    }
                    println!("bytes to match {}", target_bytes);
                    
                    let msg = encode_array(&vec!["REPLCONF".to_owned(), "GETACK".to_owned(), "*".to_owned()], true);
                    tx.send(msg.as_bytes().to_vec()).unwrap();

                    if target_bytes > 0 { // only add getack bytes to master if some writes exist
                        // println!("added {:?} bytes to {:?}", &msg, config_args);
                        config_args.bytes_rx += msg.as_bytes().len();
                    }

                    vec![cmd_wait(cmd_args[1].parse().unwrap(), cmd_args[2].parse().unwrap(), glob_config.clone(), target_bytes).await.as_bytes().to_owned()]
                },
                "TYPE" => {
                    let result = cmd_get(&cmd_args[1], &dbfilepath, storage_ref.clone()).await;
                    match result {
                        Some(rdb_value) => {
                            vec![encode_simple(&vec![rdb_value.repr().as_str()]).as_bytes().to_owned()] 
                        },
                        None => {
                            vec![encode_simple(&vec!["none"]).as_bytes().to_owned()]
                        }
                    }
                },
                "XADD" => {
                    vec![cmd_xadd(&cmd_args, storage_ref.clone(), tx.clone()).await.as_str().as_bytes().to_owned()]
                },
                "XRANGE" => {
                    vec![cmd_xrange(&cmd_args, storage_ref.clone()).await.as_str().as_bytes().to_owned()]
                },
                "XREAD" => {
                    vec![cmd_xread(&cmd_args, storage_ref.clone(), tx.subscribe()).await.as_str().as_bytes().to_owned()]
                },
                "INCR" => {
                    vec![cmd_incr(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()] 
                },
                "MULTI" => {
                    config_args.queueing = true;
                    vec![response_ok().as_bytes().to_owned()]
                },
                "DISCARD" => {
                    config_args.queueing = false;
                    vec![response_ok().as_bytes().to_owned()]
                },
                "RPUSH" => {
                    vec![cmd_list_push(&cmd_args, storage_ref.clone(), true).await.as_bytes().to_owned()]
                },
                "LRANGE" => {
                    vec![cmd_lrange(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()]
                },
                "LPUSH" => {
                    vec![cmd_list_push(&cmd_args, storage_ref.clone(), false).await.as_bytes().to_owned()]
                },
                "LLEN" =>{
                    vec![cmd_llen(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()]
                }
                "LPOP" => {
                    vec![cmd_lpop(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()]
                }

                // "EXEC" => {
                //     config_args.queueing = false;
                //     vec![redis_err(_ERROR_EXEC_WITHOUT_MULTI_).as_bytes().to_owned()]
                // }
                _ => {
                    vec![]
                    // unimplemented!("Unidentified command");
                    // continue;
                }
            };

            for response in responses {
                output.push(response);
            }
        }

        // config_args should hold bytes of write commands only 
        // add to processed bytes if either its a replica(receiving commands from master) or a master receiving from replica
        // process commands received at replica or sent by client(to master)  
        // if !config_args.replicaof.starts_with("None") || !config_args.replica_conn {
        //     config_args.bytes_rx += bytes_rx;
        // }
        // bytes are added

        return output;
        
    }
    /*
    async fn cmd_get_key_rdb() -> Option<RDBValue> {
        unimplemented!()
    }
    */
}


