pub mod methods {
    // this module contains all the redist command methods

    use core::panic;
    use std::hash::Hash;
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::{collections::HashMap, ops::BitAnd, time::{Duration, SystemTime, UNIX_EPOCH}, vec};
    use bytes::BufMut;
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

        encode_array(&matched_keys)
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
                RDBValue::Stream(_) => {
                    unimplemented!("RDB file parsing with stream data type not implemented yet.")
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
        encode_array(&vec!["REPLCONF".to_owned(), "ACK".to_owned(), bytes_offset.to_string()])
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
        
        encode_array(&result)
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
                        RDBValue::String(_) => {
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
                final_result.push(encode_array(&vec![key.to_owned(), encode_array(&result)])); 
            }
        } 

        if final_result.is_empty() {
            return encode_bulk("");
        }    
        encode_array(&final_result)
    }

    pub async fn cmd_incr(cmd_args: &Vec<String>, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {

        let mut _db =storage_ref.lock().await;
        let mut result = "0".to_owned();
        
        if let Some((rdb_value, ts)) = _db.get_mut(&cmd_args[1]) {
            match rdb_value {
                RDBValue::Stream(_) => {
                    unimplemented!("modifying stream entries not implemented yet!");
                },
                RDBValue::String(s) => {
                    if let Ok(num) = s.parse::<usize>() {
                        *s = (num + 1).to_string();
                        result = s.clone();
                    } else {
                        return redis_err(_ERROR_INCR_NOT_AN_INT_); 
                    }
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

    /*
    async fn cmd_get_key_rdb() -> Option<RDBValue> {
        unimplemented!()
    }
    */
}


