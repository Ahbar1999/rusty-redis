pub mod methods {
    use std::io::ErrorKind;
    use std::{collections::HashMap, ops::BitAnd, time::{Duration, SystemTime, UNIX_EPOCH}, vec};
    use bytes::BufMut;
    // use bytes::{Bytes, BytesMut};
    use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt}};
    use crc64::crc64;
    use crate::utils::utils::*;

    pub async fn cmd_keys(dir: &String, dbfilename: &String) -> String {
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

        let mut key: String;
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

    pub async fn cmd_save(storage: &HashMap<String, (String, SystemTime, Option<usize>)>, dir: &String, dbfilename: &String) -> String {
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
    pub fn response_ok() -> String {
        String::from("+OK\r\n")
    }

    pub fn cmd_set(key: &String, val: &String, val2: SystemTime, val3: Option<usize>,  storage: &mut HashMap<String, (String, SystemTime, Option<usize>)>) {
        println!("setting {} to {} for {:?}ms", key, val, val2);
        storage.insert(key.clone(), (val.clone(), val2, val3));
    }

    // first correct use of lifetimes ???? 
    // i used to pray for times like these 
    pub fn cmd_get<'a>(key: &String, storage: &'a mut HashMap<String, (String, SystemTime, Option<usize>)>) -> Option<&'a String> {
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
}


