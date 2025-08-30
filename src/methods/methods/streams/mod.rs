pub mod streams {
    use crate::utils::utils::*;
    use std::sync::Arc;
    use tokio::select;
    use tokio::sync::{broadcast, Mutex};
    use tokio::time::sleep;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
            return "*-1\r\n".to_owned();
        }    
        encode_array(&final_result, false)
    }
}