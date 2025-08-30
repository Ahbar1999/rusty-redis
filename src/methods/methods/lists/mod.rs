pub mod lists {
    use std::{collections::{HashMap, VecDeque}, sync::Arc, time::SystemTime};
    use tokio::sync::{broadcast, Mutex};

    use crate::utils::utils::*;

    pub async fn cmd_list_push(
        cmd_args: &Vec<String>,
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        push_back: bool,
        tx: broadcast::Sender<Vec<u8>>) -> String {

        let result;
        let key = &cmd_args[1];
        {
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

                    result =encode_int(v.len());
                },
                _ => {
                    panic!("invalid data type in cmd_list_push()");
                }
            }
        }

        // release lock on db, send message
        let mut msg = _EVENT_DB_UPDATED_LIST_.as_bytes().to_vec().to_owned();
        msg.extend_from_slice(key.as_bytes());
        pbas(&msg);
        tx.send(msg).ok();

        return result;
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

                    // if single variant was called
                    if cmd_args.len() == 2 {
                        return encode_bulk(&result[0]);
                    } else {
                        // if muti variant was called 
                        return encode_array(&result, true); 
                    }
                },
                _ => {
                    panic!("invalid data type for this key in cmd_lpop()");
                }
            }
        } else {
            return encode_bulk(""); 
        }
    }
}