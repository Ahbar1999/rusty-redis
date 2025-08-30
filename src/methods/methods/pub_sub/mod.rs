pub mod pub_sub {
    use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime}};
    use tokio::{select, sync::{broadcast, Mutex}, time::{sleep_until, Instant}};
    use crate::utils::utils::*;
    use std::collections::HashSet;

    pub async fn cmd_sub (
        glob_config_ref: Arc<Mutex<GlobConfig>>,
        config_args: &mut Args,
        cmd_args: &Vec<String>) -> String {
        let chan_name = &cmd_args[1];
        config_args.client_in_sub_mode = true;
        config_args.subbed_chans.insert(chan_name.as_bytes().to_vec(), ());
        
        let mut glob_config = glob_config_ref.lock().await;
        if let Some(clients) = glob_config.subscriptions.get_mut(chan_name) {
            clients.insert(config_args.other_port);
        } else {
            let mut new_entry = HashSet::new();
            new_entry.insert(config_args.other_port);
            glob_config.subscriptions.insert(chan_name.clone(), new_entry);
        }

        return encode_array(&vec![encode_bulk("subscribe"), encode_bulk(chan_name), encode_int(config_args.subbed_chans.len())], false);
    }

    pub async fn cmd_blpop(
        config_args: &Args,
        cmd_args: &Vec<String>, 
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        mut rx: broadcast::Receiver<Vec<u8>>,
        glob_config: Arc<Mutex<GlobConfig>>) -> String {
        
        let key = &cmd_args[1];
        let mut timeout: f32 = cmd_args[2].parse().unwrap(); 
        let mut result  = vec![];
        if timeout == 0.0 {
            timeout = 60.0 * 60.0;  // 1 hour, basically block infinitely for our purposes
        }
        let end = Instant::now() + Duration::from_secs_f32(timeout);

        loop {
            select! {
                _ = sleep_until(end) => {
                    // remove this client from the queue
                    // we could use a hashmap to make insert and removals O(1) 
                    glob_config.lock().await.blocked_clients.get_mut(&cmd_args[1]).unwrap().retain(|ele| *ele != config_args.other_port);

                    // we need to return null array but apparently i havent implemented it yet
                    return "*-1\r\n".to_owned(); 
                    // return encode_array(&vec!["".to_owned()], true);
                    // return encode_bulk("");
                },

                data = rx.recv() => {
                    if data.is_err() {
                        continue;
                    }

                    let msg = data.unwrap();

                    pbas(&msg); 
                    if msg.starts_with(_EVENT_DB_UPDATED_LIST_.as_bytes()) {
                        if msg[_EVENT_DB_UPDATED_LIST_.as_bytes().len()..].starts_with(key.as_bytes()) {
                            // if this isnt the first one waiting on this key
                            let mut glob_config_data = glob_config.lock().await;
                            // in the tests the thread is panicking here, but it passes the tests because in this case the thread is supposed to exit 
                            if glob_config_data.blocked_clients.get(&cmd_args[1]).unwrap().is_empty() || glob_config_data.blocked_clients.get(&cmd_args[1]).unwrap().front().unwrap() != &config_args.other_port {
                                // continue blocking  
                                continue;
                            }

                            // if it is 
                            glob_config_data.blocked_clients.clear();
                            let mut _db = storage_ref.lock().await;
                            let (ref mut rdb_value, _) = _db.get_mut(key).unwrap();

                            match rdb_value {
                                RDBValue::List(v) => {
                                    result = vec![key.clone(), v.pop_front().unwrap()];
                                },
                                _ => {
                                    panic!("stop ittttt");
                                }
                            }
                            break;
                        }
                    }
                }
            } 
        }

        encode_array(&result, true)
    }

    pub async fn cmd_pub(config_args: &mut Args, 
        cmd_args: &Vec<String>, 
        glob_config_ref: Arc<Mutex<GlobConfig>>,
        tx: broadcast::Sender<Vec<u8>>) -> String {
        
        let chan_name = &cmd_args[1];
        let msg = &cmd_args[2];
        let transmission = encode_array(&vec!["message".to_owned(), chan_name.clone(), msg.clone()], true);
        config_args.subbed_chans.insert(chan_name.as_bytes().to_owned(), ()); 

        let glob_config = glob_config_ref.lock().await;
        // check if there are clients subscribed to this channel
        if let Some(clients) = glob_config.subscriptions.get(chan_name) {
            if tx.send(transmission.as_bytes().to_vec()).is_err() {
                panic!("could not broadcast the publisher's msg");
            }
            println!("published: {}", transmission);
            return encode_int(clients.len()); 
        }

        return encode_int(0);
    }

    pub async fn cmd_unsub(
        config_args: &mut Args,
        cmd_args: &Vec<String>,
        glob_config: Arc<Mutex<GlobConfig>>) -> String {

        let chan_name = &cmd_args[1];
        glob_config.lock().await.subscriptions
            .get_mut(chan_name)
            .or(Some(&mut HashSet::new())).unwrap()
            .remove(&config_args.other_port);
            
        config_args.subbed_chans.remove(chan_name.as_bytes());

        return encode_array(&vec![encode_bulk("unsubscribe"), encode_bulk(chan_name), encode_int(glob_config.lock().await.subscriptions.get(chan_name).or(Some(&HashSet::new())).unwrap().len())], false); 
    }

}