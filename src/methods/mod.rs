
// this module contains all the redist command methods
pub mod methods {
    // submodule for auth methods(module tree is scuffed, need fixing i think)
    pub mod auth;
    pub mod lists;
    pub mod pub_sub;
    pub mod geospatial;
    pub mod sorted_sets;
    pub mod streams;
    pub mod replication;

    use core::panic;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::{collections::HashMap, time::{Duration, SystemTime}, vec};
    use tokio::sync::broadcast;
    use tokio::time::interval;
    use tokio::sync::Mutex;
    use crate::utils::utils::*;

    // this file should contain transaction methods only because we cant really refactor them into their own file
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

    pub async fn cmd_incr(cmd_args: &Vec<String>, storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>) -> String {

        let mut _db =storage_ref.lock().await;
        let result;
        //  = "0".to_owned();
        
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

    pub async fn cmd_exec(
        cmds: &Vec<(usize, Vec<String>)>, 
        config_args: &mut Args,
        storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>,
        tx: broadcast::Sender<Vec<u8>>,
        glob_config: Arc<Mutex<GlobConfig>>) -> Vec<Vec<u8>> {

        let dbfilepath = "".to_owned() + &config_args.dir + "/" + &config_args.dbfilename;
        let mut output = vec![];

        // bytes_rx represents the number of bytes of commands that came after handshake sequence 
        for (bytes_rx, cmd_args) in cmds {
            println!("exec: {:?}", cmd_args);
            let responses;
            //  = Vec::new();

            // check if cmd is valid for current context or not
            // for now this only checks for sub mode commands validity, at some point we might wanna return the error from this function
            // based on what caused it and what the other end expects in such a case  
            if cmd_sanity_check(cmd_args[0].as_str(), config_args.client_in_sub_mode) {
                responses = match cmd_args[0].to_uppercase().as_str() {
                    "ECHO" => {
                        vec![encode_bulk(&cmd_args[1]).as_bytes().to_owned()]
                    },
                    "PING" => {
                        if config_args.replicaof.starts_with("None") {  // if this server instance is a master, part of handshake
                            if config_args.client_in_sub_mode {
                                return vec![encode_array(&vec![encode_bulk("pong"), _RESP_EMPTY_STRING_.to_owned()], false).as_bytes().to_owned()];
                            }   
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
                        
                        let mut response = vec![replication::replication::cmd_set(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()];

                        // if a replica then dont send any response since write commands only come from the master
                        if !config_args.replicaof.starts_with("None") {
                            response.pop();
                        } 

                        response
                    },
                    "GET" => {
                        let result = replication::replication::cmd_get(&cmd_args[1], &dbfilepath, storage_ref.clone()).await;
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
                        vec![replication::replication::cmd_config(&cmd_args[2], &config_args).await.as_bytes().to_owned()]
                    },
                    "SAVE" => {
                        vec![replication::replication::cmd_save(storage_ref.clone(), &dbfilepath).await.as_bytes().to_owned()]
                    },
                    "KEYS" => {
                        vec![replication::replication::cmd_keys(&dbfilepath, storage_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "INFO" => {
                        vec![replication::replication::cmd_info(&config_args).await.as_bytes().to_owned()]
                    },
                    "REPLCONF" => {
                        if cmd_args[1] == "GETACK"{    // return number of bytes processed by this replica
                            // config_args.write_bytes_rx += bytes_rx; // replconf is also a write a command  
                            // println!("added {:?} bytes to {:?}", cmd_args, config_args);
                            if config_args.bytes_rx > 0 {
                                config_args.bytes_rx += bytes_rx;
                            }
                            vec![replication::replication::cmd_get_ack(config_args.bytes_rx).as_bytes().to_owned()]
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
                        vec![replication::replication::cmd_psync(&config_args).await.as_bytes().to_owned(), 
                            replication::replication::cmd_fullresync(&config_args).await] 
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
                        let result = replication::replication::cmd_get(&cmd_args[1], &dbfilepath, storage_ref.clone()).await;
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
                        vec![streams::streams::cmd_xadd(&cmd_args, storage_ref.clone(), tx.clone()).await.as_str().as_bytes().to_owned()]
                    },
                    "XRANGE" => {
                        vec![streams::streams::cmd_xrange(&cmd_args, storage_ref.clone()).await.as_str().as_bytes().to_owned()]
                    },
                    "XREAD" => {
                        vec![streams::streams::cmd_xread(&cmd_args, storage_ref.clone(), tx.subscribe()).await.as_str().as_bytes().to_owned()]
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
                        vec![lists::lists::cmd_list_push(&cmd_args, storage_ref.clone(), true, tx.clone()).await.as_bytes().to_owned()]
                    },
                    "LRANGE" => {
                        vec![lists::lists::cmd_lrange(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "LPUSH" => {
                        vec![lists::lists::cmd_list_push(&cmd_args, storage_ref.clone(), false, tx.clone()).await.as_bytes().to_owned()]
                    },
                    "LLEN" =>{
                        vec![lists::lists::cmd_llen(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()]
                    }
                    "LPOP" => {
                        vec![lists::lists::cmd_lpop(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "BLPOP" => {
                        // add this client to the waiting list
                        {
                            let mut map = glob_config.lock().await;
                            if let Some(clients) = map.blocked_clients.get_mut(&cmd_args[1]) {
                                clients.push_back(config_args.other_port);
                            } else {
                                let mut new_queue = VecDeque::new();
                                new_queue.push_back(config_args.other_port);
                                map.blocked_clients.insert(cmd_args[1].clone(), new_queue);
                            } 
                        } 
                        println!("client {} waiting on {}", config_args.other_port, &cmd_args[1]);
                        return vec![pub_sub::pub_sub::cmd_blpop(config_args, cmd_args, storage_ref.clone(), tx.subscribe(), glob_config).await.as_bytes().to_owned()];
                    },
                    "SUBSCRIBE" => {
                        return vec![pub_sub::pub_sub::cmd_sub(glob_config, config_args, cmd_args).await.as_bytes().to_owned()]
                    },
                    "PUBLISH" => {
                        return vec![pub_sub::pub_sub::cmd_pub(config_args, cmd_args, glob_config.clone(), tx.clone()).await.as_bytes().to_owned()]
                    },
                    "UNSUBSCRIBE" => {
                        vec![pub_sub::pub_sub::cmd_unsub(config_args, cmd_args, glob_config.clone()).await.as_bytes().to_owned()]
                    },
                    "QUIT" => {
                        unimplemented!();
                    },
                    "ZADD" => {
                        vec![sorted_sets::sorted_sets::cmd_zadd(config_args, cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "ZRANK" => {
                        vec![sorted_sets::sorted_sets::cmd_zrank(config_args, cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()] 
                    },
                    "ZRANGE" => {
                        vec![sorted_sets::sorted_sets::cmd_zrange(config_args, cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "ZCARD" => {
                        vec![sorted_sets::sorted_sets::cmd_zcard(config_args, cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "ZSCORE" => {
                        vec![sorted_sets::sorted_sets::cmd_zscore(config_args, cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "ZREM" => {
                        vec![sorted_sets::sorted_sets::cmd_zrem(config_args, cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "GEOADD" => {
                        vec![geospatial::geospatial::cmd_geoadd(config_args, cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "GEOPOS" => {
                        vec![geospatial::geospatial::cmd_geopos(cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "GEODIST" => {
                        vec![geospatial::geospatial::cmd_geodist(cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "GEOSEARCH" => {
                        vec![geospatial::geospatial::cmd_geosearch(cmd_args, sorted_set_ref.clone()).await.as_bytes().to_owned()]
                    },
                    "ACL" => {
                        match cmd_args[1].to_ascii_uppercase().as_str() {
                            "LIST" => {
                                unreachable!("you should not be here");
                                // vec![auth::auth::cmd_list(cmds, glob_config.clone()).as_bytes().to_owned()] 
                            },
                            _ => {
                                panic!("unidentified ACL command!");
                            }
                        }
                    },
                    _ => {
                        vec![]
                        // unimplemented!("Unidentified command");
                        // continue;
                    }
                };
            } else {
                responses = vec![(redis_err(&_error_sub_mode_on_msg_(&cmd_args[0])).as_bytes().to_owned())];
            }

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
}
