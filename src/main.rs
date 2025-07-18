use std::{collections::HashMap, io::ErrorKind, sync::Arc, time::SystemTime, vec};
use clap::Parser;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, select, sync::{broadcast, Mutex}};
use crate::utils::utils::*;
use crate::methods::methods::*;
pub mod methods;
pub mod utils;

// EXTRA things to do 
//  1-> find out why using read_buf doesnt work in the conn fn 

// PROBLEM: 
// send get ack to all the replica connections, recv bytes processed by them
// doing it through glob_config struct is not gonna work
// for each replica connection send GETACKs  

// all write command bytes need to be counted
// bytes of the following commands need to be counted by the slave: PING, REPLCONF, SET

#[tokio::main]
async fn main() {
    // parse command line arguments
    let mut config_args = Args::parse();
    if config_args.replicaof.starts_with("None") {
        config_args.master_replid = String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        config_args.master_repl_offset = 0;
    } 

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config_args.port)).await.unwrap();
    println!("connected on port: {}", config_args.port);
    
    // connect with master if slave
    if config_args.replicaof.starts_with("None") {
        master_conn(listener, config_args).await;
    } else {
        slave_conn(listener, config_args).await; 
    } 

    println!("rt exiting");
}

async fn slave_conn(listener :TcpListener, config_args: Args) {
    println!("is a slave");

    let _db: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>  = Arc::new(Mutex::new(HashMap::new())); 
    // handshake stage
    let mut input_buf: Vec<u8> = vec![0; 1024];
    let (master_addr, master_port) = config_args.replicaof.split_once(' ').unwrap();
    let mut master_stream = connect_to_master(master_addr, master_port).await;
    println!("connected to master");
    master_stream.write_all(encode_array(&vec![String::from("PING")]).as_bytes()).await.unwrap();
    // expect PONG
    // input_buf.fill(0);
    input_buf.clear();
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // pbas(&input_buf);
    // expect PONG

    // send 2 replconf commands
    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("listening-port"), format!("{}", config_args.port)]).as_bytes()).await.unwrap();
    // input_buf.fill(0);
    input_buf.clear();
    master_stream.read_buf(&mut input_buf).await.unwrap();  // single call works because we arent transmitting large amounts of data
                                                            // in the future we will probably need to read until EOF or we lose data       
    // expect OK
    // pbas(&input_buf);

    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("capa"), format!("npsnyc2")]).as_bytes()).await.unwrap();
    // input_buf.fill(0);
    input_buf.clear();
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect OK
    // pbas(&input_buf);

    master_stream.write_all(encode_array(&vec![format!("PSYNC"), format!("?"), format!("{}", -1)]).as_bytes()).await.unwrap();
    // input_buf.fill(0);
    // input_buf.clear();
    // dont read from stream here, read it from the thread so cmds dont get lost in case they arrive in same packets 
    // master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect FULLRESYNC, ignore respeonse
    // pbas(&input_buf);

    let glob_config_ref = Arc::new(Mutex::new(GlobConfig{
        replicas: HashMap::new(),
        // bytes_rx: 0,
        // replica_writes: 0,
    }));

    let (tx, _) = broadcast::channel::<Vec<u8>>(1024);
    let db_ref = _db.clone();
    let args_copy = config_args.clone();
    let tx1 = tx.clone();
    let rx1 = tx.subscribe();
    let glob_config_ref_copy = glob_config_ref.clone();
    tokio::spawn(async move {
        println!("lauching conn for slave-master");
        conn(master_stream, args_copy, db_ref, tx1, rx1, glob_config_ref_copy).await;
    });

    loop {
        // listen for client connections
        let (stream, _)  = listener.accept().await.unwrap();
        let db_ref = _db.clone();
        let args_copy = config_args.clone();
        let glob_config_ref_copy = glob_config_ref.clone();

        // slave will probably not communicate among its connections 
        let tx1 = tx.clone();
        let rx1 = tx.subscribe();
        tokio::spawn(async move {
            // println!("lauching conn for slave-master");
            conn(stream, args_copy, db_ref, tx1, rx1, glob_config_ref_copy).await;
        });
    }
}

async fn master_conn(listener :TcpListener, config_args: Args) {
    // println!("master connection");
    let _db: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>  = Arc::new(Mutex::new(HashMap::new()));
    let master_config_ref = Arc::new(Mutex::new(GlobConfig{ 
        replicas: HashMap::new(),
        // bytes_rx: 0,
        // replica_writes: 0,
    }));
    
    if !config_args.dir.starts_with("UNSET") { 
        tokio::fs::create_dir_all(&config_args.dir).await.unwrap();
        // println!("{} directory created", &config_args.dir);
        
        let dbfilepath = "".to_owned() + &config_args.dir + "/" + &config_args.dbfilename;
        if !dbfilepath.starts_with("UNSET") {
            cmd_sync(&dbfilepath, _db.clone()).await;
        }
    }

    let (tx, _) = broadcast::channel::<Vec<u8>>(1024); 
    loop {
        // println!("waiting for new clients or replicas");
        // this could be a replication connection or a client connection 
        let (stream, sockaddr)  = listener.accept().await.unwrap();
        // println!("new connection to master from {}:{}", &sockaddr.ip(), &sockaddr.port());
       
        // let new_shared_config_args = shared_config_args.clone();
        let db_ref = _db.clone();
        let master_config_ref_copy = master_config_ref.clone();
        
        // slave will probably not communicate among its connections 
        let tx1 = tx.clone();
        let rx1 = tx.subscribe();
        let args_copy = config_args.clone();    // why are we cloning 
        // this spawns a tokio "asyncrhonous green thread" 
        tokio::spawn(async move {
            // print!("{:?}\n", &new_shared_config_args);
            conn(stream, 
                args_copy, 
                db_ref, 
                tx1, 
                rx1, master_config_ref_copy).await;
        });
    }

}

async fn conn(mut _stream: TcpStream, 
    mut config_args: Args, 
    storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
    tx: broadcast::Sender<Vec<u8>>,
    mut rx:  broadcast::Receiver<Vec<u8>>,
    glob_config: Arc<Mutex<GlobConfig>>) { 
    // represents an incoming connection
    // need to convert thi storage int Arc<Mutex<>> so it can be shared across different connections,
    // if one connection updates, the other can see them
    
    let dbfilepath = "".to_owned() + &config_args.dir + "/" + &config_args.dbfilename;
    let mut input_buf: Vec<u8> = vec![0; 1024];

    loop {
        input_buf.fill(0);
        let mut output: Vec<Vec<u8>> = Vec::new();
        select! {
            // using read_buf doesnt work here, find why
            _ = _stream.readable() => {
                match _stream.try_read(&mut input_buf) {
                    Ok(bytes_rx) => {
                        if bytes_rx == 0 {
                           break;
                        }
                        // pbas(&input_buf);
                    },
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                        // this error handling in necessary otherwise it would necessarily block
                        continue;
                    },
                    Err(e) => {
                        println!("{}", e);
                        break;
                    } 
                }
            },
            msg = rx.recv() => {
                if !config_args.replica_conn {  // if this is not a replica connection then ignore replicated command
                    continue;
                }

                output = vec![msg.unwrap()];
                print!("recvd on rx: ");
                pbas(&output[0]);
            }
        }
        
        // println!("{:?}", input_buf);
        if output.is_empty() {  // only parse input_buf if commands recvd from a client
            // bytes_rx represents the number of bytes of commands that came after handshake sequence 
            let cmds = parse(0, &input_buf);  // these are basically commands, at one point we will have to parse commands with their parameters, they could be int, boolean etc.   
            println!("exec: {:?}", cmds);

            for (bytes_rx, mut cmd_args) in cmds {
                // println!("cmd: {} bytes: {}", cmd_args[0], bytes_rx);
                cmd_args[0] = cmd_args[0].to_uppercase();

                output = match cmd_args[0].as_str() {
                    "ECHO" => {
                        vec![encode_bulk(&cmd_args[1]).as_bytes().to_owned()]
                    },
                    "PING" => {
                        if config_args.replicaof.starts_with("None") {  // if this server instance is a master, part of handshake   
                            vec![encode_bulk("PONG").as_bytes().to_owned()] 
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
                            tx.send(encode_array(&cmd_args).as_bytes().to_vec()).unwrap();  // send replication
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
                                vec![encode_bulk(rdb_value.value.as_str()).as_bytes().to_owned()] 
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
                        
                        let msg = encode_array(&vec!["REPLCONF".to_owned(), "GETACK".to_owned(), "*".to_owned()]);
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
                                vec![encode_simple(&vec![rdb_value.value_type.repr().as_str()]).as_bytes().to_owned()] 
                            },
                            None => {
                                vec![encode_simple(&vec!["none"]).as_bytes().to_owned()]
                            }
                        }
                    },
                    _ => {
                        // unimplemented!("Unidentified command");
                        continue;
                    }
                };

                // config_args should hold bytes of write commands only 
                // add to processed bytes if either its a replica(receiving commands from master) or a master receiving from replica
                // process commands received at replica or sent by client(to master)  
                // if !config_args.replicaof.starts_with("None") || !config_args.replica_conn {
                //     config_args.bytes_rx += bytes_rx;
                // }
                // bytes are adde
            }
        }

        for out in output {
            // for debugging
            // println!("sending: {}", out.iter().map(|ch| {*ch as char}).collect::<String>());
            _stream.write_all(&out).await.unwrap();
            // if you wanna use try_write() you will need to manually send data piece by piece by tracking bytes sent
        } 
    }
}