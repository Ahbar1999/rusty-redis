use std::{collections::HashMap, io::ErrorKind, sync::Arc, time::SystemTime, vec};
use clap::Parser;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, select, sync::{broadcast, Mutex}};
use crate::utils::utils::*;
use crate::methods::methods::*;
pub mod methods;
pub mod utils;

// todo!("we need a way to process replication connections inside a master connection itself");
// if replicaof flag is set then start up as slave other as master 
// todo!("only master should accept incoming connections");
// todo!("slave should not accept any connections");

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
}

async fn slave_conn(listener :TcpListener, config_args: Args) {
    println!("is a slave");

    let _db: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>  = Arc::new(Mutex::new(HashMap::new())); 
    // handshake stage
    let mut input_buf: Vec<u8> = vec![0; 1024];
    let (master_addr, master_port) = config_args.replicaof.split_once(' ').unwrap();
    let mut master_stream = connect_to_master(master_addr, master_port).await;
    master_stream.write_all(encode_array(&vec![String::from("PING")]).as_bytes()).await.unwrap();
    // expect PONG
    input_buf.fill(0);
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect PONG

    // send 2 replconf commands
    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("listening-port"), format!("{}", config_args.port)]).as_bytes()).await.unwrap();
    input_buf.fill(0);
    master_stream.read_buf(&mut input_buf).await.unwrap();  // single call works because we arent transmitting large amounts of data
                                                            // in the future we will probably need to read until EOF or we lose data       
    // expect OK

    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("capa"), format!("npsnyc2")]).as_bytes()).await.unwrap();
    input_buf.fill(0);
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect OK

    master_stream.write_all(encode_array(&vec![format!("PSYNC"), format!("?"), format!("{}", -1)]).as_bytes()).await.unwrap();
    input_buf.fill(0);
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect FULLRESYNC, ignore respeonse

    // let shared_config_args = Arc::new(Mutex::new(config_args));
    let (tx, rx) = broadcast::channel::<Vec<u8>>(1024);

    loop {
        let (stream, _)  = listener.accept().await.unwrap();
        let config_args_cpy = config_args.clone();
        // let new_shared_config_args = shared_config_args.clone();
        let db_ref = _db.clone();
        let tx1 = tx.clone();
        let rx1 = tx.subscribe();
        // this spawns a tokio "asyncrhonous green thread" 
        tokio::spawn(async move {
            conn(stream, config_args_cpy, db_ref, tx1, rx1).await;
        });
    }
}

async fn master_conn(listener :TcpListener, config_args: Args) {
    let _db: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>  = Arc::new(Mutex::new(HashMap::new()));
    let (tx, _) = broadcast::channel::<Vec<u8>>(1024); 
    loop {
        // this could be a replication connection or a client connection 
        let (stream, _)  = listener.accept().await.unwrap();
        // let new_shared_config_args = shared_config_args.clone();
        let db_ref = _db.clone();
        
        // slave will probably not communicate among its connections 
        let tx1 = tx.clone();
        let rx1 = tx.subscribe();
        let args_copy = config_args.clone();
        // this spawns a tokio "asyncrhonous green thread" 
        tokio::spawn(async move {
            // println!("new connection to master from {}:{} with config: ", &sockaddr.ip(), &sockaddr.port());
            // print!("{:?}\n", &new_shared_config_args);
            conn(stream, args_copy, db_ref, tx1, rx1).await;
        });
    }
}

async fn conn(mut _stream: TcpStream, 
    mut config_args: Args, 
    storage_ref: Arc<Mutex<HashMap<String, (String, Option<SystemTime>)>>>,
    tx: broadcast::Sender<Vec<u8>>,
    mut rx:  broadcast::Receiver<Vec<u8>>) { 
    // represents an incoming connection
    // need to convert thi storage int Arc<Mutex<>> so it can be shared across different connections,
    // if one connection updates, the other can see them
   
    // println!("a"); 
    // {
        // let data = config_args.lock().await; 
    if !config_args.dir.starts_with("UNSET") { 
        tokio::fs::create_dir_all(&config_args.dir).await.unwrap();
        println!("{} directory created", &config_args.dir);
    }

    let dbfilepath = "".to_owned() + &config_args.dir + "/" + &config_args.dbfilename; 
    // }
    let mut input_buf: Vec<u8> = vec![0; 1024];

    println!("d");
    loop {
        input_buf.fill(0);
        let mut output: Vec<Vec<u8>> = Vec::new();

        select! {
            _ = _stream.readable() => {
                match _stream.try_read(&mut input_buf) {
                    Ok(bytes_rx) => {
                        if bytes_rx == 0 {
                            // println!("conn loop exited");
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
            },
            msg = rx.recv() => {
                if config_args.replicaof.starts_with("None") {
                    continue;
                }
                output = vec![msg.unwrap()];
            }
        }
        
        // match _stream.try_read(&mut input_buf) {
        //     Ok(bytes_rx) => {
        //         if bytes_rx == 0 {
        //             // println!("conn loop exited");
        //             break;
        //         }
        //     },
        //     Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
        //         // this error handling in necessary otherwise it would necessarily block
        //         continue;
        //     },
        //     Err(e) => {
        //         println!("{}", e);
        //     }
        // }

        if output.is_empty() {  // only parse input_buf if commands recvd from a client
            let mut cmd_args = parse(0, &input_buf);  // these are basically commands, at one point we will have to parse commands with their parameters, they could be int, boolean etc.   
            cmd_args[0] = cmd_args[0].to_uppercase();

            output = match cmd_args[0].as_str() {
                "ECHO" => {
                    vec![encode_bulk(&cmd_args[1]).as_bytes().to_owned()]
                },
                "PING" => {
                    // assuming if this connection is to a replica 
                    // in this copy of config_args set replicaof to "Some"
                    // if some connection call pings then it has to be a replica/slave
                    if config_args.replicaof.starts_with("None") {
                        config_args.replicaof = "Some".to_owned();
                    }
                    vec![encode_bulk("PONG").as_bytes().to_owned()]
                },
                "SET" => {
                    // (only)send to replicas awaiting
                    if config_args.replicaof.starts_with("None") {
                        tx.send(encode_array(&cmd_args).as_bytes().to_vec()).unwrap();
                    }
                    vec![cmd_set(&cmd_args, storage_ref.clone()).await.as_bytes().to_owned()]
                },
                "GET" => {
                    vec![cmd_get(&cmd_args[1], &dbfilepath, storage_ref.clone()).await.as_bytes().to_owned()] 
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
                    vec![encode_simple(&vec!["OK"]).as_bytes().to_owned()]
                },
                "PSYNC" => {
                    vec![cmd_psync(&config_args).await.as_bytes().to_owned(), cmd_fullresync(&config_args).await] 
                },
                _ => {
                    unimplemented!("Unidentified command");
                }
            };
        }

        for out in output {
            // for debugging
            // println!("sending: {}", out.iter().map(|ch| {*ch as char}).collect::<String>());
            _stream.write_all(&out).await.unwrap();
            // if you wanna use try_write() you will need to manually send data piece by piece by tracking bytes sent
        } 
    }
}