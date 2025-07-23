use std::{collections::HashMap, io::ErrorKind, sync::Arc, time::SystemTime, vec};
use clap::Parser;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, select, sync::{broadcast, Mutex}};
use crate::utils::utils::*;
use crate::methods::methods::*;
pub mod methods;
pub mod utils;

// EXTRA things to do 
//  1-> find out why using read_buf doesnt work in the conn fn 

// all write command bytes need to be counted
// bytes of the following commands need to be counted by the slave: PING, REPLCONF, SET

#[tokio::main]
async fn main() {
    // parse command line arguments
    let mut config_args = Args::parse();
    if config_args.replicaof.starts_with("None") {
        config_args.master_replid = String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        config_args.master_repl_offset = 0;
        config_args.pending_cmds = vec![];
        config_args.queueing = false; 
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
    master_stream.write_all(encode_array(&vec![String::from("PING")], true).as_bytes()).await.unwrap();
    // expect PONG
    // input_buf.fill(0);
    input_buf.clear();
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // pbas(&input_buf);
    // expect PONG

    // send 2 replconf commands
    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("listening-port"), format!("{}", config_args.port)], true).as_bytes()).await.unwrap();
    // input_buf.fill(0);
    input_buf.clear();
    master_stream.read_buf(&mut input_buf).await.unwrap();  // single call works because we arent transmitting large amounts of data
                                                            // in the future we will probably need to read until EOF or we lose data       
    // expect OK
    // pbas(&input_buf);

    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("capa"), format!("npsnyc2")], true).as_bytes()).await.unwrap();
    // input_buf.fill(0);
    input_buf.clear();
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect OK
    // pbas(&input_buf);

    master_stream.write_all(encode_array(&vec![format!("PSYNC"), format!("?"), format!("{}", -1)], true).as_bytes()).await.unwrap();
    // input_buf.fill(0);
    // input_buf.clear();
    // dont read from stream here, read it from the thread so cmds dont get lost in case they arrive in same packets 
    // master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect FULLRESYNC, ignore respeonse
    // pbas(&input_buf);

    let glob_config_ref = Arc::new(Mutex::new(GlobConfig{
        replicas: HashMap::new(),
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
        let (stream, _)  = listener.accept().await.unwrap();
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
    
    let mut input_buf: Vec<u8> = vec![0; 1024];
    let mut cmds = vec![];
    let mut output: Vec<Vec<u8>> = Vec::new();

    loop {
       
        // 0s are needed to mark end of cmds 
        input_buf.fill(0);
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
                // print!("recvd on rx: ");
                // pbas(&output[0]);
            }
        }

        if output.is_empty() {  // only parse input_buf if commands recvd over client
            let mut flag = false;   // for detecting if last issued command was exec
            for cmd in parse(0, &input_buf) {
                if cmd.1[0].to_uppercase() == "EXEC" {
                    if !config_args.queueing {  // if MULTI wasnt issued
                        output = vec![redis_err(_ERROR_EXEC_WITHOUT_MULTI_).as_bytes().to_owned()];
                    }
                    config_args.queueing = false;
                    flag = true;
                    continue;
                } else if cmd.1[0].to_uppercase() == "DISCARD" {
                    if !config_args.queueing {
                        output = vec![redis_err(_ERROR_DISCARD_WITHOUT_MULTI_).as_bytes().to_owned()];
                        continue;
                    }
                    config_args.queueing = false;
                    cmds.clear();
                }
                cmds.push(cmd);
            }  // these are basically commands, at one point we will have to parse commands with their parameters, they could be int, boolean etc.   
            
            // println!("exec: {:?}", cmds);
            if config_args.queueing {
                output = vec![encode_simple(&vec!["QUEUED"]).as_bytes().to_owned()];
            } else {
                if output.is_empty() {
                    output = exec(&cmds,
                        &mut config_args,
                        storage_ref.clone(),
                        tx.clone(),
                        glob_config.clone()).await;

                    if flag {
                        // if last command was EXEC then encode all the output into an array
                        output = vec![encode_array(&output.iter().map(|bytes| {
                            let mut cmd = String::new();
                            for &byte in bytes {
                                cmd.push(byte as char);
                            }

                            cmd
                        }).collect(), false).as_bytes().to_vec().to_owned()];
                    }
                }
                cmds.clear();
            }
        } 

        for out in &output {
            // for debugging
            // println!("sending: {}", out.iter().map(|ch| {*ch as char}).collect::<String>());
            _stream.write_all(out).await.unwrap();
            // if you wanna use try_write() you will need to manually send data piece by piece by tracking bytes sent
        }
        output.clear();
    }
}