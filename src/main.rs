use std::{collections::HashMap, fmt::format, io::ErrorKind, sync::Arc, time::{Duration, SystemTime}, vec};
// use clap::{Arg, ArgAction, Args};
use clap::Parser;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use crate::utils::utils::*;
use crate::methods::methods::*;
pub mod methods;
pub mod utils;

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

    // handshake stage
    let mut input_buf: Vec<u8> = vec![0; 1024];
    let (master_addr, master_port) = config_args.replicaof.split_once(' ').unwrap();
    let mut master_stream = connect_to_master(master_addr, master_port).await;
    master_stream.write_all(encode_array(&vec![String::from("PING")]).as_bytes()).await.unwrap();
    input_buf.fill(0);
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect PONG

    // send 2 replconf commands
    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("listening-port"), format!("{}", config_args.port)]).as_bytes()).await.unwrap();
    input_buf.fill(0);
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect OK

    master_stream.write_all(encode_array(&vec![format!("REPLCONF"), format!("capa"), format!("npsnyc2")]).as_bytes()).await.unwrap();
    input_buf.fill(0);
    master_stream.read_buf(&mut input_buf).await.unwrap();
    // expect OK

    loop {
        let (stream, _)  = listener.accept().await.unwrap();
        let args_copy = config_args.clone();
        // this spawns a tokio "asyncrhonous green thread" 
        tokio::spawn(async move {
            conn(stream, args_copy).await;
        });
    }
}

async fn master_conn(listener :TcpListener, config_args: Args) {
    println!("is a master");
    loop {
        let (stream, _)  = listener.accept().await.unwrap();
        let args_copy = config_args.clone();
        // this spawns a tokio "asyncrhonous green thread" 
        tokio::spawn(async move {
            conn(stream, args_copy).await;
        });
    }
}

async fn conn(mut _stream: TcpStream, config_args: Args) { // represents an incoming connection
    let mut storage: HashMap<String, (String, Option<SystemTime>)> = HashMap::new();

    if !config_args.dir.starts_with("UNSET") {
        tokio::fs::create_dir_all(&config_args.dir).await.unwrap();
        println!("{} directory created", &config_args.dir);
    }

    let dbfilepath = config_args.dir.clone() + "/" + config_args.dbfilename.as_str();

    let mut input_buf: Vec<u8> = vec![0; 1024];

    loop {
        input_buf.fill(0);
        _stream.readable().await.unwrap();
        match _stream.try_read(&mut input_buf) {
            Ok(bytes_rx) => {
                if bytes_rx == 0 {
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

        // we know that its a string (for now)
        // respond to th command; currently without any error handling
        // _stream.writable().await.unwrap();
        // let data = input_buf[0..bytes_rx]; 
        let mut cmd_args = parse(0, &input_buf);  // these are basically commands, at one point we will have to parse commands with their parameters, they could be int, boolean etc.   
        cmd_args[0] = cmd_args[0].to_uppercase();
        
        let output = match cmd_args[0].as_str() {
            "ECHO" => {
                encode_bulk(&cmd_args[1])
            },
            "PING" => {
                encode_bulk("PONG")
            },
            "SET" => {
                cmd_set(&cmd_args, &mut storage)
            },
            "GET" => {
                cmd_get(&cmd_args[1], &dbfilepath, &mut storage).await 
            },
            "CONFIG" => {
                cmd_config(&cmd_args[2], &config_args)
            },
            "SAVE" => {
                cmd_save(&storage, &dbfilepath).await
            },
            "KEYS" => {
                cmd_keys(&dbfilepath, &mut storage).await
            },
            "INFO" => {
                cmd_info(&config_args)
            }
            _ => {
                unimplemented!("Unidentified command");
            }
        };

        println!("sending: {}", output);
        _stream.write_all(output.as_bytes()).await.unwrap();
        // if you wanna use try_write() you will need to manually send data piece by piece by tracking bytes sent 
    }
}