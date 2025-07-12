use std::{collections::HashMap, fmt::format, io::ErrorKind, sync::Arc, time::{Duration, SystemTime}, vec};
// use clap::{Arg, ArgAction, Args};
use clap::Parser;
use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}};
use crate::utils::utils::*;
use crate::methods::methods::*;
pub mod methods;
pub mod utils;

#[tokio::main]
async fn main() {
    // parse command line arguments
    let config_args = Args::parse();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config_args.port)).await.unwrap();

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

    tokio::fs::create_dir_all(&config_args.dir).await.unwrap();
    println!("{} directory created", &config_args.dir);

    let dbfilepath = config_args.dir.clone() + "/" + config_args.dbfilename.as_str();

    // println!("{:?}", args);
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
        let mut output: String = String::from("");
        let mut cmd_args = parse(0, &input_buf);  // these are basically commands, at one point we will have to parse commands with their parameters, they could be int, boolean etc.   
        cmd_args[0] = cmd_args[0].to_uppercase();
        

        match cmd_args[0].as_str() {
            "ECHO" => {
                output = encode_bulk(&cmd_args[1]);
            },
            "PING" => {
                output = encode_bulk("PONG");
            },
            "SET" => {
                // todo!("refactor following code");
                output = cmd_set(&cmd_args, &mut storage);
            },
            "GET" => {
                output = cmd_get(&cmd_args[1], &dbfilepath, &mut storage).await; 
            },
            "CONFIG" => {
                output = cmd_config(&cmd_args[2], &config_args);
            },
            "SAVE" => {
                output = cmd_save(&storage, &dbfilepath).await;
            },
            "KEYS" => {
                output = cmd_keys(&dbfilepath, &mut storage).await;
            },
            "INFO" => {
                output = cmd_info();
            }
            _ => {
                unimplemented!("Unidentified command");
            }
        } 

        println!("sending: {}", output);
        _stream.write_all(output.as_bytes()).await.unwrap();
        // if you wanna use try_write() you will need to manually send data piece by piece by tracking bytes sent 
    }
}