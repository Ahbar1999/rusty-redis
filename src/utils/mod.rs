pub mod utils {
    use std::time::SystemTime;
    use clap::Parser;
    
    // this module provides frequently used funtions, constants, types

    /* 
    pub union RDBValue {
        s: ManuallyDrop<String>,
        n: i64, 
    }
    */

    #[derive(Debug, Clone)]
    pub struct StorageKV { 
        pub key     :String,
        pub value   :String, 
        pub exp_ts  :Option<SystemTime>,
    }

    // for parsing command line args
    #[derive(Parser, Debug, Clone)]
    pub struct Args {
        #[arg(short, long, default_value_t=String::from("UNSET"))]
        pub dir: String,

        #[arg(long, default_value_t=String::from("UNSET"))]
        pub dbfilename: String,

        #[arg(long, default_value_t=6379)]
        pub port: u16,

        #[arg(long, default_value_t=String::from("None"))]
        pub replicaof: String,

        #[arg(long, default_value_t=false)]
        pub replica_conn: bool,
        // #[arg(long, default_value_t=false)]
        // pub is_master: bool,

        #[arg(long, default_value_t=String::from("None"))]
        pub master_replid: String,
        
        #[arg(long, default_value_t=0)]
        pub master_repl_offset: u32,

        #[arg(long, default_value_t=0)]
        pub bytes_rx: usize,
    }

    pub struct GlobConfig {
        pub repl_count: usize,
    }

    pub const DELIM: u8 = b'\r';
    pub const SKIP_LEN: usize = 2;
    pub const _RDB_METADATA_SECTION_FLAG_: u8 = 0xFA;
    pub const _RDB_DATA_SECTION_FLAG_: u8 = 0xFE;
    pub const _RDB_END_: u8 = 0xFF;
    pub const _RDB_TIMESTAMP_MS_FLAG: u8 = 0xFC;
    pub const _RDB_TIMESTAMP_S_FLAG: u8 = 0xFD;
    pub const _EMPTY_RDB_FILE_: &str= "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    // print bytes as string
    pub fn pbas(buf: &Vec<u8>) {
        println!("{}", buf.iter().map(|ch| {*ch as char}).collect::<String>());
    }

    pub fn encode_array(vals: &Vec<String>) -> String {
        let mut output = format!("*{}\r\n", vals.len());
        let sentinel  = "$";
        for v in vals {
            output += &sentinel;
            output += v.len().to_string().as_str();
            output += "\r\n";
            output += v;
            output += "\r\n"; 
        }

        output
    }

    pub fn encode_bulk(s: &str) -> String {
        if s.is_empty() {
            // return NULL string
            return format!("$-1\r\n");
        }

        let data = String::from(s);
        
        format!("${}\r\n{}\r\n", data.len(), data)
    }

    pub fn encode_int(n: usize) -> String {
        format!(":{}\r\n", n)
    }

    // parse a single command
    pub fn parse(mut ptr: usize, buf: &[u8]) -> Vec<(usize, Vec<String>)> {
        // print!("{:?}", buf);
        // for b in buf {
        //     print!("{} ", *b as char);
        // }
        // println!("");
        // print!("parsing: ");
        // pbas(&buf.to_vec());
        let mut cmds: Vec<(usize, Vec<String>)>  = vec![];
        while ptr < buf.len() {
            // let mut bytes_parsed = 0;   // represents bytes of (valid) commands 
            match buf[ptr] {
                b'$' => {
                    let (new_ptr, s) = parse_string(ptr, buf);
                    // bytes_parsed += new_ptr - ptr;
                    cmds.push((new_ptr -ptr, vec![s]));
                    ptr = new_ptr;
                },
                b'*' => {
                    let (new_ptr, v) = parse_array(ptr, buf);
                    // bytes_parsed += new_ptr - ptr;
                    cmds.push((new_ptr -ptr, v));
                    ptr = new_ptr; 
                },
                b'+' => {   // simple strings
                    // just skip them for now 
                    let mut new_ptr = ptr;
                    while buf[new_ptr] != b'\n' {
                        new_ptr += 1;
                    }
                    ptr = new_ptr + 1;
                },
                0x0 => {
                    // no enocding/data should start from 0 
                    // reached the end of the buffer
                    // since we are passing the whole input buffer after the valid bytes the 0s will be read
                    // hoping it occurs at the end in some cases where '\r\n' is encountered which wasn't skipped previously
                    // needs better case handling
                    // println!("reached the end of the buffer, if some commands remained, you might wanna check this code");
                    break;
                },
                _ => {
                    // continue;
                    print!("problem: ");
                    pbas(&buf[ptr..].to_vec());
                    unimplemented!("if you are seeing this, you are screwed")
                }
            }
        }

        cmds
    }

    // you might have to call this recursively
    pub fn parse_array(ptr: usize, buf: &[u8]) -> (usize, Vec<String>) {
        let mut size =0;
        let mut i = ptr + 1;
        while buf[i] != DELIM {
            size = 10 * size + (buf[i] as char).to_digit(10).unwrap() as usize;
            i += 1;
        }
        i += SKIP_LEN;
        // println!("size of array: {}", size);

        let mut result:Vec<String> = vec![];
        for _ in 0..size {
            match buf[i] as char {
                '$' => {
                    let (new_ptr, out) = parse_string(i, buf);
                    i = new_ptr;
                    result.push(out);
                }
                _ => {
                    unimplemented!()
                }
            }
        }

        (i, result)
    }

    pub fn parse_string(ptr: usize, buf: &[u8]) -> (usize, String) {
        let mut size =0 as usize;
        let mut i = ptr + 1; // ptr points to string identifier: '$'
        while buf[i] != DELIM {
            size = 10 * size + (buf[i] as char).to_digit(10).unwrap() as usize;
            i += 1;
        }
        i += SKIP_LEN; // skip both \r and \n

        let mut result = String::new();

        for j in 0..size {
            result.push(buf[j as usize + i] as char);
        }

        i += size;
        if buf[i] == b'\r' {  // if clrf exists, in cases like file transmission it doesnt exist
            i += SKIP_LEN;
        }

        println!("string parsed: {}", result);
        return (i, result); 
    }

    /*
    pub async fn propagate(msg: &String, replica_conns: Vec<TcpStream>) {
        // for conn in replica_conns {
        //     // println!("trying to connect to replica with port no.:{}\npropagating {}", socket, msg);
        //     // replica = TcpStream::connect(format!("127.0.0.1:{}", socket)).await.unwrap();
        //     conn.write_all(msg.clone().as_bytes()).await.unwrap();
        //     // dont wait for reply
        // }
        unimplemented!()
    }
    */
}