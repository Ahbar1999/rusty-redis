pub mod utils {
    use std::{mem::ManuallyDrop, time::SystemTime};
    use clap::Parser;
    // this module provides frequently used funtions, constants, types
    pub union RDBValue {
        s: ManuallyDrop<String>,
        n: i64, 
    }

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

        #[arg(short, long, default_value_t=String::from("UNSET"))]
        pub dbfilename: String,

        #[arg(short, long, default_value_t=6379)]
        pub port: u16,

        #[arg(short, long, default_value_t=String::from("None"))]
        pub replicaof: String,
    }

    pub const DELIM: u8 = b'\r';
    pub const SKIP_LEN: usize = 2;
    pub const _RDB_METADATA_SECTION_FLAG_: u8 = 0xFA;
    pub const _RDB_DATA_SECTION_FLAG_: u8 = 0xFE;
    pub const _RDB_END_: u8 = 0xFF;
    pub const _RDB_TIMESTAMP_MS_FLAG: u8 = 0xFC;
    pub const _RDB_TIMESTAMP_S_FLAG: u8 = 0xFD;

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

    // parse a single command
    pub fn parse(ptr: usize, buf: &[u8]) -> Vec<String> {
        // print!("{:?}", buf);
        // for b in buf {
        //     print!("{} ", *b as char);
        // }
        // println!("");

        match buf[ptr] {
            b'$' => {
                let (_, s) = parse_string(ptr, buf);
                vec![s]
            },
            b'*' => {
                let (_, v) = parse_array(ptr, buf);
                v
            }
            _ => {
                vec![]
            }
        }
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
        i += SKIP_LEN;

        println!("string parsed: {}", result);
        return (i, result); 
    }
}