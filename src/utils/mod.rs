pub mod utils {
    use std::{cmp::Ordering, collections::{BTreeSet, HashMap, HashSet, VecDeque}, time::SystemTime};
    use clap::Parser;
    use serde::{Deserialize, Serialize};
    use tokio::net::TcpStream;
   
    // this module provides frequently used funtions, constants, types

    #[derive(Debug, Clone)]
    pub enum RDBValue {
        String(String),
        // change the underlying data type for stream variant to something like rbtree keyed on id field of stream entry
        // this will allow the most important operation on streams(range operation) to be executed effeciently 
        Stream(Vec<StreamEntry>),
        List(VecDeque<String>), 
    }

    impl RDBValue {
        // return string representation or name of a variant of RDBValueType enum 
        pub fn repr(&self) -> String {
            match self {
                Self::String(_) => {
                    "string".to_owned()
                },
                Self::Stream(_) => {
                    "stream".to_owned() 
                },
                Self::List(_) => {
                    "list".to_owned()
                }
            }
       } 
    }


    #[derive(Debug, Clone)]
    pub struct StreamEntry {
        pub id: (usize, usize),
        // pub key: String,
        pub value: Vec<(String, String)>, // each entry consists of a number of k, v pairs 
    }

    impl StreamEntry {
        // serialize a RDBValue::Stream to redist array  
        pub fn serialize(&self) -> String {
            let mut res = vec![];
            // encode each k,v pair of this entry as bulk string
            for (k, v) in &self.value {
                res.push(k.clone());
                res.push(v.clone()); 
            } 
            // encode all bulk strings as an array 
            encode_array(&vec![encode_bulk(&format!("{}-{}", self.id.0, self.id.1)), encode_array(&res, true)], false)
        }
    }

    #[derive(Debug, Clone)]
    pub struct StorageKV { 
        pub key     :String,
        pub value   :RDBValue, 
        pub exp_ts  :Option<SystemTime>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
    pub struct SortableF64(pub f64);

    impl Eq for SortableF64 {}

    impl PartialOrd for SortableF64 {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.0.partial_cmp(&other.0)
        }
    }
    
    impl Ord for SortableF64 {
        fn cmp(&self, other: &Self) -> Ordering {
            self.0.total_cmp(&other.0)
        }
    }

    #[derive(Serialize, Deserialize)]
    pub struct GEOlocation {
        pub lat:    SortableF64,
        pub long:   SortableF64,
        pub member: String 
    }

    #[derive(Debug, Clone, Default)]
    pub struct SortedSet {
        // SortableF64 represents score value
        pub kv      :HashMap<String, SortableF64>,
        // need to change this, we need to support O(1) order find in addition to insert, delete in O(logn)
        pub st      :BTreeSet<(SortableF64, String)>,
    } 

    impl SortedSet {
        pub fn insert(&mut self, key: &String, score: &SortableF64, value: &String) -> usize {
            let mut ans= 1;
            
            if let Some(old_score) = self.kv.insert(key.clone(), *score) {    // insert updated entry in hash map
                self.st.remove(&(old_score, key.clone()));

                ans = 0;    // new key was inserted in this set
            }

            // insert updated version in the ordered set 
            self.st.insert((score.clone(), value.clone()));

            // return number of new elements inserted
            ans 
        }
    }

    // impl Default for SortedSet {
    //     fn default() -> Self {
    //         SortedSet{ map1: HashMap::new(), map2: BTreeMap::new() }
    //     }
    // }

    // #[derive(Debug, Clone, Default)]
    // pub struct AUTH_INFO {
    //     pub username: String,
    //     pub password: String,
    //     pub allowed_commands: Vec<String>,
    // }

    // for parsing command line args
    #[derive(Parser, Debug, Clone)]
    pub struct Args {
        #[arg(short, long, default_value_t=String::from("UNSET"))]
        pub dir: String,

        #[arg(long, default_value_t=String::from("UNSET"))]
        pub dbfilename: String,

        // this is the port number that's being used by the instance of the program to listen to connections on 
        #[arg(long, default_value_t=6379)]
        pub port: u16,

        // the port that this connection is curretnly connected to
        #[arg(long, default_value_t=0)]
        pub other_port: u16,

        // if this flag was passed then it represents the master of which this intance is a replica of 
        #[arg(long, default_value_t=String::from("None"))]
        pub replicaof: String,

        // represents if this connection is to a replica or not(it could be to a client as well)
        #[arg(long, default_value_t=false)]
        pub replica_conn: bool,
        
        #[arg(long, default_value_t=String::from("None"))]
        pub master_replid: String,

        // acl authentication
        // #[clap(skip)] 
        // pub user_auth: AUTH_INFO, 
        
        // number of bytes processed by this instance
        #[clap(skip)]
        pub master_repl_offset: u32,

        // number of bytes processed by this instance
        #[clap(skip)]
        pub bytes_rx: usize,

        #[clap(skip)]
        pub queueing: bool,

        #[clap(skip)]
        pub pending_cmds: Vec<Vec<(usize, Vec<String>)>>,

        #[clap(skip)]
        pub subbed_chans: HashMap<Vec<u8>, ()>,

        // if this is a client and it is in sub mode 
        #[clap(skip)]
        pub client_in_sub_mode: bool, 
    }

    pub struct ReplicaInfo {    // for master to gather information about the connected clients
        pub bytes_rx: usize,    // number of bytes processed by this client   
    }

    pub struct GlobConfig {
        pub replicas: HashMap::<u16, ReplicaInfo>,
        pub blocked_clients: HashMap<String, VecDeque<u16>>,
        pub subscriptions: HashMap<String, HashSet<u16>>,
        pub users: HashMap<(String, String), Vec<String>>, 
    }

    // ---GEOSPATIAL/SCORE ENCODE/DECODE CODE SECTION BEGINS---
    // this part of code has been copied from other sources 

    fn spread_int32_to_int64(v: u32) -> u64 {
        let mut result = v as u64;
        result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
        result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
        result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
        result = (result | (result << 2)) & 0x3333333333333333;

        (result | (result << 1)) & 0x5555555555555555
    }
    
    fn interleave(x: u32, y: u32) -> u64 {
        let x_spread = spread_int32_to_int64(x);
        let y_spread = spread_int32_to_int64(y);
        let y_shifted = y_spread << 1;
        
        x_spread | y_shifted
    }
    
    pub fn geo_encode(latitude: f64, longitude: f64) -> u64 {
        // Normalize to the range 0-2^26
        let normalized_latitude = 2.0_f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
        let normalized_longitude = 2.0_f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;
    
        // Truncate to integers
        let lat_int = normalized_latitude as u32;
        let lon_int = normalized_longitude as u32;
    
        interleave(lat_int, lon_int)
    }

    const MIN_LATITUDE: f64 = -85.05112878;
    const MAX_LATITUDE: f64 = 85.05112878;
    const MIN_LONGITUDE: f64 = -180.0;
    const MAX_LONGITUDE: f64 = 180.0;

    const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
    const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

    #[derive(Debug, Clone)]
    pub struct Coordinates {
        pub latitude: f64,
        pub longitude: f64,
    }

    fn compact_int64_to_int32(v: u64) -> u32 {
        let mut result = v & 0x5555555555555555;
        result = (result | (result >> 1)) & 0x3333333333333333;
        result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
        result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
        result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
        ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32  // Cast to u32
    }

    fn convert_grid_numbers_to_coordinates(grid_latitude_number: u32, grid_longitude_number: u32) -> Coordinates {
        // Calculate the grid boundaries
        let grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
        let grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
        let grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
        let grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));
        
        // Calculate the center point of the grid cell
        let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
        let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;
        
        Coordinates { latitude, longitude }
    }

    pub fn geo_decode(geo_code: u64) -> Coordinates {
        // Align bits of both latitude and longitude to take even-numbered position
        let y = geo_code >> 1;
        let x = geo_code;
        
        // Compact bits back to 32-bit ints
        let grid_latitude_number = compact_int64_to_int32(x);
        let grid_longitude_number = compact_int64_to_int32(y);
        
        convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
    }

    // ---GEOSPATIAL/SCORE ENCODE/DECODE CODE SECTION ENDS---
    
    pub const DELIM: u8 = b'\r';
    pub const SKIP_LEN: usize = 2;
    pub const _RDB_METADATA_SECTION_FLAG_: u8 = 0xFA;
    pub const _RDB_DATA_SECTION_FLAG_: u8 = 0xFE;
    pub const _RDB_END_: u8 = 0xFF;
    pub const _RDB_TIMESTAMP_MS_FLAG: u8 = 0xFC;
    pub const _RDB_TIMESTAMP_S_FLAG: u8 = 0xFD;
    pub const _EMPTY_RDB_FILE_: &str= "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    pub const _EVENT_DB_UPDATED_: &str = "DB_UPDATED";
    pub const _EVENT_DB_UPDATED_LIST_: &str = "DB_UPDATED_LIST";
    pub const _ERROR_STREAM_GEQ_ID_EXISTS_: &str = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
    pub const _ERROR_STREAM_NULL_ID_: &str = "ERR The ID specified in XADD must be greater than 0-0";
    pub const _ERROR_INCR_NOT_AN_INT_: &str = "ERR value is not an integer or out of range";
    pub const _ERROR_EXEC_WITHOUT_MULTI_: &str = "ERR EXEC without MULTI";
    pub const _ERROR_DISCARD_WITHOUT_MULTI_: &str = "ERR DISCARD without MULTI";
    pub const _RESP_EMPTY_STRING_: &str = "$0\r\n\r\n"; // different from resp nil string which is generated by bulk_encode when you pass it an empty string
    pub const _ERROR_OUT_OF_RANGE_GEOCOORDS_ : &str = "ERR invalid longitude,latitude pair"; 
    // pub const _ERROR_SUB_MODE_ON_: &str = "ERR Can't execute 'set': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context";

    pub const _SUB_MODE_CMDS_: [&str; 6] = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"];

    // print bytes as string
    pub fn pbas(buf: &Vec<u8>) {
        println!("{}", buf.iter().map(|ch| {*ch as char}).collect::<String>());
    }

    pub fn _error_sub_mode_on_msg_(cmd: &str) -> String {
        format!("ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", cmd.to_uppercase())
    }

    // pub fn geocoords_sanity_check()

    pub fn cmd_sanity_check(cmd_name: &str, client_mode: bool) -> bool {
        return !client_mode || _SUB_MODE_CMDS_.iter().any(|&mode| mode == cmd_name.to_uppercase());
    }

    pub fn encode_array(vals: &Vec<String>, raw: bool) -> String {
        // null array is not same as empty array, for now its better to hardcode null array wherever it is is the required response 
        // if vals.is_empty() {
        //     return format!("*-1\r\n");
        // }
         
        let mut output = format!("*{}\r\n", vals.len());
        for v in vals {
            // if v == "" {
            //     continue;
            // }
            if raw {    // if arguments arent already encoded, encode them as string
                output += &encode_bulk(v);
            } else {
                output += v;
            }
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

        // println!("string parsed: {}", result);
        return (i, result); 
    }

    pub fn array_append(array: &str, new_val: &str) -> String {
        let mut new_array = String::from(array);

        new_array.push_str(new_val);

        new_array
    }

    pub fn redis_err(msg: &str) -> String {
        format!("-{}\r\n", msg)
    } 

    pub fn encode_simple(vals: &Vec<&str>) -> String {
        let mut result = vec![];
        
        for &v in vals {
            result.push(v);
        }

        let mut s = result.join(" ");
        s.push_str("\r\n");
        s = "+".to_owned() + &s;

        s
    }

    // returns +OK\r\n
    pub fn response_ok() -> String {
        String::from("+OK\r\n")
    }

    pub async fn connect_to_master(addr: &str, socket: &str) -> TcpStream {
        println!("slave connecting to master on:{}", socket);
        TcpStream::connect(format!("{}:{}", addr, socket)).await.unwrap() 
    } 
}