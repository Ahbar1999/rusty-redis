use ::codecrafters_redis::redis_cli;
use codecrafters_redis::utils::utils::{geo_decode, geo_encode};
use std::fs::File;
use std::net::TcpStream;
use std::io::{BufReader, Read, Write};
use std::process::exit;
use std::thread;
use serde::Deserialize;

const SERVER_PORT: &str = "6380";

#[derive(Debug, Deserialize)]
pub struct Test {
    pub seq_no: usize, 
    pub input: String,
    pub output: String,
    pub about: String,
}


#[test]
fn test_ping() {
    // define args of this test
    let args = vec!["redis-cli".to_owned(), "--port".to_owned(), SERVER_PORT.to_owned()];
    // start the server 
    let _ = thread::spawn(|| { redis_cli(args.into_iter()); });
    // connect to server 
    let mut stream = TcpStream::connect("localhost:6380").unwrap();

    if stream.write_all("*1\r\n$4\r\nPING\r\n".as_bytes()).is_err() {
        println!("couldn't PING the server!, exiting.");
        assert!(false);
        exit(1);
    }

    let mut buffer = vec![0; 1024]; 
    let bytes_read = stream.read(&mut buffer).unwrap(); 
   
    assert!(&buffer[0..bytes_read] == "+PONG\r\n".as_bytes()); 
}

#[test]
fn test_acl_auth() {
    let args = vec!["redis-cli".to_owned(), "--port".to_owned(), SERVER_PORT.to_owned()]; 
    let _ = thread::spawn(|| { redis_cli(args.into_iter()); });
    let mut stream = TcpStream::connect("localhost:6380").unwrap();
    let mut buffer = vec![0; 1024]; 

    let mut test_file_path = std::env::current_dir().unwrap();
    test_file_path.push("tests/config/redis-acl-tests_sequential.csv");
    // println!("{}", test_file_path.to_string_lossy());
    let tests_file = File::open(test_file_path).unwrap();

    let mut rdr = csv::Reader::from_reader(BufReader::new(tests_file));

    for result in rdr.deserialize() {
        buffer.clear(); 
        let test: Test = result.unwrap();
        println!("{:?}", test);
        if stream.write_all(test.input.as_bytes()).is_err() {
            println!("failed to transmit input for test: {:?}", test);
            exit(1);
        }  

        let bytes_read = stream.read(&mut buffer).unwrap(); 
    
       assert!(&buffer[0..bytes_read] == test.output.as_bytes());
    }  
}



#[test]
fn test_geo_decoding() {
    struct TestCase {
        name: &'static str,
        expected_latitude: f64,
        expected_longitude: f64,
        score: u64,
    }

    let test_cases = vec![
        TestCase { name: "Bangkok", expected_latitude: 13.722000686932997, expected_longitude: 100.52520006895065, score: 3962257306574459 },
        TestCase { name: "Beijing", expected_latitude: 39.9075003315814, expected_longitude: 116.39719873666763, score: 4069885364908765 },
        TestCase { name: "Berlin", expected_latitude: 52.52439934649943, expected_longitude: 13.410500586032867, score: 3673983964876493 },
        TestCase { name: "Copenhagen", expected_latitude: 55.67589927498264, expected_longitude: 12.56549745798111, score: 3685973395504349 },
        TestCase { name: "New Delhi", expected_latitude: 28.666698899347338, expected_longitude: 77.21670180559158, score: 3631527070936756 },
        TestCase { name: "Kathmandu", expected_latitude: 27.701700137333084, expected_longitude: 85.3205993771553, score: 3639507404773204 },
        TestCase { name: "London", expected_latitude: 51.50740077990134, expected_longitude: -0.12779921293258667, score: 2163557714755072 },
        TestCase { name: "New York", expected_latitude: 40.712798986951505, expected_longitude: -74.00600105524063, score: 1791873974549446 },
        TestCase { name: "Paris", expected_latitude: 48.85340071224621, expected_longitude: 2.348802387714386, score: 3663832752681684 },
        TestCase { name: "Sydney", expected_latitude: -33.86880091934156, expected_longitude: 151.2092998623848, score: 3252046221964352 },
        TestCase { name: "Tokyo", expected_latitude: 35.68950126697936, expected_longitude: 139.691701233387, score: 4171231230197045 },
        TestCase { name: "Vienna", expected_latitude: 48.20640046271915, expected_longitude: 16.370699107646942, score: 3673109836391743 },
    ];

    for test_case in test_cases {
        let result = geo_decode(test_case.score);
        
        // Check if decoded coordinates are close to original (within 10e-6 precision)
        let lat_diff = (result.latitude - test_case.expected_latitude).abs();
        let lon_diff = (result.longitude - test_case.expected_longitude).abs();
        
        let success = lat_diff < 0.000001 && lon_diff < 0.000001;
        let status = if success { "✅" } else { "❌" };
        println!("{}: (lat={:.15}, lon={:.15}) ({})", test_case.name, result.latitude, result.longitude, status);
        
        if !success {
            println!("  Expected: lat={:.15}, lon={:.15}", test_case.expected_latitude, test_case.expected_longitude);
            println!("  Diff: lat={:.6}, lon={:.6}", lat_diff, lon_diff);
        }
    }
}

#[test]
fn test_geo_encoding() {
    struct TestCase {
        name: &'static str,
        latitude: f64,
        longitude: f64,
        expected_score: u64,
    }

    let test_cases = vec![
        TestCase { name: "Bangkok", latitude: 13.7220, longitude: 100.5252, expected_score: 3962257306574459 },
        TestCase { name: "Beijing", latitude: 39.9075, longitude: 116.3972, expected_score: 4069885364908765 },
        TestCase { name: "Berlin", latitude: 52.5244, longitude: 13.4105, expected_score: 3673983964876493 },
        TestCase { name: "Copenhagen", latitude: 55.6759, longitude: 12.5655, expected_score: 3685973395504349 },
        TestCase { name: "New Delhi", latitude: 28.6667, longitude: 77.2167, expected_score: 3631527070936756 },
        TestCase { name: "Kathmandu", latitude: 27.7017, longitude: 85.3206, expected_score: 3639507404773204 },
        TestCase { name: "London", latitude: 51.5074, longitude: -0.1278, expected_score: 2163557714755072 },
        TestCase { name: "New York", latitude: 40.7128, longitude: -74.0060, expected_score: 1791873974549446 },
        TestCase { name: "Paris", latitude: 48.8534, longitude: 2.3488, expected_score: 3663832752681684 },
        TestCase { name: "Sydney", latitude: -33.8688, longitude: 151.2093, expected_score: 3252046221964352 },
        TestCase { name: "Tokyo", latitude: 35.6895, longitude: 139.6917, expected_score: 4171231230197045 },
        TestCase { name: "Vienna", latitude: 48.2064, longitude: 16.3707, expected_score: 3673109836391743 },
    ];

    for test_case in test_cases {
        let actual_score = geo_encode(test_case.latitude, test_case.longitude);
        let success = actual_score == test_case.expected_score;
        let status = if success { "✅" } else { "❌" };
        println!("{}: {} ({})", test_case.name, actual_score, status);
    }
}