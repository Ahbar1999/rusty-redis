use ::codecrafters_redis::redis_cli;

#[test]
fn test_ping() {
    // define args of this test
    let args = vec!["redis-cli".to_owned(), "--port".to_owned(), "6380".to_owned()];
    redis_cli(args.into_iter());
}

