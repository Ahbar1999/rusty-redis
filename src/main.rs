use ::codecrafters_redis::redis_cli;

fn main() {
    redis_cli(std::env::args());
}