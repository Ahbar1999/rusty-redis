# RedisClone-RS

A high-performance Redis clone implemented in Rust, leveraging Tokio for asynchronous event handling and concurrent client connections.

## 🚀 Features

- **Asynchronous I/O** using Tokio for efficient, concurrent connections
- **Redis Protocol Compatible** (RESP)
- **Concurrent Connection Handling** with async/await
- **Memory-Safe** and thread-safe, thanks to Rust
- **High Performance** via zero-cost abstractions and efficient async I/O
- **Extensible Architecture** for easy feature additions

## 📋 Supported Commands

- `GET`
- `SET`
- `DEL`
- `PING`
- `ECHO`

## 🛠️ Installation

- Requires Rust (latest stable recommended) and Cargo
- Clone the repository and build with Cargo

## 🔧 Configuration

- Uses Tokio, anyhow, log, and env_logger as dependencies
- Configurable host, port, and logging level via environment variables

## 🚦 Usage

- Start the server with Cargo
- Connect using `redis-cli` or any Redis-compatible client
- Compatible with standard Redis client libraries in various languages

## 🏗️ Architecture

- **Async Event Loop:** Uses Tokio's runtime for handling asynchronous operations
- **Concurrent Connection Handling:** Each client connection runs in its own async task
- **Memory Management:** Key-value data is stored in a thread-safe, shared structure

## 🧪 Testing

- Includes unit and integration tests
- Can be benchmarked using tools like `redis-benchmark`

## 🔮 Roadmap

- Persistence (RDB/AOF)
- Pub/Sub messaging
- Lua scripting support
- Cluster mode
- Additional data types (Lists, Sets, Sorted Sets)
- Memory optimization
- Metrics and monitoring

## 🤝 Contributing

- Contributions are welcome via Pull Requests
- Please open an issue for major changes before submitting a PR

## 📝 License

MIT License

## 🙏 Acknowledgments

- Inspired by the Tokio tutorial and mini-redis implementation
- Built with the Tokio async runtime
- Thanks to the Rust community

## ⚠️ Disclaimer

This is a learning project and not intended for production use. For production, use the official Redis server or other stable alternatives.