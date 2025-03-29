# demand-cli

A Rust-based CLI proxy for connecting miners to the Demand Pool using the next-generation StratumV2 protocol. This tool enables StratumV1 miners to connect seamlessly to the Demand Pool without requiring firmware updates.

[![Stars](https://img.shields.io/github/stars/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)
[![Forks](https://img.shields.io/github/forks/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)

## Features

- **Full StratumV2 Support**: Leverages the modern mining protocol for secure and efficient communication.
- **Translation Proxy**: Allows StratumV1 miners to connect to the Demand Pool without firmware updates.
- **Flexible Configuration**: Configure downstream hashrate (`-d`) and log level (`--loglevel`) as per your mining setup.
- **Job Declaration**: Implements efficient mining job distribution using the StratumV2 job declaration protocol, reducing latency between block templates and miner notifications to minimize orphan blocks and maximize revenue.

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (version 1.70.0 or higher recommended)
- Cargo (included with Rust installation)

### Installation

Clone the repository and build the project:

```bash
git clone https://github.com/demand-open-source/demand-cli.git
cd demand-cli
cargo build --release
```

The executable will be located in the `target/release/` directory.

## Configuration

Before running the CLI, configure the necessary environment variables and CLI arguments.

### Environment Variables

- **`TOKEN` (Required)**: Your Demand Pool authentication token.  
  [Obtain this from your Demand Pool account](https://curse-stoat-b4b.notion.site/How-to-Start-Mining-with-Demand-19844f4a23ef80e8a775cc03c85cb575) and export:

  ```bash
  export TOKEN=<your_token>
  ```

- **`TP_ADDRESS` (Optional)**: Template Provider address (default: pool-provided endpoint).

### CLI Arguments

- **`-d`**: Expected downstream hashrate (e.g., `10T`, `2.5P`, `5E`). Default: `100TH/s` (100 Terahashes/second).  
  This helps optimize pool-side performance for your hashrate level.
- **`--loglevel`**: Logging verbosity (`info`, `debug`, `error`, `warn`). Default: `info`.

## Running the CLI

After configuration, run the proxy as follows:

```bash
./target/release/demand-cli -d 50T --loglevel info
```

The proxy listens for connections on `0.0.0.0:32767`, allowing StratumV1 miners to connect to the Demand Pool.

## Contributing

Contributions are welcome! Please refer to our [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## Support

Report issues via [GitHub Issues](https://github.com/demand-open-source/demand-cli/issues).  
For additional documentation, visit our [Mining Guide](https://curse-stoat-b4b.notion.site/How-to-Start-Mining-with-Demand-19844f4a23ef80e8a775cc03c85cb575).
