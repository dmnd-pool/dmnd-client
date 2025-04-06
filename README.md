# demand-cli

A Rust-based CLI proxy that connects miners to the Demand Pool using the StratumV2 protocol. This tool lets StratumV1 miners connect to the Demand Pool without requiring firmware updates.

[![Stars](https://img.shields.io/github/stars/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)
[![Forks](https://img.shields.io/github/forks/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)

## Features

- **Full StratumV2 Support**: Uses the modern mining protocol for secure communication.
- **Job Declaration**: Lets the pool accept custom jobs from miners using StratumV2. This helps make mining more decentralized by allowing miners to pick the transactions they want to include in a block.
- **Translation Proxy**: Allows StratumV1 miners to connect to the Demand Pool without firmware updates.
- **Flexible Configuration**: Set your downstream hashrate (`-d`) and log level (`--loglevel`) to match your mining setup.

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (version 1.70.0 or higher recommended)
- Cargo (included with Rust)

### Installation

There are two options to get started:

#### Option 1: Download Pre-built Binaries

1. Visit the [releases page](https://github.com/demand-open-source/demand-cli/releases/tag/v0.1.1).
2. Download the binary for your system.
3. Extract and run the binary directly.

#### Option 2: Build from Source

Clone the repository and build the project:

```bash
git clone https://github.com/demand-open-source/demand-cli.git
cd demand-cli
cargo build --release
```

The executable will be located in the `target/release/` directory.

## Configuration

Before running the CLI, set up the necessary environment variables and CLI arguments.

### Environment Variables

- **`TOKEN` (Required)**: Your Demand Pool authentication token.  
  Export it with:

  ```bash
  export TOKEN=<your_token>
  ```

- **`TP_ADDRESS` (Optional)**: Template Provider address (default: pool-provided endpoint).

### CLI Arguments

- **`-d`**: Expected downstream hashrate (e.g., `10T`, `2.5P`, `5E`). Default is `100TH/s` (100 Terahashes/second).  
  This helps the pool adjust to your hashrate.
- **`--loglevel`**: Logging verbosity (`info`, `debug`, `error`, `warn`). Default is `info`.

## Running the CLI

After configuration, run the proxy with:

```bash
./target/release/demand-cli -d 50T --loglevel info
```

The proxy listens for connections on `0.0.0.0:32767`, allowing StratumV1 miners to connect to the Demand Pool.

## Contributing

Contributions are welcome. Please refer to our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Support

Report issues via [GitHub Issues](https://github.com/demand-open-source/demand-cli/issues).
