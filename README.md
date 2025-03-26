# demand-cli

A Rust-based CLI tool for connecting to the first FULLY StratumV2 Bitcoin Mining Pool! This tool implements a translation proxy that enables both StratumV1 and StratumV2 miners to connect to StratumV2 pools.

[![Stars](https://img.shields.io/github/stars/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)
[![Forks](https://img.shields.io/github/forks/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)

## Features

- **Full StratumV2 Support**: Implements the next-generation mining protocol with improved security, efficiency, and features.
- **Translation Proxy**: Enables StratumV1 miners to connect to StratumV2 pools without firmware updates.
- **Enhanced Security**: Implements NOISE protocol authentication and encryption to prevent man-in-the-middle attacks.
- **Optimized Performance**: Reduces bandwidth usage and improves job distribution latency.
- **Flexible Configuration**: Supports both test and production mining pools.
- **Automatic Difficulty Adjustment**: Implements PID controller for optimal share submission rates.
- **Efficient Job Distribution**: Separates prevhash and future job distribution for faster mining transitions.

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (Minimum version 1.70.0 recommended)
- Cargo (Rust's package manager, included with Rust installation)

### Installation

Clone the repository and build the project:

```bash
git clone https://github.com/demand-open-source/demand-cli.git
cd demand-cli
cargo build --release
```

The executable will be created in the `target/release/` directory.

### Running the CLI

To run the CLI in test mode:

```bash
export TOKEN=oFzg1EUmceEcDuvzT3qt
./target/release/demand-cli --test -d 50T
```

Options:

- `--test`: Connects to the test endpoint.
- `--d`: Specifies the expected downstream hashrate (e.g., `10T`, `2.5P`, `5E`). Defaults to `100TH/s` if not provided.

The proxy listens for connections on `0.0.0.0:32767`, allowing StratumV1 miners to connect.

### Environment Variables

- `TOKEN` (Required): Your mining pool authentication token.

  - For testing, use this token: `oFzg1EUmceEcDuvzT3qt`.
  - Set the token using:

  ```bash
  export TOKEN=<your_token>
  ```

- `SV1_DOWN_LISTEN_ADDR` (Optional): Address for StratumV1 downstream connections (default: `0.0.0.0:32767`).
- `TP_ADDRESS` (Optional): Template Provider address for job declaration.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or fix.
3. Commit your changes with clear messages.
4. Submit a pull request.

## Issues

Report issues via the [GitHub issue tracker](https://github.com/demand-open-source/demand-cli/issues).
