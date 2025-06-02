# Demand-cli
[![Stars](https://img.shields.io/github/stars/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)
[![Forks](https://img.shields.io/github/forks/demand-open-source/demand-cli?style=social)](https://github.com/demand-open-source/demand-cli)
![Release](https://img.shields.io/github/v/release/demand-open-source/demand-cli)

**Demand CLI** is a proxy that let miners to connect to and mine with [Demand Pool](https://dmnd.work). It serves three primary purposes: 
  1. Translation: Enables miners using StratumV1 to connect to the Demand Pool without requiring firmware updates. Sv1 messages gets translated to Sv2.
  2. Job Declaration (JD): Allows miners to declare custom jobs to the pool using StratumV2.
  3. Solo Mining: Skip pool entirely and mine solo


## Features
- **Stratum V2 Support**: Uses the secure and efficient Stratum V2 protocol for communication with the pool.
- **Job Declaration**: Enables miners to propose custom block templates to the pool. This helps make mining more decentralized by allowing miners to pick the transactions they want to include in a block.
- **Stratum V1 Translation**: Acts as a bridge, allowing StratumV1 miners to connect to the Demand Pool without firmware updates.
- **Solo Mining Mode**: Allows miners to mine independently without a pool. 
- **Flexible Configuration**: Provides options for customization. This allows users to optimize the tool for their specific mining environment.
- **Monitoring API**: Provides HTTP endpoints to monitor proxy health, pool connectivity, miner performace, and system resource usage in real-time.

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (version 1.70.0 or higher recommended)
- Cargo (included with Rust)
- [Bitcoin Node](https://github.com/Sjors/bitcoin) (Optional): Required for job declaration.

### Installation

There are two options to get started:

#### Option 1: Download Pre-built Binaries

  - Visit the [releases page](https://github.com/demand-open-source/demand-cli/releases/tag/v0.1.1).
  - Download the binary for your system.

#### Option 2: Build from Source

- Clone the repository and build the project:
    ```bash
    git clone https://github.com/demand-open-source/demand-cli.git
    cd demand-cli
    cargo build --release
    ```

The executable will be located in the `target/release/` directory.

## Configuration

Before running the CLI, set up the necessary environment variables.

### Environment Variables

- **`TOKEN` (Required)**: Your Demand Pool authentication token.  
  Export it with:

  ```bash
  export TOKEN=<your_token>
  ```

- **`TP_ADDRESS` (Optional)**: Template Provider address (default: `127.0.0.1:8442`). Set this if you want to use job declaration feature.

  Export it with
  ```bash
  export TP_ADDRESS=<node_ip:port>
  ```
Note: if `TP_ADDRESS` id not set, job declaration is disabled, and the proxy uses  templates provided by the pool.

### Config File (for Solo Mining)
Create config.toml with payout address, network and withhold.
  Example config file:
  ```toml
  payout_address = "bc1qn2ckg6c2329e3g7w8sqlwqrg7f0hgcrv2fp2hv"
  withhold = false
  network = "bitcoin"
  ```
  - payout_address: Bitcoin address for coinbase output (where your reward will be sent)
  - withhold:  specifies whether or not to withhold mining rewards (default: false). 
  - network <NETWORK>: Bitcoin network (bitcoin, testnet, regtest, signet; default: bitcoin)

  These can also be passed as CLI args. Supported script types include P2PKH, P2SH, P2WPKH, P2WSH,  and P2TR
  
## Running the CLI

Depending on whether you built from source or downloaded a binary, the command to run the proxy is slightly different. There are also different options you can use. 
Below are 3 example setups to get you started.

#### Example 1: Built from Source with Job Declaration

This example assumes you’ve built `demand-cli` from source and want to enable job declaration.

   ```bash
   export TOKEN=abc123xyz
   export TP_ADDRESS=127.0.0.1:8442
   ./target/release/demand-cli -d 100T --loglevel debug --nc on
  ```

Connect Miners:
Point your Stratum V1 miners  to <your_proxy_ip>:32767.

#### Example 2: Pre Binary without Job Declaration
This example assumes you downloaded a pre-built binary (for linux in this case) for from release page and wants to connect test endpoint
Set Environment Variable:
  ```bash
  export TOKEN=xyz789abc
  ./demand-cli-linux-x64 -d 10T --test
  ```

Point your Stratum V1 miners  to <your_proxy_ip>:32767.

#### Example 3: Solo Mining (No Pool, No TOKEN)
```bash
 export TP_ADDRESS=127.0.0.1:8442
./demand-cli-linux-x64 --solo --payout_address bc1qn2ckg6c2329e3g7w8sqlwqrg7f0hgcrv2fp2hv --network bitcoin
```
This runs in solo mode, you can specify the payout_address and network in `toml` file  as pass its path with `-c path/to/config.toml`. if `config.toml` is the directory as your binary `-c` is not required. Ensure Bitcoin node is running.

Point your Stratum V1 miners  to <your_proxy_ip>:32767

### Options

  - **`--test`**: Connects to test endpoint
  - **`-d`**: Expected downstream hashrate (e.g., `10T`, `2.5P`, `5E`). Default is `100TH/s` (100 Terahashes/second).  
    This helps the pool adjust to your hashrate.
  - **`--loglevel`**: Logging verbosity (`info`, `debug`, `error`, `warn`). Default is `info`.
  - **`--nc`**: Noise connection logging verbosity (`info`, `debug`, `error`, `warn`). Default is `off`.
  - **`--solo`**: Enable solo mining mode. Requires `TP_ADDRESS` and a config file.
  - **`-c, --config`**: Path to your solo mining config.toml. Default is config.toml


## Monitoring API:

  The proxy exposes REST API enspoints to monitor its health, pool connectivity, connected mining devices performance and system resource usage. All endpoints are served on `http://0.0.0.0:3001` by default and return JSON responses in the format:

  ```
  {
    "success": boolean,
    "message": string | null,
    "data": object | string | null
  }
  ```

  ### Endpoint Overview


  | Endpoint             | Method | Description                                                                 | Response Status Codes |
  |----------------------|--------|-----------------------------------------------------------------------------|-----------------------|
  | `/api/health`            | GET    | Checks the health status of the proxy.                                       | 200, 503              |
  | `/api/pool/info`         | GET    | Retrieves the current pool address and latency.                              | 200, 404                   |
  | `/api/stats/miners`      | GET    | Returns device_name, hashrate, accepted and rejected shares count, and current_difficulty for all connected downstream devices (empty if none). | 200, 500              |
  | `/api/stats/aggregate`   | GET    | Provides aggregated stats of all connected downstream devices.                     | 200, 500              |
  | `/api/stats/system`      | GET    | Returns system resource usage (CPU and memory) for the proxy process.        | 200                   |


### Endpoint Details

<details>
<summary><strong>GET /api/health</strong> - Retrieves the health of the proxy</summary>

- **Responses**:
  - **200 OK** (Healthy):
    ```json
    { "success": true, "message": null, "data": "Proxy OK" }
    ```
  - **503 Service Unavailable** (Unhealthy):
    ```json
    { "success": false, "message": "<reason>", "data": null }
    ```

</details>

<details>
<summary><strong>GET /api/pool/info</strong> - Retrieves the current pool’s address and latency in milliseconds or null if not currently connected to pool</summary>

- **Responses**:
  - **200 OK** (Connected to Pool):
    ```json
    {
      "success": true,
      "data": {
        "address": "<pool_address>",
        "latency": "5072"
      }
    }
    ```
  - **200 OK** (Solo mining):
    ```json
    {
      "success": true,
      "data": {
        "pool": "Not connected to pool. Mining solo :)",
      }
    }
    ```
  - **400 NOT_FOUND** (Not connected to Pool):
    ```json
    { "success": true, "data": { "address": null, "latency": null } }
    ```

</details>

<details>
<summary><strong>GET /api/stats/miners</strong> - Shows stats for connected devices, empty if none.</summary>

- **Responses**:
  - **200 OK** (Miing devices connected):
    ```json
    {
      "success":true,
      "message":null,
      "data":{
        "3":{
          "device_name":"cpuminer/2.5.1",
          "hashrate":4721719.5,
          "accepted_shares":273,
          "rejected_shares":22,
          "current_difficulty":0.0065961657
          },
          "2":{
            "device_name":"cpuminer/2.5.1",
            "hashrate":6464463.0,
            "accepted_shares":413,
            "rejected_shares":64,
            "current_difficulty":0.00903075
            }
          }
      }
    ```
  - **200 OK** (No connected devices):
    ```json
    { "success": true, "message": null,"data": {} }
    ```
  - **500 Internal Server Error**:
    ```json
    { "success": false, "message": "Failed to collect stats: <error>", "data": null }
    ```

</details>

<details>
<summary><strong>GET /api/stats/aggregate</strong> - Aggregates metrics across all devices</summary>

- **Responses**:
  - **200 OK**:
    ```json
    {
      "success":true,
      "message":null,
      "data":{
        "total_connected_device":2,
        "aggregate_hashrate":11186182.5,
        "aggregate_accepted_shares":686,
        "aggregate_rejected_shares":86,
        "aggregate_diff":0.01562692
        }
    }
    ```
  - **500 Internal Server Error**:
    ```json
    { "success": false, "message": "Failed to collect stats: <error>", "data": null }
    ```

</details>

<details>
<summary><strong>GET /api/stats/system</strong> - Reports CPU and memory usage (0 - 100%).</summary>

- **Responses**:
  - **200 OK**:
    ```json
    {"success":true,"message":null,"data":{"cpu_usage_%":"0.565","memory_usage_bytes":25935872}}
    ```

</details>


## Contributing

Contributions are welcome. Please refer to our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Support

Report issues via [GitHub Issues](https://github.com/demand-open-source/demand-cli/issues).
