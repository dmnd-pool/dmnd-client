# DMND Stratum V2 Client — Getting Started Guide

For local mining integration tests, use the [Docker runner](tests/README.md), which
includes the TP simulator and cpuminer binaries.

## 1. Introduction

This guide walks you through setting up the DMND Stratum V2 Client and connecting it to the DMND pool. When you're done, you will have a fully functional Stratum V2 mining setup with **Job Declaration** — meaning *you* build your own block templates from your own Bitcoin node, instead of the pool deciding which transactions you mine.

**Your ASICs do not need to support Stratum V2.** The DMND Client accepts standard Stratum V1 connections from your miners and handles the SV2 protocol to the pool. Any stock-firmware miner works.

### How the pieces fit together

```
┌─────────┐  SV1 (stratum+tcp)  ┌──────────────┐   SV2 + Job Declaration   ┌───────────┐
│  ASICs  │ ──────────────────► │ DMND Client  │ ────────────────────────► │ DMND Pool │
└─────────┘       :32767        └──────┬───────┘          :20000           └───────────┘
                                       │ templates
                                ┌──────┴───────┐    IPC     ┌──────────────┐
                                │    sv2-tp    │ ◄────────► │ Bitcoin Core │
                                │ (Template    │  (unix     │  (your node) │
                                │  Provider)   │   socket)  └──────────────┘
                                └──────────────┘
                                     :8336
```

### Default ports

| Component | Port | Direction | Notes |
|---|---|---|---|
| sv2-tp (Template Provider) | 8336 | local only | DMND Client connects to it |
| DMND Client (stratum) | 32767 | LAN | Point your miners here |
| DMND Client (tx API) | 3001 | local only | Optional — see Section 7. **Do not expose publicly** |
| DMND Pool | 20000 | outbound | Client → pool |

Your firewall only needs to allow **outbound** connections to the pool, and LAN access from your miners to port 32767. Nothing here requires inbound internet access.

### System requirements

- Release binaries are published for multiple platforms and architectures — grab the one matching your system from the releases page, or build from source (see [Note B](#notes))
- A machine capable of running a Bitcoin node — a **pruned node works fine**. 4 GB+ RAM recommended. See [Note A](#notes)
- Bitcoin Core **v30 or later** with multiprocess/IPC support

## 2. What You Need Before Starting

To mine with the DMND pool you first need a **DMND token**. Complete the registration form at https://join.dmnd.work and wait for our confirmation email — it contains your token (an alphanumeric string you'll use in Section 4). If you don't see the email, check your spam folder before contacting support.

## 3. Enable Job Declaration Support

Job Declaration is the key Stratum V2 feature that lets miners build their own block templates, improving decentralization, censorship resistance, and latency. To use it, you run two components on your own infrastructure:

1. **Bitcoin Core** (v30+) with IPC enabled — your own node.
2. **Stratum V2 Template Provider (`sv2-tp`)** — a separate binary that connects to Bitcoin Core via IPC and serves block templates to the DMND Client.

Follow the setup instructions in the sv2-tp README — it covers both running Bitcoin Core with IPC enabled and running the Template Provider, and is always up to date:

**https://github.com/stratum-mining/sv2-tp#readme**

The Template Provider listens on port **8336** by default — you'll need that in the next section.

**✅ Verify:** the sv2-tp log should show a successful IPC connection to Bitcoin Core and new templates being generated as blocks arrive.

### Dashboard

The client serves a local dashboard at `http://127.0.0.1:3001` (set the port with `--api-server-port`). It shows pool and miner status and the block templates sv2-tp is offering, and lets you ask bitcoind to prioritise a transaction. It opens in a browser on start; set `HEADFUL=false` to stop that.

Use the sidebar to open the **mempool.space accelerations** or **RSK merge mining** page. Both need the client's `API_TX_TOKEN`.

- **mempool.space accelerations:** This page shows the accelerated transactions the client has fetched from mempool.space.
- **RSK merge mining:** This page shows whether merge mining is running and how many RSK candidates your miners have found. A candidate is a proof that meets the RSK target; the bridge collects them and sends them to RSK.

The RSK page refreshes every five seconds, the accelerations page every thirty. Neither refreshes while you are looking at another page or another tab.

## 4. Run the DMND Client

### 4.1 Download the DMND Stratum V2 Client

Download the release binary for your platform from:

https://github.com/dmnd-pool/dmnd-client/releases

#### Signatures and verification

Each release includes `sha256-checksums.txt`, which you can use to confirm that your downloaded binary is byte-for-byte identical to the published release artifact.

Each binary also has a detached Minisign signature that verifies it as an authentic DMND release. Download the binary and its `.minisig` file, then verify the signature using [`release-signing.pub`](release-signing.pub), replacing the filename as needed:

```bash
minisign -Vm dmnd-client-linux -x dmnd-client-linux.minisig -p release-signing.pub
```

Automatic updates perform signature verification before installing a new binary.


Make the binary executable and run it:

```
chmod +x dmnd-client
TOKEN=<DMND-token> ./dmnd-client -l info -d <hashrate> --tp-address="127.0.0.1:<port>"
```

Where:

- `<DMND-token>` — the token you received by email during registration (Section 2).
- `<port>` — the Template Provider port (default **8336**).
- `<hashrate>` — the hashrate of the **least powerful machine** that will connect to this client. As a rule of thumb: use `250T` if your miners connect directly, or `20P` if you connect aggregator proxies. This only seeds the starting difficulty; the dynamic difficulty adjustment algorithm handles the rest.

Example (miners connecting directly):

```
TOKEN=abc123 ./dmnd-client -l info -d 250T --tp-address="127.0.0.1:8336"
```

**✅ Verify:** the client log should show a successful connection to the Template Provider and to the DMND pool, and templates being declared.

> **Building from source instead?** See [Note B](#notes).

### Configuration precedence

Every setting in this guide can be provided three ways, with the following precedence (highest wins):

1. CLI flags (e.g. `--api-base-url`)
2. `config.toml`
3. Environment variables (e.g. `API_BASE_URL`)

### 4.2 Endpoint configuration (optional)

By default, the client discovers pool addresses from the dashboard API and sends worker telemetry to the same API. You only need this section if you run behind a private gateway, proxy, or custom deployment.

- **Dashboard API base URL:** `--api-base-url` / `api_base_url` / `API_BASE_URL` / `DMND_CLIENT_API_BASE_URL`. Provide the base URL only; the client appends `/api/pool/urls` (pool discovery) and `/api/worker/entry` (telemetry).
- **Direct pool addresses:** `--pool-address` (repeatable) / `pool_addresses` / `POOL_ADDRESSES` / `POOL_ADDRESS` / `DMND_CLIENT_POOL_ADDRESSES`. When set, the client skips dashboard pool discovery and connects directly.

Example `config.toml`:

```toml
api_base_url = "https://api.example.com"
pool_addresses = ["pool-a.example.com:20000", "pool-b.example.com:20000"]
```

Example environment variables:

```
TOKEN=<DMND-token> \
API_BASE_URL=https://api.example.com \
POOL_ADDRESSES=pool-a.example.com:20000,pool-b.example.com:20000 \
./dmnd-client -l info -d 250T --tp-address="127.0.0.1:8336"
```

### Miner coinbase tag

The coinbase tag defaults to `/DMND/<miner_name>/`. Set `--miner-name` (or `miner_name` in
`config.toml`, or `MINER_NAME`) to provide the name. The prefix defaults to `/DMND/` and can be
overridden with `--tag-prefix`, `tag_prefix` in `config.toml`, or `TAG_PREFIX`. For example,
`--tag-prefix "/POOL/" --miner-name alice` produces `/POOL/alice/`.

### 4.3 RSK merge mining

See [MERGE_MINING.md](MERGE_MINING.md) for the proxy design, safety model, complete bridge HTTP
contract, RskJ proof format, byte-order rules, and a bridge conformance checklist.

RSK merge mining requires Job Declaration mode (`--tp-address`), a Template Provider that honors
the advertised coinbase-output allowance, RskJ, and the `demand-rsk-op-return-bridge` service from
the adjacent DEMAND repository. The client reserves 100 additional serialized coinbase-output
bytes when it advertises capacity to the Template Provider. If that reservation or an RSK
operation fails, the original Bitcoin template and Bitcoin share paths remain authoritative.

Before an RSK proof can be queued, the client also enforces RskJ's raw-coinbase rule: the desired
payload must start at the last raw `RSKBLOCK:` marker and no more than 128 bytes may follow its hash
in the witness-stripped coinbase. It validates the prospective outputs plus locktime before
publishing the job, including markers that could be assembled across serialized field boundaries.

Configure a non-empty secret on the DMND client and run it with the Template Provider:

```sh
API_BIND_ADDRESS=127.0.0.1 \
API_SECRET=<shared-secret> \
TOKEN=<DMND-token> \
./dmnd-client -l info -d 250T --tp-address="127.0.0.1:8336"
```

RskJ's HTTP RPC configuration must enable both merge-mining work and the miner server:

```text
-Drpc.modules.mnr.enabled=true -Dminer.server.enabled=true
```

From `../demand/rust-backend`, run the bridge with the same secret:

```sh
DMND_CLIENT_API_SECRET=<shared-secret> \
DMND_CLIENT_OP_RETURN_URL=http://127.0.0.1:3001/api/coinbase/op-return \
DMND_CLIENT_FOUND_JOB_URL=http://127.0.0.1:3001/api/merge-mining/found-job \
RSK_RPC_URL=http://127.0.0.1:4444 \
cargo run -p demand-rsk-op-return-bridge
```

The companion bridge interoperates with this proxy but currently relies on the proxy and RskJ for
some proof-coherence validation, and its pending proof retry queue has no hard item cap. See the
production validation and bounded-state requirements in
[MERGE_MINING.md](MERGE_MINING.md#5-byte-order-and-proof-validation) when building or hardening a
bridge across a separate trust boundary.

The bridge posts each atomic RSK payload/target pair to the first endpoint and destructively polls
proof candidates from the second. The client keeps at most 32 proof candidates in memory and drops
the oldest candidate on overflow; no merge-mining queue operation blocks Bitcoin share handling.
Merge mining never lowers a miner's normal Bitcoin difficulty. The bounded MM observer evaluates
every authenticated, structurally valid share the miner submits before normal Bitcoin-difficulty
filtering. If the RSK target is easier than the miner's assigned target, however, the ASIC does not
report every RSK-only hash and those unreported hashes cannot be observed. This intentional
stability-first policy avoids increasing miner traffic or changing Bitcoin difficulty.

Run the bridge and DMND client as separate supervised processes. A bridge crash or restart must not
restart the DMND client. Whenever the DMND client process restarts, restart the bridge after the
client API is healthy so the bridge reposts the current RSK work. Pool or Job Declaration
reconnects inside the same DMND client process retain the desired pair. Synchronize the client and
bridge hosts to UTC with NTP or chrony because proof expiration uses the client's
`observed_at_unix_ts` value.

The merge-mining endpoints use a shared secret in request data and do not provide TLS. Keep the API
on a trusted network or place it behind an authenticated TLS reverse proxy and firewall; do not
expose port 3001 directly to the Internet.

## 5. Connect Your Miners

With Bitcoin Core, sv2-tp, and the DMND Client all running, point your ASICs at the machine running the DMND Client:

```
stratum+tcp://<dmnd-client-machine-ip>:32767
```

- **URL:** the LAN IP of the machine running the DMND Client, port **32767** (default).
- **Password:** your DMND token.
- **Username / worker name:** anything you like, or leave empty.

This is a standard Stratum V1 connection — no special firmware needed. The client handles all SV2 communication with the pool.

**✅ Verify:** miners should connect almost immediately. Expect the first shares to be accepted **within about 6 minutes** of the miner submitting its first share — this is normal, not a fault.

## 6. Track Hashrate and Earnings

Track hashrate and earnings on the DMND dashboard:

https://dashboard.dmnd.work

Log in with the credentials you used during registration.

> Hashrate statistics are averaged over time — the dashboard typically reflects your full hashrate well within the first hour after your miners connect. A low reading right after connecting is normal.

## 7. Prioritize Transactions (optional)

The DMND Client can expose an API endpoint that asks your Bitcoin Core node to prioritize a transaction for block template selection (via the `prioritisetransaction` RPC).

> **Priority ownership:** A transaction must be either manually prioritized through this API or accelerated by mempool.space, never both. Do not manually prioritize a txid while it is present in the mempool.space accelerator feed. The client assumes these sources are mutually exclusive; overlapping them can cause incorrect fee-delta restoration.

There is no global fee-delta setting. Manual fee deltas are supplied per API request, while mempool.space fee deltas come from the accelerator feed.

The feature is enabled **only when all** of the following are configured:

| Setting | CLI flag | config.toml | Env var | Description |
|---|---|---|---|---|
| RPC URL | `--rpc-url` | `rpc_url` | `RPC_URL` | Bitcoin Core RPC, e.g. `http://127.0.0.1:8332` |
| RPC user | `--rpc-user` | `rpc_user` | `RPC_USER` | Bitcoin Core RPC username |
| RPC password | `--rpc-pwd` | `rpc_pwd` | `RPC_PWD` | Bitcoin Core RPC password |
| API token | `--api-tx-token` | `api_tx_token` | `API_TX_TOKEN` | Bearer token required by this API |

Background transaction prioritization runs in production and local mode. It remains disabled in
staging and testnet3 mode.

> **Startup reset:** Whenever all four settings are configured, the client resets every non-zero fee delta currently reported by Bitcoin Core to zero at startup. This reset runs in every environment, including staging, testnet3, and local.

> **Security:**
> - The tx API (default port **3001**) should never be exposed to the public internet. Bind it to localhost or protect it behind your own gateway.
> - `config.toml` stores RPC credentials in plaintext — restrict file permissions (`chmod 600 config.toml`).
> - Treat `API_TX_TOKEN` like a password.

Example environment variables:

```
TOKEN=<DMND-token> \
RPC_URL=http://127.0.0.1:8332 \
RPC_USER=<bitcoin-rpc-user> \
RPC_PWD=<bitcoin-rpc-password> \
API_TX_TOKEN=<api-token> \
./dmnd-client -l info -d 250T --tp-address="127.0.0.1:8336"
```

Example `config.toml`:

```toml
rpc_url = "http://127.0.0.1:8332"
rpc_user = "<bitcoin-rpc-user>"
rpc_pwd = "<bitcoin-rpc-password>"
api_tx_token = "<api-token>"
```

### Using the API

Prioritize a transaction by txid and fee delta:

The `<fee-delta>` value is a signed virtual fee adjustment in satoshis. It affects template selection on your node but does not spend funds.

```
curl -X POST \
  -H "Authorization: Bearer <api-token>" \
  "http://127.0.0.1:3001/api/tx/prioritize/<txid>/<fee-delta>"
```

Restore a transaction's DMND-managed fee delta to zero and remove it from the client's local ownership stores:

```
curl -X POST \
  -H "Authorization: Bearer <api-token>" \
  "http://127.0.0.1:3001/api/tx/prioritize/restore/<txid>"
```

List all transactions currently prioritized in Bitcoin Core:

```
curl \
  -H "Authorization: Bearer <api-token>" \
  "http://127.0.0.1:3001/api/tx/prioritized"
```

The response is the result of Bitcoin Core's `getprioritisedtransactions`, divided by ownership. Transactions absent from both local ownership stores are reported as `unknown`. Each category is keyed by txid; `modified_fee` is present only for transactions currently in the mempool:

```json
{
  "success": true,
  "message": null,
  "data": {
    "mempool_space": {
      "<accelerated-txid>": {
        "fee_delta": 100000,
        "in_mempool": true,
        "modified_fee": 110000
      }
    },
    "manually_prioritized": {},
    "unknown": {}
  }
}
```

The API server port defaults to **3001** and can be changed with `--api-server-port`, `api_server_port`, or `API_SERVER_PORT`.

If the prioritization configuration is incomplete, these endpoints are disabled: the client logs that transaction prioritization is not enabled and the endpoints return `503 Service Unavailable`.

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| sv2-tp can't connect to Bitcoin Core | IPC not enabled, or custom datadir | Follow the sv2-tp README setup exactly; check the socket path |
| DMND Client can't reach Template Provider | Wrong `--tp-address` or sv2-tp not running | Confirm sv2-tp is listening on 8336 (`ss -tlnp \| grep 8336`) |
| Client runs but no templates | Bitcoin Core still syncing | Wait for full sync (`bitcoin-cli getblockchaininfo`) |
| Miner connects but no shares yet | Normal within the first ~6 min | Wait; first share acceptance takes about 6 minutes |
| Still no shares after 10+ min | Wrong token in password field, or `-d` far off | Re-check token; set `-d` to your least powerful machine (250T direct / 20P proxies) |
| Dashboard shows zero / low hashrate | Statistics lag | Give it up to an hour after connecting |
| tx API returns 503 | Prioritization config incomplete | All four settings in Section 7 must be set |

Still stuck? Reach out through the support channel listed in your registration confirmation email.

Happy mining!

## Notes

**Note A — Node disk requirements.** A pruned Bitcoin node needs only a few GB of disk and is fully sufficient for Job Declaration. A full archival node needs ~800 GB. Running a node is outside the scope of this guide — see the [Bitcoin Core documentation](https://bitcoincore.org/en/download/) for details.

**Note B — Building from source.** Install the Rust toolchain via [rustup](https://rustup.rs), clone the repository, and build a production binary with:

    cargo build --release

The binary is produced at `./target/release/dmnd-client` — use it exactly as shown in Section 4.1.

## Native SRI migration

The CLI uses [`roles_logic_sv2` 4.0.0](https://github.com/dmnd-pool/roles-logic/tree/95839819d259fedf80d69cbfba5eab4f7857c206).
The revision is `95839819d259fedf80d69cbfba5eab4f7857c206`. Cargo fetches this
library from Git. Its source is not copied into this repository.

Production uses `binary_sv2` 7.0.0, `codec_sv2` 7.0.0, `framing_sv2` 8.0.0,
`noise_sv2` 2.0.0, and `sv1_api` 6.0.0. The production dependency graph contains
no crates from the old DMND Stratum repository. The existing development
dependencies remain available to the independent mining test runner.

Two dependencies need local changes because their upstream revisions use the
old SRI types. A third copy removes a measured performance regression in the
new binary codec:

| Local source | Upstream source | Original revision | Original source commit |
|---|---|---|---|
| `vendor/demand-sv2-connection` | [demand-sv2-connection](https://github.com/dmnd-pool/demand-sv2-connection) | `edeb677c9e2aa07de99f6b7a9ce5b627ff0e1899` | `d96968b` |
| `vendor/share-accounting-ext` | [share-accounting-ext](https://github.com/dmnd-pool/share-accounting-ext) | `c661e936264cd8cc6b13b51fc06a79e04a6ae76e` | `eb9e2a1` |
| `vendor/binary-sv2` | [SRI binary_sv2 7.0.0](https://crates.io/crates/binary_sv2/7.0.0) | `e11881b372c0f0f4cd6bc1b0cf08ae65e4a001fd`, `sv2/binary-sv2` | `5584799` |

Each original source commit precedes all changes to that dependency. The
transport and share accounting upstream manifests declare `MIT OR Apache-2.0`, but neither source tree includes
license files. The imports also include the unchanged `LICENSE-MIT` and
`LICENSE-APACHE` files from [DMND Stratum revision
`f037e6db3568454cdf7973aa6f457dd41d0856e1`](https://github.com/dmnd-pool/stratum/tree/f037e6db3568454cdf7973aa6f457dd41d0856e1).

The binary codec copy contains the complete, unchanged published package. Its
license files come from the [same upstream revision](https://github.com/stratum-mining/stratum/tree/e11881b372c0f0f4cd6bc1b0cf08ae65e4a001fd).
The later patch removes a second traversal during encoding and allows the
compiler to inline primitive decoding. It retains the native wire format,
error checks, and iterative handling of deeply nested data.

Messages in queues own their data. The receive parser borrows frame data until
it must retain a message. The connection adapter uses the native SRI codec and
frames. It exports `HandshakeRole`, `Initiator`, `Responder`,
`StandardEitherFrame`, and `StandardSv2Frame` at its crate root. Message and
frame size queries do not serialize data or allocate heap memory.

Job Declaration uses transaction witness IDs, `PushSolution`, and the native
coinbase output constraints. Token and custom-job output lists include their
Bitcoin count field. Template output lists retain the separate count field.
Channel group changes pass to the translator. Share validation hashes the
coinbase parts directly and uses a fixed buffer for each Merkle branch step.

Coinbase output limits use BIP141 signature-operation cost. A P2PKH output
reserves four units. Costs that do not fit the protocol field are rejected.
Each token is checked against both advertised limits. If either limit must
increase, the CLI retains the larger limits and restarts the mining session
with the same pool. The new template connection advertises those limits before
it uses a template. This prevents queued templates from using the earlier,
smaller reservation. Smaller tokens do not reduce the limits. Unchanged token
outputs require no heap allocation. Merge-mining output space remains reserved.

The translator stores miner channels and extranonce prefixes in hash indexes.
Channel setup and removal use constant expected lookup time. Disconnects remove
both records. The native factory keeps the job, previous-hash, and group state;
the CLI uses the native version, target, and Merkle helpers to check shares.
This avoids the upstream factory's scan through all existing miner prefixes.
It also avoids per-miner job copies because SV1 miners use the existing job
broadcast. Unit tests compare channel assignments and share results with the
native factory, including serialized bytes and protocol rejections.

### Checks without the mining suite

Run these commands from the repository root:

```sh
rustfmt --edition 2021 src/lib.rs src/main.rs
cargo check --locked --lib --bin dmnd-client
cargo clippy --lib --bin dmnd-client --all-features -- -D warnings
cargo test --locked --lib -- --test-threads=1
cargo test --locked --bin dmnd-client
cargo test --locked --manifest-path vendor/demand-sv2-connection/Cargo.toml --lib
cargo test --locked --features mining-e2e --lib native_transport_interface_roundtrip
```

This run starts from master. It does not contain the prepared public example.
The `native_transport_interface_roundtrip` unit test checks the required crate
exports, both handshake roles, encrypted send and receive, frame conversion,
headers, payload access, serialized bytes, and extension messages against the
production dependency graph. The prepared example was not added.

The final library test run passed 202 tests. Two existing tests remain ignored.
All 12 transport tests passed. The locked production check and Clippy check
passed without warnings. The CLI help command also passed.

Library unit tests check message bytes, malformed payloads, invalid headers,
version rejection, channel assignments, transaction selection, and allocation
counts. Transport unit tests also check encrypted frames, fragmented reads,
large messages, concurrent handshakes, authentication rejection, and connection
closure. These checks do not prove that the private mining suite passes. That
suite must still report all 18 tests and all 17 scenario summaries as passed,
with no unexpected errors or panics. It was not accessed or run in this session.

### Local performance measurements

Measurements compare the starting production source at
`4c2a7341d3a9e1b1b46d063770a1e2c3bff005d7` with this migration. The machine has an
Intel Core i9-14900K. Both versions use Rust 1.88.0, release optimization, and
the System allocator. An allocation counter counts allocations and
reallocations. The share and codec measurements also enable link-time
optimization. Times below are medians; they are local measurements, not network
or full mining-suite results.

| Operation | Starting code | Migrated code |
|---|---:|---:|
| Share validation | 3108.21 ns | 2855.96 ns |
| Allocations per share validation | 34 | 0 |
| Message size query | 1.10 ns | 0.84 ns |
| Frame size query | 1.42 ns | 0.55 ns |
| Allocations per size query | 0 | 0 |
| Raw extended-share encoding | 115.44 ns | 95.21 ns |
| Raw extended-share decoding | 486.88 ns | 486.75 ns |
| Allocations per raw encoding | 4 | 3 |
| Allocations per raw decoding | 18 | 18 |
| Encrypted roundtrip | 28.64 µs | 27.84 µs |
| Encrypted pipeline throughput | 96,170 frames/s | 112,223 frames/s |
| Allocations per small encrypted roundtrip | 1 | 0 |

Share validation uses 12 Merkle nodes, a 180-byte coinbase prefix, a 48-byte
suffix, and two 8-byte extranonce parts. Each version runs nine rounds of
150,000 calls on CPU 2 after warmup. A fixed hash test checks the resulting
protocol bytes. The migrated path is 8.1% faster in this measurement.

The codec measurement uses a 32-byte extranonce. Each round runs 5,000,000 size
queries and 100,000 encode or decode operations on CPU 4. The table combines
four runs of five rounds per version, with both execution orders. The initial
unpatched native codec measured 149.86 ns for share encoding. The local binary
patch removes that regression. Final encoding is 17.5% faster in this check;
decoding is comparable. Decode ranges overlap: 480.93–491.33 ns for the starting
code and 479.22–501.79 ns for the migrated code.

The transport measurement uses loopback TCP, `TCP_NODELAY`, a four-byte payload,
and four Tokio worker threads on CPUs 8, 10, 12, and 14. Each version runs 1,000
warmup exchanges, then seven rounds of 10,000 roundtrips and seven rounds of
100,000 pipeline frames. The observed roundtrip ranges overlap: 28.15–29.35 µs
for the starting code and 27.56–28.19 µs for the migrated code. Pipeline
throughput increases by 16.7% in the final run.

Larger-message checks also compare serialized lengths and hashes. A template
with 12 Merkle hashes and 165 coinbase output bytes encodes to 594 bytes. Its
encoding time is comparable in both execution orders; allocations decrease
from 6 to 5. In the reverse-order run, declarations with 500 and 2,000 witness
IDs take 10.84 µs and 42.15 µs, compared with 12.09 µs and 48.37 µs for the
starting code. Their allocations decrease from 7 to 4. These measurements use
CPU 10, seven rounds, and release builds with link-time optimization. Message
construction and cloning occur before timing.

The channel check compares the starting code, the native revision without the
prefix index, and the final CLI adapter. Values are median nanoseconds per
operation from eight rounds on CPU 4, after warmup, with both execution orders.
Each setup run starts with an empty factory. The share run uses one active
channel, an empty Merkle path, and 100,000 submissions; it includes share
cloning and checks that each share is accepted for upstream transmission.

| Operation | Starting code | Native without index | Final CLI |
|---|---:|---:|---:|
| Channel setup, 256 miners | 266.73 ns | 793.72 ns | 228.05 ns |
| Channel setup, 1,024 miners | 220.17 ns | 2,227.21 ns | 185.69 ns |
| Channel setup, 4,096 miners | 211.86 ns | 7,533.57 ns | 185.35 ns |
| Channel setup, 16,384 miners | 212.47 ns | 28,544.34 ns | 186.19 ns |
| Bridge share processing | 740.01 ns | 687.06 ns | 630.65 ns |

At 16,384 miners, final setup is 12.4% faster than the starting code. Bridge
share processing is 14.8% faster. The prefix index removes the quadratic
comparison growth. The adapter also returns the channel assignment directly,
without allocating unused protocol response messages.

The measurement sources are in `src/performance/`. Use Python 3.11 or later.
Prepare separate temporary Cargo projects with:

```sh
python3 src/performance/prepare.py /tmp/demand-performance
```

Run both `share-baseline` and `share-native` with CPU affinity 2. Run both
`codec-baseline` and `codec-native` with CPU affinity 4. Run both
`message-baseline` and `message-native` with CPU affinity 10. For example:

```sh
taskset -c 2 cargo run --release --manifest-path /tmp/demand-performance/share-native/Cargo.toml
taskset -c 4 cargo run --release --manifest-path /tmp/demand-performance/codec-native/Cargo.toml
taskset -c 10 cargo run --release --manifest-path /tmp/demand-performance/message-native/Cargo.toml
taskset -c 4 cargo run --release --manifest-path /tmp/demand-performance/channels-native/Cargo.toml
taskset -c 8,10,12,14 cargo run --release --locked --manifest-path /tmp/demand-performance/transport/Cargo.toml -- reverse
```

Use available CPU numbers on other machines. Repeat both orders and compare
several rounds. The temporary baseline projects use the old dependencies only
for measurement. They do not change the CLI dependency graph or its targets.
For the channel check, compare `channels-baseline`, `channels-unindexed`, and
`channels-native`. Build before timing and run the resulting binaries directly.
