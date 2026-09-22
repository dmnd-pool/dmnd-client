# DMND Stratum V2 Client — Getting Started Guide

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

The client serves a local dashboard at `http://127.0.0.1:3001` (set the port with `--api-server-port`). It shows pool and miner status, the block templates sv2-tp is offering, and the history of declared templates; from it you can choose how much history to keep and ask bitcoind to prioritise a transaction. It opens in a browser on start; set `HEADFUL=false` to stop that.

Use the sidebar to open the **mempool.space accelerations** or **RSK merge mining** page. Both need the client's `API_TX_TOKEN`.

- **mempool.space accelerations:** This page shows the accelerated transactions the client has fetched from mempool.space.
- **RSK merge mining:** This page shows whether merge mining is running and how many RSK candidates your miners have found. A candidate is a proof that meets the RSK target; the bridge collects them and sends them to RSK.

The RSK page refreshes every five seconds, the accelerations page every thirty. Neither refreshes while you are looking at another page or another tab.

## 4. Run the DMND Client

### 4.1 Download the DMND Stratum V2 Client

Download the release binary for your platform from:

https://github.com/dmnd-pool/dmnd-client/releases

Verify checksums/signatures where provided — standard practice for any software that touches mining revenue.

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
