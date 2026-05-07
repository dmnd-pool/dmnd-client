DMND Stratum V2 Client – Getting Started Guide
========================================

# 1. Introduction
---------------

Through this guide we will setup DMND Stratum V2 client and connect to DMND pool. After completing
this guide, you will have a fully functional Stratum V2 mining setup connected to DMND pool with
full Job Declaration.

# 2. What You Need Before Starting
-------------------------------

To mine with DMND pool you must first obtain DMND token.  Please complete the registration form at
https://onboarding.dmnd.work and await our confirmation email before proceeding.

# 3. Enable Job Declaration Support
-------------------------

To use Stratum V2 with Job Declaration, you must run Bitcoin Core along Stratum V2 Template
Provider. Job declaration is one of the key features of Stratum V2 that allow miners to build their
own blocks, improving decentralization, efficiency, and latency.


What you need:
- Bitcoin Core(At least version 30) with IPC enabled.
- Stratum V2 Template Provider, which connect to Bitcoin Core via IPC and provide templates to the
  DMND Stratum V2 Client.

#### 3.1 Run Bitcoin Core
Follow instruction to download and install Bitcoin Core as describe in the official website:

https://bitcoincore.org/en/releases/30.2/

Then make sure to start Bitcoin Core with IPC enabled.

    bitcoin -m node -chain=main -ipcbind=unix

Note that the `ipcbind=unix` is required and Stratum V2 will not work without it.

#### 3.2 Run Template Provider
Download the Template Provider binary.
https://github.com/stratum-mining/sv2-tp/releases/tag/v1.0.6

Run the Template Provider:

    sv2-tp -debug=sv2 -loglevel=sv2:trace

If you have changed Bitcoin Core’s default datadir, you may need to specify the
Unix socket path manually by adding the following option:

    -ipcconnect=unix:<path-to-bitcoin-dir>/node.sock

The default Template Provider port is **8336**.

# 4. Run DMND Client
-----------------------------------

#### 4.1 Download DMND Stratum V2 Client
You can download the latest release of DMND Stratum V2 Client from:
https://github.com/dmnd-pool/dmnd-client/releases/tag/v0.2.9


Assuming that `dmnd-client-linux` is the executable you are using, run:

    TOKEN=<DMND-token> cargo run -- -l info -d <avg-hashrate>T --tp-address="127.0.0.1:<port>"

Where:
- `<avg-hashrate>` = average hashrates of all your miners in TH/s. For example,
if you have three machines of 100Th/s, 200Th/s and 300Th/s, then the average 
hashrate is (100 + 200 + 300) / 3 = 200 TH/s.  Our dynamic difficulty 
adjustment algorithm will take care of the rest.

- `<port>` is the Template Provider listening port (default 8336).

- `<DMND-token>` is the token you received via email from DMND pool during registration.

Example:

    TOKEN=abc123 cargo run -- -l info -d 200T --tp-address="127.0.0.1:8336"

# 5. Connect Your Miner
-----------------------------

After you have Bitcoin Core, Stratum V2 Template Provider and DMND Stratum V2 Client running, you
can point your miner to the DMND Stratum V2 Client.

Enter your DMND token in the password field and point your miners to the machine running the DMND Stratum V2 Client. The username field can be left empty or filled with anything you like. If not changed, the default port of the
DMND Stratum V2 Client is **32767**. So you should obtain the IP address of the machine running the
DMND Stratum V2 Client and point your miner to:

    stratum+tcp://<machine_running_dmnd_client_ip>:32767


# 6. Track Hashrate and Earnings
--------------------------------------
You can track your hashrate and earnings on the DMND pool dashboard:

    https://dashboard.dmnd.work

Login with the same credentials you used during registration.

# 7. Prioritize Transactions
--------------------------------------
The DMND Stratum V2 Client can expose an API endpoint that sends a raw transaction to Bitcoin Core
and asks Bitcoin Core to prioritize that transaction for block template selection.

This feature is enabled only when all of the following values are configured:

- `RPC_URL`: Bitcoin Core RPC URL, for example `http://127.0.0.1:8332`.
- `RPC_USER`: Bitcoin Core RPC username.
- `RPC_PWD`: Bitcoin Core RPC password.
- `RPC_FEE_DELTA`: fee delta, in satoshis, passed to Bitcoin Core's `prioritisetransaction` RPC.
- `API_TX_TOKEN`: bearer token required by the DMND Client transaction API.

You can set these values with CLI flags (`--rpc-url`, `--rpc-user`, `--rpc-pwd`,
`--rpc-fee-delta`, `--api-tx-token`), in `config.toml`, or with environment variables. CLI flags
take precedence over `config.toml`, and `config.toml` takes precedence over environment variables.

Example using environment variables:

    TOKEN=<DMND-token> \
    RPC_URL=http://127.0.0.1:8332 \
    RPC_USER=<bitcoin-rpc-user> \
    RPC_PWD=<bitcoin-rpc-password> \
    RPC_FEE_DELTA=100000000 \
    API_TX_TOKEN=<api-token> \
    cargo run -- -l info -d <avg-hashrate>T --tp-address="127.0.0.1:8336"

Example using `config.toml`:

    rpc_url = "http://127.0.0.1:8332"
    rpc_user = "<bitcoin-rpc-user>"
    rpc_pwd = "<bitcoin-rpc-password>"
    rpc_fee_delta = 100000000
    api_tx_token = "<api-token>"

When the client is running, submit a raw transaction hex to:

    POST http://<dmnd-client-host>:<api-server-port>/api/tx/<raw-transaction-hex>

The API server port defaults to `3001` and can be changed with `--api-server-port`,
`api_server_port`, or `API_SERVER_PORT`.

Example:

    curl -X POST \
      -H "Authorization: Bearer <api-token>" \
      "http://127.0.0.1:3001/api/tx/<raw-transaction-hex>"

If the prioritization configuration is incomplete, this endpoint is disabled. In that case the
client logs that transaction prioritization is not enabled and the endpoint returns `503 Service
Unavailable`.


Happy Mining!
