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

https://bitcoincore.org/en/releases/30.0/

Then make sure to start Bitcoin Core with IPC enabled.

    bitcoin -m node -chain=main -ipcbind=unix

Note that the `ipcbind=unix` is required and Stratum V2 will not work without it.

#### 3.2 Run Template Provider
Download the Template Provider binary.
https://github.com/stratum-mining/sv2-tp/releases/tag/v1.0.5

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
https://github.com/dmnd-pool/dmnd-client/releases/tag/v0.2.7


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

Leave the username and password fields empty in your miner configuration. And you should point the
miners to the machine running the DMND Stratum V2 Client. If not changed, the default port of the
DMND Stratum V2 Client is **32767**. So you should obtain the IP address of the machine running the
DMND Stratum V2 Client and point your miner to:

    stratum+tcp://<machine_running_dmnd_client_ip>:32767


# 6. Track Hashrate and Earnings
--------------------------------------
You can track your hashrate and earnings on the DMND pool dashboard:

    https://dashboard.dmnd.work

Login with the same credentials you used during registration.


Happy Mining!
