# Mining integration tests

The repository includes the actual TP simulator and cpuminer executables in
[`bin/linux-amd64`](bin/linux-amd64), with checksums, licenses, and cpuminer's source
archive. Docker supplies their runtime libraries and builds the proxy and test runner
with Rust 1.88.0. You need Docker; no local Rust installation, private TP repository,
external binary paths, or Bitcoin node are required.

Build and run from the repository root:

```bash
docker build --platform linux/amd64 -f tests/Dockerfile -t dmnd-mining-tests . &&
mkdir -p target/mining-e2e &&
docker run --rm --init --network none --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$(pwd)/target/mining-e2e,dst=/artifacts" \
  dmnd-mining-tests
```

If the build fails, these commands stop before a container starts.

The same test files support master and `NewSRI`. The build selects the older
transport API when `Cargo.toml` declares a direct `codec_sv2` dependency. Otherwise,
the tests use the connection library's public API. The test assertions stay the
same. Keep `build.rs` and its TOML build dependency when you prepare either version
for a benchmark. Freeze the test files and `build.rs` before an agent starts work.

The image contains both supplied binaries, the compiled proxy, and the compiled mining
test executable. Building needs network access for public Cargo dependencies and Debian
packages; running needs no downloads or external network. The default entry point runs
all 17 mining scenarios plus the pool regression, prints their results, and exits with
the suite's status. Append `--exact jd_mining` after the image name to run one scenario.
The supplied binaries target Linux x86-64; `--platform linux/amd64` also selects Docker's
emulation on ARM hosts.

The template provider for these tests is DEMAND's `tp-simulator`, which generates SV2
templates and simulated Bitcoin JSON-RPC. Its bundled binary supports
`--rpc-listen-addr`, `--sv2-listen-addr`, `--seed`, `--time-multiplier`,
`--max-stored-templates`, and `--network-hashpower`. It authenticates
`getblockcount`/`submitblock` with its default `username`/`password` credentials.
Both single and batch RPC responses are understood. See [binary provenance and update
instructions](bin/README.md) before replacing either executable.

On Linux x86-64 with Rust and the runtime/build libraries installed, you can also use
the checked-in binaries directly:

```sh
cargo +1.88.0 test --locked --features mining-e2e --test mining_e2e -- --nocapture --test-threads=1
cargo +1.88.0 test --locked --test cli_args --test pool_simulator
```

`TP_SIMULATOR_BIN` and `CPUMINER_BIN` optionally override the bundled executables.
Invalid overrides fail clearly; no scenarios are silently skipped.
`TP_SIMULATOR_VERSION` is an optional artifact identifier; the harness always records
each binary's SHA256 and exact invocation.

The `Mining end-to-end tests` workflow builds and runs the same Docker image and uploads
logs even when tests fail. It needs no private artifact credentials. Ordinary CI tests
run without `mining-e2e`; Clippy still compiles all features and targets.

Each scenario launches a fresh proxy with controlled environment variables, auto-update,
browser opening, and monitoring disabled, isolated working directories, and dynamic
loopback ports. Scenarios serialize their external processes within the test executable;
pool peers and miners run concurrently. Normal/error exits terminate and reap children,
shut down the pool actor and its tasks, and preserve artifacts. Drop also terminates
children and aborts tasks if a scenario panics. Port allocation has a short bind-to-launch
race because the external binaries accept addresses rather than prebound sockets.

Artifacts are kept under `target/mining-e2e/<scenario>-<random>/`, or
`MINING_E2E_ARTIFACTS` when set. They contain `events.jsonl`, per-pool event journals,
`summary.json`, scenario configuration/seed, process arguments/environment/working
directories/binary checksums, and separate TP, proxy, and miner logs. Per-pool journals
are written as events occur, so interrupted scenarios retain their evidence. Sequence
numbers are scoped to each pool. A successful mining test requires independently
validated pool submissions; miner acknowledgement logs are an additional check.
The Docker command mounts this host directory at `/artifacts`, which is the path printed
inside the container.

To see service logs from startup, run this in a second Bash terminal from the repository
root while the suite is running:

```sh
RUN=$(ls -td -- target/mining-e2e/*/ | head -n 1)
tail -n +1 -F "$RUN"/{tp.log,proxy.log,miner-0.log,pool-0-events.jsonl}
```

This follows one scenario. Rerun it to select the next scenario's directory; services
stop after each scenario finishes. Disconnect/rejection scenarios deliberately generate
errors, so check the test result and `summary.json` for the outcome. Invalid-token tests
do not start a miner.

The actor supports mining and JD connections on one listener, including router probe
connections, target changes, peer disconnects, response pause/resume, forced share and
declaration rejection, snapshots, and deterministic synthetic templates/tips for protocol
tests. Job keys contain peer, channel, and job IDs. Its verifier uses Bitcoin transaction
and header primitives, enforces extranonce length/time/version/tip/target constraints, and
keeps a bounded duplicate set per retained job. JD checks token ownership, transaction
hashes, declaration coinbase, custom-job coinbase/merkle/payout, and current chain tip.

The new-tip scenario mines a valid empty block at an easy network difficulty and submits
it through TP's existing `submitblock` RPC, then checks the announced hash and fresh work.
It records the block and does not rely on the next random background block arriving.
Scripted SV1 negative cases assert rejection at the proxy boundary,
while pool protocol tests assert stale/duplicate/unknown work at the pool boundary.
