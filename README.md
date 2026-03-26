# INF-3203

This repository contains an implementation of **Áika** for a distributed image-classification system.
Raft is implemented for consensus among the Cluster Controllers, and a watchdog process ensures high availability by
monitoring and restarting failed nodes.

Both the Áika node and deploy/watchdog scripts are written in Rust. A GitHub CI/CD pipeline builds static binaries to
be used on the IFI cluster nodes.

**Version:** v0.0.23

---

## Deployment on IFI cluster

> Run the following on a cluster node in a folder on the distributed filesystem (user/home folder).
> The deploy script installs `aika-node` in the current directory, which every
> cluster node can reach via NFS — no per-node download needed.

```bash
curl -fsSL https://github.com/augustsh/inf3203/releases/download/v0.0.23/deploy-x86_64-unknown-linux-musl.tar.gz | tar xz
chmod +x deploy
./deploy N        # N = total cluster nodes (minimum 3)
```

The deploy binary:
1. Downloads `aika-node` and `classify.py` to the current directory
2. Creates an `inf3203_aika` symlink (satisfies the cluster's `inf3203_` process-naming requirement)
3. Discovers available non-GPU nodes via `/share/ifi/available-nodes.sh` and `/share/ifi/list-cluster-static.sh`
4. Allocates the first 3 nodes as **Cluster Controllers** (Raft quorum)
5. Allocates 1 node as a **standby replica** Local Controller (`--agents 0`)
6. Allocates remaining nodes as **active** Local Controllers (`--agents 4`)
7. SSH-starts each component; the binary is read directly from the distributed filesystem
8. **Stays running as a watchdog** — monitors all nodes and restarts crashed processes automatically

### Watchdog behaviour

- **Local Controllers**: restarted after a 150-second delay (giving the CC time to promote the replica first). Always
    restarts as a standby replica. If the node is fully down we restart on a different node.
- **Cluster Controllers**: restarted on the same node without wiping Raft state (no dynamic join/leave in Raft implementation).

### Check status

```bash
curl http://<cc-node>:<port>/status    # task progress + node health
curl http://<cc-node>:<port>/leader    # current Raft leader
```

### Collect results

Once `/status` reports all tasks completed:

```bash
./deploy merge     # deduplicates results_*.ndjson → inf3203_data/results_final.ndjson
```

This is needed as a leader change during the run will produce multiple result files as the new leader will append
all completed tasks to a new file. For convenience, the deploy script merges them into a single deduplicated file.

### Teardown

```bash
./deploy cleanup   # removes Raft state dirs (/tmp/inf3203_<user>_*) on all nodes
/share/ifi/cleanup.sh   # kills all processes started by the current user (including the watchdog if still running)
```

---

## Local development

Requires Rust and Cargo (set up using [Rustup](https://rust-lang.org/tools/install/)). Run the following from the project root:

```bash
# Navigate to the aika-node source directory
cd ./src/aika-node

# Build (dev)
cargo build

# Run a 3-node CC cluster
cargo run -- cluster-controller --node-id 1 --bind 0.0.0.0:8001 \
    --peers localhost:8001,localhost:8002,localhost:8003
cargo run -- cluster-controller --node-id 2 --bind 0.0.0.0:8002 \
    --peers localhost:8001,localhost:8002,localhost:8003
cargo run -- cluster-controller --node-id 3 --bind 0.0.0.0:8003 \
    --peers localhost:8001,localhost:8002,localhost:8003

# Run a Local Controller with 2 agents
cargo run -- local-controller --node-id lc-1 --bind 0.0.0.0:9001 \
    --cc-addrs localhost:8001,localhost:8002,localhost:8003 \
    --agents 2 \
    --extractor-script ./INF-3203_Assignment/classify.py \
    --image-base-path /share/inf3203/unlabeled_images

# Run tests (tests the Raft implementation)
cargo test
```

---

## Architecture

Three roles compiled into the single `aika-node` binary, selected by subcommand:

```
┌─────────────────────────────────────────────────────────────┐
│              Cluster Controllers  (×3, Raft quorum)          │
│  Owns: replicated task queue, node registry, TTL reaper      │
└──────────┬──────────────────────────────┬────────────────────┘
           │ HTTP heartbeat / task API    │
     ┌─────▼──────┐                ┌─────▼──────┐
     │   Local     │                │   Local     │
     │ Controller  │                │ Controller  │
     │  (active)   │                │  (replica)  │
     │  agents: 4  │                │  agents: 0  │
     └──┬──────────┘                └─────────────┘
        │ spawn/monitor                 ↑
     Agent  Agent  Agent  Agent    activated by CC on LC failure
```

### Cluster Controller
- Participates in Raft consensus (election, log replication, commit)
- Manages the **replicated task queue** — all task state goes through Raft
- Runs a **TTL reaper**: assignments expire after 120 s and return to `Pending`
- On leader election, resumes image ingestion from where the previous leader left off (resumable — skips already-committed batches)
- Monitors Local Controllers via heartbeats; on confirmed failure (2× timeout), sends `POST /activate` to the standby replica LC
- Periodically flushes completed labels to `<results-dir>/results_<node-id>.ndjson` every 30 s

### Local Controller
- One per physical node; spawns `--agents N` child processes, restarts on crash
- Sends heartbeats to the CC leader every 10 s (including agent count + config)
- Proxies agent task requests and completions to the CC leader
- Standby replica (`--agents 0`): accepts `POST /activate` from CC to take over a failed node, spawning agents with the failed node's config

### Agent
- Pull-based work loop: request batch → run `classify.py` → report labels → repeat
- Falls back to contacting CC directly if the LC is unreachable
- Heartbeats its LC every 5 s with the current batch ID

---

## Task lifecycle

```
Pending → Assigned (TTL 120 s) → Completed
              ↑ TTL expiry returns to Pending
```

- **Batching**: ~100 images per batch → one Raft log entry per batch, not per image
- **Exactly-once**: `CompleteTask` is idempotent in the Raft state machine; ingestion is resumable across leader changes
- **No data transfer**: all nodes read `/share/inf3203/unlabeled_images` directly

---

## HTTP API

### Cluster Controller

| Method | Path             | Description                                |
|--------|------------------|--------------------------------------------|
| POST   | `/task/request`  | Assign the next pending batch to an agent  |
| POST   | `/task/complete` | Record completed labels (idempotent)       |
| POST   | `/heartbeat`     | LC liveness + node info                    |
| GET    | `/leader`        | Current Raft leader address                |
| GET    | `/status`        | Task counts + node health overview         |

Non-leader CCs return `307 Temporary Redirect` to the leader.

### Local Controller

| Method | Path                  | Description                            |
|--------|-----------------------|----------------------------------------|
| POST   | `/agent/request_task` | Proxied to CC leader                   |
| POST   | `/agent/complete`     | Proxied to CC leader                   |
| POST   | `/agent/heartbeat`    | Agent liveness (local only)            |
| POST   | `/activate`           | CC instructs replica to boot up agents |

---

## Configuration flags

### `cluster-controller`

| Flag                          | Default                           | Description                                      |
|-------------------------------|-----------------------------------|--------------------------------------------------|
| `--node-id`                   | required                          | Unique numeric ID within the CC cluster          |
| `--bind`                      | `0.0.0.0:8000`                    | Address to listen on                             |
| `--peers`                     | required                          | Comma-separated list of all CC addresses         |
| `--task-ttl-secs`             | `120`                             | Seconds before an assignment expires             |
| `--image-dir`                 | `/share/inf3203/unlabeled_images` | Image directory                                  |
| `--batch-size`                | `100`                             | Images per task batch                            |
| `--lc-heartbeat-timeout-secs` | `60`                              | Seconds of silence before LC is suspected failed |
| `--data-dir`                  | `/tmp/inf3203_<user>_<id>`        | Node-local dir for Raft state (must NOT be NFS)  |
| `--results-dir`               | `$HOME/inf3203_data`              | Directory for results NDJSON output              |

### `local-controller`

| Flag                  | Default                           | Description                          |
|-----------------------|-----------------------------------|--------------------------------------|
| `--node-id`           | required                          | Unique string ID                     |
| `--bind`              | `0.0.0.0:9000`                    | Address to listen on                 |
| `--cc-addrs`          | required                          | Comma-separated CC addresses         |
| `--agents`            | `4`                               | Worker agents to spawn (0 = replica) |
| `--extractor-script`  | `./feature_extractor.py`          | Path to `classify.py`                |
| `--image-base-path`   | `/share/inf3203/unlabeled_images` | Image root                           |

---

## Results

The leader CC appends completed labels to `<results-dir>/results_<node-id>.ndjson` every 30 s.
Each line is a JSON object:

```json
{"batch_id": 42, "labels": [["img001.JPEG", "tabby cat"], ["img002.JPEG", "balloon"]]}
```

If leadership changed during the run, multiple result files will exist (one per leader).
Run `./deploy merge` to deduplicate them into a single `results_final.ndjson`:

```bash
./deploy merge
# → inf3203_data/results_final.ndjson
```

---

## Building for the cluster

```bash
# Static musl binary (required for IFI cluster nodes)
cargo build --release --target x86_64-unknown-linux-musl \
    --manifest-path src/aika-node/Cargo.toml

# Build deploy tool
cargo build --release --target x86_64-unknown-linux-musl \
    --manifest-path src/deploy/Cargo.toml
```

Releases are built automatically by CI on `v*.*.*` tags and published to GitHub Releases.