# LittleBigCluster 🐣🐘

**LittleBigCluster** is a Rust library for building cluster applications using an object store (like S3) as the only dependency. Nodes are diskless — all durable state lives in the object store — and each node runs an in-process SQLite database replicated from that store. The leader has read-write access; followers have read-only access. This SQLite database is ideal for managing the control plane of a cluster. The tradeoff is write latency (~1 second with the default settings), which is often acceptable for a control plane.

## What do you get?

- **No central database needed**: Every node has access to an in-process SQLite database. The leader writes; followers serve consistent reads.
- **Automatic leader election**: Built-in leader election with support for standby leaders. A follower promotes itself when the current leader stops advancing epochs.
- **Effortless backups and rollbacks**: Cluster state is stored in the object store. Snapshots and epoch history let you recover to any point within the retention window.
- **gRPC cluster membership**: Each node runs a membership service. Members heartbeat to the leader, which distributes the roster to all nodes.
- **Object store as the only dependency**: Works with S3, MinIO, a local filesystem, or any backend supported by the [`object_store`](https://docs.rs/object_store) crate.

## Why?

This experiment explores an idea I've always wanted to pursue: building a clustered application without relying on a central database or coordinator (like Zookeeper or etcd). With atomic put support in S3 (since September 2024), this is now feasible. The goal is to use only an object store — universally available across cloud providers, on-premise (e.g. MinIO), and local development (using a POSIX filesystem). All nodes are diskless, making them easy to replace and delete.

## How it works

1. **Epoch management**: The cluster advances a monotonic epoch counter. Each epoch is an atomic write to the object store.

2. **Changesets and snapshots**:
   - Each epoch file (`epochs/{epoch:020}`) contains a **logical changeset** captured via the SQLite session extension — not a raw WAL file.
   - Periodic **snapshots** (`snapshots/{epoch:020}`) are full database copies produced with `VACUUM INTO`.
   - The latest snapshot epoch is tracked in `last_snapshot`.

3. **Node join process**:
   - A new node reads `last_snapshot` to find the latest snapshot, downloads it, then applies every successive epoch to catch up.
   - It then follows new epochs as they appear.

4. **Leader information**: The current leader's identity and address are recorded in each epoch's metadata.

5. **Following epochs**: Followers continuously fetch and apply epoch `N + 1` to stay current with the leader.

6. **Leader election**:
   - A node eligible for leadership waits until the current leader's epochs go stale.
   - It attempts to write the next epoch with `PutMode::Create`. If it succeeds, it becomes leader; if another node got there first, it stays a follower.

7. **Epoch advancement**: The leader captures a changeset each tick and writes a new epoch atomically. A failed write means another leader has taken over — this node is fenced off and demotes itself.

```
{prefix}epochs/{epoch:020}      # lz4+protobuf WalEpoch (Changeset | Migration)
{prefix}snapshots/{epoch:020}   # VACUUM INTO copy
{prefix}last_snapshot
{prefix}epoch_watermark
```

## Example

There is a `demo` example you can run locally. The first node creates the object store and becomes leader; additional nodes join as followers.

```bash
# terminal 1 — first node (creates store, initializes DB, becomes leader)
cargo run --example demo -- --object-store /tmp/lbc-store --id node-a --addr 127.0.0.1:5001
# web UI: http://127.0.0.1:6001

# terminal 2 — follower
cargo run --example demo -- --object-store /tmp/lbc-store --id node-b --addr 127.0.0.1:5002
# web UI: http://127.0.0.1:6002

# or S3 (uses AWS_* env vars)
cargo run --example demo -- --object-store s3://my-bucket/lbc/demo --id node-a --addr 127.0.0.1:5001
```

Each node serves a plain HTML UI on port **gRPC + 1000** (5001 → 6001). Run SQL from the leader; `SELECT` works on any node.

Run the test suite:

```bash
cargo test --workspace
```

## Crates

| Crate | Role |
|-------|------|
| `lbc-db` | Replication engine — epoch chain, changesets, snapshots, leader election |
| `lbc-cluster` | Node orchestration — tick loop, gRPC membership client and service |

## Schema contract

- Every replicated table must have a **PRIMARY KEY** (required by the SQLite changeset extension).
- All application DDL — including the initial schema — ships as ordered **Migration** epochs (`schema_version` in `_lbc_meta`). Ad-hoc DDL in `write()` is rejected.

## Prior art

- **Litestream**: Inspired the idea of replicating a SQLite database by replicating its write log. This library uses logical changesets instead of physical WAL files.
- **Delta Lake**: Shares the concept of using an object store as the only dependency. The epoch protocol is similar, though write conflicts are not expected under normal operations (conflict detection follows a similar approach).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE.md) file for details.
