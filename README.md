# LittleBigCluster 🐣🐘

**LittleBigCluster** is a Rust library for building cluster applications using an object store (like S3) as the only dependency. Nodes are diskless, with all state replicated in the object store. It provides a replicated SQLite database, with a leader-follower model: the leader has read-write access, and followers have read-only access. This SQLite database is ideal for managing the control plane of the cluster, the tradeoff being high write latency (~1 second with the default settings).

## What do you get?

- **No Central Database Needed**: Every node has access to an in-process SQLite database. The leader has read-write access, while followers have read-only access.
- **Automatic Leader Election**: Built-in leader election with support for standby leaders.
- **Effortless Backups and Rollbacks**: State is safely stored in the object store, eliminating the need for backups. Rollbacks are available to any point in time within the retention period (default 7 days).
- **Built-in HTTP/2 Communication**: Each node includes built-in HTTP/2 servers and clients. Nodes maintain an always-open connection to the current leader, mitigating high write latency with multiple in-flight streams on a single TCP connection.
- **Cluster Membership Management**: Member discovery is managed using a gossip-like protocol. Each member reports its status to the leader, which then distributes the membership view to all nodes.
- **Object Store Access**: Direct access to the object store for managing the data plane of your cluster application.

## Why?

This experiment explores an idea I've always wanted to pursue: creating a clustered application without relying on a central database or coordinator (like Zookeeper or etcd). With the addition of atomic put support in S3 (since September 2024), this is now feasible. The goal is to use only an object store, which is universally available across cloud providers, on-premise (e.g., MinIO), and local development (using a POSIX filesystem). This approach offers durability, availability, and simplicity: nodes are diskless, making them easy to replace and delete.

## How it works?

1. **Epoch Management**: The cluster starts with an epoch value of 0, which is incremented monotonically.

2. **WAL and Snapshots**:

   - The `/.lbc/` folder on the object store contains WAL files and database snapshots.
   - Each epoch has a corresponding WAL file (e.g., `00000000000000000001.wal`), which contains SQLite Write-Ahead Log data.
   - Full SQLite database snapshots are stored in files like `00000000000000000020.db`.
   - The latest snapshot epoch is tracked in the `/.lbc/.last_snapshot` file.

3. **Node Join Process**:

   - Nodes start as followers and read `/.lbc/.last_snapshot` to determine the latest snapshot.
   - They make a LIST request to the object store using the latests snapshot epoch as prefix.
   - They download the most recent snapshot and apply all successive WAL files to catch up to the current epoch.

4. **Leader Information**: The current leader's coordinates are stored in a system table (`_lbc`) within the database.

5. **Following Epochs**: Followers continuously fetch and apply new WAL files (`epoch + 1`) to stay updated with the leader.

6. **Leader Election**:

   - A node started in leader mode waits for the previous leader to step down or become inactive.
   - If it successfully writes the next epoch, it becomes the new leader, otherwise it remains a follower.

7. **Epoch Advancement**: The leader writes a new WAL file for each epoch. This is an atomic operation: if it fails, it indicates that another leader has already taken over, and this one has been fenced off.

## Example

There is a `lol_cluster` example you can start locally:

First create a fake object store in your file system

```mkdir -p /tmp/lol```

Then initialize the cluster:

```

$ cargo run --example lol_cluster -- -p /tmp/lol init

2024-10-06T23:10:01.765891Z  INFO lol_cluster: Initializing cluster...
2024-10-06T23:10:01.830268Z  INFO lol_cluster: Cluster initialized!
```

You can then start leader and follower nodes:

```
$ cargo run --example lol_cluster -- -p /tmp/lol leader

2024-10-06T23:10:16.499624Z  INFO lol_cluster::leader: Joining cluster...
2024-10-06T23:10:16.499647Z  INFO lol_cluster::leader: Joined cluster! Listening on http://192.168.1.175:40243
2024-10-06T23:10:16.500117Z  INFO lol_cluster::utils: Initial members:

┌───┬──────────────────────────────────────┬─────────────────────┬─────┬───────┐
│   │ UUID(1)                              │ Address             │ AZ  │ Roles │
├───┼──────────────────────────────────────┼─────────────────────┼─────┼───────┤
│ * │ 0192671a-1830-77f0-98d6-66847b348915 │ 192.168.1.175:40243 │ AZ0 │       │
└───┴──────────────────────────────────────┴─────────────────────┴─────┴───────┘

2024-10-06T23:10:16.500137Z  INFO lol_cluster::leader: Waiting for leadership...
2024-10-06T23:10:16.516264Z  INFO lol_cluster::leader: We are the new leader!
2024-10-06T23:10:16.516448Z  INFO lol_cluster::utils: Leader changed:

┌───┬──────────────────────────────────────┬─────────────────────┬─────┬───────┐
│   │ UUID(1)                              │ Address             │ AZ  │ Roles │
├───┼──────────────────────────────────────┼─────────────────────┼─────┼───────┤
│ * │ 0192671a-1830-77f0-98d6-66847b348915 │ 192.168.1.175:40243 │ AZ0 │       │
└───┴──────────────────────────────────────┴─────────────────────┴─────┴───────┘
```

```
$ cargo run --example lol_cluster -- -p /tmp/lol follower

2024-10-06T23:10:36.508850Z  INFO lol_cluster::follower: Joining cluster...
2024-10-06T23:10:36.646191Z  INFO lol_cluster::follower: Joined cluster! Listening on http://192.168.1.175:45135
2024-10-06T23:10:36.646685Z  INFO lol_cluster::utils: Initial members:

┌───┬──────────────────────────────────────┬─────────────────────┬─────┬───────┐
│   │ UUID(1)                              │ Address             │ AZ  │ Roles │
├───┼──────────────────────────────────────┼─────────────────────┼─────┼───────┤
│ * │ 0192671a-66dc-7340-b550-393f01d5b57e │ 192.168.1.175:45135 │ AZ0 │       │
└───┴──────────────────────────────────────┴─────────────────────┴─────┴───────┘

2024-10-06T23:10:36.647739Z  INFO lol_cluster::utils: Members changed:

┌───┬──────────────────────────────────────┬─────────────────────┬─────┬───────┐
│   │ UUID(2)                              │ Address             │ AZ  │ Roles │
├───┼──────────────────────────────────────┼─────────────────────┼─────┼───────┤
│   │ 0192671a-1830-77f0-98d6-66847b348915 │ 192.168.1.175:40243 │ AZ0 │       │
│ * │ 0192671a-66dc-7340-b550-393f01d5b57e │ 192.168.1.175:45135 │ AZ0 │       │
└───┴──────────────────────────────────────┴─────────────────────┴─────┴───────┘
```

You can now keep adding more followers (or standby leaders) and see how they join the cluster.

## Prior Art

- **Litestream**: Inspired the idea of replicating a SQLite database by physically replicating WAL files, similar to the approach used here.
- **DeltaLake**: Shares the concept of using an object store as the only dependency. The protocol here is similar, though we do not expect write conflicts under normal operations (but conflict detection follows a similar approach).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE.md) file for details.
