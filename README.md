# litecluster

An experimental Rust library designed for building clustered applications with a simple leader-follower architecture. This is a work in progress and in its early stages of development, aimed at exploring clustered applications that rely on external object stores for state management.

## Project Overview

- The main goal is to have no other dependency than an object store (e.g., no need for a separate database like PostgreSQL) while supporting a resilient architecture with standby nodes, multi-availability zone (AZ) deployment, and built-in backups. The system is designed so that the loss of any single node does not result in an outage.

- A framework to build applications with one leader and multiple follower nodes.

- Nodes are designed to be as stateless as possible, meaning they won't rely on local storage for durable state. Essentially, nodes are diskless.

- The state is stored and managed in an object store, such as S3 (though initial versions will not support S3 due to the need for atomic operations).

- A SQLite database is used to manage cluster state, where only the leader node can write and follower nodes have read-only access.

- Write operations will be relatively high latency, targeting roughly \~1 second, since writes will be acknowledged only once they are safely replicated to the object store.

- The database is suited for managing the control plane of the cluster, not the data plane, as this project targets applications that use an object store for data.

## Rough Design

- The library will use SQLite to manage state, with the Write-Ahead Log (WAL) synced to an object store, similar to [litestream](https://github.com/benbjohnson/litestream).
- The approach will be simplified, as the SQLite database will be managed entirely in-process.
- Follower nodes will replicate the WAL log in near real time, aiming for \~1 second latency.
