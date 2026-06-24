# Distributed store (work in progress)

This directory holds the in-progress work to let Nix run as a **symmetric
high-availability cluster**: multiple nodes sharing one store (content on
shared storage), backed by a **truly distributed transactional database**
for metadata, fronted by a **gRPC** API with pluggable authentication.

It is an opt-in alternative to the single-machine `LocalStore`; nothing
here changes existing single-node behaviour.

## Why

`LocalStore` bakes in single-machine assumptions: an embedded SQLite DB
(WAL does not work over NFS), `flock`-based locks (unreliable across NFS
clients), PID-named GC temp roots, and `/proc`-local liveness scanning for
garbage collection. Sharing a store across nodes safely therefore needs a
real coordination substrate. A distributed transactional DB provides
exactly that — atomic cross-node operations, replication, and leader
failover — which lets the nodes be symmetric (any node reads and writes)
and reduces Nix's own job to porting a small metadata model behind a clean
seam.

## Architecture (target)

- **Content plane:** `/nix/store` on shared storage; immutable and
  content-addressed, so no cross-node content locking is needed.
- **Metadata plane:** the four tables of the store's relational model
  (valid paths, the reference graph, derivation outputs, realisations) in
  a distributed transactional DB. See `schema.sql`.
- **Coordination plane:** GC lease, per-node roots, and build coordination
  as rows/transactions in the same DB — replacing `flock`, the gc-socket,
  and PID temp roots.
- **Transport plane:** a gRPC server per node (network, authenticated) plus
  the existing unix-socket worker protocol for local clients. A `GrpcStore`
  client mirrors `RemoteStore`.

## The seam

`MetadataBackend` (`../include/nix/store/metadata-backend.hh`) is the
abstract interface that decouples the metadata model from its storage
engine. It is keyed on `StorePath`, never on a storage-engine rowid, and
documents the atomic boundaries every backend must preserve. Today only
SQLite (inside `LocalStore`) implements the equivalent operations; the
distributed work adds a second implementation behind this interface.

## Status / roadmap

- [x] `MetadataBackend` interface (the seam).
- [x] `schema.sql` — portable, `StorePath`-keyed port of the SQLite schema
      (no `AUTOINCREMENT`, no triggers; FK / app-transaction integrity).
- [x] Optional `postgres` build feature (libpq) wired into meson/package.nix.
- [x] `PostgresMetadataBackend` (libpq) — implements the seam against the
      PostgreSQL wire protocol. **Chosen substrate: YugabyteDB**, whose
      YSQL layer is Postgres-wire-compatible, so this one backend serves
      both local prototyping (plain PostgreSQL) and the production
      distributed cluster (YugabyteDB) unchanged. (Compiles; runtime
      testing pending a Postgres/YugabyteDB instance.)
- [x] `DistributedStore` — a `LocalFSStore` (content on shared storage)
      whose metadata virtuals are served by a `MetadataBackend`. Registered
      as `distributed://`; additive (does not touch `LocalStore`/SQLite).
      Read/query/registration routed to the backend; content from the
      shared filesystem. (Compiles; runtime testing pending a database.)
- [ ] `addToStore` on `DistributedStore` — write content to the shared
      filesystem and register metadata via the backend (currently throws).
- [ ] Atomic derivation-output registration (fold `registerDerivationOutputs`
      into `registerValidPaths`).
- [ ] DB-coordinated GC: roots table + GC lease (replaces gc-socket and
      `/proc`-local liveness); `findRoots`/`collectGarbage` (currently throw).
- [ ] gRPC transport + pluggable auth (app-keys first).
- [ ] `SQLiteMetadataBackend` — optionally relocate `LocalStore`'s SQL behind
      the seam for code sharing (not required for the distributed store).
- [ ] DB-coordinated GC (roots table + GC lease) replacing the gc-socket
      and `/proc`-local liveness.
- [ ] gRPC transport (`.proto`, server reusing the `Store` dispatch,
      `GrpcStore` client, streaming for NAR transfer).
- [ ] Pluggable auth interceptor (app/API keys first, then mTLS, OIDC).

See the design plan for the full rationale, the database-substrate
comparison (including MongoDB), client connectivity, and failover/rejoin
semantics.
