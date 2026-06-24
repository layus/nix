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

## Validation

The store has been exercised end-to-end against a **live database** — not
just compiled. Using a `nix` built with the `postgres` feature, the
`smoke-test.sh` round-trip (store open + schema init → `nix copy` →
`path-info` → `store verify` → closure copy with references) passed against
both **PostgreSQL 16** and **CockroachDB v23.1** (the YugabyteDB-class
target). Validated paths:

- store registration, store-URI/config parsing, libpq connection, idempotent
  `initSchema`;
- `addToStore`: content written to the shared dir, canonicalised, NAR hash and
  size verified, metadata registered;
- `queryPathInfo` field round-trip (NAR hash, size, content address, references);
- reference graph: a closure copied in topological order, `Refs` rows recorded,
  references read back;
- NAR integrity via `nix store verify`;
- the FK `RESTRICT` integrity guard (deleting a still-referenced path fails).

See `smoke-test.sh` to reproduce.

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
- [x] `addToStore` / `addToStoreFromDump` on `DistributedStore` — write
      content to the shared filesystem (NAR import and content-addressed
      adds) and register metadata via the backend. The store is now fully
      populatable (`nix copy --to distributed://…`) and queryable.
- [ ] Atomic derivation-output registration (fold `registerDerivationOutputs`
      into `registerValidPaths`).
- [x] DB-coordinated GC: a shared `GCRoots` table (replacing the per-node
      gcroots scan) and a single-row `GCLease` (one collector at a time).
      `addPermRoot` registers roots, `findRoots` reads the cluster-wide union,
      and `collectGarbage` does a lease-guarded mark-and-sweep (live = closure
      of roots over references; dead set deleted from the shared FS and the
      database). Runtime-validated on CockroachDB: closure preserved, garbage
      collected, lease mutual-exclusion enforced.
- [x] GC liveness for in-use paths: a `TempRoots` table where each node
      registers paths it is actively using (e.g. during `addToStore`), kept
      alive by a per-node heartbeat and reclaimed by TTL after a crash. The
      collector treats every non-expired temp root as a root, so a path in use
      on any node is protected cluster-wide. Runtime-validated on CockroachDB:
      a copied-but-unrooted path survives GC while its temp root is live and is
      collected once it expires.
- [ ] Runtime (`/proc`) roots: a per-node agent reporting paths held by
      running processes into `TempRoots`, for processes that hold a path
      without going through `addTempRoot` (defence in depth).
- [x] gRPC transport + app-key auth (core). The `grpc` build feature
      (protoc codegen + grpc++/protobuf), a `NixStore` gRPC service serving any
      `Store` (`grpc-server.cc` + the `nix-grpc-store-server` launcher), a
      `grpc://host:port` client store (`grpc-store.cc`), and app/API-key auth
      (`grpc-auth.cc`). Unary queries plus client-streaming `addToStore` and
      server-streaming `narFromPath`. Runtime-validated against a CockroachDB-
      backed distributed store: query and copy to/from `grpc://` work, and the
      app-key check accepts valid keys and rejects missing/invalid ones.
- [ ] Remaining gRPC ops: builds, GC, realisations, `getFSAccessor`,
      `addToStoreFromDump` (currently throw/no-op over gRPC), plus mTLS and
      OAuth2/OIDC auth providers and gRPC ↔ status error mapping refinements.
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
