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

### Two-node cooperation + failover (Docker)

`cluster-test.sh` proves that **two separate nodes cooperate**. It brings up a
CockroachDB container and two gRPC store nodes as independent Docker containers
that share one database and one content volume (the nodes never talk to each
other — they cooperate only through that shared backing state). Using the host's
`nix` as the client, it: writes a path via node1 only, reads and verifies it via
node2 only (shared metadata + shared content), then stops node1 and confirms the
cluster URI (`grpc://h1?nodes=h1,h2`) keeps serving via node2 (client failover),
and that node1 serves again after rejoining.

It reuses the binaries from the meson build directory (bind-mounting the host
`/nix/store` read-only), so it needs a grpc+postgres build present in
`$BUILD_DIR` (default `./build-both`) and Docker. Run from the repo root:

```
./src/libstore/distributed/cluster-test.sh
```

Builds are out of scope here: a node's backing store is `distributed://` (a
content+metadata store, not a builder). Build-over-gRPC is validated separately
against a daemon-backed server (see `grpc.md`).

### Concurrent builds over a real NFS share (Docker)

`stress-test.sh` goes further than `cluster-test.sh`: instead of faking the
shared content plane with a bind mount, it uses a **genuine NFS share** as the
store (the host exports `/mnt/nix-store` over NFSv4; both nodes are NFS clients
mounting it read-write), and it makes the nodes **actually build** — with real
`nixbld` build users and the Nix sandbox (namespaces nested inside privileged
containers). It then drives concurrent builds:

- **Phase 1** fires the *same* slow derivation at both nodes at once, racing the
  per-derivation DB build lock (one node builds; the other waits on the lock,
  then reuses the result). A read-only NFS mount independently confirms the
  output landed on the share.
- **Phase 2** builds a large realistic closure (`home-manager` activation
  package, or any flake attr via `HM_FLAKE`/`HM_ATTR`) at both nodes at once as
  a throughput stress.

The pass bar is *no crash*: both nodes stay up, and every output is valid on the
shared store. This test found and fixed a real bug — `addToStore`/
`addToStoreFromDump` canonicalised with an empty ACL-ignore set, so the
un-removable `system.nfs4_acl` xattr that NFSv4 synthesises on every file made
*every* write to an NFS-backed store fail with `EINVAL`; the fix honours the
`ignored-acls` setting as `LocalStore` does. Run from the repo root (needs the
host `nfsd` module and an NFSv4 export of `/mnt/nix-store`; see the script
header for the exact NixOS config):

```
./src/libstore/distributed/stress-test.sh
```

### Simple two-node cooperation over NFS (Docker)

`simple-test.sh` is the minimal end-to-end story, kept deliberately small so a
regression is easy to localise: two gRPC nodes (NFS clients of the host's
export, sharing one CockroachDB), a derivation **built via node1 only**, and
the pass bar checked from the *other* node — node2 must report the output as a
valid path, `nix store verify` must succeed through node2, and an independent
read-only NFS mount must find the output physically on the export. The build
is wrapped in `timeout` and a timeout is reported loudly as `HANG`, so it
doubles as a cheap regression guard for the intermittent gRPC client hang.

It also establishes the **shared-store layout convention**: the NFS export is
no longer the bare store but a structured share, mounted at `/cluster`, with
two well-known directories:

- `/store` — the store contents (`real=/cluster/store`);
- `/var/replicas` — the node registry. Nodes **discover each other** through
  it: `nix-grpc-store-server`, given an advertise address, registers it in
  `/var/replicas/<hostname>` on startup and unregisters on graceful shutdown
  (SIGTERM/SIGINT). Registrations carry a **freshness TTL** (TempRoots-style):
  each node's heartbeat refreshes its file every TTL/3 and sweeps peers'
  entries that outlived their declared TTL, so a *crashed* node's entry
  disappears too. A client passes `grpc://?registry=<dir>` and its failover
  node list is read from the registry, skipping stale entries — no static
  node list anywhere. Discovery requires seeing the share: a client that
  cannot read the registry (or finds no live entry in it) reports an error
  and shuts down — there is deliberately no fallback discovery path. The test
  asserts all of it: the registry-discovering URI serves paths, an unreadable
  registry is fatal, and a planted dead-node entry is both skipped by clients
  and swept by the live nodes' heartbeats.

Same host prerequisites as `stress-test.sh`; run from the repo root:

```
./src/libstore/distributed/simple-test.sh
```

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
- [x] **Distributed builds.** `DistributedStore` is now a `LocalStore` (was a
      plain `LocalFSStore`), so each node builds derivations locally with the
      full build machinery, writing outputs into the shared `real=` filesystem
      and registering their metadata — including each derivation's
      `DerivationOutputs` map — in the shared database (the node-local SQLite a
      `LocalStore` opens is vestigial; every metadata virtual the build/GC need
      is overridden to the backend). Runtime-validated: `nix build` against a
      `distributed://` node executes a derivation, the output lands on the
      shared dir and in Postgres/CockroachDB, and a second node sees it. So a
      node's gRPC server, backing a `distributed://` store, can now actually
      run builds (not just forward them). Each node needs its own node-local
      `state=` dir.
- [x] **Per-derivation DB build lock** so two nodes never build the same
      derivation at once: a `BuildLocks` table (`drv_path` PK + holder + TTL,
      modelled on `GCLease`), `acquire/release/renewBuildLock` on the backend,
      a `Store::tryLockBuild` seam (default no-op handle), and a hook in the
      build goal that holds a single cluster lock for the build's duration
      alongside the existing machine-local `PathLocks`. Held and released
      correctly around a build; isolating its cross-node effect on a single
      host is masked by `PathLocks`/SQLite serialisation, so multi-host
      enforcement is pending verification on a real cluster.
- [x] **Race-free schema initialisation.** Several nodes opening a brand-new
      database at once previously raced on the `CREATE TABLE`s (the database
      rejects concurrent DDL). Now a one-row `SchemaInit` table is bootstrapped
      first (the only still-concurrent DDL, retried on the transient error),
      then a single node is elected — via a `creator` claim with an `expires`
      lease so the election survives a crashed creator — to build the rest of
      the schema while the others wait on a `ready` flag. Runtime-validated:
      5 nodes opening one fresh database concurrently all succeed, with the
      schema created exactly once.
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
- [x] gRPC transport + preshared-token auth (core). The `grpc` build feature
      (protoc codegen + grpc++/protobuf), a `NixStore` gRPC service serving any
      `Store` (`grpc-server.cc` + the `nix-grpc-store-server` launcher), a
      `grpc://host:port` client store (`grpc-store.cc`), and a dumb preshared
      token check (`grpc-auth.cc`). Unary queries plus client-streaming
      `addToStore` and server-streaming `narFromPath`. Runtime-validated against
      a CockroachDB-backed distributed store: query and copy to/from `grpc://`
      work, and the token check accepts the right token and rejects others.
- [x] Remaining gRPC ops fleshed out: reference queries, signatures,
      realisations, roots, garbage collection (`GrpcStore` is now also a
      `GcStore`), and `BuildPaths`. GC over `grpc://` runtime-validated against
      the distributed store (garbage collected, rooted/temp-rooted paths kept).
- [x] `BuildDerivation` over gRPC, with the derivation sent as a JSON blob
      (Nix's standard `BasicDerivation` JSON format).
- [x] `QueryMissing`, `addToStoreFromDump`, and `getFSAccessor` over gRPC
      (the last via a `RemoteFSAccessor` over `NarFromPath`). Add-file and
      `store cat` over `grpc://` runtime-validated. The store interface is now
      essentially complete over gRPC.
- [x] Client-side cluster failover, **no node failure fatal**:
      `grpc://host?nodes=h1:p,h2:p,…` restarts each operation from scratch on
      the next node when one is unreachable. Streaming ops buffer their payload
      (uploads replay it; downloads buffer the NAR and write the sink only on
      success) so even a mid-stream node death just restarts elsewhere.
      Runtime-validated (kill a node; queries, copies to/from, add-file, cat,
      and GC all fail over to a live node).
- [x] Builds over gRPC: `nix-store --realise` and `nix build --store grpc://`
      both forward the build (the latter via `buildPathsWithResults`), with the
      builder's output streamed back live (`-L`). Behaves identically to a
      direct build.
- [x] **Fixed: spurious `MissingRealisation` error after successful gRPC
      builds.** A client with `ca-derivations` enabled (as in this host's
      `/etc/nix/nix.conf`) failed every `nix build --store grpc://… <drv>^*`
      with `cannot operate on output 'out' of the unbuilt derivation …` even
      though the build had succeeded server-side: `GrpcStore::
      buildPathsWithResults`, when synthesising per-path results, demanded a
      registered realisation for every output, but a server without the CA
      feature never registers realisations for input-addressed builds. This
      was deterministic, not cosmetic (it had been misread as a `^*` exit-code
      quirk in the stress test). The fix falls back to the statically resolved
      output path when no realisation is registered — safe because a genuinely
      unbuilt CA derivation already throws during `resolveDerivedPath`. (The
      same latent pattern exists upstream in `RemoteStore`'s pre-1.34
      fallback, `remote-store.cc`.) Runtime-validated: fresh build over
      `grpc://` with `ca-derivations` on now exits 0.
- [x] **Structured build-log forwarding** — the gRPC analogue of the daemon's
      `TunnelLogger`/`processStderr` pair. The server-side `GrpcLogger` used to
      flatten everything (activities, progress, log lines) into bare strings
      that the client printed at `lvlInfo`, so levels, the activity tree, and
      progress were lost and builder output spammed the client even without
      `-L`. Now `BuildEvent` carries the `Logger` calls verbatim (log messages
      with their level, activity start/stop with type/fields/parent, results
      including `resBuildLogLine` and progress), and the client replays them
      into its own logger, mapping server activity ids onto fresh local RAII
      `Activity` objects (a mid-stream failover retry stops any orphans). The
      client sends its verbosity with the build request; the server gates
      plain log messages on it. Runtime-validated: `nix build -L` over
      `grpc://` renders builder lines with the local `drvname>` prefix exactly
      like a local build, without `-L` they are suppressed, and a failing
      build streams its last lines live before the structured error.
- [x] **Node discovery via the share** (`/var/replicas`). Server side:
      `nix-grpc-store-server <listen> <store> [token] [advertise-addr]` —
      given an advertise address, the node registers it in the share's
      `var/replicas/<hostname>` on startup and unregisters on graceful
      shutdown (SIGTERM/SIGINT via a signal-safe shutdown pipe). Registration
      is opt-in precisely because daemon-backed stores can also be served and
      must not scribble a registry next to the host's real `/nix/store`; a
      node *asked* to register but unable to is mis-mounted and fails hard.
      Client side: `grpc://?registry=<dir>` reads the failover node list from
      the registry (sorted, deduplicated, authority first if given) — no
      static node list; mutually exclusive with `nodes=`. A client that
      cannot read the registry, or finds it empty, errors out: there is
      deliberately no fallback discovery path. Runtime-validated by
      `simple-test.sh`.
- [x] **Stale-registration TTL** (crashed nodes leave no trace). Modelled on
      `TempRoots`, but on the share: a registration's second line declares a
      freshness TTL (default 60 s, `NIX_REPLICA_TTL` to override) within
      which its node promises to refresh the file's mtime; a heartbeat
      thread refreshes it every TTL/3 (atomically, write + rename, so
      readers never see a half-written entry) and sweeps peers' entries
      that outlived their own TTL. Clients skip stale entries when reading
      the registry ("names no live nodes" if none survive). Sweeping is
      safe against races (ENOENT ignored) and against false positives (a
      live node's next beat rewrites its file whole). NFS attribute caching
      is defused by reading each entry *before* checking its mtime — over
      NFS the open() forces attribute revalidation (close-to-open
      consistency); reasonably synchronised clocks (NTP) are assumed.
      Runtime-validated: heartbeat refreshes observed, a planted dead entry
      (TTL 1 s) skipped by a `--debug` client and swept by a live node, live
      registrations untouched, graceful unregistration still clean.
- [x] Error-mapping cleanup: remote errors are now sent without the rendered
      `error:` prefix (`Error::message()` instead of `msg()` in the gRPC
      result/status paths), so clients no longer print `error: … error: …`.
- [ ] gRPC ↔ status error-mapping refinements (fidelity of status codes,
      structured traces); richer progress forwarding for non-build operations
      (copy/GC over gRPC still report only coarse client-side activities).
- [ ] **Investigate an intermittent gRPC build-client hang.** During the NFS
      concurrent-build stress test (`stress-test.sh`) the `nix build … ^*` client
      was observed **once** to hang indefinitely *after* the build had already
      succeeded server-side (the output was valid on the shared store). It
      happened on a cold-cache `nixpkgs#hello` built at both nodes simultaneously
      while both were substituting the closure from `cache.nixos.org`. It could
      not be reproduced in ~8 varied isolation attempts (single node, same-drv
      races, dependency chains, and the substitute-during-build scenario all
      returned cleanly), so it is likely a timing-sensitive interaction (a
      deadline-less client `Read` stalling while a slow substitution holds the
      backend's single libpq mutex) rather than a structural deadlock. The stress
      test wraps every build in `timeout` so it cannot wedge the harness.
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
