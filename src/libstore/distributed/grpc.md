# gRPC transport for the distributed store (DRAFT)

This is the design for the cluster's network transport: a gRPC API
(`nix-store.proto`) with modern authentication, replacing the
worker-protocol-over-SSH (`ssh-ng`) for remote/cluster traffic. The local
unix-socket worker protocol is unchanged.

Status: **core implemented and runtime-validated.** The `grpc` build feature,
protoc codegen, the server (`grpc-server.cc` + the `nix-grpc-store-server`
launcher), the `grpc://` client store (`grpc-store.cc`), and app-key auth
(`grpc-auth.cc`) exist and pass an end-to-end test (query + copy to/from a
`grpc://` store backed by a CockroachDB distributed store). What remains:
bridging the rest of the operations (builds, GC, realisations, `getFSAccessor`,
`addToStoreFromDump`) and the additional auth providers (mTLS, OIDC) — see the
checklist at the end.

## Why gRPC here

- Remote clients (dev machines, CI) use the cluster as a remote store. A gRPC
  endpoint behind an L7 load balancer (which understands HTTP/2 and per-RPC
  routing) gives transparent load-balancing, health checks, and auth
  termination — a better fit than L4-fronting raw SSH.
- gRPC brings deadlines, retries, streaming, and mature client libraries.
- It decouples client identity from SSH key distribution: app keys / OAuth /
  mTLS instead of `authorized_keys`.

Clients remain **cluster-unaware**: one stable endpoint URL + a credential.

## The seam (why this is not a rewrite)

The `Store` virtual interface is already the RPC boundary. `daemon.cc`'s
`performOp` reads a worker-protocol op and dispatches to a `Store` method;
`RemoteStore` is the mirror-image client. Everything flows through abstract
`Source`/`Sink`. So:

- **Server**: a gRPC service implementation is an *alternative dispatcher* that
  calls the **same `Store` methods** `performOp` calls. Wrap the gRPC
  reader/writer in `Source`/`Sink` adapters (`GrpcSource`/`GrpcSink`) and the
  existing NAR streaming code is reused unchanged.
- **Client**: a `GrpcStore : RemoteStore`-shaped subclass marshals each `Store`
  method over a gRPC channel, reusing `RemoteStore`'s connection-pool pattern.

## Streaming

- `AddToStore` / `AddMultipleToStore` → **client-streaming**: first message is
  the `PathInfo`, subsequent messages are NAR byte chunks. The server feeds the
  chunk stream into a `GrpcSource` and calls `addToStore`.
- `NarFromPath` → **server-streaming**: `narFromPath` writes into a `GrpcSink`
  that emits `NarChunk`s.
- Build log/progress (today's `STDERR_NEXT` / `STDERR_START_ACTIVITY` … on the
  worker protocol) → the server-streamed `BuildEvent` channel of `BuildPaths` /
  `BuildDerivation`, terminated by a `BuildResult`. This replaces inlining log
  control messages into the data stream.

Backpressure is handled by gRPC flow control; chunk size ~64 KiB (matching the
worker protocol's framing chunks).

## Authentication

A dumb preshared token. The server is configured with a token; the client
sends it in the `auth-token` metadata header; the server checks it for
equality (`grpc-auth.cc`). An empty token disables auth. That's the whole
scheme — no providers, principals, scopes, or certificate handling.

Run it over TLS if the token must not travel in the clear (`grpc++` channel
credentials); the token itself stays a simple shared secret.

## Build wiring (first implementation step)

Add an optional `grpc` meson feature (mirroring the `postgres` feature):

- depend on `grpc++` and `protobuf` (and `protobuf-compiler` / `grpc_cpp_plugin`
  as native build tools);
- a meson custom target runs `protoc` on `nix-store.proto` to generate
  `nix-store.pb.{h,cc}` and `nix-store.grpc.pb.{h,cc}`;
- compile the generated sources + the server/client behind `NIX_WITH_GRPC`,
  exactly as the postgres backend is behind `NIX_WITH_POSTGRES`;
- add `grpc++` / `protobuf` to `package.nix` under a `withGrpc` flag.

## Implementation checklist

- [x] `grpc` meson feature + `protoc` codegen target + `package.nix` wiring.
- [x] `ChunkSink` / `ChunkSource` (`Sink`/`Source` over gRPC streams), in
      `grpc-common.hh`.
- [x] gRPC server: service impl serving a `Store` (`grpc-server.cc`), with the
      `nix-grpc-store-server` launcher. Core ops: `IsValidPath`,
      `QueryValidPaths`, `QueryPathInfo`, `QueryPathFromHashPart`, `AddToStore`,
      `NarFromPath`.
- [x] `GrpcStore` client (`grpc://host:port`), `grpc-store.cc`.
- [x] Preshared-token auth (`grpc-auth.cc`).
- [x] Reference queries (`QueryReferrers`, `QueryValidDerivers`), `AddSignatures`,
      realisations (`RegisterDrvOutput`, `QueryRealisation`), roots/GC
      (`AddTempRoot`, `AddPermRoot`, `FindRoots`, `CollectGarbage`), and
      `BuildPaths`. `GrpcStore` is now also a `GcStore`. GC over `grpc://`
      runtime-validated against the distributed store.
- [x] `BuildDerivation`: the derivation travels as a **JSON blob** in the
      request (`BuildDerivationRequest.drv`), serialised and parsed with Nix's
      standard `BasicDerivation` `adl_serializer` on both ends. The result
      (success + built outputs, or the error) comes back on the `BuildEvent`
      stream.
- [x] `QueryMissing`; `AddToStoreFromDump` (header + streamed dump, content
      method/algo carried as a `renderWithAlgo` string); `getFSAccessor` (a
      `RemoteFSAccessor` over `NarFromPath`). The store surface is now
      essentially complete over gRPC.
- [x] Client-side cluster failover, with **no node failure ever fatal**. The
      `grpc://` store takes a `nodes` parameter (comma-separated `host:port`);
      every operation runs through `withFailover`, which on a transport failure
      (`UNAVAILABLE`/`DEADLINE_EXCEEDED`) advances to the next node and **restarts
      the operation from scratch**, cycling through all nodes before giving up.
      A genuine operation error is not retried (it would fail the same
      everywhere). To make restart safe for the streaming ops, the payload is
      buffered to a temp file: uploads (`addToStore`/`addToStoreFromDump`)
      replay the buffer on each attempt; downloads (`narFromPath`) buffer the
      whole NAR and only write the caller's sink after a fully successful
      fetch. Unary ops and builds/GC re-run directly.
- [ ] Forwarding build logs/progress through the `BuildEvent` stream
      (result-only for now); map errors ↔ gRPC status codes; node-discovery
      beyond a static list (DNS / coordinator).

## How it composes with the rest

A node runs: its local unix-socket daemon (unchanged), the gRPC server (this
doc), and a `DistributedStore` (shared FS + Postgres/YugabyteDB metadata, with
coordinated GC). Remote clients hit the gRPC endpoint behind an L7 LB; the gRPC
server services them via the `DistributedStore`, whose metadata writes are made
safe across nodes by the database. See the top-level design plan for the full
cluster picture (topology, failover, client connectivity).
