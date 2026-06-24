-- Portable, StorePath-keyed metadata schema for a distributed Nix store
-- backend (Phase 0 of the distributed-store effort).
--
-- This is the relational model of src/libstore/schema.sql and
-- src/libstore/ca-specific-schema.sql, re-expressed so that it can be
-- hosted by a distributed SQL database (PostgreSQL / CockroachDB /
-- YugabyteDB) or a Raft-replicated SQLite (dqlite / rqlite). It is the
-- reference DDL for the prototype PostgresMetadataBackend.
--
-- Differences from the embedded SQLite schema, and why:
--
--   * No integer AUTOINCREMENT rowid. The SQLite schema joins on an
--     internal `id`, obtained via last_insert_rowid(). That id is never
--     cached in C++ across calls, so it is pure within-transaction glue.
--     Here the store path text (already UNIQUE) is the primary key, which
--     removes the insert-then-read-rowid round trip — a poor fit for a
--     distributed DB — and makes every write idempotent on the natural key.
--
--   * No triggers. SQLite uses a DeleteSelfRefs trigger to drop the (N, N)
--     self-reference before deleting a path (otherwise the `on delete
--     restrict` on Refs.reference fires). Triggers are not portable across
--     all candidate backends, so a backend MUST instead delete a path's
--     self-reference inside the same transaction as the path deletion (see
--     MetadataBackend::invalidatePath / invalidatePathChecked). On
--     backends that do support triggers this can be reinstated verbatim.
--
--   * Foreign keys are kept where the backend supports them (Postgres,
--     Cockroach, Yugabyte, SQLite). On a transactional KV backend with no
--     declarative FKs, the same cascade/restrict semantics are enforced in
--     the operation's transaction. The semantics, not the mechanism, are
--     the contract.
--
-- Types are written in portable SQL. `text` and `bigint` are accepted by
-- all SQL candidates; SQLite treats them with its usual affinity rules.

create table if not exists ValidPaths (
    path             text primary key,            -- full store path; natural key
    hash             text not null,               -- base16 NAR hash
    registrationTime bigint not null,             -- epoch seconds
    deriver          text,                        -- store path of the deriver, or null
    narSize          bigint,                       -- NAR size in bytes, or null
    ultimate         boolean not null default false,
    sigs             text,                         -- space-separated signatures
    ca               text                          -- content-address assertion, or null
);

create table if not exists Refs (
    referrer  text not null,
    reference text not null,
    primary key (referrer, reference),
    foreign key (referrer)  references ValidPaths(path) on delete cascade,
    foreign key (reference) references ValidPaths(path) on delete restrict
);

create index if not exists IndexReference on Refs(reference);

create table if not exists DerivationOutputs (
    drv  text not null,                            -- deriver store path
    id   text not null,                            -- symbolic output id, usually "out"
    path text not null,                            -- output store path
    primary key (drv, id),
    foreign key (drv) references ValidPaths(path) on delete cascade
);

create index if not exists IndexDerivationOutputs on DerivationOutputs(path);

-- Realisations of content-addressed derivation outputs (the BuildTraceV3
-- table). The SQLite version carries an unused AUTOINCREMENT id; the
-- natural key is (drvPath, outputName), which becomes the primary key
-- here. Signatures accumulate via upsert.
create table if not exists Realisations (
    drvPath    text not null,
    outputName text not null,                      -- symbolic output id, usually "out"
    outputPath text not null,
    signatures text,                               -- space-separated list
    primary key (drvPath, outputName)
);

create index if not exists IndexRealisations on Realisations(drvPath, outputName);

-- Garbage-collector roots, shared across the whole cluster. Each row roots a
-- store path; `link` is the cluster-unique identifier of the root (e.g. the
-- absolute path of a gcroots symlink). This replaces the per-node gcroots
-- directory scan: every node registers its roots here and the collector reads
-- the union.
create table if not exists GCRoots (
    link text primary key,
    path text not null
);

create index if not exists IndexGCRootsPath on GCRoots(path);

-- Single-row lease ensuring at most one node runs the garbage collector at a
-- time. A node acquires the lease by claiming the row when it is unheld or its
-- lease has expired; `expires` (epoch seconds) bounds a crashed holder.
create table if not exists GCLease (
    id      integer primary key,                   -- always 1 (single lease)
    holder  text,                                  -- node id currently holding it
    expires bigint not null                        -- epoch seconds when it lapses
);

-- Per-node temporary roots: paths a node is actively using (e.g. a path being
-- copied/realised) and must not be collected. Each node keeps its rows alive
-- with a heartbeat that pushes `expires` forward; a crashed node's temp roots
-- lapse once `expires` passes. The collector treats every non-expired temp
-- root as a GC root, so a path in use on any node is protected cluster-wide.
create table if not exists TempRoots (
    node    text not null,
    path    text not null,
    expires bigint not null,                       -- epoch seconds when it lapses
    primary key (node, path)
);

create index if not exists IndexTempRootsExpires on TempRoots(expires);
