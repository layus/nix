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
-- absolute path of a gcroots symlink, qualified with the client hostname for
-- remote clients). This replaces the per-node gcroots directory scan: every
-- node registers its roots here and the collector reads the union.
--
-- Roots behave as LEASES: clients register them remotely and may disappear
-- without ever cleaning them up, so `registered` records the last
-- (re-)registration (adding an existing root refreshes it) and the collector
-- deletes roots older than the store's `gc-root-lifetime` (one week by
-- default) before marking, reclaiming the paths they held in the same run.
create table if not exists GCRoots (
    link text primary key,
    path text not null,
    registered bigint not null default 0           -- epoch seconds of the last (re-)registration
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

-- Per-derivation build lock: ensures two nodes never build the same derivation
-- at once. A node claims the row for a drv when it is unheld or its holder's
-- lease has expired; `expires` (epoch seconds) bounds a crashed builder. The
-- holder renews via a heartbeat and deletes the row when the build finishes.
-- (Also used, keyed by output store path, as the cluster-wide lease that
-- serialises writers of one path across all nodes and workers — the
-- replicated-database replacement for flock, which is unreliable on shared
-- (NFS) filesystems.)
create table if not exists BuildLocks (
    drv_path text primary key,                     -- the lock key (a .drv path, or a store path for write leases)
    holder   text,                                 -- unique per acquisition: <nodeId>#<token>
    expires  bigint not null                       -- epoch seconds when it lapses
);

create index if not exists IndexBuildLocksHolder on BuildLocks(holder);

-- Ready-to-build derivations advertised for WORK STEALING. A node whose build
-- slots are saturated advertises derivations that are ready to build (inputs
-- all valid); an idle node may steal one and build it. Actual execution is
-- still arbitrated by BuildLocks, so advertising is always safe (duplicates
-- are impossible) and a stealable entry is one with no live build lock.
-- Advertisements are renewed by the advertiser's heartbeat and withdrawn when
-- its goal completes; `expires` bounds a crashed advertiser.
create table if not exists BuildQueue (
    drvPath  text primary key,
    node     text not null,                        -- advertising node
    enqueued bigint not null,                      -- epoch seconds of first advertisement
    expires  bigint not null                       -- epoch seconds when the ad lapses
);

create index if not exists IndexBuildQueueExpires on BuildQueue(expires);

-- Build logs, shared across the whole cluster. The worker building a
-- derivation tees its (uncompressed) log into here in ordered chunks as it
-- goes; a worker waiting on the derivation's build lock follows the chunks
-- and streams them to its own client, so every requester of a build sees its
-- log, and `nix log` works from any node. `final` marks the last chunk. The
-- chunks are opaque bytes (the plain builder output for now; a structured
-- format, e.g. protobuf JSON, could replace it without a schema change).
-- Rows are deleted with their derivation when it is garbage-collected.
create table if not exists BuildLogs (
    drvPath text not null,
    seq     bigint not null,                       -- chunk index, from 0
    chunk   bytea not null,
    final   boolean not null default false,
    primary key (drvPath, seq)
);

-- Schema-initialisation coordination. When several nodes open a brand-new
-- database at once, running the DDL above from all of them races (the database
-- rejects concurrent CREATE TABLEs). Instead this single tiny table is created
-- first (the only DDL the nodes still run concurrently; the backend retries it),
-- then exactly one node is elected to create the rest of the schema while the
-- others wait: a node claims the row by setting `creator` (with an `expires`
-- lease so the election survives a crashed creator), builds the schema, and
-- sets `ready = 1`; the waiters poll `ready` and then do nothing.
create table if not exists SchemaInit (
    id      integer primary key,                     -- always 1 (single row)
    ready   integer not null default 0,              -- 1 once the schema exists
    creator text,                                    -- node currently initialising
    expires bigint not null default 0                -- epoch seconds; creator lease
);
