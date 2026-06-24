#!/usr/bin/env bash
#
# End-to-end smoke test for the distributed store.
#
# Exercises the real C++ code path (PostgresMetadataBackend + DistributedStore)
# against a live PostgreSQL-wire database: store open + schema init, addToStore
# (NAR import with content write + canonicalisation + hash verification),
# queryPathInfo, the reference graph (closure copy in topological order), and
# NAR integrity (`nix store verify`).
#
# This has been run successfully against both PostgreSQL 16 and CockroachDB
# v23.1 (the YugabyteDB-class target).
#
# Prerequisites:
#   * a `nix` built with the `postgres` feature (-Dlibstore:postgres=enabled);
#   * a reachable PostgreSQL-wire database and an empty database to use.
#
# Usage:
#   NIX=/path/to/nix \
#   PGURL='postgresql://root@localhost:26257/nixstore' \
#   REAL=/tmp/diststore \
#   ./smoke-test.sh
set -euo pipefail

NIX=${NIX:-nix}
PGURL=${PGURL:?set PGURL to a libpq connection string for an empty database}
REAL=${REAL:-$(mktemp -d)}
STORE="distributed://?metadata-db-url=${PGURL}&real=${REAL}"
nix_cmd=("$NIX" --extra-experimental-features nix-command)

echo "== store info (opens the store, connects, runs initSchema) =="
"${nix_cmd[@]}" store info --store "$STORE"

echo "== addToStore: add a path locally, copy it into the distributed store =="
tmp=$(mktemp)
echo "distributed nix smoke test" > "$tmp"
src=$(nix-store --add "$tmp")
"${nix_cmd[@]}" copy --no-check-sigs --to "$STORE" "$src"

echo "== queryPathInfo round-trip =="
"${nix_cmd[@]}" path-info --store "$STORE" --json "$src"

echo "== NAR integrity (re-reads content, recomputes & checks the NAR hash) =="
"${nix_cmd[@]}" store verify --store "$STORE" "$src"

echo "== reference graph: copy a closure that has a real reference =="
# Pick the smallest local path that references another (non-self) path.
ref_path=$(sqlite3 -readonly "${NIX_STATE_DIR:-/nix/var/nix}/db/db.sqlite" \
  "select v.path from ValidPaths v
     where exists(select 1 from Refs r where r.referrer=v.id and r.reference!=r.referrer)
     order by v.narSize asc limit 1;")
echo "closure top: $ref_path"
"${nix_cmd[@]}" copy --no-check-sigs --to "$STORE" "$ref_path"
echo "references read back from the distributed store:"
"${nix_cmd[@]}" path-info --store "$STORE" --json "$ref_path"

echo "== OK: distributed store round-trip succeeded =="
