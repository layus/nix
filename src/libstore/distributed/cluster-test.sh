#!/usr/bin/env bash
#
# Two-node cooperation + failover test for the distributed store over gRPC.
#
# Brings up a CockroachDB instance and TWO independent gRPC store nodes as
# separate Docker containers. The nodes do not talk to each other: they are
# symmetric and stateless, cooperating only through shared backing state --
#   * a shared metadata database (CockroachDB), and
#   * a shared content volume (the `real=` directory, i.e. the store bytes).
# The host's `nix` is the client. The test proves:
#   1. a path written via node1 is queryable and verifiable via node2
#      (shared metadata + shared content), and
#   2. after node1 is killed, the cluster URI keeps working via node2
#      (client-side failover; no node failure is fatal).
#
# Packaging note: rather than building a nix Docker image, this reuses the
# already-built binaries from the meson build directory, bind-mounting the host
# `/nix/store` (read-only, for their closure) and the build dir into minimal
# containers. So it needs a grpc+postgres-enabled build present in $BUILD_DIR.
#
# NOT covered here: builds. A node's backing store is `distributed://` (a
# content+metadata store, not a builder), so derivations cannot be executed
# against the cluster. Build-over-gRPC is validated separately against a
# daemon-backed server (see grpc.md).
#
# Prerequisites:
#   * Docker;
#   * a `nix` + `nix-grpc-store-server` built with `-Dgrpc=enabled
#     -Dpostgres=enabled` in $BUILD_DIR (defaults to ./build-both).
#
# Usage (from the repo root):
#   ./src/libstore/distributed/cluster-test.sh
# Override any of the env vars below as needed.
set -euo pipefail

BUILD_DIR=$(realpath "${BUILD_DIR:-build-both}")
NIX=${NIX:-$BUILD_DIR/src/nix/nix}
SERVER_IN_CONTAINER=/build/src/libstore/nix-grpc-store-server
NODE_IMAGE=${NODE_IMAGE:-busybox}
COCKROACH_IMAGE=${COCKROACH_IMAGE:-cockroachdb/cockroach:latest}
TOKEN=${TOKEN:-cluster-secret}
NET=${NET:-nixcluster}
DB=${DB:-nixcluster}

nix_cmd=("$NIX" --extra-experimental-features nix-command)
CONTENT=$(mktemp -d)

cleanup() {
  echo "== cleanup =="
  docker rm -f node1 node2 cockroach >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  # Copied store paths are made read-only and root-owned; relax perms in a
  # throwaway root container so the host can delete the content dir.
  docker run --rm --user 0 -v "$CONTENT:/c" "$NODE_IMAGE" chmod -R u+w /c >/dev/null 2>&1 || true
  rm -rf "$CONTENT" || true
}
trap cleanup EXIT

# Idempotent pre-clean in case a previous run left things around.
docker rm -f node1 node2 cockroach >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true

echo "== network + CockroachDB (single-node, insecure) =="
docker network create "$NET" >/dev/null
docker run -d --name cockroach --network "$NET" "$COCKROACH_IMAGE" \
  start-single-node --insecure >/dev/null
for _ in $(seq 1 60); do
  if docker exec cockroach cockroach sql --insecure -e 'select 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec cockroach cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS $DB" >/dev/null
echo "   CockroachDB ready, database '$DB' created"

DBURL="postgresql://root@cockroach:26257/$DB"
BACKING="distributed://?metadata-db-url=$DBURL&real=/cluster/store"

run_node() {
  local name=$1
  docker run -d --name "$name" --network "$NET" --user 0 \
    -v /nix/store:/nix/store:ro \
    -v "$BUILD_DIR:/build:ro" \
    -v "$CONTENT:/cluster/store" \
    -e HOME=/tmp -e NIX_STATE_DIR=/tmp/nix-state -e NIX_CONF_DIR=/tmp \
    "$NODE_IMAGE" \
    "$SERVER_IN_CONTAINER" 0.0.0.0:5570 "$BACKING" "$TOKEN" >/dev/null
}

# The host reaches each node directly on the user-defined bridge (no published
# ports needed -- host port publishing is unavailable on some firewall setups).
node_ip() { docker inspect -f "{{.NetworkSettings.Networks.$NET.IPAddress}}" "$1"; }

wait_node() {
  local name=$1
  for _ in $(seq 1 30); do
    if docker logs "$name" 2>&1 | grep -qi 'listening'; then return 0; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
      echo "node $name exited early:"; docker logs "$name"; return 1
    fi
    sleep 1
  done
  echo "node $name did not become ready:"; docker logs "$name"; return 1
}

# Start node1 first and let it create the schema before node2 joins, to avoid
# two concurrent CREATE TABLE IF NOT EXISTS races against CockroachDB.
echo "== start node1 (creates schema) =="
run_node node1; wait_node node1
echo "== start node2 =="
run_node node2; wait_node node2

IP1=$(node_ip node1); IP2=$(node_ip node2)
echo "   node1=$IP1  node2=$IP2"
N1="grpc://$IP1:5570?auth-token=$TOKEN"
N2="grpc://$IP2:5570?auth-token=$TOKEN"
CLUSTER="grpc://$IP1:5570?nodes=$IP1:5570,$IP2:5570&auth-token=$TOKEN"

echo "== write a path via node1 ONLY =="
tmp=$(mktemp)
echo "distributed cluster cooperation test $(date)" > "$tmp"
SRC=$(nix-store --add "$tmp")
echo "   added $SRC"
"${nix_cmd[@]}" copy --no-check-sigs --to "$N1" "$SRC"

echo "== read it back via node2 ONLY (proves shared metadata DB) =="
"${nix_cmd[@]}" path-info --store "$N2" "$SRC"

echo "== verify content via node2 (re-reads the NAR; proves shared content volume) =="
"${nix_cmd[@]}" store verify --store "$N2" "$SRC"

echo "== failover: stop node1, query via the cluster URI =="
docker stop node1 >/dev/null
"${nix_cmd[@]}" path-info --store "$CLUSTER" "$SRC"

echo "== rejoin: restart node1, query node1 directly to confirm it serves again =="
docker start node1 >/dev/null; wait_node node1
IP1=$(node_ip node1)
"${nix_cmd[@]}" path-info --store "grpc://$IP1:5570?auth-token=$TOKEN" "$SRC" >/dev/null

echo "== OK: two nodes cooperate and the client fails over =="
