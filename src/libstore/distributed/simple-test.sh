#!/usr/bin/env bash
#
# SIMPLE two-node cooperation test over a real NFS share.
#
# The minimal end-to-end story the distributed store promises, and nothing
# else (stress-test.sh is the heavy concurrent variant):
#
#   1. two symmetric gRPC store nodes come up as NFS clients of the host's
#      export, sharing one CockroachDB metadata plane;
#   2. a derivation is BUILT VIA NODE1 ONLY;
#   3. NODE2 must then report the output as a valid path (metadata visible
#      through the shared database) and serve its content;
#   4. the output must be PHYSICALLY PRESENT ON THE NFS EXPORT (checked
#      through an independent read-only mount).
#
# Share layout: unlike stress-test.sh (which used the export root as the
# store), the share is now STRUCTURED. The export is mounted at /cluster and
# contains two well-known directories:
#
#   /store         -- the shared store contents (`real=/cluster/store`);
#   /var/replicas  -- the node registry. Nodes DISCOVER EACH OTHER through it:
#                     nix-grpc-store-server ITSELF registers its advertised
#                     gRPC address in /var/replicas/<hostname> on startup
#                     (and unregisters on graceful shutdown). The client then
#                     needs no static node list: this test uses
#                     `grpc://?registry=<dir>` so the failover list is read
#                     from the registry (the host reads it straight from the
#                     export it serves, /mnt/nix-store/var/replicas).
#                     Discovery REQUIRES seeing the share: a client that
#                     cannot read the registry reports an error and shuts
#                     down (no fallback) -- this test checks that too.
#
# KNOWN ISSUE this test guards against: an intermittent gRPC build-client hang
# after a successful server-side build (see README.md roadmap). The build is
# wrapped in `timeout` and a timeout is reported loudly as HANG, but the test
# still passes if the output is valid on the store -- validity from the OTHER
# node is the actual pass bar.
#
# HOST PREREQUISITES: same as stress-test.sh --
#   * Docker;
#   * a grpc+postgres-enabled `nix` + `nix-grpc-store-server` in $BUILD_DIR
#     (defaults to ./build-both);
#   * this machine serving an NFSv4 export of /mnt/nix-store reachable from
#     the Docker bridge (fsid=0, so clients mount <host>:/); the CockroachDB
#     metadata plane also runs here, as a container. See stress-test.sh's
#     header for the exact NixOS export config.
#
# Usage (from the repo root):
#   ./src/libstore/distributed/simple-test.sh
set -euo pipefail

BUILD_DIR=$(realpath "${BUILD_DIR:-build-both}")
NIX=${NIX:-$BUILD_DIR/src/nix/nix}
SERVER_IN_CONTAINER=/build/src/libstore/nix-grpc-store-server
NODE_IMAGE=${NODE_IMAGE:-alpine:3.20}
COCKROACH_IMAGE=${COCKROACH_IMAGE:-cockroachdb/cockroach:latest}
TOKEN=${TOKEN:-cluster-secret}
NET=${NET:-nixsimple}
DB=${DB:-nixsimple}
PORT=5570

HOST_IP=${HOST_IP:-}          # auto-discovered from the docker network gateway
EXPORT=${EXPORT:-/}           # NFSv4 root (fsid=0 -> /mnt/nix-store)
NFS_OPTS=${NFS_OPTS:-vers=4,rw,noatime,hard,timeo=50}
# The registry as seen from THIS host (which serves the export itself).
REGISTRY=${REGISTRY:-/mnt/nix-store/var/replicas}

BUILD_TIMEOUT=${BUILD_TIMEOUT:-120}
NIXBLD_N=${NIXBLD_N:-4}

nix_cmd=("$NIX" --extra-experimental-features 'nix-command flakes')

# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------
cleanup() {
  echo "== cleanup =="
  for n in node1 node2 verifier; do
    docker exec "$n" umount -f -l /cluster >/dev/null 2>&1 || true
    docker exec "$n" umount -f -l /nfs     >/dev/null 2>&1 || true
  done
  docker rm -f node1 node2 verifier cockroach >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f node1 node2 verifier cockroach >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# network + database
# ---------------------------------------------------------------------------
echo "== network =="
docker network create "$NET" >/dev/null
if [ -z "$HOST_IP" ]; then
  HOST_IP=$(docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' "$NET")
fi
echo "   host (NFS server) as seen from containers: $HOST_IP"

echo "== CockroachDB (single-node, insecure, on this host) =="
docker run -d --name cockroach --network "$NET" "$COCKROACH_IMAGE" \
  start-single-node --insecure >/dev/null
for _ in $(seq 1 60); do
  if docker exec cockroach cockroach sql --insecure -e 'select 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec cockroach cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS $DB" >/dev/null
echo "   CockroachDB ready, database '$DB' created"

DBURL="postgresql://root@cockroach:26257/$DB"
# The share is mounted at /cluster; the store lives in its /store subdir.
BACKING="distributed://?metadata-db-url=$DBURL&real=/cluster/store"

# ---------------------------------------------------------------------------
# node helpers (same recipe as stress-test.sh, share mounted at /cluster)
# ---------------------------------------------------------------------------
node_ip() { docker inspect -f "{{.NetworkSettings.Networks.$NET.IPAddress}}" "$1"; }

start_container() {
  local name=$1
  # --hostname: the server registers itself as /var/replicas/<hostname>.
  docker run -d --name "$name" --hostname "$name" --network "$NET" --privileged --user 0 \
    -v /nix/store:/nix/store:ro \
    -v "$BUILD_DIR:/build:ro" \
    -e HOME=/root -e NIX_STATE_DIR=/var/nix-state -e NIX_CONF_DIR=/etc/nix \
    "$NODE_IMAGE" sleep infinity >/dev/null
  docker exec "$name" sh -c 'mkdir -p /var/nix-state /etc/nix && chmod 0755 /var /var/nix-state'
}

install_nfs_client() {
  local name=$1
  echo "   [$name] installing nfs-utils..."
  docker exec "$name" sh -c 'apk add --no-cache nfs-utils shadow >/dev/null 2>&1' \
    || { echo "!!! [$name] apk add nfs-utils failed (needs network egress)"; return 1; }
}

setup_build_users() {
  local name=$1
  docker exec "$name" sh -c "
    addgroup -g 30000 nixbld 2>/dev/null || true
    i=1; while [ \$i -le $NIXBLD_N ]; do
      adduser -S -D -H -G nixbld -u \$((30000 + i)) nixbld\$i 2>/dev/null || true
      i=\$((i + 1))
    done
    getent group nixbld >/dev/null || { echo 'nixbld group missing'; exit 1; }
  " || { echo "!!! [$name] could not create nixbld users"; return 1; }
}

mount_share() {
  local name=$1 target=$2 mode=$3
  local opts="$NFS_OPTS"
  [ "$mode" = ro ] && opts="vers=4,ro,noatime,hard,timeo=50"
  docker exec "$name" sh -c "
    set -e
    mkdir -p '$target'
    mount -t nfs -o '$opts' '$HOST_IP:$EXPORT' '$target'
  " || { echo "!!! [$name] NFS mount of $HOST_IP:$EXPORT at $target failed"; return 1; }
}

launch_server() {
  local name=$1
  docker exec "$name" sh -c "printf '%s\n' \
    'build-users-group = nixbld' \
    'sandbox = true' \
    'builders =' \
    'max-jobs = $NIXBLD_N' \
    'experimental-features = nix-command flakes' > /tmp/nix.conf"
  # The advertised address (4th arg) is what the server registers in the
  # share's /var/replicas registry for clients to discover.
  docker exec -d "$name" sh -c \
    "umask 022; export PATH=/build/src/nix:\$PATH; export NIX_CONFIG=\"\$(cat /tmp/nix.conf)\"; \
     $SERVER_IN_CONTAINER 0.0.0.0:$PORT '$BACKING' '$TOKEN' '$(node_ip "$name"):$PORT' >/tmp/server.log 2>&1"
  for _ in $(seq 1 30); do
    if docker exec "$name" grep -qi 'listening' /tmp/server.log 2>/dev/null; then return 0; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
      echo "!!! node $name exited early:"; docker exec "$name" cat /tmp/server.log 2>/dev/null; return 1
    fi
    sleep 1
  done
  echo "!!! node $name did not become ready:"; docker exec "$name" cat /tmp/server.log 2>/dev/null; return 1
}

bring_up_node() {
  local name=$1 first=${2:-}
  echo "== bring up $name (NFS client, share at /cluster) =="
  start_container "$name"
  install_nfs_client "$name"
  setup_build_users "$name"
  mount_share "$name" /cluster rw
  # Well-known share layout: /store (contents) + /var/replicas (node registry).
  docker exec "$name" mkdir -p /cluster/store /cluster/var/replicas
  # First node up wipes registrations left over from a previous run.
  [ "$first" = first ] && docker exec "$name" sh -c 'rm -f /cluster/var/replicas/*'
  launch_server "$name"
  # The server registers ITSELF; check the registration landed on the share.
  reg=$(docker exec "$name" cat "/cluster/var/replicas/$name" 2>/dev/null) \
    || { echo "!!! $name did not register itself in /var/replicas"; docker exec "$name" cat /tmp/server.log; exit 1; }
  echo "   $name up, self-registered in /var/replicas/$name as $reg"
}

# node1 first, so it creates the DB schema and cleans the registry.
bring_up_node node1 first
bring_up_node node2

N1="grpc://$(node_ip node1):$PORT?auth-token=$TOKEN"
N2="grpc://$(node_ip node2):$PORT?auth-token=$TOKEN"

# ---------------------------------------------------------------------------
# discovery: the client reads the node registry itself (registry= parameter)
# ---------------------------------------------------------------------------
echo
echo "== discovery: client-side registry=$REGISTRY =="
[ "$(ls "$REGISTRY" 2>/dev/null | wc -l)" = 2 ] \
  || { echo "!!! expected two registrations in $REGISTRY:"; ls -la "$REGISTRY" 2>/dev/null; exit 1; }
echo "   registry entries: $(ls "$REGISTRY" | paste -sd' ' -): $(cat "$REGISTRY"/* | paste -sd' ' -)"
# No static node list: the client discovers the whole cluster from the share.
CLUSTER="grpc://?registry=$REGISTRY&auth-token=$TOKEN"

fail=0

echo "== discovery failure: unreadable registry must be a fatal error =="
if "${nix_cmd[@]}" store info --store "grpc://?registry=/nonexistent/replicas&auth-token=$TOKEN" >/dev/null 2>&1; then
  echo "!!! a client with an unreadable registry should error out, but succeeded"
  fail=1
else
  echo "   OK: unreadable registry rejected"
fi

# ---------------------------------------------------------------------------
# build via node1 ONLY
# ---------------------------------------------------------------------------
echo
echo "== build a derivation via node1 only =="
# Keep the builder to plain sh builtins: the sandbox offers /bin/sh and
# nothing else (no coreutils), so e.g. `sleep` or `hostname` would just fail.
expr=$(cat <<'NIXEXPR'
builtins.unsafeDiscardStringContext (derivation {
  name = "simple-cooperation";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo done > \$out" ];
}).drvPath
NIXEXPR
)
DRV=$("${nix_cmd[@]}" eval --raw --impure --expr "$expr")
OUT=$(set +o pipefail; "${nix_cmd[@]}" derivation show "$DRV" 2>/dev/null \
        | grep -oE '/nix/store/[a-z0-9]{32}-[^"]+' | grep -v '\.drv"\?$' | head -1)
echo "   drv: $DRV"
echo "   expected out: $OUT"

"${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N1" "$DRV" >/dev/null

rc=0
timeout "$BUILD_TIMEOUT" "${nix_cmd[@]}" build --no-link -L --store "$N1" "$DRV^*" \
  >/tmp/simple-build.out 2>/tmp/simple-build.err || rc=$?
if [ "$rc" = 124 ]; then
  echo "!!! HANG: build client exceeded ${BUILD_TIMEOUT}s (the known intermittent"
  echo "    gRPC client hang -- see README.md); judging by store validity anyway."
elif [ "$rc" != 0 ]; then
  echo "!!! build client exited rc=$rc; judging by store validity below, but a"
  echo "    nonzero rc is unexpected since the MissingRealisation fix (a client"
  echo "    with ca-derivations enabled used to error out after a successful"
  echo "    build); see the build log:"
  tail -n 10 /tmp/simple-build.err
fi

# ---------------------------------------------------------------------------
# the actual pass bar: NODE2 sees a valid path it never built
# ---------------------------------------------------------------------------
echo
echo "== node2: is the path valid? (built via node1; node2 shares DB + NFS) =="
if "${nix_cmd[@]}" path-info --store "$N2" "$OUT" >/dev/null 2>&1; then
  echo "   OK: node2 reports $OUT valid"
else
  echo "!!! node2 does NOT consider $OUT valid"
  echo "--- build err (tail) ---"; tail -n 20 /tmp/simple-build.err
  fail=1
fi

echo "== node2: content integrity (nix store verify) =="
if "${nix_cmd[@]}" store verify --store "$N2" "$OUT" >/dev/null 2>&1; then
  echo "   OK: NAR hash verified through node2"
else
  echo "!!! nix store verify via node2 failed"; fail=1
fi

echo "== discovery-based cluster URI serves the path too =="
if "${nix_cmd[@]}" path-info --store "$CLUSTER" "$OUT" >/dev/null 2>&1; then
  echo "   OK: path valid via grpc://?registry=... (no static node list)"
else
  echo "!!! path-info via the registry-discovering cluster URI failed"; fail=1
fi

# ---------------------------------------------------------------------------
# physical presence on the NFS export (independent read-only mount)
# ---------------------------------------------------------------------------
echo
echo "== verifier: output physically on the NFS export, under /store =="
start_container verifier
install_nfs_client verifier
mount_share verifier /nfs ro
rel=$(basename "$OUT")
docker exec verifier sh -c "
  if [ -e /nfs/store/$rel ]; then
    echo '   OK: store/$rel present on the read-only NFS export'
  else
    echo '!!! store/$rel NOT on the NFS export; /nfs/store contains:'
    ls /nfs/store 2>/dev/null | head; exit 1
  fi
" || fail=1

echo
if [ "$fail" = 0 ]; then
  echo "== OK: built on node1, valid + verified via node2, present on NFS =="
else
  echo "== FAIL: see !!! markers above =="
  exit 1
fi
