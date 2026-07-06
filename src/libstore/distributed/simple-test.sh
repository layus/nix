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
NFS_OPTS=${NFS_OPTS:-vers=4,rw,noatime,hard,timeo=600}
# The registry as seen from THIS host (which serves the export itself).
REGISTRY=${REGISTRY:-/mnt/nix-store/var/replicas}

BUILD_TIMEOUT=${BUILD_TIMEOUT:-120}
NIXBLD_N=${NIXBLD_N:-4}
# Registration freshness TTL for the nodes (heartbeat = TTL/3). Short, so the
# stale-registration sweep is observable within the test.
REPLICA_TTL=${REPLICA_TTL:-6}
# GC-root lease lifetime (defaults to one week in production). Short, so root
# expiry is observable within the test.
ROOT_LIFETIME=${ROOT_LIFETIME:-5}

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
# work-stealing: idle nodes poll (every 1s here) for builds advertised by
# saturated nodes and build them, arbitrated by the per-drv build lock.
BACKING="distributed://?metadata-db-url=$DBURL&real=/cluster/store&gc-root-lifetime=$ROOT_LIFETIME&work-stealing=true&work-stealing-interval=1"

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
  [ "$mode" = ro ] && opts="vers=4,ro,noatime,hard,timeo=600"
  docker exec "$name" sh -c "
    set -e
    mkdir -p '$target'
    mount -t nfs -o '$opts' '$HOST_IP:$EXPORT' '$target'
  " || { echo "!!! [$name] NFS mount of $HOST_IP:$EXPORT at $target failed"; return 1; }
}

launch_server() {
  local name=$1
  # max-jobs = 1 so a node saturates with a single build, making work
  # stealing observable (a second ready derivation goes to the other node).
  docker exec "$name" sh -c "printf '%s\n' \
    'build-users-group = nixbld' \
    'sandbox = true' \
    'builders =' \
    'max-jobs = 1' \
    'experimental-features = nix-command flakes' > /tmp/nix.conf"
  # The advertised address (4th arg) is what the server registers in the
  # share's /var/replicas registry for clients to discover.
  docker exec -d "$name" sh -c \
    "umask 022; export PATH=/build/src/nix:\$PATH; export NIX_CONFIG=\"\$(cat /tmp/nix.conf)\"; \
     export NIX_REPLICA_TTL=$REPLICA_TTL; \
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
  # The server registers ITSELF; check the registration landed on the share
  # (line 1 = advertised address; line 2 = its freshness TTL).
  reg=$(docker exec "$name" head -1 "/cluster/var/replicas/$name" 2>/dev/null) \
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
echo "   registry entries: $(ls "$REGISTRY" | paste -sd' ' -): $(head -qn1 "$REGISTRY"/* | paste -sd' ' -)"
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
# stale-registration TTL: a dead node's entry is skipped and swept
# ---------------------------------------------------------------------------
echo
echo "== stale-registration TTL (nodes heartbeat every $((REPLICA_TTL / 3))s) =="
# Plant a dead node's registration (declared TTL 1s) via node1's rw mount.
docker exec node1 sh -c 'printf "10.255.255.1:5570\n1\n" > /cluster/var/replicas/deadnode'
sleep 2 # let its 1s TTL expire
# A client reading the registry now must skip it (unless a node's heartbeat
# already swept it, which is equally correct).
if "${nix_cmd[@]}" store info --store "$CLUSTER" --debug 2>&1 | grep -q 'ignoring stale replica registration'; then
  echo "   OK: client skipped the stale registration"
elif [ -e "$REGISTRY/deadnode" ]; then
  echo "!!! stale entry present but the client did not skip it"; fail=1
else
  echo "   (swept before the client looked -- equally fine)"
fi
# A live node's heartbeat must sweep it off the share.
for _ in $(seq 1 15); do [ -e "$REGISTRY/deadnode" ] || break; sleep 1; done
if [ -e "$REGISTRY/deadnode" ]; then
  echo "!!! stale registration never swept by the nodes"; fail=1
else
  echo "   OK: stale registration swept by a live node's heartbeat"
fi
# The live nodes' own registrations must have survived the sweeping.
[ "$(ls "$REGISTRY" | wc -l)" = 2 ] \
  || { echo "!!! live registrations went missing:"; ls -la "$REGISTRY"; fail=1; }

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
# work stealing: an idle node2 must pick up node1's queued builds
# ---------------------------------------------------------------------------
echo
echo "== work stealing (node1 saturated at max-jobs=1 with two slow deps) =="
SEXPR=$(cat <<'NIXEXPR'
let dep = n: derivation {
  name = "steal-dep-${n}";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "j=0; while [ $j -lt 20000000 ]; do j=$((j+1)); done; echo ${n} > $out" ];
}; in builtins.unsafeDiscardStringContext (derivation {
  name = "steal-top";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  depA = dep "a";
  depB = dep "b";
  args = [ "-c" "test -e $depA -a -e $depB || exit 1; echo done > $out" ];
}).drvPath
NIXEXPR
)
SDRV=$("${nix_cmd[@]}" eval --raw --impure --expr "$SEXPR")
SOUT=$(set +o pipefail; "${nix_cmd[@]}" derivation show "$SDRV" 2>/dev/null \
         | grep -oE '/nix/store/[a-z0-9]{32}-[^"]+' | grep -v '\.drv"\?$' | head -1)
"${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N1" "$SDRV" >/dev/null 2>&1
# Fire at node1 ONLY: it builds one dep (its single slot), advertises the
# other, and the idle node2 must steal it.
timeout "$BUILD_TIMEOUT" "${nix_cmd[@]}" build --no-link --store "$N1" "$SDRV^*" \
  >/tmp/simple-steal.out 2>/tmp/simple-steal.err || true
if "${nix_cmd[@]}" path-info --store "$N2" "$SOUT" >/dev/null 2>&1; then
  echo "   OK: the full closure built and is valid cluster-wide"
else
  echo "!!! steal-top output not valid:"; tail -n 10 /tmp/simple-steal.err; fail=1
fi
# Keep full node logs and the export's scratch state for postmortem.
docker exec node1 cat /tmp/server.log > /tmp/simple-node1.log 2>/dev/null || true
docker exec node2 cat /tmp/server.log > /tmp/simple-node2.log 2>/dev/null || true
ls -la /mnt/nix-store/store/ 2>/dev/null | grep -i chroot > /tmp/simple-chroots.txt || true
stolen=$(docker exec node2 grep -c 'work stealing: built' /tmp/server.log 2>/dev/null || true)
if [ "${stolen:-0}" -ge 1 ] 2>/dev/null; then
  echo "   OK: node2 stole and built $stolen derivation(s):"
  docker exec node2 grep 'work stealing: built' /tmp/server.log | sed 's/^/      /'
else
  echo "!!! node2 stole nothing; node2 log tail:"
  docker exec node2 tail -n 10 /tmp/server.log; fail=1
fi

# ---------------------------------------------------------------------------
# GC roots are leases: registered remotely, refreshed on re-add, expired by GC
# ---------------------------------------------------------------------------
echo
echo "== GC roots as leases (gc-root-lifetime=${ROOT_LIFETIME}s) =="
ROOTLINK=$(mktemp -u /tmp/simple-root.XXXXXX)
reg_time() {
  docker exec cockroach cockroach sql --insecure -d "$DB" --format tsv \
    -e "select coalesce(max(registered), 0) from GCRoots where link like '%$(basename "$ROOTLINK")'" \
    2>/dev/null | tail -1
}
# Building with -o against the gRPC store must create the local symlink AND
# register a hostname-qualified root in the shared database.
timeout 60 "${nix_cmd[@]}" build --store "$N1" -o "$ROOTLINK" "$DRV^*" >/dev/null 2>&1 || true
[ -L "$ROOTLINK" ] \
  && echo "   OK: local result symlink created ($ROOTLINK)" \
  || { echo "!!! no local result symlink"; fail=1; }
R1=$(reg_time)
[ "${R1:-0}" -gt 0 ] \
  && echo "   OK: root registered in the shared DB (registered=$R1)" \
  || { echo "!!! root not registered in GCRoots"; fail=1; }
# Re-adding the same root refreshes its lease.
sleep 2
timeout 60 "${nix_cmd[@]}" build --store "$N1" -o "$ROOTLINK" "$DRV^*" >/dev/null 2>&1 || true
R2=$(reg_time)
[ "${R2:-0}" -gt "${R1:-0}" ] \
  && echo "   OK: re-adding refreshed the lease ($R1 -> $R2)" \
  || { echo "!!! lease not refreshed ($R1 -> $R2)"; fail=1; }
# A root not refreshed within the lifetime is deleted by the next GC.
sleep $((ROOT_LIFETIME + 1))
timeout 60 "${nix_cmd[@]}" store gc --store "$N2" >/dev/null 2>&1 || true
[ "$(reg_time)" = 0 ] \
  && echo "   OK: expired root removed by GC" \
  || { echo "!!! expired root still present after GC"; fail=1; }
rm -f "$ROOTLINK"

# ---------------------------------------------------------------------------
# error fidelity: a failing remote build behaves exactly like a local one
# ---------------------------------------------------------------------------
echo
echo "== error fidelity: failing build over gRPC =="
FEXPR=$(cat <<'NIXEXPR'
builtins.unsafeDiscardStringContext (derivation {
  name = "simple-fail";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo doomed; exit 3" ];
}).drvPath
NIXEXPR
)
FDRV=$("${nix_cmd[@]}" eval --raw --impure --expr "$FEXPR")
"${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N1" "$FDRV" >/dev/null
rc=0
timeout 60 "${nix_cmd[@]}" build --no-link --store "$N1" "$FDRV^*" \
  >/tmp/simple-fail.out 2>/tmp/simple-fail.err || rc=$?
# Local parity: same exit status (100), same BuildError message, no wrapper.
if [ "$rc" = 100 ]; then
  echo "   OK: exit status 100 (local-parity build failure)"
else
  echo "!!! expected exit status 100, got $rc:"; tail -n 5 /tmp/simple-fail.err; fail=1
fi
grep -q 'builder failed with exit code 3' /tmp/simple-fail.err \
  && echo "   OK: original BuildError message preserved" \
  || { echo "!!! BuildError message lost:"; tail -n 5 /tmp/simple-fail.err; fail=1; }
grep -q 'error: error:\|remote gRPC store' /tmp/simple-fail.err \
  && { echo "!!! error still wrapped/nested:"; tail -n 5 /tmp/simple-fail.err; fail=1; } \
  || echo "   OK: no wrapper, no nested 'error:' prefix"

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
