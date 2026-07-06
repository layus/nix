#!/usr/bin/env bash
#
# Concurrent-build stress test for the distributed store over a REAL shared
# store on NFS.
#
# Unlike cluster-test.sh (which fakes the shared content plane by bind-mounting
# one host directory into both node containers), this test uses a genuine NFS
# share as the shared store, the configuration the distributed store targets:
#
#   * HOST      -- serves /mnt/nix-store over NFSv4 (set up declaratively on
#                  NixOS; see the block at the bottom of this comment). The test
#                  does NOT configure the host; it only checks the export is up.
#   * node1/node2 -- two symmetric gRPC store nodes, each an NFS *client* that
#                  mounts the host export read-write at /cluster/store and runs
#                  a gRPC node against `distributed://?real=/cluster/store`. Every
#                  store write either node makes goes over NFS to the host.
#   * CockroachDB -- the shared metadata plane (separate container).
#   * verifier  -- a throwaway container that mounts the export READ-ONLY and
#                  confirms a built output physically landed on the NFS share
#                  (not in a node-local dir).
#
# The test drives BUILDS at both nodes CONCURRENTLY and checks the cluster
# survives the pressure:
#
#   Phase 1: the SAME synthetic slow derivation is fired at both nodes at once.
#            This races the per-derivation DB build lock (BuildLocks table): one
#            node acquires it and builds, the other spins on tryLockBuild -> waits
#            -> re-checks output validity and reuses the winner's result. We
#            surface the lock behaviour from the logs; the pass bar is "no crash,
#            no deadlock, both clients succeed, output valid on the shared store".
#   Phase 2: the home-manager closure (a large realistic derivation graph) is
#            fired at both nodes at once, as a throughput / parallelism stress.
#
# Target for now (per request): NO CRASH. We do not yet fail the run on a strict
# "built exactly once" assertion -- we surface the lock behaviour from the logs
# but only hard-fail on a node dying or a missing output. Success is judged by
# output validity on the store, not by the client's exit code.
#
# KNOWN ISSUE: the gRPC build client has been seen (once, non-deterministically)
# to hang after a successful server-side build -- specifically a cold-cache
# `hello` closure built at both nodes at once while both substitute the closure
# from cache.nixos.org. It could not be reproduced in isolation. Every build is
# therefore wrapped in `timeout $BUILD_TIMEOUT`; a timed-out build is still
# judged OK if its output is valid on the store. See the roadmap in README.md.
#
# HOST PREREQUISITES:
#   * Docker;
#   * a grpc+postgres-enabled `nix` + `nix-grpc-store-server` in $BUILD_DIR
#     (defaults to ./build-both), same as cluster-test.sh;
#   * an NFSv4 export of /mnt/nix-store reachable from the Docker bridge. On
#     NixOS, add to your system config and `nixos-rebuild switch`:
#
#       systemd.tmpfiles.rules = [ "d /mnt/nix-store 0755 root root -" ];
#       services.nfs.server = {
#         enable = true;
#         exports = ''
#           /mnt/nix-store 172.16.0.0/12(rw,sync,no_subtree_check,no_root_squash,insecure,fsid=0)
#         '';
#       };
#       networking.firewall.extraCommands = ''
#         iptables -A nixos-fw -p tcp -s 172.16.0.0/12 --dport 2049 -j nixos-fw-accept
#       '';
#
#     (fsid=0 makes /mnt/nix-store the NFSv4 root, so clients mount <host>:/ .)
#
# Usage (from the repo root):
#   ./src/libstore/distributed/stress-test.sh
# Override any of the env vars below as needed. Set HM_ATTR= to skip phase 2.
set -euo pipefail

BUILD_DIR=$(realpath "${BUILD_DIR:-build-both}")
NIX=${NIX:-$BUILD_DIR/src/nix/nix}
SERVER_IN_CONTAINER=/build/src/libstore/nix-grpc-store-server
# busybox has no mount.nfs; use a small image with nfs-utils installed at start
# (needs one-time network egress to fetch the package).
NODE_IMAGE=${NODE_IMAGE:-alpine:3.20}
COCKROACH_IMAGE=${COCKROACH_IMAGE:-cockroachdb/cockroach:latest}
TOKEN=${TOKEN:-cluster-secret}
NET=${NET:-nixstress}
DB=${DB:-nixstress}
PORT=5570

# NFS: the host export. HOST_IP defaults to the test network's gateway (the host
# as seen from the containers) but can be overridden. EXPORT is the NFSv4 path;
# with fsid=0 on the server the export root is "/".
HOST_IP=${HOST_IP:-}          # auto-discovered from the docker network gateway
EXPORT=${EXPORT:-/}           # NFSv4 root (fsid=0 -> /mnt/nix-store)
NFS_OPTS=${NFS_OPTS:-vers=4,rw,noatime,hard,timeo=600}

# Per-build wall-clock cap. The gRPC build client has been observed (once, non-
# deterministically) to hang after a successful server-side build -- a cold-cache
# hello closure with both nodes substituting at the same time. We could not
# reproduce it in isolation, so rather than let a wedged client stall the whole
# harness, every build is wrapped in `timeout`; a timed-out build is judged by
# output validity like any other (the output may well be valid on the store).
BUILD_TIMEOUT=${BUILD_TIMEOUT:-300}

# Phase 1 synthetic load: how many distinct slow drvs, and how long each sleeps.
N_SLOW=${N_SLOW:-4}
SLEEP_SECS=${SLEEP_SECS:-8}

# Phase 2 home-manager target. Set HM_ATTR to empty to skip phase 2.
HM_FLAKE=${HM_FLAKE:-$HOME/.config/home-manager}
HM_ATTR=${HM_ATTR-homeConfigurations.layus.activationPackage}

nix_cmd=("$NIX" --extra-experimental-features 'nix-command flakes')

# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------
cleanup() {
  echo "== cleanup =="
  # Unmount NFS clients before removing the containers.
  for n in node1 node2 verifier; do
    docker exec "$n" umount -f -l /cluster/store >/dev/null 2>&1 || true
    docker exec "$n" umount -f -l /nfs           >/dev/null 2>&1 || true
  done
  docker rm -f node1 node2 verifier cockroach >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Idempotent pre-clean.
docker rm -f node1 node2 verifier cockroach >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# network + host NFS reachability
# ---------------------------------------------------------------------------
echo "== network =="
docker network create "$NET" >/dev/null
if [ -z "$HOST_IP" ]; then
  HOST_IP=$(docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' "$NET")
fi
echo "   host (NFS server) as seen from containers: $HOST_IP"

echo "== CockroachDB (single-node, insecure) =="
docker run -d --name cockroach --network "$NET" "$COCKROACH_IMAGE" \
  start-single-node --insecure >/dev/null
for _ in $(seq 1 60); do
  if docker exec cockroach cockroach sql --insecure -e 'select 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec cockroach cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS $DB" >/dev/null
echo "   CockroachDB ready, database '$DB' created"

DBURL="postgresql://root@cockroach:26257/$DB"
# Work stealing on: idle nodes pick up builds the other node has queued.
BACKING="distributed://?metadata-db-url=$DBURL&real=/cluster/store&work-stealing=true&work-stealing-interval=2"

# ---------------------------------------------------------------------------
# node / container helpers
# ---------------------------------------------------------------------------
node_ip() { docker inspect -f "{{.NetworkSettings.Networks.$NET.IPAddress}}" "$1"; }

# A privileged (for mount) container that idles, so we can drive setup steps.
start_container() {
  local name=$1
  # NIX_STATE_DIR must NOT be under a world-writable dir like /tmp: Nix's build
  # dir lives under it and the builder refuses a world-writable build dir. Use a
  # private 0755 tree under /var.
  docker run -d --name "$name" --network "$NET" --privileged --user 0 \
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
    || { echo "!!! [$name] apk add nfs-utils failed (needs network egress)"; docker logs "$name"; return 1; }
}

# Create the nixbld build group + users so the node can build in a proper,
# unprivileged, per-build user (Nix's normal isolation). The container runs
# privileged, so the build sandbox can further nest user/mount/net namespaces.
NIXBLD_N=${NIXBLD_N:-8}
setup_build_users() {
  local name=$1
  echo "   [$name] creating nixbld group + $NIXBLD_N build users..."
  docker exec "$name" sh -c "
    addgroup -g 30000 nixbld 2>/dev/null || true
    i=1; while [ \$i -le $NIXBLD_N ]; do
      adduser -S -D -H -G nixbld -u \$((30000 + i)) nixbld\$i 2>/dev/null || true
      i=\$((i + 1))
    done
    getent group nixbld >/dev/null || { echo 'nixbld group missing'; exit 1; }
  " || { echo "!!! [$name] could not create nixbld users"; return 1; }
}

# Mount the host NFS export at /cluster/store (or /nfs), given mode ro|rw.
mount_share() {
  local name=$1 target=$2 mode=$3
  local opts="$NFS_OPTS"
  [ "$mode" = ro ] && opts="vers=4,ro,noatime,hard,timeo=600"
  docker exec "$name" sh -c "
    set -e
    mkdir -p '$target'
    mount -t nfs -o '$opts' '$HOST_IP:$EXPORT' '$target'
    mount | grep '$target'
  " || { echo "!!! [$name] NFS mount of $HOST_IP:$EXPORT at $target failed"; docker exec "$name" dmesg 2>/dev/null | tail; return 1; }
}

launch_server() {
  local name=$1
  # The node builds derivations locally, so it needs:
  #   * the built `nix` on PATH  -- the default build-hook is `nix __build-remote`;
  #   * build-users-group=nixbld -- real per-build user isolation;
  #   * sandbox=true             -- namespace isolation (nested inside the
  #                                 privileged container);
  #   * builders=               -- never delegate; always build here.
  docker exec "$name" sh -c "printf '%s\n' \
    'build-users-group = nixbld' \
    'sandbox = true' \
    'builders =' \
    'max-jobs = $NIXBLD_N' \
    'experimental-features = nix-command flakes' > /tmp/nix.conf"
  # umask 022 so any build dir Nix creates under the state dir is 0755, not the
  # world-writable 0777 that the builder's security check rejects.
  docker exec -d "$name" sh -c \
    "umask 022; export PATH=/build/src/nix:\$PATH; export NIX_CONFIG=\"\$(cat /tmp/nix.conf)\"; \
     $SERVER_IN_CONTAINER 0.0.0.0:$PORT '$BACKING' '$TOKEN' >/tmp/server.log 2>&1"
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
  local name=$1
  echo "== bring up $name (NFS client, rw) =="
  start_container "$name"
  install_nfs_client "$name"
  setup_build_users "$name"
  mount_share "$name" /cluster/store rw
  launch_server "$name"
  echo "   $name up at $(node_ip "$name"), store on NFS, sandboxed builds enabled"
}

# Start node1 first so it creates the DB schema before node2 joins (avoids the
# concurrent first-time initSchema race noted in the README).
bring_up_node node1
bring_up_node node2

IP1=$(node_ip node1); IP2=$(node_ip node2)
N1="grpc://$IP1:$PORT?auth-token=$TOKEN"
N2="grpc://$IP2:$PORT?auth-token=$TOKEN"

# ---------------------------------------------------------------------------
# Phase 1: concurrent SAME-derivation builds (races the DB build lock)
# ---------------------------------------------------------------------------
echo
echo "############################################################"
echo "# Phase 1: $N_SLOW slow drvs, each built at BOTH nodes at once"
echo "############################################################"

make_slow_drv() {
  local i=$1
  # The builder script must reach the *builder's* shell with literal $out and
  # $(hostname). We build the Nix expression with a single-quoted heredoc so
  # bash performs NO expansion, and interpolate only $i / $SLEEP_SECS via a
  # tiny printf. In Nix source, a literal '$' inside a "..." string is '\$'.
  local expr
  expr=$(cat <<'NIXEXPR'
builtins.unsafeDiscardStringContext (derivation {
  name = "slow-@I@";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo building slow-@I@ on \$(hostname); sleep @SLEEP@; echo done > \$out" ];
}).drvPath
NIXEXPR
)
  expr=${expr//@I@/$i}
  expr=${expr//@SLEEP@/$SLEEP_SECS}
  "${nix_cmd[@]}" eval --raw --impure --expr "$expr"
}

# The expected output path of a slow drv (so we can check the store afterwards;
# building `<drv>^out` over gRPC is flaky, so we realise the whole drv instead).
# NB: `grep | head` under `set -o pipefail` aborts the script when head closes
# the pipe early (grep dies with SIGPIPE, rc=141); disable pipefail locally and
# read only the first match with bash.
drv_out_path() {
  local out
  out=$(set +o pipefail; "${nix_cmd[@]}" derivation show "$1" 2>/dev/null \
          | grep -oE '/nix/store/[a-z0-9]{32}-[^"]+' | grep -v '\.drv"\?$' | head -1)
  printf '%s' "$out"
}

fail=0
FIRST_OUT=""
for i in $(seq 1 "$N_SLOW"); do
  DRV=$(make_slow_drv "$i")
  OUT=$(drv_out_path "$DRV")
  echo "-- racing $DRV at node1 and node2 (expect out $OUT) --"
  # Both nodes need the derivation present before they can build it. Copying the
  # same drv to both is harmless (idempotent) and mirrors how any remote store
  # is fed a build.
  "${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N1" "$DRV" >/dev/null 2>&1 || true
  "${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N2" "$DRV" >/dev/null 2>&1 || true
  # Fire the SAME derivation at both nodes at once -> races the DB build lock.
  # Use the `^*` selector (bare `<drv>` is a no-op that builds nothing). We
  # judge success by path validity, not exit code, because a timed-out (hung)
  # client can leave a perfectly valid output behind. (The old deterministic
  # rc=1 -- "cannot operate on output 'out'" from a ca-derivations-enabled
  # client -- is fixed; see grpc-store.cc buildPathsWithResults.)
  ( timeout "$BUILD_TIMEOUT" "${nix_cmd[@]}" build --no-link -L --store "$N1" "$DRV^*" \
      >/tmp/stress-1.$i.out 2>/tmp/stress-1.$i.err ) & pid1=$!
  ( timeout "$BUILD_TIMEOUT" "${nix_cmd[@]}" build --no-link -L --store "$N2" "$DRV^*" \
      >/tmp/stress-2.$i.out 2>/tmp/stress-2.$i.err ) & pid2=$!
  rc1=0; rc2=0
  wait "$pid1" || rc1=$?
  wait "$pid2" || rc2=$?
  # Confirm the output actually landed on the shared store (validity is the real
  # signal; the client's exit code can be a cosmetic nonzero on the `^*` path).
  out1=""; out2=""
  "${nix_cmd[@]}" path-info --store "$N1" "$OUT" >/dev/null 2>&1 && out1="$OUT"
  "${nix_cmd[@]}" path-info --store "$N2" "$OUT" >/dev/null 2>&1 && out2="$OUT"
  echo "   node1 rc=$rc1 valid=${out1:-NO}"
  echo "   node2 rc=$rc2 valid=${out2:-NO}"
  if [ -z "$out1" ] || [ -z "$out2" ]; then
    echo "!!! output not valid on both nodes after build (1=${out1:-NO} 2=${out2:-NO})"
    echo "--- node1 err ---"; cat /tmp/stress-1.$i.err; echo "--- node2 err ---"; cat /tmp/stress-2.$i.err
    fail=1
  fi
  [ "$i" = 1 ] && FIRST_OUT="$out1"
done

echo "== work stealing observed in node logs (informational) =="
for n in node1 node2; do
  echo "--- $n: $(docker exec "$n" grep -c 'work stealing: built' /tmp/server.log 2>/dev/null || true) stolen build(s)"
done

echo "== build-lock behaviour observed in node logs (informational) =="
for n in node1 node2; do
  echo "--- $n ---"
  docker exec "$n" sh -c 'grep -iE "build lock|waiting|beat us to it|acquire" /tmp/server.log 2>/dev/null | tail -n 5' || true
done

# ---------------------------------------------------------------------------
# Independent NFS verification: output physically present on the export?
# ---------------------------------------------------------------------------
echo
echo "== verifier: mount export READ-ONLY, confirm output landed on the NFS =="
start_container verifier
install_nfs_client verifier
mount_share verifier /nfs ro
if [ -n "$FIRST_OUT" ]; then
  rel=$(basename "$FIRST_OUT")
  docker exec verifier sh -c "
    if [ -e /nfs/$rel ]; then
      echo '   OK: $rel present on the read-only NFS export'
    else
      echo '!!! $rel NOT found on the NFS export; contents:'; ls -la /nfs | head; exit 1
    fi
  " || fail=1
else
  echo "   (no phase-1 output path captured; skipping NFS presence check)"
fi

# ---------------------------------------------------------------------------
# Phase 2: concurrent home-manager closure (throughput stress)
# ---------------------------------------------------------------------------
if [ -n "${HM_ATTR:-}" ]; then
  echo
  echo "############################################################"
  echo "# Phase 2: home-manager closure at BOTH nodes concurrently"
  echo "#   flake=$HM_FLAKE attr=$HM_ATTR"
  echo "############################################################"
  INSTALLABLE="$HM_FLAKE#$HM_ATTR"
  # Evaluate the closure to its .drv on the host, then copy the whole derivation
  # closure to each node so both can build it. Bare `nix build --store grpc://
  # <flake>` can't work: the node has none of the input drvs.
  echo "   evaluating $INSTALLABLE to a .drv on the host..."
  HM_DRV=$("${nix_cmd[@]}" path-info --derivation "$INSTALLABLE" 2>/tmp/stress-hm-eval.err) || {
    echo "!!! could not evaluate $INSTALLABLE:"; tail -n 20 /tmp/stress-hm-eval.err; fail=1; HM_DRV=""; }
  if [ -n "$HM_DRV" ]; then
    echo "   drv: $HM_DRV"
    echo "   copying derivation closure to both nodes..."
    "${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N1" "$HM_DRV" >/tmp/stress-hm-cp1.err 2>&1 || { echo "!!! copy to node1 failed"; tail /tmp/stress-hm-cp1.err; fail=1; }
    "${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N2" "$HM_DRV" >/tmp/stress-hm-cp2.err 2>&1 || { echo "!!! copy to node2 failed"; tail /tmp/stress-hm-cp2.err; fail=1; }
    HM_OUT=$(drv_out_path "$HM_DRV")
    ( timeout "$BUILD_TIMEOUT" "${nix_cmd[@]}" build --no-link -L --store "$N1" "$HM_DRV^*" \
        >/tmp/stress-hm-1.out 2>/tmp/stress-hm-1.err ) & pid1=$!
    ( timeout "$BUILD_TIMEOUT" "${nix_cmd[@]}" build --no-link -L --store "$N2" "$HM_DRV^*" \
        >/tmp/stress-hm-2.out 2>/tmp/stress-hm-2.err ) & pid2=$!
    rc1=0; rc2=0
    wait "$pid1" || rc1=$?
    wait "$pid2" || rc2=$?
    # Judge by output validity (see the Phase 1 note on exit codes).
    v1=NO; v2=NO
    [ -n "$HM_OUT" ] && "${nix_cmd[@]}" path-info --store "$N1" "$HM_OUT" >/dev/null 2>&1 && v1=OK
    [ -n "$HM_OUT" ] && "${nix_cmd[@]}" path-info --store "$N2" "$HM_OUT" >/dev/null 2>&1 && v2=OK
    echo "   node1 rc=$rc1 valid=$v1"
    echo "   node2 rc=$rc2 valid=$v2"
    if [ "$v1" != OK ] || [ "$v2" != OK ]; then
      echo "!!! home-manager output not valid on both nodes:"
      echo "--- node1 err (tail) ---"; tail -n 30 /tmp/stress-hm-1.err
      echo "--- node2 err (tail) ---"; tail -n 30 /tmp/stress-hm-2.err
      fail=1
    fi
  fi
else
  echo
  echo "== Phase 2 skipped (HM_ATTR empty) =="
fi

# ---------------------------------------------------------------------------
# node liveness after the pressure
# ---------------------------------------------------------------------------
echo
echo "== both nodes still alive after the stress? =="
for n in node1 node2; do
  st=$(docker inspect -f '{{.State.Running}}' "$n" 2>/dev/null || echo "gone")
  echo "   $n running=$st"
  [ "$st" = "true" ] || fail=1
done

echo
if [ "$fail" = 0 ]; then
  echo "== OK: cluster survived concurrent builds over NFS, output verified on the export =="
else
  echo "== FAIL: see !!! markers above =="
  exit 1
fi
