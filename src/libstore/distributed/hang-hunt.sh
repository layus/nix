#!/usr/bin/env bash
#
# Reproduction hunt for the intermittent gRPC build-client hang (README
# roadmap): the client was once seen to hang indefinitely AFTER the build had
# succeeded server-side, on a cold-cache closure built at both nodes at once
# while both nodes were substituting it. Suspected: a deadline-less client
# stream Read stalling while a slow substitution holds the backend's single
# libpq mutex.
#
# This harness recreates that scenario N times (default 100), as cheaply and
# as close to the original as possible:
#
#   * two gRPC store nodes on the host's NFS export + one CockroachDB, as in
#     simple-test.sh;
#   * the nodes' ONLY substituter is a deliberately THROTTLED local binary
#     cache (a rate-limited HTTP server in a container), pre-seeded with two
#     UNIQUE ~256 KiB dependency outputs PER ITERATION (built host-side into
#     a chroot store the cluster never sees) -- so every iteration is a cold,
#     slow substitution by construction, with nothing to evict;
#   * each iteration evaluates a FRESH top-level derivation depending on its
#     two deps and fires it at BOTH nodes at once (racing the per-drv DB
#     build lock, with both nodes substituting the deps concurrently);
#   * every client runs under `gdb --batch --return-child-result` (yama
#     ptrace_scope=1 forbids attaching later, but gdb-as-ancestor may trace):
#     if a client overruns HANG_TIMEOUT while its output is already valid on
#     the store -- the exact hang signature -- the harness SIGINTs gdb, which
#     stops the inferior and dumps `thread apply all bt` into the iteration
#     log, THE artifact this hunt exists to capture. The environment is left
#     running for postmortem and the harness exits 2.
#
# Usage (from the repo root; same prerequisites as simple-test.sh):
#   ./src/libstore/distributed/hang-hunt.sh
# Knobs: N_ITERS, HANG_TIMEOUT, RATE (cache bytes/sec), LOG_DIR.
set -euo pipefail

BUILD_DIR=$(realpath "${BUILD_DIR:-build-both}")
NIX=${NIX:-$BUILD_DIR/src/nix/nix}
SERVER_IN_CONTAINER=/build/src/libstore/nix-grpc-store-server
NODE_IMAGE=${NODE_IMAGE:-alpine:3.20}
COCKROACH_IMAGE=${COCKROACH_IMAGE:-cockroachdb/cockroach:latest}
TOKEN=${TOKEN:-cluster-secret}
NET=${NET:-nixhunt}
DB=${DB:-nixhunt}
PORT=5570
CACHE_PORT=8666

HOST_IP=${HOST_IP:-}
EXPORT=${EXPORT:-/}
NFS_OPTS=${NFS_OPTS:-vers=4,rw,noatime,hard,timeo=600}
NIXBLD_N=${NIXBLD_N:-4}

N_ITERS=${N_ITERS:-100}
HANG_TIMEOUT=${HANG_TIMEOUT:-90}   # per-iteration client wall-clock cap
RATE=${RATE:-150000}               # cache throughput, bytes/sec
LOG_DIR=${LOG_DIR:-/tmp/hang-hunt}

nix_cmd=("$NIX" --extra-experimental-features 'nix-command flakes')
mkdir -p "$LOG_DIR"
CACHE_DIR="$LOG_DIR/cache"
mkdir -p "$CACHE_DIR"

KEEP=0
cleanup() {
  if [ "$KEEP" = 1 ]; then
    echo "== KEEPING the environment for postmortem (docker rm -f node1 node2 cockroach cache; docker network rm $NET) =="
    return
  fi
  echo "== cleanup =="
  for n in node1 node2; do
    docker exec "$n" umount -f -l /cluster >/dev/null 2>&1 || true
  done
  docker rm -f node1 node2 cockroach cache >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f node1 node2 cockroach cache >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# infrastructure: network, DB, throttled binary cache, two NFS nodes
# ---------------------------------------------------------------------------
echo "== network + CockroachDB =="
docker network create "$NET" >/dev/null
if [ -z "$HOST_IP" ]; then
  HOST_IP=$(docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' "$NET")
fi
docker run -d --name cockroach --network "$NET" "$COCKROACH_IMAGE" \
  start-single-node --insecure >/dev/null
for _ in $(seq 1 60); do
  docker exec cockroach cockroach sql --insecure -e 'select 1' >/dev/null 2>&1 && break
  sleep 1
done
docker exec cockroach cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS $DB" >/dev/null

echo "== throttled binary cache (${RATE} B/s) =="
cat > "$LOG_DIR/throttle.py" <<'PY'
import http.server, socketserver, time, sys, functools
RATE = int(sys.argv[2]); CHUNK = 16384
class H(http.server.SimpleHTTPRequestHandler):
    def copyfile(self, src, dst):
        while True:
            b = src.read(CHUNK)
            if not b: break
            dst.write(b); dst.flush()
            time.sleep(len(b) / RATE)
    def log_message(self, *a): pass
class S(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
with S(("0.0.0.0", int(sys.argv[1])), functools.partial(H, directory="/cache")) as s:
    s.serve_forever()
PY
docker run -d --name cache --network "$NET" \
  -v "$CACHE_DIR:/cache:ro" -v "$LOG_DIR/throttle.py:/throttle.py:ro" \
  "$NODE_IMAGE" sh -c "apk add --no-cache python3 >/dev/null 2>&1 && python3 /throttle.py $CACHE_PORT $RATE" >/dev/null

BACKING="distributed://?metadata-db-url=postgresql://root@cockroach:26257/$DB&real=/cluster/store"

node_ip() { docker inspect -f "{{.NetworkSettings.Networks.$NET.IPAddress}}" "$1"; }

bring_up_node() {
  local name=$1
  echo "== bring up $name =="
  docker run -d --name "$name" --hostname "$name" --network "$NET" --privileged --user 0 \
    -v /nix/store:/nix/store:ro \
    -v "$BUILD_DIR:/build:ro" \
    -e HOME=/root -e NIX_STATE_DIR=/var/nix-state -e NIX_CONF_DIR=/etc/nix \
    "$NODE_IMAGE" sleep infinity >/dev/null
  docker exec "$name" sh -c 'mkdir -p /var/nix-state /etc/nix && chmod 0755 /var /var/nix-state'
  docker exec "$name" sh -c 'apk add --no-cache nfs-utils shadow >/dev/null 2>&1'
  docker exec "$name" sh -c "
    addgroup -g 30000 nixbld 2>/dev/null || true
    i=1; while [ \$i -le $NIXBLD_N ]; do
      adduser -S -D -H -G nixbld -u \$((30000 + i)) nixbld\$i 2>/dev/null || true
      i=\$((i + 1))
    done"
  docker exec "$name" sh -c "mkdir -p /cluster && mount -t nfs -o '$NFS_OPTS' '$HOST_IP:$EXPORT' /cluster"
  docker exec "$name" mkdir -p /cluster/store /cluster/var/replicas
  # The throttled cache is the ONLY substituter: every substitution is slow.
  docker exec "$name" sh -c "printf '%s\n' \
    'build-users-group = nixbld' \
    'sandbox = true' \
    'builders =' \
    'max-jobs = $NIXBLD_N' \
    'substituters = http://cache:$CACHE_PORT' \
    'require-sigs = false' \
    'experimental-features = nix-command flakes' > /tmp/nix.conf"
  docker exec -d "$name" sh -c \
    "umask 022; export PATH=/build/src/nix:\$PATH; export NIX_CONFIG=\"\$(cat /tmp/nix.conf)\"; \
     $SERVER_IN_CONTAINER 0.0.0.0:$PORT '$BACKING' '$TOKEN' '$(node_ip "$name"):$PORT' >/tmp/server.log 2>&1"
  for _ in $(seq 1 30); do
    docker exec "$name" grep -qi 'listening' /tmp/server.log 2>/dev/null && return 0
    sleep 1
  done
  echo "!!! $name not ready"; docker exec "$name" cat /tmp/server.log; exit 1
}
bring_up_node node1
bring_up_node node2
N1="grpc://$(node_ip node1):$PORT?auth-token=$TOKEN"
N2="grpc://$(node_ip node2):$PORT?auth-token=$TOKEN"

# ---------------------------------------------------------------------------
# seed the cache: 2 UNIQUE ~256 KiB deps per iteration, built on the host
# ---------------------------------------------------------------------------
# Unique deps per iteration guarantee every round is a COLD substitution
# without evicting anything from the cluster (a live node's heartbeat keeps
# its temp roots renewed, so recently-touched paths cannot be deleted). They
# are built host-side into a throwaway chroot store -- the cluster only ever
# sees them through the throttled cache. In the sandbox /bin/sh is the only
# tool, so the payload is built by string doubling (16 B * 2^14 = 256 KiB);
# in Nix strings only `${` interpolates, so the sh `$j`/`$s`/`$out` are
# literal.
echo "== seed the throttled cache: $((2 * N_ITERS)) unique deps, built host-side =="
CHROOT="$LOG_DIR/hoststore"
DEPS_EXPR=$(cat <<'NIXEXPR'
let dep = t: i: derivation {
  name = "hunt-dep-${t}-${toString i}";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "s=${toString i}x0123456789abcdef; j=0; while [ $j -lt 14 ]; do s=$s$s; j=$((j+1)); done; printf %s $s > $out" ];
}; in builtins.concatLists (builtins.genList (i: [ (dep "a" (i + 1)) (dep "b" (i + 1)) ]) @N@)
NIXEXPR
)
DEPS_EXPR=${DEPS_EXPR//@N@/$N_ITERS}
DEP_DRVS=$("${nix_cmd[@]}" eval --json --impure \
  --expr "map (d: builtins.unsafeDiscardStringContext d.drvPath) ($DEPS_EXPR)" | tr -d '[]"' | tr ',' ' ')
DEP_OUTS=$("${nix_cmd[@]}" eval --json --impure \
  --expr "map (d: builtins.unsafeDiscardStringContext d.outPath) ($DEPS_EXPR)" | tr -d '[]"' | tr ',' ' ')
# shellcheck disable=SC2086
"${nix_cmd[@]}" copy --no-check-sigs --derivation --to "local?root=$CHROOT" $DEP_DRVS >/dev/null 2>&1
# shellcheck disable=SC2086
NIX_CONFIG='sandbox = false' "${nix_cmd[@]}" build --no-link --max-jobs 16 \
  --store "local?root=$CHROOT" $(for d in $DEP_DRVS; do printf '%s^* ' "$d"; done) >/dev/null 2>&1 \
  || { echo "!!! host-side dep builds failed"; exit 1; }
# shellcheck disable=SC2086
"${nix_cmd[@]}" copy --no-check-sigs --from "local?root=$CHROOT" --to "file://$CACHE_DIR" $DEP_OUTS >/dev/null 2>&1
echo "   $(ls "$CACHE_DIR"/*.narinfo 2>/dev/null | wc -l) narinfos in the cache"

# ---------------------------------------------------------------------------
# the hunt
# ---------------------------------------------------------------------------
top_expr() {
  local e
  e=$(cat <<'NIXEXPR'
let dep = t: i: derivation {
  name = "hunt-dep-${t}-${toString i}";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "s=${toString i}x0123456789abcdef; j=0; while [ $j -lt 14 ]; do s=$s$s; j=$((j+1)); done; printf %s $s > $out" ];
}; in builtins.unsafeDiscardStringContext (derivation {
  name = "hunt-top-@I@";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  depA = dep "a" @I@;
  depB = dep "b" @I@;
  args = [ "-c" "test -e $depA -a -e $depB || exit 1; echo done-@I@ > $out" ];
}).drvPath
NIXEXPR
)
  printf %s "${e//@I@/$1}"
}

# Run one client under gdb so we can dump its threads if it wedges
# (ptrace_scope=1 forbids attaching afterwards). --return-child-result keeps
# the nix exit code observable.
launch_client() { # $1 store URI, $2 drv, $3 log file; sets LAUNCHED_PID
  # NB: no command substitution here -- the background job must be a child of
  # THIS shell for `wait` to see its exit code.
  gdb --batch --return-child-result -nx \
    -ex 'set pagination off' -ex run -ex 'thread apply all bt' \
    --args "$NIX" --extra-experimental-features 'nix-command flakes' \
    build --no-link -L --store "$1" "$2^*" >"$3" 2>&1 &
  LAUNCHED_PID=$!
}

hangs=0; fails=0
echo
echo "############################################################"
echo "# hunting: $N_ITERS iterations, cache at ${RATE} B/s, cap ${HANG_TIMEOUT}s"
echo "############################################################"
for i in $(seq 1 "$N_ITERS"); do
  t0=$SECONDS
  TOPDRV=$("${nix_cmd[@]}" eval --raw --impure --expr "$(top_expr "$i")")
  TOP_OUT=$(set +o pipefail; "${nix_cmd[@]}" derivation show "$TOPDRV" 2>/dev/null \
              | grep -oE '/nix/store/[a-z0-9]{32}-[^"]+' | grep -v '\.drv"\?$' | head -1)
  "${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N1" "$TOPDRV" >/dev/null 2>&1
  "${nix_cmd[@]}" copy --no-check-sigs --derivation --to "$N2" "$TOPDRV" >/dev/null 2>&1

  launch_client "$N1" "$TOPDRV" "$LOG_DIR/iter-$i.n1.log"; pid1=$LAUNCHED_PID
  sleep "0.$((i % 5))"   # cycle a small launch offset to scan timing windows
  launch_client "$N2" "$TOPDRV" "$LOG_DIR/iter-$i.n2.log"; pid2=$LAUNCHED_PID

  end=$((SECONDS + HANG_TIMEOUT))
  while [ "$SECONDS" -lt "$end" ]; do
    kill -0 "$pid1" 2>/dev/null || kill -0 "$pid2" 2>/dev/null || break
    sleep 1
  done

  hung=""
  kill -0 "$pid1" 2>/dev/null && hung="$pid1:n1"
  kill -0 "$pid2" 2>/dev/null && hung="$hung $pid2:n2"

  if [ -n "$hung" ]; then
    # The signature requires the output to already be valid server-side.
    valid=no
    timeout 15 "${nix_cmd[@]}" path-info --store "$N2" "$TOP_OUT" >/dev/null 2>&1 && valid=yes
    echo "!! iter $i: client(s) still running after ${HANG_TIMEOUT}s ($hung), output valid=$valid"
    for h in $hung; do
      pid=${h%%:*}; tag=${h##*:}
      # SIGINT makes batch gdb stop the inferior and run 'thread apply all bt'.
      kill -INT "$pid" 2>/dev/null || true
    done
    sleep 10 # give gdb time to unwind
    for h in $hung; do
      pid=${h%%:*}
      kill -9 "$pid" 2>/dev/null || true
    done
    docker exec node1 sh -c 'tail -n 50 /tmp/server.log' > "$LOG_DIR/iter-$i.server1.log" 2>&1 || true
    docker exec node2 sh -c 'tail -n 50 /tmp/server.log' > "$LOG_DIR/iter-$i.server2.log" 2>&1 || true
    if [ "$valid" = yes ]; then
      hangs=$((hangs+1)); KEEP=1
      echo "############################################################"
      echo "# HANG REPRODUCED at iteration $i (post-success client hang)"
      echo "# stacks + logs: $LOG_DIR/iter-$i.*  -- environment kept running"
      echo "############################################################"
      exit 2
    fi
    fails=$((fails+1))
    continue
  fi

  rc1=0; rc2=0
  wait "$pid1" || rc1=$?
  wait "$pid2" || rc2=$?
  ok=yes
  timeout 15 "${nix_cmd[@]}" path-info --store "$N2" "$TOP_OUT" >/dev/null 2>&1 || ok=no
  [ "$ok" = no ] && fails=$((fails+1))
  echo "   iter $i: rc=($rc1,$rc2) valid=$ok $((SECONDS - t0))s"
done

echo
echo "############################################################"
echo "# DONE: $N_ITERS iterations, hangs=$hangs, failures=$fails"
echo "############################################################"
[ "$hangs" = 0 ] && echo "# no hang reproduced"
