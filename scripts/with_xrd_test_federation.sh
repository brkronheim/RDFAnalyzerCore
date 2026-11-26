#!/usr/bin/env bash
# Run a command against an isolated, read-only, two-site XRootD federation.
set -euo pipefail

if [[ ${1:-} != --inside ]]; then
  if (( $# < 3 )); then
    echo "Usage: bash $0 INPUT_DIRECTORY NEW_RECEIPT_DIRECTORY COMMAND [ARG ...]" >&2
    exit 2
  fi
  inputs=$(realpath -e -- "$1")
  receipt=$(realpath -m -- "$2")
  [[ -d $inputs ]] || { echo "Missing input directory: $inputs" >&2; exit 2; }
  if [[ $receipt =~ [[:space:]] || ${#receipt} -gt 65 ]]; then
    echo "Use a short receipt path without whitespace (cmsd creates Unix sockets there)." >&2
    exit 2
  fi
  mkdir -- "$receipt"
  shift 2
  exec unshare --user --map-current-user --net --mount --keep-caps \
    bash "$0" --inside "$inputs" "$receipt" "$@"
fi

inputs=$2
receipt=$3
shift 3
rate=${RDF_BENCHMARK_LINK_MBIT:-100}
delay=${RDF_BENCHMARK_LINK_DELAY_MS:-18}
shared=${RDF_BENCHMARK_SHARED_MBIT:-0}
[[ $rate =~ ^[1-9][0-9]*$ && $delay =~ ^[0-9]+$ && $shared =~ ^[0-9]+$ ]] || exit 2
mkdir "$receipt/data" "$receipt/log"
ln -s -- "$inputs" "$receipt/data/events"
if [[ -n ${RDF_BENCHMARK_SECONDARY_INPUT:-} ]]; then
  mkdir "$receipt/data-b"
  ln -s -- "$(realpath -e -- "$RDF_BENCHMARK_SECONDARY_INPUT")" "$receipt/data-b/events"
fi
mount --make-rprivate /
cat >"$receipt/hosts" <<EOF
127.0.0.1 localhost $(hostname)
10.78.4.1 manager.bench manager
10.78.4.3 a.bench a
10.78.4.4 b.bench b
EOF
mount --bind "$receipt/hosts" /etc/hosts
ip link set lo up
ip link add brbench type bridge
ip addr add 10.78.4.1/24 dev brbench
ip link set brbench up

children=()
cleanup() {
  for child in "${children[@]}"; do kill "$child" 2>/dev/null || true; done
  wait || true
}
trap cleanup EXIT
echo "$$" >"$receipt/controller.pid"
client=()
if (( shared > 0 )); then
  # All worker processes share this namespace and receive server responses
  # through one shaped link, including responses arriving from different sites.
  unshare --net sleep infinity &
  ns=$!
  children+=("$ns")
  while [[ $(readlink "/proc/$ns/ns/net") == "$(readlink /proc/self/ns/net)" ]]; do sleep .02; done
  ip link add r-client type veth peer name uplink
  ip link set uplink netns "$ns"
  ip link set r-client master brbench
  ip link set r-client up
  tc qdisc add dev r-client root netem rate "${shared}mbit" limit 20000
  client=(nsenter -t "$ns" -n)
  "${client[@]}" ip addr add 10.78.4.8/24 dev uplink
  "${client[@]}" ip link set uplink up
  "${client[@]}" ip link set lo up
fi
native=(env -u LD_LIBRARY_PATH -u LD_PRELOAD)
for site in manager a b; do
  command=()
  role=manager port=21094 cport=21194
  if [[ $site != manager ]]; then
    role=server
    if [[ $site == a ]]; then addr=3 port=21095 cport=21195
    else addr=4 port=21096 cport=21196; fi
    unshare --net sleep infinity &
    ns=$!
    children+=("$ns")
    while [[ $(readlink "/proc/$ns/ns/net") == "$(readlink /proc/self/ns/net)" ]]; do sleep .02; done
    ip link add "r-$site" type veth peer name uplink
    ip link set uplink netns "$ns"
    ip link set "r-$site" master brbench
    ip link set "r-$site" up
    command=(nsenter -t "$ns" -n)
    "${command[@]}" ip addr add "10.78.4.$addr/24" dev uplink
    "${command[@]}" ip link set uplink up
    "${command[@]}" ip link set lo up
    "${command[@]}" tc qdisc add dev uplink root netem delay "${delay}ms" rate "${rate}mbit" limit 20000
    echo "$ns" >"$receipt/$site.ns"
  fi
  mkdir "$receipt/$site"
  store="$receipt/data"
  if [[ $site == b && -d $receipt/data-b ]]; then store="$receipt/data-b"; fi
  cat >"$receipt/$site.cfg" <<EOF
all.adminpath $receipt/$site
all.pidpath $receipt/$site
all.role $role
all.manager manager.bench 21194
all.export /events r/o
oss.localroot $store
xrd.port $port
if exec cmsd
xrd.port $cport
fi
cms.delay startup 1
EOF
  for daemon in cmsd xrootd; do
    "${command[@]}" "${native[@]}" "/usr/bin/$daemon" -I v4 -N "$site.bench" -S "$site" \
      -c "$receipt/$site.cfg" >"$receipt/log/$site-$daemon" 2>&1 &
    children+=("$!")
    echo "$!" >"$receipt/$site.$daemon.pid"
  done
  if [[ $site == manager ]]; then
    # A server that attempts its first cmsd login before listen() can spend
    # an entire reconnect interval outside a short fixture's measurement.
    for ((i=0; i<100; ++i)); do
      if [[ -n $(ss -H -lnt 'sport = :21194') ]]; then break; fi
      sleep .1
    done
    [[ -n $(ss -H -lnt 'sport = :21194') ]] || { echo "cmsd did not listen" >&2; exit 1; }
  fi
done

export RDF_BENCHMARK_XRD_URL=root://manager.bench:21094//events
export RDF_BENCHMARK_XRD_RECEIPT="$receipt"
sample=$(find -L "$inputs" -maxdepth 1 -type f -printf '%f\n' -quit)
[[ -n $sample ]] || { echo "Export at least one regular file" >&2; exit 2; }
for ((i=0; i<90; ++i)); do
  timeout 5 "${native[@]}" /usr/bin/xrdfs manager.bench:21094 locate -n "/events/$sample" \
    >"$receipt/locations" 2>"$receipt/locate-error" || true
  if [[ $(wc -l <"$receipt/locations") -ge 2 ]]; then break; fi
  sleep 1
done
[[ $(wc -l <"$receipt/locations") -ge 2 ]] || { echo "Two replicas unavailable. See $receipt/log" >&2; exit 1; }
qdisc() {
  for site in a b; do
    nsenter -t "$(cat "$receipt/$site.ns")" -n tc -s qdisc show dev uplink
  done
  if (( shared > 0 )); then tc -s qdisc show dev r-client; fi
}
qdisc >"$receipt/qdisc-before"
status=0
"${client[@]}" "$@" || status=$?
qdisc >"$receipt/qdisc-after"
exit "$status"
