#!/usr/bin/env bash
# Run only on a disposable Linux development/hosted CI runner. Creates no
# host interfaces or external routes; invokes no Kubernetes or release writer.
set -euo pipefail

if [[ $(uname -s) != Linux ]]; then
  echo 'Edge relay network rehearsal requires disposable Linux with iproute2' >&2
  exit 2
fi
command -v ip >/dev/null
sudo -n true
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
dir="$(mktemp -d "${TMPDIR:-/tmp}/fugue-origin-relay.XXXXXXXX")"
suffix="${dir##*.}"
source_ns="fugue-relay-a-$suffix"
peer_ns="fugue-relay-b-$suffix"
origin_ns="fugue-relay-o-$suffix"
namespaces=()
host_files=()
children=()
cleanup() {
  local rc=$? ns path pid
  trap - EXIT
  touch "$dir/stop"
  for pid in "${children[@]}"; do
    wait "$pid" || rc=1
  done
  for ns in "${namespaces[@]}"; do
    sudo -n ip netns del "$ns" || rc=1
  done
  for path in "${host_files[@]}"; do
    sudo -n rm -- "$path/hosts" || rc=1
    sudo -n rmdir -- "$path" || rc=1
  done
  if (( rc != 0 )); then
    for path in "$dir/egress.log" "$dir/origin.log"; do
      if [[ -f "$path" ]]; then sed -n '1,120p' "$path"; fi
    done
  fi
  # All files below this newly allocated lab-only path are synthetic.
  rm -r -- "$dir"
  exit "$rc"
}
trap cleanup EXIT

cd "$root"
go test -c -o "$dir/rehearsal.test" ./internal/edge
FUGUE_RELAY_REHEARSAL_ROLE=material FUGUE_RELAY_REHEARSAL_DIR="$dir" \
  "$dir/rehearsal.test" -test.run '^TestOriginRelayPrivateNetworkRehearsal$' -test.timeout=10s

for ns in "$source_ns" "$peer_ns" "$origin_ns"; do
  sudo -n ip netns add "$ns"
  namespaces+=("$ns")
  sudo -n ip -n "$ns" link set lo up
done
sudo -n ip -n "$source_ns" link add to-peer type veth peer name to-source netns "$peer_ns"
sudo -n ip -n "$peer_ns" link add to-origin type veth peer name to-peer netns "$origin_ns"
while read -r ns link addr; do
  sudo -n ip -n "$ns" address add "$addr" dev "$link"
  sudo -n ip -n "$ns" link set "$link" up
done <<EOF
$source_ns to-peer 10.231.1.1/30
$peer_ns to-source 10.231.1.2/30
$peer_ns to-origin 10.231.2.1/30
$origin_ns to-peer 10.231.2.2/30
EOF
for ns in "$source_ns" "$peer_ns"; do
  path="/etc/netns/$ns"
  sudo -n mkdir -m 0755 -- "$path"
  host_files+=("$path")
  printf '10.231.2.2 origin.rehearsal.svc.cluster.local\n' | sudo -n tee "$path/hosts" >/dev/null
done
for ns in "${namespaces[@]}"; do
  if [[ -n "$(sudo -n ip -n "$ns" route show default)" || -n "$(sudo -n ip -n "$ns" -6 route show default)" ]]; then
    echo "unexpected external route in fixture namespace" >&2
    exit 1
  fi
done

run_role() {
  sudo -n ip netns exec "$1" /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    GODEBUG=netdns=go FUGUE_RELAY_REHEARSAL_ROLE="$2" FUGUE_RELAY_REHEARSAL_DIR="$dir" \
    "$dir/rehearsal.test" -test.v -test.run '^TestOriginRelayPrivateNetworkRehearsal$' -test.timeout="$3"
}
run_role "$origin_ns" origin 105s >"$dir/origin.log" 2>&1 &
children+=("$!")
run_role "$peer_ns" egress 105s >"$dir/egress.log" 2>&1 &
children+=("$!")
run_role "$source_ns" ingress 90s
