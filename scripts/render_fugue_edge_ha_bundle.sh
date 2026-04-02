#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: render_fugue_edge_ha_bundle.sh [--output-dir <dir>]

Renders an HAProxy + Keepalived bundle for placing a health-checked VIP in front
of multiple Fugue edge nodes that each run the Route A Caddy config.

Required environment:
  FUGUE_EDGE_VIP           Virtual IP exposed to clients.
  FUGUE_EDGE_INTERFACE     Network interface that owns the VIP.
  FUGUE_EDGE_AUTH_PASS     VRRP auth password.
  FUGUE_EDGE_BACKENDS      Comma-separated list of edge node IPs or hostnames.
  FUGUE_EDGE_PEER_IPS      Comma-separated list of the HAProxy/Keepalived peer IPs.

Optional environment:
  FUGUE_EDGE_ROUTER_ID         VRRP router id. Default: 51
  FUGUE_EDGE_PRIMARY_PRIORITY  Primary node priority. Default: 120
  FUGUE_EDGE_SECONDARY_PRIORITY Secondary node priority. Default: 100
EOF
}

OUTPUT_DIR=".dist/edge-ha"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf '[fugue-edge-ha] ERROR: unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    printf '[fugue-edge-ha] ERROR: missing required environment variable %s\n' "${name}" >&2
    exit 1
  fi
}

trim() {
  printf '%s' "$1" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

require_env FUGUE_EDGE_VIP
require_env FUGUE_EDGE_INTERFACE
require_env FUGUE_EDGE_AUTH_PASS
require_env FUGUE_EDGE_BACKENDS
require_env FUGUE_EDGE_PEER_IPS

FUGUE_EDGE_ROUTER_ID="${FUGUE_EDGE_ROUTER_ID:-51}"
FUGUE_EDGE_PRIMARY_PRIORITY="${FUGUE_EDGE_PRIMARY_PRIORITY:-120}"
FUGUE_EDGE_SECONDARY_PRIORITY="${FUGUE_EDGE_SECONDARY_PRIORITY:-100}"

mkdir -p "${OUTPUT_DIR}"

IFS=',' read -r -a backend_items <<<"${FUGUE_EDGE_BACKENDS}"
IFS=',' read -r -a peer_items <<<"${FUGUE_EDGE_PEER_IPS}"

http_servers=""
https_servers=""
index=0
for raw in "${backend_items[@]}"; do
  backend="$(trim "${raw}")"
  [[ -n "${backend}" ]] || continue
  index=$((index + 1))
  http_servers="${http_servers}  server edge${index} ${backend}:80 check\n"
  https_servers="${https_servers}  server edge${index} ${backend}:443 check\n"
done

peer_lines=""
for raw in "${peer_items[@]}"; do
  peer="$(trim "${raw}")"
  [[ -n "${peer}" ]] || continue
  peer_lines="${peer_lines}    ${peer}\n"
done

cat >"${OUTPUT_DIR}/haproxy.cfg" <<EOF
global
  log /dev/log local0
  log /dev/log local1 notice
  daemon
  maxconn 4096
  stats socket /run/haproxy/admin.sock mode 660 level admin

defaults
  mode tcp
  log global
  option tcplog
  timeout connect 5s
  timeout client 60s
  timeout server 60s

frontend fugue-http
  bind :80
  default_backend fugue-http-backend

frontend fugue-https
  bind :443
  default_backend fugue-https-backend

backend fugue-http-backend
  balance roundrobin
  option tcp-check
$(printf '%b' "${http_servers}")

backend fugue-https-backend
  balance roundrobin
  option tcp-check
$(printf '%b' "${https_servers}")

listen stats
  bind :8404
  mode http
  stats enable
  stats uri /stats
EOF

cat >"${OUTPUT_DIR}/keepalived-primary.conf" <<EOF
vrrp_script chk_haproxy {
  script "/usr/bin/pgrep haproxy"
  interval 2
  timeout 1
  fall 2
  rise 2
}

vrrp_instance VI_FUGUE_EDGE {
  state MASTER
  interface ${FUGUE_EDGE_INTERFACE}
  virtual_router_id ${FUGUE_EDGE_ROUTER_ID}
  priority ${FUGUE_EDGE_PRIMARY_PRIORITY}
  advert_int 1
  authentication {
    auth_type PASS
    auth_pass ${FUGUE_EDGE_AUTH_PASS}
  }
  unicast_peer {
$(printf '%b' "${peer_lines}")
  }
  virtual_ipaddress {
    ${FUGUE_EDGE_VIP}
  }
  track_script {
    chk_haproxy
  }
}
EOF

cat >"${OUTPUT_DIR}/keepalived-secondary.conf" <<EOF
vrrp_script chk_haproxy {
  script "/usr/bin/pgrep haproxy"
  interval 2
  timeout 1
  fall 2
  rise 2
}

vrrp_instance VI_FUGUE_EDGE {
  state BACKUP
  interface ${FUGUE_EDGE_INTERFACE}
  virtual_router_id ${FUGUE_EDGE_ROUTER_ID}
  priority ${FUGUE_EDGE_SECONDARY_PRIORITY}
  advert_int 1
  authentication {
    auth_type PASS
    auth_pass ${FUGUE_EDGE_AUTH_PASS}
  }
  unicast_peer {
$(printf '%b' "${peer_lines}")
  }
  virtual_ipaddress {
    ${FUGUE_EDGE_VIP}
  }
  track_script {
    chk_haproxy
  }
}
EOF

cat >"${OUTPUT_DIR}/README.txt" <<EOF
Rendered Fugue edge HA bundle

Files:
  - haproxy.cfg
  - keepalived-primary.conf
  - keepalived-secondary.conf

Use these files on a pair of dedicated edge/VIP nodes. The backends listed in
haproxy.cfg should be Fugue edge nodes already running the Route A Caddy config.
EOF

printf '[fugue-edge-ha] rendered bundle under %s\n' "${OUTPUT_DIR}"
