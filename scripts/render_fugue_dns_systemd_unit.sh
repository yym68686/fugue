#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  render_fugue_dns_systemd_unit.sh --output-dir <dir> \
    --api-url <fugue-api-url> \
    --dns-node-id <node> \
    --edge-group-id <edge-group> \
    --answer-ips <ip[,ip...]> \
    [--route-a-answer-ips <ip[,ip...]>] \
    --token-env-file /etc/fugue/fugue-dns-token.env

Renders fugue-dns.service and fugue-dns.env for a systemd escape hatch.
The token env file must define FUGUE_DNS_TOKEN and is intentionally separate
so generated artifacts can be reviewed without embedding secrets.
USAGE
}

output_dir=""
api_url="${FUGUE_API_URL:-}"
dns_node_id="${FUGUE_DNS_NODE_ID:-$(hostname -s 2>/dev/null || hostname)}"
edge_group_id="${FUGUE_EDGE_GROUP_ID:-}"
zone="${FUGUE_DNS_ZONE:-}"
answer_ips="${FUGUE_DNS_ANSWER_IPS:-}"
route_a_answer_ips="${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-}"
token_env_file="/etc/fugue/fugue-dns-token.env"
binary_path="/usr/local/bin/fugue-dns"
cache_path="/var/lib/fugue/dns/dns-cache.json"
listen_addr="127.0.0.1:7834"
udp_addr=":53"
tcp_addr=":53"
ttl="60"
sync_interval="15s"
heartbeat_interval="30s"
http_timeout="10s"
nameservers=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) output_dir="${2:-}"; shift 2 ;;
    --api-url) api_url="${2:-}"; shift 2 ;;
    --dns-node-id) dns_node_id="${2:-}"; shift 2 ;;
    --edge-group-id) edge_group_id="${2:-}"; shift 2 ;;
    --zone) zone="${2:-}"; shift 2 ;;
    --answer-ips) answer_ips="${2:-}"; shift 2 ;;
    --route-a-answer-ips) route_a_answer_ips="${2:-}"; shift 2 ;;
    --token-env-file) token_env_file="${2:-}"; shift 2 ;;
    --binary) binary_path="${2:-}"; shift 2 ;;
    --cache-path) cache_path="${2:-}"; shift 2 ;;
    --listen-addr) listen_addr="${2:-}"; shift 2 ;;
    --udp-addr) udp_addr="${2:-}"; shift 2 ;;
    --tcp-addr) tcp_addr="${2:-}"; shift 2 ;;
    --ttl) ttl="${2:-}"; shift 2 ;;
    --sync-interval) sync_interval="${2:-}"; shift 2 ;;
    --heartbeat-interval) heartbeat_interval="${2:-}"; shift 2 ;;
    --http-timeout) http_timeout="${2:-}"; shift 2 ;;
    --nameservers) nameservers="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "${output_dir}" || -z "${api_url}" || -z "${dns_node_id}" || -z "${edge_group_id}" || -z "${answer_ips}" ]]; then
  usage >&2
  exit 2
fi

mkdir -p "${output_dir}"

cat >"${output_dir}/fugue-dns.env" <<ENV
FUGUE_API_URL=${api_url}
FUGUE_DNS_NODE_ID=${dns_node_id}
FUGUE_EDGE_GROUP_ID=${edge_group_id}
FUGUE_DNS_ZONE=${zone}
FUGUE_DNS_ANSWER_IPS=${answer_ips}
FUGUE_DNS_ROUTE_A_ANSWER_IPS=${route_a_answer_ips}
FUGUE_DNS_CACHE_PATH=${cache_path}
FUGUE_DNS_LISTEN_ADDR=${listen_addr}
FUGUE_DNS_UDP_ADDR=${udp_addr}
FUGUE_DNS_TCP_ADDR=${tcp_addr}
FUGUE_DNS_TTL=${ttl}
FUGUE_DNS_SYNC_INTERVAL=${sync_interval}
FUGUE_DNS_HEARTBEAT_INTERVAL=${heartbeat_interval}
FUGUE_DNS_HTTP_TIMEOUT=${http_timeout}
FUGUE_DNS_NAMESERVERS=${nameservers}
ENV

cat >"${output_dir}/fugue-dns.service" <<SERVICE
[Unit]
Description=Fugue authoritative DNS shadow node
Documentation=https://github.com/yym68686/fugue
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/fugue/fugue-dns.env
EnvironmentFile=-${token_env_file}
ExecStart=${binary_path}
Restart=always
RestartSec=5s
User=root
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/fugue/dns
PrivateTmp=true

[Install]
WantedBy=multi-user.target
SERVICE

echo "rendered ${output_dir}/fugue-dns.env"
echo "rendered ${output_dir}/fugue-dns.service"
echo "install with: sudo install -m 0644 ${output_dir}/fugue-dns.env /etc/fugue/fugue-dns.env"
echo "install with: sudo install -m 0644 ${output_dir}/fugue-dns.service /etc/systemd/system/fugue-dns.service"
echo "token file must contain: FUGUE_DNS_TOKEN=<edge-scoped-token>"
