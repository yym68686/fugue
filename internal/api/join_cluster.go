package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type joinClusterPlan struct {
	ServerURL  string   `json:"server_url"`
	Token      string   `json:"token"`
	NodeName   string   `json:"node_name"`
	NodeLabels []string `json:"node_labels"`
	NodeTaints []string `json:"node_taints"`
	RuntimeID  string   `json:"runtime_id"`
}

func (s *Server) handleJoinClusterNode(w http.ResponseWriter, r *http.Request) {
	if !s.clusterJoinConfigured() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "cluster join is not configured")
		return
	}

	var req struct {
		NodeKey     string            `json:"node_key"`
		NodeName    string            `json:"node_name"`
		RuntimeName string            `json:"runtime_name"`
		Endpoint    string            `json:"endpoint"`
		Labels      map[string]string `json:"labels"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	key, node, join, err := s.bootstrapJoinClusterNode(req.NodeKey, coalesceNodeName(req.NodeName, req.RuntimeName), req.Endpoint, req.Labels)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   key.ID,
		TenantID:  key.TenantID,
	}, "node.join_cluster", "node", node.ID, key.TenantID, map[string]string{
		"name":        node.Name,
		"node_key_id": key.ID,
		"runtime_id":  node.ID,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"node": node,
		"join": join,
	})
}

func (s *Server) handleJoinClusterNodeEnv(w http.ResponseWriter, r *http.Request) {
	if !s.clusterJoinConfigured() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "cluster join is not configured")
		return
	}
	if err := r.ParseForm(); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid form body")
		return
	}

	labels := parseCSVLabels(r.Form.Get("labels"))
	key, node, join, err := s.bootstrapJoinClusterNode(
		r.Form.Get("node_key"),
		coalesceNodeName(r.Form.Get("node_name"), r.Form.Get("runtime_name")),
		r.Form.Get("endpoint"),
		labels,
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   key.ID,
		TenantID:  key.TenantID,
	}, "node.join_cluster", "node", node.ID, key.TenantID, map[string]string{
		"name":        node.Name,
		"node_key_id": key.ID,
		"runtime_id":  node.ID,
	})

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "FUGUE_JOIN_SERVER=%s\n", shellQuote(join.ServerURL))
	fmt.Fprintf(w, "FUGUE_JOIN_TOKEN=%s\n", shellQuote(join.Token))
	fmt.Fprintf(w, "FUGUE_JOIN_NODE_NAME=%s\n", shellQuote(join.NodeName))
	fmt.Fprintf(w, "FUGUE_JOIN_NODE_LABELS=%s\n", shellQuote(strings.Join(join.NodeLabels, ",")))
	fmt.Fprintf(w, "FUGUE_JOIN_NODE_TAINTS=%s\n", shellQuote(strings.Join(join.NodeTaints, ",")))
	fmt.Fprintf(w, "FUGUE_JOIN_RUNTIME_ID=%s\n", shellQuote(join.RuntimeID))
}

func (s *Server) handleJoinClusterInstallScript(w http.ResponseWriter, r *http.Request) {
	if !s.clusterJoinConfigured() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "cluster join is not configured")
		return
	}
	w.Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
	_, _ = fmt.Fprint(w, s.joinClusterInstallScript(publicBaseURL(r)))
}

func (s *Server) bootstrapJoinClusterNode(nodeKey, nodeName, endpoint string, labels map[string]string) (model.NodeKey, model.Runtime, joinClusterPlan, error) {
	nodeKey = strings.TrimSpace(nodeKey)
	nodeName = strings.TrimSpace(nodeName)
	if nodeKey == "" || nodeName == "" {
		return model.NodeKey{}, model.Runtime{}, joinClusterPlan{}, store.ErrInvalidInput
	}

	key, runtimeObj, err := s.store.BootstrapClusterNode(nodeKey, nodeName, strings.TrimSpace(endpoint), labels)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, joinClusterPlan{}, err
	}
	join := joinClusterPlan{
		ServerURL:  s.clusterJoinServer,
		Token:      s.clusterJoinToken,
		NodeName:   runtimeObj.Name,
		NodeLabels: runtime.JoinNodeLabels(runtimeObj),
		NodeTaints: runtime.JoinNodeTaints(runtimeObj),
		RuntimeID:  runtimeObj.ID,
	}
	return key, runtimeObj, join, nil
}

func (s *Server) clusterJoinConfigured() bool {
	return strings.TrimSpace(s.clusterJoinServer) != "" && strings.TrimSpace(s.clusterJoinToken) != ""
}

func coalesceNodeName(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func parseCSVLabels(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	labels := map[string]string{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		value := ""
		if len(parts) == 2 {
			value = strings.TrimSpace(parts[1])
		}
		labels[key] = value
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func publicBaseURL(r *http.Request) string {
	scheme := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	return scheme + "://" + r.Host
}

func (s *Server) joinClusterInstallScript(apiBase string) string {
	return fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

FUGUE_API_BASE=${FUGUE_API_BASE:-%s}
FUGUE_K3S_CHANNEL="${FUGUE_K3S_CHANNEL:-stable}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

detect_default_node_name() {
  hostname -s 2>/dev/null || hostname
}

detect_public_ip() {
  if [ -n "${FUGUE_NODE_EXTERNAL_IP:-}" ]; then
    printf '%%s' "${FUGUE_NODE_EXTERNAL_IP}"
    return 0
  fi
  if command -v curl >/dev/null 2>&1; then
    local ip=""
    ip="$(curl -fsS --max-time 10 https://api.ipify.org 2>/dev/null || true)"
    if [ -n "${ip}" ]; then
      printf '%%s' "${ip}"
      return 0
    fi
  fi
  local route_ip=""
  route_ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')"
  if [ -n "${route_ip}" ]; then
    printf '%%s' "${route_ip}"
    return 0
  fi
  return 1
}

csv_to_yaml_list() {
  local key="$1"
  local csv="$2"
  local old_ifs="${IFS}"
  [ -n "${csv}" ] || return 0
  printf '%%s:\n' "${key}"
  IFS=','
  for item in ${csv}; do
    item="${item#"${item%%%%[![:space:]]*}"}"
    item="${item%%"${item##*[![:space:]]}"}"
    [ -n "${item}" ] || continue
    printf '  - "%%s"\n' "${item}"
  done
  IFS="${old_ifs}"
}

if [ "$(id -u)" -ne 0 ]; then
  echo "run with sudo, for example: curl -fsSL ${FUGUE_API_BASE}/install/join-cluster.sh | sudo FUGUE_NODE_KEY=... bash" >&2
  exit 1
fi

require_cmd curl
require_cmd systemctl
require_cmd ip

if [ -f /etc/systemd/system/k3s.service ] || systemctl list-unit-files 2>/dev/null | grep -q '^k3s\.service'; then
  echo "this VPS already runs k3s server; join-cluster.sh only supports agent nodes" >&2
  exit 1
fi

: "${FUGUE_NODE_KEY:?FUGUE_NODE_KEY is required}"

node_name="${FUGUE_NODE_NAME:-$(detect_default_node_name)}"
node_endpoint="${FUGUE_NODE_ENDPOINT:-${node_name}}"
node_external_ip="$(detect_public_ip || true)"
if [ -n "${node_external_ip}" ] && [ "${node_endpoint}" = "${node_name}" ]; then
  node_endpoint="${node_external_ip}"
fi

join_env="$(mktemp)"
cleanup() {
  rm -f "${join_env}"
}
trap cleanup EXIT

curl -fsSL --retry 3 --retry-delay 2 -X POST "${FUGUE_API_BASE}/v1/nodes/join-cluster/env" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "node_key=${FUGUE_NODE_KEY}" \
  --data-urlencode "node_name=${node_name}" \
  --data-urlencode "endpoint=${node_endpoint}" \
  --data-urlencode "labels=${FUGUE_RUNTIME_LABELS:-}" \
  >"${join_env}"

# shellcheck disable=SC1090
. "${join_env}"

mkdir -p /etc/rancher/k3s
{
  printf 'server: "%%s"\n' "${FUGUE_JOIN_SERVER}"
  printf 'token: "%%s"\n' "${FUGUE_JOIN_TOKEN}"
  printf 'node-name: "%%s"\n' "${FUGUE_JOIN_NODE_NAME}"
  printf 'write-kubeconfig-mode: "644"\n'
  if [ -n "${node_external_ip}" ]; then
    printf 'node-external-ip: "%%s"\n' "${node_external_ip}"
    printf 'flannel-external-ip: true\n'
  fi
  csv_to_yaml_list node-label "${FUGUE_JOIN_NODE_LABELS:-}"
  csv_to_yaml_list node-taint "${FUGUE_JOIN_NODE_TAINTS:-}"
} >/etc/rancher/k3s/config.yaml

if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL="${FUGUE_K3S_CHANNEL}" INSTALL_K3S_EXEC="agent" sh -
else
  systemctl enable k3s-agent
  systemctl restart k3s-agent
fi

systemctl enable k3s-agent
systemctl restart k3s-agent
systemctl is-active --quiet k3s-agent

cat <<EOF
Fugue node joined.
runtime_id=${FUGUE_JOIN_RUNTIME_ID}
node_name=${FUGUE_JOIN_NODE_NAME}
server=${FUGUE_JOIN_SERVER}
labels=${FUGUE_JOIN_NODE_LABELS}
taints=${FUGUE_JOIN_NODE_TAINTS}
EOF
`, strconv.Quote(apiBase))
}
