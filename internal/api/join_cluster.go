package api

import (
	"context"
	"errors"
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
	ServerURL        string   `json:"server_url"`
	Token            string   `json:"token"`
	BootstrapTokenID string   `json:"bootstrap_token_id,omitempty"`
	NodeName         string   `json:"node_name"`
	NodeLabels       []string `json:"node_labels"`
	NodeTaints       []string `json:"node_taints"`
	RuntimeID        string   `json:"runtime_id"`
	RegistryBase     string   `json:"registry_base,omitempty"`
	RegistryEndpoint string   `json:"registry_endpoint,omitempty"`
	MeshProvider     string   `json:"mesh_provider,omitempty"`
	MeshLoginServer  string   `json:"mesh_login_server,omitempty"`
	MeshAuthKey      string   `json:"mesh_auth_key,omitempty"`
}

func (s *Server) handleJoinClusterNode(w http.ResponseWriter, r *http.Request) {
	if !s.clusterJoinConfigured() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "cluster join is not configured")
		return
	}

	var req struct {
		NodeKey            string            `json:"node_key"`
		NodeName           string            `json:"node_name"`
		RuntimeName        string            `json:"runtime_name"`
		MachineName        string            `json:"machine_name"`
		MachineFingerprint string            `json:"machine_fingerprint"`
		Endpoint           string            `json:"endpoint"`
		Labels             map[string]string `json:"labels"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	key, node, join, err := s.bootstrapJoinClusterNode(
		r.Context(),
		req.NodeKey,
		coalesceNodeName(req.NodeName, req.RuntimeName),
		req.Endpoint,
		req.Labels,
		req.MachineName,
		req.MachineFingerprint,
	)
	if err != nil {
		s.writeJoinClusterError(w, err)
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
		r.Context(),
		r.Form.Get("node_key"),
		coalesceNodeName(r.Form.Get("node_name"), r.Form.Get("runtime_name")),
		r.Form.Get("endpoint"),
		labels,
		r.Form.Get("machine_name"),
		r.Form.Get("machine_fingerprint"),
	)
	if err != nil {
		s.writeJoinClusterError(w, err)
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
	fmt.Fprintf(w, "FUGUE_JOIN_BOOTSTRAP_TOKEN_ID=%s\n", shellQuote(join.BootstrapTokenID))
	fmt.Fprintf(w, "FUGUE_JOIN_NODE_NAME=%s\n", shellQuote(join.NodeName))
	fmt.Fprintf(w, "FUGUE_JOIN_NODE_LABELS=%s\n", shellQuote(strings.Join(join.NodeLabels, ",")))
	fmt.Fprintf(w, "FUGUE_JOIN_NODE_TAINTS=%s\n", shellQuote(strings.Join(join.NodeTaints, ",")))
	fmt.Fprintf(w, "FUGUE_JOIN_RUNTIME_ID=%s\n", shellQuote(join.RuntimeID))
	fmt.Fprintf(w, "FUGUE_JOIN_REGISTRY_BASE=%s\n", shellQuote(join.RegistryBase))
	fmt.Fprintf(w, "FUGUE_JOIN_REGISTRY_ENDPOINT=%s\n", shellQuote(join.RegistryEndpoint))
	fmt.Fprintf(w, "FUGUE_JOIN_MESH_PROVIDER=%s\n", shellQuote(join.MeshProvider))
	fmt.Fprintf(w, "FUGUE_JOIN_MESH_LOGIN_SERVER=%s\n", shellQuote(join.MeshLoginServer))
	fmt.Fprintf(w, "FUGUE_JOIN_MESH_AUTH_KEY=%s\n", shellQuote(join.MeshAuthKey))
}

func (s *Server) handleJoinClusterCleanup(w http.ResponseWriter, r *http.Request) {
	if !s.clusterJoinConfigured() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "cluster join is not configured")
		return
	}
	if err := r.ParseForm(); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid form body")
		return
	}

	nodeKey := strings.TrimSpace(r.Form.Get("node_key"))
	machineFingerprint := strings.TrimSpace(r.Form.Get("machine_fingerprint"))
	currentNodeName := strings.TrimSpace(r.Form.Get("current_node_name"))
	bootstrapTokenID := strings.TrimSpace(r.Form.Get("bootstrap_token_id"))
	if nodeKey == "" || machineFingerprint == "" || currentNodeName == "" {
		httpx.WriteError(w, http.StatusBadRequest, "node_key, machine_fingerprint, and current_node_name are required")
		return
	}

	key, err := s.store.AuthenticateNodeKey(nodeKey)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	snapshots, err := client.listClusterNodeInventory(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	currentNodeSeen := false
	staleSnapshots := make([]clusterNodeSnapshot, 0)
	for _, snapshot := range snapshots {
		if !snapshot.managedOwned || !clusterNodeSnapshotMatchesFingerprint(snapshot, machineFingerprint) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(snapshot.node.Name), currentNodeName) {
			currentNodeSeen = true
			continue
		}
		staleSnapshots = append(staleSnapshots, snapshot)
	}
	if !currentNodeSeen {
		httpx.WriteError(w, http.StatusConflict, "current node is not registered yet")
		return
	}

	principal := model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   key.ID,
		TenantID:  key.TenantID,
	}
	removedNodes := make([]string, 0, len(staleSnapshots))
	removedRuntimeIDs := make([]string, 0, len(staleSnapshots))
	for _, snapshot := range staleSnapshots {
		if err := client.deleteNode(r.Context(), snapshot.node.Name); err != nil && !isKubernetesNodeNotFound(err) {
			httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		removedNodes = append(removedNodes, snapshot.node.Name)

		runtimeID := strings.TrimSpace(snapshot.runtimeID)
		if runtimeID == "" {
			continue
		}
		detached, err := s.store.DetachRuntimeOwnership(runtimeID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			s.writeStoreError(w, err)
			return
		}
		removedRuntimeIDs = append(removedRuntimeIDs, detached.ID)
		s.appendAudit(principal, "node.cleanup_stale_cluster_node", "node", detached.ID, detached.TenantID, map[string]string{
			"name":              detached.Name,
			"cluster_node_name": snapshot.node.Name,
			"node_key_id":       key.ID,
		})
	}
	bootstrapTokenRemoved := false
	if bootstrapTokenID != "" {
		bootstrapTokenRemoved, err = client.deleteBootstrapTokenIfOwned(r.Context(), bootstrapTokenID, key.ID)
		if err != nil && s.log != nil {
			s.log.Printf("delete bootstrap token %s for node key %s: %v", bootstrapTokenID, key.ID, err)
		}
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "FUGUE_JOIN_CLEANUP_NODE_COUNT=%s\n", shellQuote(strconv.Itoa(len(removedNodes))))
	fmt.Fprintf(w, "FUGUE_JOIN_CLEANUP_NODES=%s\n", shellQuote(strings.Join(removedNodes, ",")))
	fmt.Fprintf(w, "FUGUE_JOIN_CLEANUP_RUNTIME_IDS=%s\n", shellQuote(strings.Join(removedRuntimeIDs, ",")))
	fmt.Fprintf(w, "FUGUE_JOIN_CLEANUP_BOOTSTRAP_TOKEN_ID=%s\n", shellQuote(bootstrapTokenID))
	fmt.Fprintf(w, "FUGUE_JOIN_CLEANUP_BOOTSTRAP_TOKEN_REMOVED=%s\n", shellQuote(strconv.FormatBool(bootstrapTokenRemoved)))
}

func (s *Server) handleJoinClusterInstallScript(w http.ResponseWriter, r *http.Request) {
	if !s.clusterJoinConfigured() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "cluster join is not configured")
		return
	}
	w.Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
	_, _ = fmt.Fprint(w, s.joinClusterInstallScript(publicBaseURL(r)))
}

func (s *Server) bootstrapJoinClusterNode(ctx context.Context, nodeKey, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, joinClusterPlan, error) {
	nodeKey = strings.TrimSpace(nodeKey)
	nodeName = strings.TrimSpace(nodeName)
	if nodeKey == "" {
		return model.NodeKey{}, model.Runtime{}, joinClusterPlan{}, store.ErrInvalidInput
	}

	key, runtimeObj, err := s.store.BootstrapClusterNode(nodeKey, nodeName, strings.TrimSpace(endpoint), labels, machineName, machineFingerprint)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, joinClusterPlan{}, err
	}
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, joinClusterPlan{}, err
	}
	token, bootstrapTokenID, err := client.createBootstrapToken(ctx, key.ID, runtimeObj.ID, s.clusterJoinCAHash, s.clusterJoinBootstrapTokenTTL)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, joinClusterPlan{}, err
	}
	join := joinClusterPlan{
		ServerURL:        s.clusterJoinServer,
		Token:            token,
		BootstrapTokenID: bootstrapTokenID,
		NodeName:         runtimeObj.Name,
		NodeLabels:       runtime.JoinNodeLabels(runtimeObj),
		NodeTaints:       runtime.JoinNodeTaints(runtimeObj),
		RuntimeID:        runtimeObj.ID,
		RegistryBase:     s.registryPullBase,
		RegistryEndpoint: s.clusterJoinRegistryEndpoint,
		MeshProvider:     s.clusterJoinMeshProvider,
		MeshLoginServer:  s.clusterJoinMeshLoginServer,
		MeshAuthKey:      s.clusterJoinMeshAuthKey,
	}
	return key, runtimeObj, join, nil
}

func (s *Server) clusterJoinConfigured() bool {
	if strings.TrimSpace(s.clusterJoinServer) == "" || s.clusterJoinBootstrapTokenTTL <= 0 {
		return false
	}
	if strings.TrimSpace(s.registryPullBase) == "" || strings.TrimSpace(s.clusterJoinRegistryEndpoint) == "" {
		return false
	}
	if s.clusterJoinMeshProvider == "" {
		return true
	}
	return strings.TrimSpace(s.clusterJoinMeshLoginServer) != "" && strings.TrimSpace(s.clusterJoinMeshAuthKey) != ""
}

type revokeNodeKeyCleanupResult struct {
	DeletedClusterNodes      []string `json:"deleted_cluster_nodes,omitempty"`
	DeletedBootstrapTokenIDs []string `json:"deleted_bootstrap_token_ids,omitempty"`
	DetachedRuntimeIDs       []string `json:"detached_runtime_ids,omitempty"`
	Warnings                 []string `json:"warnings,omitempty"`
}

func (s *Server) cleanupRevokedNodeKey(ctx context.Context, key model.NodeKey) revokeNodeKeyCleanupResult {
	result := revokeNodeKeyCleanupResult{}
	runtimes, err := s.store.ListRuntimesByNodeKey(key.ID, key.TenantID, false)
	if err != nil {
		result.Warnings = append(result.Warnings, "list runtimes for node key cleanup: "+err.Error())
		return result
	}
	needsClusterClient := s.clusterJoinConfigured()
	if !needsClusterClient {
		for _, runtimeObj := range runtimes {
			if runtimeObj.Type == model.RuntimeTypeManagedOwned {
				needsClusterClient = true
				break
			}
		}
	}
	if !needsClusterClient {
		return result
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, clientErr := clientFactory()
	if clientErr != nil {
		result.Warnings = append(result.Warnings, "connect to kubernetes for node key cleanup: "+clientErr.Error())
	} else {
		deletedTokenIDs, err := client.deleteBootstrapTokensByNodeKey(ctx, key.ID)
		if err != nil {
			result.Warnings = append(result.Warnings, "delete bootstrap tokens for node key cleanup: "+err.Error())
		} else {
			result.DeletedBootstrapTokenIDs = deletedTokenIDs
		}
	}

	for _, runtimeObj := range runtimes {
		if runtimeObj.Type != model.RuntimeTypeManagedOwned {
			continue
		}
		nodeName := strings.TrimSpace(runtimeObj.ClusterNodeName)
		switch {
		case nodeName == "":
		case clientErr != nil:
			result.Warnings = append(result.Warnings, "delete cluster node "+runtimeObj.Name+": kubernetes client unavailable")
			continue
		default:
			if err := client.deleteNode(ctx, nodeName); err != nil && !isKubernetesNodeNotFound(err) {
				result.Warnings = append(result.Warnings, "delete cluster node "+nodeName+": "+err.Error())
				continue
			}
			result.DeletedClusterNodes = append(result.DeletedClusterNodes, nodeName)
		}

		detached, err := s.store.DetachRuntimeOwnership(runtimeObj.ID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			result.Warnings = append(result.Warnings, "detach runtime "+runtimeObj.ID+": "+err.Error())
			continue
		}
		result.DetachedRuntimeIDs = append(result.DetachedRuntimeIDs, detached.ID)
	}
	return result
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

func (s *Server) writeJoinClusterError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrInvalidInput):
		s.writeStoreError(w, err)
	default:
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
	}
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

log_step() {
  printf '[fugue] %%s\n' "$*" >&2
}

wait_for_systemd_unit_active() {
  local unit="$1"
  local timeout_seconds="${2:-600}"
  local interval_seconds="${3:-10}"
  local elapsed=0
  local state=""
  local substate=""
  while [ "${elapsed}" -lt "${timeout_seconds}" ]; do
    if systemctl is-active --quiet "${unit}"; then
      log_step "${unit} is active."
      return 0
    fi
    state="$(systemctl is-active "${unit}" 2>/dev/null || true)"
    substate="$(systemctl show "${unit}" --property=SubState --value 2>/dev/null || true)"
    log_step "Waiting for ${unit} to become active (state=${state:-unknown}, substate=${substate:-unknown}, elapsed=${elapsed}s)..."
    sleep "${interval_seconds}"
    elapsed=$((elapsed + interval_seconds))
  done
  log_step "${unit} did not become active within ${timeout_seconds}s."
  systemctl status "${unit}" --no-pager || true
  return 1
}

run_systemd_action_and_wait() {
  local action="$1"
  local unit="$2"
  local timeout_seconds="${3:-600}"
  log_step "Requesting ${action} for ${unit}..."
  systemctl "${action}" --no-block "${unit}"
  wait_for_systemd_unit_active "${unit}" "${timeout_seconds}"
}

write_file_if_changed() {
  local source_path="$1"
  local target_path="$2"
  if [ -f "${target_path}" ] && cmp -s "${source_path}" "${target_path}"; then
    rm -f "${source_path}"
    return 1
  fi
  cp "${source_path}" "${target_path}"
  rm -f "${source_path}"
  return 0
}

remove_file_if_present() {
  local target_path="$1"
  if [ -f "${target_path}" ]; then
    rm -f "${target_path}"
    return 0
  fi
  return 1
}

detect_default_node_name() {
  hostname -s 2>/dev/null || hostname
}

detect_machine_fingerprint() {
  local value=""
  for path in /etc/machine-id /var/lib/dbus/machine-id /sys/class/dmi/id/product_uuid; do
    if [ -r "${path}" ]; then
      value="$(tr -d '[:space:]' < "${path}" 2>/dev/null || true)"
      if [ -n "${value}" ]; then
        printf '%%s' "${value}"
        return 0
      fi
    fi
  done
  detect_default_node_name
}

detect_public_ip() {
  if [ -n "${FUGUE_NODE_PUBLIC_IP:-}" ]; then
    printf '%%s' "${FUGUE_NODE_PUBLIC_IP}"
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

detect_public_country_json() {
  if ! command -v curl >/dev/null 2>&1; then
    return 1
  fi
  curl -fsS --max-time 5 "${FUGUE_NODE_GEO_URL:-https://ipapi.co/json/}" 2>/dev/null || true
}

extract_json_string() {
  local json="$1"
  local key="$2"
  printf '%%s' "${json}" | sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p"
}

detect_node_zone() {
  if [ -n "${FUGUE_NODE_ZONE:-}" ]; then
    printf '%%s' "${FUGUE_NODE_ZONE}"
    return 0
  fi
  return 1
}

detect_node_region() {
  if [ -n "${FUGUE_NODE_REGION:-}" ]; then
    printf '%%s' "${FUGUE_NODE_REGION}"
    return 0
  fi
  return 1
}

detect_node_country_code() {
  if [ -n "${FUGUE_NODE_COUNTRY_CODE:-}" ]; then
    printf '%%s' "${FUGUE_NODE_COUNTRY_CODE}" | tr '[:upper:]' '[:lower:]'
    return 0
  fi
  local json=""
  local country_code=""
  json="$(detect_public_country_json)"
  json="$(printf '%%s' "${json}" | tr -d '\r\n')"
  [ -n "${json}" ] || return 1
  country_code="$(extract_json_string "${json}" "country_code")"
  [ -n "${country_code}" ] || return 1
  printf '%%s' "${country_code}" | tr '[:upper:]' '[:lower:]'
}

csv_has_label_key() {
  local csv="$1"
  local key="$2"
  local old_ifs="${IFS}"
  local item=""
  local item_key=""
  IFS=','
  for item in ${csv}; do
    item="${item#"${item%%%%[![:space:]]*}"}"
    item="${item%%"${item##*[![:space:]]}"}"
    [ -n "${item}" ] || continue
    item_key="${item%%%%=*}"
    if [ "${item_key}" = "${key}" ]; then
      IFS="${old_ifs}"
      return 0
    fi
  done
  IFS="${old_ifs}"
  return 1
}

csv_append_label() {
  local csv="$1"
  local key="$2"
  local value="$3"
  if [ -z "${value}" ] || csv_has_label_key "${csv}" "${key}"; then
    printf '%%s' "${csv}"
    return 0
  fi
  if [ -n "${csv}" ]; then
    printf '%%s,%%s=%%s' "${csv}" "${key}" "${value}"
    return 0
  fi
  printf '%%s=%%s' "${key}" "${value}"
}

append_location_node_labels() {
  local labels="${FUGUE_JOIN_NODE_LABELS:-}"
  local zone=""
  local region=""
  local country_code=""
  local public_ip=""
  zone="$(detect_node_zone || true)"
  region="$(detect_node_region || true)"
  country_code="$(detect_node_country_code || true)"
  public_ip="${node_public_ip:-}"
  labels="$(csv_append_label "${labels}" "topology.kubernetes.io/region" "${region}")"
  labels="$(csv_append_label "${labels}" "topology.kubernetes.io/zone" "${zone}")"
  labels="$(csv_append_label "${labels}" "fugue.io/location-country-code" "${country_code}")"
  labels="$(csv_append_label "${labels}" "fugue.io/public-ip" "${public_ip}")"
  printf '%%s' "${labels}"
}

install_tailscale() {
  if command -v tailscale >/dev/null 2>&1; then
    return 0
  fi
  curl -fsSL https://tailscale.com/install.sh | sh 1>&2
}

wait_for_tailscale_ipv4() {
  local ip=""
  local attempt=""
  for attempt in $(seq 1 30); do
    ip="$(tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}')"
    if [ -n "${ip}" ]; then
      log_step "tailscale assigned IPv4 ${ip}."
      printf '%%s' "${ip}"
      return 0
    fi
    if [ "${attempt}" -eq 1 ] || [ $((attempt %% 5)) -eq 0 ]; then
      log_step "Waiting for tailscale IPv4 address (${attempt}/30)..."
    fi
    sleep 2
  done
  return 1
}

connect_mesh() {
  local provider="$1"
  local hostname="$2"
  case "${provider}" in
    '')
      return 1
      ;;
    tailscale)
      : "${FUGUE_JOIN_MESH_LOGIN_SERVER:?FUGUE_JOIN_MESH_LOGIN_SERVER is required for tailscale joins}"
      : "${FUGUE_JOIN_MESH_AUTH_KEY:?FUGUE_JOIN_MESH_AUTH_KEY is required for tailscale joins}"
      log_step "Connecting node to the tailscale mesh..."
      install_tailscale
      systemctl enable tailscaled
      if ! systemctl is-active --quiet tailscaled; then
        run_systemd_action_and_wait start tailscaled 60
      else
        log_step "tailscaled is already active."
      fi
      log_step "Running tailscale up for ${hostname}..."
      tailscale up \
        --login-server "${FUGUE_JOIN_MESH_LOGIN_SERVER}" \
        --authkey "${FUGUE_JOIN_MESH_AUTH_KEY}" \
        --hostname "${hostname}" \
        --accept-dns=false \
        --reset
      wait_for_tailscale_ipv4
      ;;
    *)
      echo "unsupported mesh provider: ${provider}" >&2
      exit 1
      ;;
  esac
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

cleanup_stale_cluster_nodes() {
  local attempt=""
  local http_code=""
  local response_file=""
  response_file="$(mktemp)"
  log_step "Cleaning up stale cluster-node records..."
  for attempt in $(seq 1 10); do
	    http_code="$(
	      curl -sS -o "${response_file}" -w '%%{http_code}' --max-time 5 -X POST "${FUGUE_API_BASE}/v1/nodes/join-cluster/cleanup" \
	      -H "Content-Type: application/x-www-form-urlencoded" \
	      --data-urlencode "node_key=${FUGUE_NODE_KEY}" \
	      --data-urlencode "machine_fingerprint=${machine_fingerprint}" \
	      --data-urlencode "current_node_name=${FUGUE_JOIN_NODE_NAME}" \
	      --data-urlencode "bootstrap_token_id=${FUGUE_JOIN_BOOTSTRAP_TOKEN_ID:-}" \
	      2>/dev/null || printf '000'
	    )"
    case "${http_code}" in
      200|204)
        log_step "Cluster-node cleanup completed."
        rm -f "${response_file}"
        return 0
        ;;
      000|409|429|5??)
        log_step "Cluster-node cleanup got HTTP ${http_code}; retrying (${attempt}/10)..."
        sleep 2
        ;;
      *)
        log_step "Cluster-node cleanup returned HTTP ${http_code}; skipping retries."
        rm -f "${response_file}"
        return 0
        ;;
    esac
  done
  log_step "Cluster-node cleanup timed out after repeated transient errors; continuing."
  rm -f "${response_file}"
  return 0
}

restart_k3s_agent_if_needed() {
  local config_changed="$1"
  systemctl enable k3s-agent
  if ! systemctl is-active --quiet k3s-agent; then
    if [ "${config_changed}" -eq 1 ]; then
      log_step "k3s-agent is not active; starting it with the updated configuration."
    else
      log_step "k3s-agent is not active; starting it."
    fi
    run_systemd_action_and_wait start k3s-agent 900
  elif [ "${config_changed}" -eq 1 ]; then
    log_step "k3s agent configuration changed; restarting k3s-agent."
    run_systemd_action_and_wait restart k3s-agent 900
  else
    log_step "k3s-agent configuration unchanged; service already active, skipping restart."
  fi
  systemctl is-active --quiet k3s-agent
}

if [ "$(id -u)" -ne 0 ]; then
  echo "run with sudo, for example: curl -fsSL ${FUGUE_API_BASE}/install/join-cluster.sh | sudo FUGUE_NODE_KEY=... bash" >&2
  exit 1
fi

require_cmd curl
require_cmd systemctl
require_cmd ip
require_cmd cmp

if [ -f /etc/systemd/system/k3s.service ] || systemctl list-unit-files 2>/dev/null | grep -q '^k3s\.service'; then
  echo "this VPS already runs k3s server; join-cluster.sh only supports agent nodes" >&2
  exit 1
fi

: "${FUGUE_NODE_KEY:?FUGUE_NODE_KEY is required}"

node_name="${FUGUE_NODE_NAME:-$(detect_default_node_name)}"
machine_name="${FUGUE_MACHINE_NAME:-${node_name}}"
machine_fingerprint="${FUGUE_MACHINE_FINGERPRINT:-$(detect_machine_fingerprint)}"
node_endpoint="${FUGUE_NODE_ENDPOINT:-${node_name}}"
node_public_ip="$(detect_public_ip || true)"
node_external_ip="${FUGUE_NODE_EXTERNAL_IP:-${node_public_ip}}"
if [ -n "${node_public_ip}" ] && [ "${node_endpoint}" = "${node_name}" ]; then
  node_endpoint="${node_public_ip}"
fi

join_env="$(mktemp)"
cleanup() {
  rm -f "${join_env}"
}
trap cleanup EXIT

log_step "Requesting join parameters from control plane..."
curl -fsSL --retry 3 --retry-delay 2 -X POST "${FUGUE_API_BASE}/v1/nodes/join-cluster/env" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "node_key=${FUGUE_NODE_KEY}" \
  --data-urlencode "node_name=${node_name}" \
  --data-urlencode "machine_name=${machine_name}" \
  --data-urlencode "machine_fingerprint=${machine_fingerprint}" \
  --data-urlencode "endpoint=${node_endpoint}" \
  --data-urlencode "labels=${FUGUE_RUNTIME_LABELS:-}" \
  >"${join_env}"

# shellcheck disable=SC1090
. "${join_env}"
FUGUE_JOIN_NODE_LABELS="$(append_location_node_labels)"
log_step "Join parameters received for node ${FUGUE_JOIN_NODE_NAME}."

mesh_provider="${FUGUE_JOIN_MESH_PROVIDER:-}"
flannel_iface=""
if [ -n "${mesh_provider}" ]; then
  node_external_ip="$(connect_mesh "${mesh_provider}" "${FUGUE_JOIN_NODE_NAME}")"
  flannel_iface="tailscale0"
fi

log_step "Preparing k3s agent configuration..."
mkdir -p /etc/rancher/k3s
k3s_config_tmp="$(mktemp)"
{
  printf 'server: "%%s"\n' "${FUGUE_JOIN_SERVER}"
  printf 'token: "%%s"\n' "${FUGUE_JOIN_TOKEN}"
  printf 'node-name: "%%s"\n' "${FUGUE_JOIN_NODE_NAME}"
  if [ -n "${node_external_ip}" ]; then
    printf 'node-external-ip: "%%s"\n' "${node_external_ip}"
  fi
  if [ -n "${flannel_iface}" ]; then
    printf 'flannel-iface: "%%s"\n' "${flannel_iface}"
  fi
  csv_to_yaml_list node-label "${FUGUE_JOIN_NODE_LABELS:-}"
  csv_to_yaml_list node-taint "${FUGUE_JOIN_NODE_TAINTS:-}"
} >"${k3s_config_tmp}"

k3s_config_changed=0
if write_file_if_changed "${k3s_config_tmp}" /etc/rancher/k3s/config.yaml; then
  log_step "Updated /etc/rancher/k3s/config.yaml."
  k3s_config_changed=1
else
  log_step "/etc/rancher/k3s/config.yaml is unchanged."
fi

if [ -n "${FUGUE_JOIN_REGISTRY_BASE:-}" ] && [ -n "${FUGUE_JOIN_REGISTRY_ENDPOINT:-}" ]; then
  k3s_registry_tmp="$(mktemp)"
  cat >"${k3s_registry_tmp}" <<EOF_REG
mirrors:
  "${FUGUE_JOIN_REGISTRY_BASE}":
    endpoint:
      - "http://${FUGUE_JOIN_REGISTRY_ENDPOINT}"
configs:
  "${FUGUE_JOIN_REGISTRY_BASE}":
    tls:
      insecure_skip_verify: true
EOF_REG
  if write_file_if_changed "${k3s_registry_tmp}" /etc/rancher/k3s/registries.yaml; then
    log_step "Updated /etc/rancher/k3s/registries.yaml."
    k3s_config_changed=1
  else
    log_step "/etc/rancher/k3s/registries.yaml is unchanged."
  fi
elif remove_file_if_present /etc/rancher/k3s/registries.yaml; then
  log_step "Removed /etc/rancher/k3s/registries.yaml."
  k3s_config_changed=1
fi

if ! command -v k3s >/dev/null 2>&1; then
  log_step "k3s agent is not installed; downloading and installing binaries..."
  curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL="${FUGUE_K3S_CHANNEL}" INSTALL_K3S_EXEC="agent" INSTALL_K3S_SKIP_START="true" sh -
  log_step "k3s agent install completed; starting service."
  restart_k3s_agent_if_needed 1
else
  restart_k3s_agent_if_needed "${k3s_config_changed}"
fi

systemctl is-active --quiet k3s-agent
cleanup_stale_cluster_nodes
log_step "Cluster node join finished."

cat <<EOF
Fugue node joined.
runtime_id=${FUGUE_JOIN_RUNTIME_ID}
node_name=${FUGUE_JOIN_NODE_NAME}
server=${FUGUE_JOIN_SERVER}
registry_base=${FUGUE_JOIN_REGISTRY_BASE:-}
registry_endpoint=${FUGUE_JOIN_REGISTRY_ENDPOINT:-}
labels=${FUGUE_JOIN_NODE_LABELS}
taints=${FUGUE_JOIN_NODE_TAINTS}
EOF
`, strconv.Quote(apiBase))
}
