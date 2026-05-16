package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type joinClusterPlan struct {
	ServerURL        string   `json:"server_url"`
	ServerFallbacks  []string `json:"server_fallback_urls,omitempty"`
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

const auditActionNodeJoinClusterRequested = "node.join_cluster_requested"

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

	key, machine, runtimeObj, join, err := s.bootstrapJoinClusterNode(
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
	targetType := "machine"
	targetID := machine.ID
	targetTenantID := machine.TenantID
	auditMetadata := map[string]string{
		"name":              join.NodeName,
		"cluster_node_name": join.NodeName,
		"node_key_id":       key.ID,
	}
	if runtimeObj != nil {
		targetType = "node"
		targetID = runtimeObj.ID
		targetTenantID = runtimeObj.TenantID
		auditMetadata["runtime_id"] = runtimeObj.ID
	}
	s.appendAudit(model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   key.ID,
		TenantID:  key.TenantID,
	}, auditActionNodeJoinClusterRequested, targetType, targetID, targetTenantID, auditMetadata)
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"join":    join,
		"machine": machine,
		"node":    joinNodeResponse(machine, runtimeObj),
		"runtime": runtimeObj,
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
	key, machine, runtimeObj, join, err := s.bootstrapJoinClusterNode(
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

	targetType := "machine"
	targetID := machine.ID
	targetTenantID := machine.TenantID
	auditMetadata := map[string]string{
		"name":              join.NodeName,
		"cluster_node_name": join.NodeName,
		"node_key_id":       key.ID,
	}
	if runtimeObj != nil {
		targetType = "node"
		targetID = runtimeObj.ID
		targetTenantID = runtimeObj.TenantID
		auditMetadata["runtime_id"] = runtimeObj.ID
	}
	s.appendAudit(model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   key.ID,
		TenantID:  key.TenantID,
	}, auditActionNodeJoinClusterRequested, targetType, targetID, targetTenantID, auditMetadata)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "FUGUE_JOIN_SERVER=%s\n", shellQuote(join.ServerURL))
	fmt.Fprintf(w, "FUGUE_JOIN_SERVER_FALLBACKS=%s\n", shellQuote(strings.Join(join.ServerFallbacks, ",")))
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

	snapshots, err := s.loadClusterNodeInventory(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
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

	currentNodeSeen := false
	staleSnapshots := make([]clusterNodeSnapshot, 0)
	for _, snapshot := range snapshots {
		if !clusterNodeSnapshotManaged(snapshot) || !clusterNodeSnapshotMatchesFingerprint(snapshot, machineFingerprint) {
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
	_, _ = fmt.Fprint(w, s.joinClusterInstallScript(s.publicInstallAPIBaseURL(r)))
}

func (s *Server) bootstrapJoinClusterNode(ctx context.Context, nodeKey, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Machine, *model.Runtime, joinClusterPlan, error) {
	nodeKey = strings.TrimSpace(nodeKey)
	nodeName = strings.TrimSpace(nodeName)
	if nodeKey == "" {
		return model.NodeKey{}, model.Machine{}, nil, joinClusterPlan{}, store.ErrInvalidInput
	}

	key, machine, runtimeObj, err := s.store.BootstrapClusterAttachment(nodeKey, nodeName, strings.TrimSpace(endpoint), labels, machineName, machineFingerprint)
	if err != nil {
		return model.NodeKey{}, model.Machine{}, nil, joinClusterPlan{}, err
	}
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return model.NodeKey{}, model.Machine{}, nil, joinClusterPlan{}, err
	}
	runtimeID := ""
	if runtimeObj != nil {
		runtimeID = runtimeObj.ID
	}
	token, bootstrapTokenID, err := client.createBootstrapToken(ctx, key.ID, runtimeID, s.clusterJoinCAHash, s.clusterJoinBootstrapTokenTTL)
	if err != nil {
		return model.NodeKey{}, model.Machine{}, nil, joinClusterPlan{}, err
	}
	labelMap := joinClusterLabelMap(machine, runtimeObj)
	join := joinClusterPlan{
		ServerURL:        s.clusterJoinServer,
		ServerFallbacks:  append([]string(nil), s.clusterJoinServerFallbacks...),
		Token:            token,
		BootstrapTokenID: bootstrapTokenID,
		NodeName:         firstNonEmpty(machine.ClusterNodeName, nodeName),
		NodeLabels:       joinClusterLabels(labelMap),
		RuntimeID:        runtimeID,
		RegistryBase:     s.registryPullBase,
		RegistryEndpoint: s.clusterJoinRegistryEndpoint,
		MeshProvider:     s.clusterJoinMeshProvider,
		MeshLoginServer:  s.clusterJoinMeshLoginServer,
		MeshAuthKey:      s.clusterJoinMeshAuthKey,
	}
	if runtimeObj != nil {
		join.NodeTaints = runtime.JoinNodeTaints(*runtimeObj)
		if join.NodeName == "" {
			join.NodeName = firstNonEmpty(runtimeObj.ClusterNodeName, runtimeObj.Name, nodeName)
		}
	} else {
		join.NodeTaints = machineJoinTaints(machine)
	}
	return key, machine, runtimeObj, join, nil
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
	machines, err := s.store.ListMachinesByNodeKey(key.ID, key.TenantID, true)
	if err != nil {
		result.Warnings = append(result.Warnings, "list machines for node key cleanup: "+err.Error())
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
		for _, machine := range machines {
			if strings.TrimSpace(machine.ClusterNodeName) != "" {
				needsClusterClient = true
				break
			}
		}
	}

	var client *clusterNodeClient
	var clientErr error
	if needsClusterClient {
		clientFactory := s.newClusterNodeClient
		if clientFactory == nil {
			clientFactory = newClusterNodeClient
		}
		client, clientErr = clientFactory()
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
	}

	for _, runtimeObj := range runtimes {
		if runtimeObj.Type == model.RuntimeTypeManagedOwned {
			nodeName := strings.TrimSpace(runtimeObj.ClusterNodeName)
			switch {
			case nodeName == "":
			case clientErr != nil:
				result.Warnings = append(result.Warnings, "delete cluster node "+nodeName+": kubernetes client unavailable")
			default:
				if err := client.deleteNode(ctx, nodeName); err != nil && !isKubernetesNodeNotFound(err) {
					result.Warnings = append(result.Warnings, "delete cluster node "+nodeName+": "+err.Error())
				} else {
					result.DeletedClusterNodes = append(result.DeletedClusterNodes, nodeName)
				}
			}
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
	deletedNodes := make(map[string]struct{}, len(result.DeletedClusterNodes))
	for _, name := range result.DeletedClusterNodes {
		deletedNodes[name] = struct{}{}
	}
	for _, machine := range machines {
		if strings.TrimSpace(machine.RuntimeID) != "" {
			continue
		}
		nodeName := strings.TrimSpace(machine.ClusterNodeName)
		if nodeName == "" {
			continue
		}
		if _, exists := deletedNodes[nodeName]; exists {
			continue
		}
		switch {
		case clientErr != nil:
			result.Warnings = append(result.Warnings, "delete cluster node "+nodeName+": kubernetes client unavailable")
		default:
			if err := client.deleteNode(ctx, nodeName); err != nil && !isKubernetesNodeNotFound(err) {
				result.Warnings = append(result.Warnings, "delete cluster node "+nodeName+": "+err.Error())
			} else {
				result.DeletedClusterNodes = append(result.DeletedClusterNodes, nodeName)
				deletedNodes[nodeName] = struct{}{}
			}
		}
	}
	return result
}

func joinNodeResponse(machine model.Machine, runtimeObj *model.Runtime) any {
	if runtimeObj != nil {
		return *runtimeObj
	}
	return machine
}

func joinClusterLabelMap(machine model.Machine, runtimeObj *model.Runtime) map[string]string {
	labels := machineJoinLabelMap(machine)
	if runtimeObj == nil {
		return labels
	}
	for key, value := range runtime.JoinNodeLabelMap(*runtimeObj) {
		labels[key] = value
	}
	return labels
}

func machineJoinLabelMap(machine model.Machine) map[string]string {
	labels := map[string]string{
		runtime.NodeKeyIDLabelKey:    strings.TrimSpace(machine.NodeKeyID),
		runtime.MachineIDLabelKey:    strings.TrimSpace(machine.ID),
		runtime.MachineScopeLabelKey: model.NormalizeMachineScope(machine.Scope),
	}
	if machine.Policy.AllowBuilds {
		labels[runtime.BuildNodeLabelKey] = runtime.BuildNodeLabelValue
		labels[runtime.BuilderRoleLabelKey] = runtime.NodeRoleLabelValue
	}
	if machine.Policy.AllowAppRuntime {
		labels[runtime.AppRuntimeRoleLabelKey] = runtime.NodeRoleLabelValue
	}
	if machine.Policy.AllowEdge {
		labels[runtime.EdgeRoleLabelKey] = runtime.NodeRoleLabelValue
	}
	if machine.Policy.AllowDNS {
		labels[runtime.DNSRoleLabelKey] = runtime.NodeRoleLabelValue
	}
	if machine.Policy.AllowInternalMaintenance {
		labels[runtime.InternalMaintenanceLabelKey] = runtime.NodeRoleLabelValue
	}
	if role := model.NormalizeMachineControlPlaneRole(machine.Policy.DesiredControlPlaneRole); role != "" && role != model.MachineControlPlaneRoleNone {
		labels[runtime.ControlPlaneDesiredRoleKey] = role
	}
	if machine.RuntimeID == "" && machine.Policy.AllowSharedPool {
		labels[runtime.SharedPoolLabelKey] = runtime.SharedPoolLabelValue
	}
	for key, value := range labels {
		if strings.TrimSpace(value) == "" {
			delete(labels, key)
		}
	}
	return labels
}

func machineJoinTaints(machine model.Machine) []string {
	dedicatedValue, ok := machineDedicatedTaintValue(machine.Policy)
	if !ok {
		return nil
	}
	return []string{
		runtime.DedicatedTaintKey + "=" + dedicatedValue + ":NoSchedule",
	}
}

func joinClusterLabels(labels map[string]string) []string {
	if len(labels) == 0 {
		return nil
	}
	keys := make([]string, 0, len(labels))
	for key, value := range labels {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+labels[key])
	}
	return out
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
	case errors.Is(err, store.ErrInvalidInput):
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrConflict):
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

func (s *Server) publicInstallAPIBaseURL(r *http.Request) string {
	domain := strings.TrimRight(strings.TrimSpace(s.apiPublicDomain), "/")
	if domain == "" {
		return publicBaseURL(r)
	}
	if strings.Contains(domain, "://") {
		return domain
	}
	return "https://" + domain
}

func (s *Server) joinClusterInstallScript(apiBase string) string {
	return fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

FUGUE_API_BASE=${FUGUE_API_BASE:-%s}
FUGUE_JOIN_SCRIPT_VERSION="${FUGUE_JOIN_SCRIPT_VERSION:-v1}"
FUGUE_K3S_CHANNEL="${FUGUE_K3S_CHANNEL:-stable}"
FUGUE_LIMIT_CPU="${FUGUE_LIMIT_CPU:-}"
FUGUE_LIMIT_MEMORY="${FUGUE_LIMIT_MEMORY:-}"
FUGUE_LIMIT_DISK="${FUGUE_LIMIT_DISK:-}"
FUGUE_LIMIT_DISK_PATH="${FUGUE_LIMIT_DISK_PATH:-/}"
FUGUE_PROGRESS_HEARTBEAT_SECONDS="${FUGUE_PROGRESS_HEARTBEAT_SECONDS:-15}"
FUGUE_NODE_UPDATER_ENABLED="${FUGUE_NODE_UPDATER_ENABLED:-true}"
FUGUE_NODE_UPDATER_POLL_INTERVAL="${FUGUE_NODE_UPDATER_POLL_INTERVAL:-5min}"
FUGUE_DISCOVERY_STATE_DIR="${FUGUE_DISCOVERY_STATE_DIR:-/var/lib/fugue-node-updater}"
FUGUE_DISCOVERY_BUNDLE_FILE="${FUGUE_DISCOVERY_BUNDLE_FILE:-${FUGUE_DISCOVERY_STATE_DIR}/discovery-bundle.json}"
FUGUE_DISCOVERY_ENV_FILE="${FUGUE_DISCOVERY_ENV_FILE:-${FUGUE_DISCOVERY_STATE_DIR}/discovery.env}"
FUGUE_DISCOVERY_STATE_ENV_FILE="${FUGUE_DISCOVERY_STATE_ENV_FILE:-${FUGUE_DISCOVERY_STATE_DIR}/state.env}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

log_step() {
  printf '[fugue] %%s\n' "$*" >&2
}

format_duration() {
  local total_seconds="${1:-0}"
  local hours=0
  local minutes=0
  local seconds=0
  case "${total_seconds}" in
    ''|*[!0-9]*)
      total_seconds=0
      ;;
  esac
  hours=$((total_seconds / 3600))
  minutes=$(((total_seconds %% 3600) / 60))
  seconds=$((total_seconds %% 60))
  if [ "${hours}" -gt 0 ]; then
    printf '%%dh%%02dm%%02ds' "${hours}" "${minutes}" "${seconds}"
    return 0
  fi
  if [ "${minutes}" -gt 0 ]; then
    printf '%%dm%%02ds' "${minutes}" "${seconds}"
    return 0
  fi
  printf '%%ss' "${seconds}"
}

registry_endpoint_url() {
  case "${1:-}" in
    http://*|https://*)
      printf '%%s' "$1"
      ;;
    *)
      printf 'http://%%s' "$1"
      ;;
  esac
}

json_quote_env() {
  local value="$1"
  value="${value//\'/\'\\\'\'}"
  printf "'%%s'" "${value}"
}

render_discovery_env() {
  local bundle_file="$1"
  if command -v python3 >/dev/null 2>&1; then
    python3 - "${bundle_file}" <<'PY_DISCOVERY'
import base64
import hashlib
import hmac
import json
import os
import shlex
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    bundle = json.load(fh)

schema_version = str(bundle.get("schema_version", "")).strip()
if not schema_version:
    raise SystemExit("DiscoveryBundle schema_version is empty")
if schema_version.split(".", 1)[0] != "1":
    raise SystemExit(f"unsupported DiscoveryBundle schema_version: {schema_version}")
if not str(bundle.get("signature", "")).strip():
    raise SystemExit("DiscoveryBundle signature is empty")

def payload_for_signature(key_id, valid_until):
    payload = {}
    for key in (
        "schema_version",
        "generation",
        "previous_generation",
        "generated_at",
        "valid_until",
        "issuer",
        "key_id",
        "api_endpoints",
        "kubernetes",
        "registry",
        "edge_groups",
        "edge_nodes",
        "dns_nodes",
        "platform_routes",
        "public_runtime_env",
    ):
        value = bundle.get(key)
        if key == "valid_until":
            value = valid_until
        elif key == "key_id":
            value = key_id
        if value in ("", None, [], {}):
            continue
        payload[key] = value
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def verify_signature():
    keys = {}
    active_key = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY", "").strip()
    active_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY_ID", bundle.get("key_id", "")).strip()
    previous_key = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY", "").strip()
    previous_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID", "").strip()
    if active_key and active_key_id:
        keys[active_key_id.lower()] = active_key
    if previous_key and previous_key_id:
        keys[previous_key_id.lower()] = previous_key
    if not keys:
        return
    revoked = {item.strip().lower() for item in os.environ.get("FUGUE_BUNDLE_REVOKED_KEY_IDS", "").replace(";", ",").split(",") if item.strip()}
    candidates = [(bundle.get("key_id", ""), bundle.get("signature", ""), bundle.get("valid_until", ""))]
    for item in bundle.get("signatures") or []:
        candidates.append((item.get("key_id", ""), item.get("signature", ""), item.get("valid_until", bundle.get("valid_until", ""))))
    for key_id, signature, valid_until in candidates:
        key_id = str(key_id or "").strip()
        signature = str(signature or "").strip()
        if not key_id or not signature or key_id.lower() in revoked:
            continue
        key = keys.get(key_id.lower())
        if not key:
            continue
        digest = hmac.new(key.encode("utf-8"), payload_for_signature(key_id, valid_until), hashlib.sha256).digest()
        expected = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        if hmac.compare_digest(signature, expected):
            return
    raise SystemExit("DiscoveryBundle signature verification failed")

verify_signature()

def first_named(items, name):
    for item in items or []:
        if str(item.get("name", "")).strip() == name:
            return item
    return {}

def emit(key, value):
    if value is None:
        value = ""
    if isinstance(value, list):
        value = ",".join(str(item) for item in value)
    print(f"{key}={shlex.quote(str(value))}")

runtime = bundle.get("public_runtime_env") or {}
kube = first_named(bundle.get("kubernetes"), "cluster-join")
registry = first_named(bundle.get("registry"), "registry")
emit("FUGUE_DISCOVERY_SCHEMA_VERSION", bundle.get("schema_version", ""))
emit("FUGUE_DISCOVERY_GENERATION", bundle.get("generation", ""))
emit("FUGUE_DISCOVERY_VALID_UNTIL", bundle.get("valid_until", ""))
emit("FUGUE_DISCOVERY_API_URL", runtime.get("FUGUE_API_URL") or first_named(bundle.get("api_endpoints"), "public").get("url", ""))
emit("FUGUE_DISCOVERY_REGISTRY_PULL_BASE", runtime.get("FUGUE_REGISTRY_PULL_BASE") or registry.get("pull_base", ""))
emit("FUGUE_DISCOVERY_REGISTRY_MIRROR", runtime.get("FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT") or registry.get("mirror", ""))
emit("FUGUE_DISCOVERY_K3S_SERVER", kube.get("server", ""))
emit("FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS", ",".join(kube.get("fallback_servers") or []))
emit("FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT", kube.get("registry_endpoint", ""))
PY_DISCOVERY
    return 0
  fi
  local generation=""
  local schema_version=""
  schema_version="$(sed -n 's/.*"schema_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${bundle_file}" | head -n 1)"
  case "${schema_version}" in
    1|1.*) ;;
    *) echo "unsupported DiscoveryBundle schema_version: ${schema_version:-missing}" >&2; return 1 ;;
  esac
  generation="$(sed -n 's/.*"generation"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${bundle_file}" | head -n 1)"
  printf 'FUGUE_DISCOVERY_SCHEMA_VERSION=%%s\n' "$(json_quote_env "${schema_version}")"
  printf 'FUGUE_DISCOVERY_GENERATION=%%s\n' "$(json_quote_env "${generation}")"
}

fetch_discovery_bundle() {
  local tmp=""
  local env_tmp=""
  mkdir -p "${FUGUE_DISCOVERY_STATE_DIR}"
  tmp="$(mktemp)"
  if ! curl -fsSL --retry 3 --retry-delay 2 "${FUGUE_API_BASE}/v1/discovery/bundle" -o "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  write_file_if_changed "${tmp}" "${FUGUE_DISCOVERY_BUNDLE_FILE}" || true
  env_tmp="$(mktemp)"
  if render_discovery_env "${FUGUE_DISCOVERY_BUNDLE_FILE}" >"${env_tmp}"; then
    write_file_if_changed "${env_tmp}" "${FUGUE_DISCOVERY_ENV_FILE}" || true
  else
    rm -f "${env_tmp}"
    return 1
  fi
}

apply_discovery_join_defaults() {
  if [ -r "${FUGUE_DISCOVERY_ENV_FILE}" ]; then
    # shellcheck disable=SC1090
    . "${FUGUE_DISCOVERY_ENV_FILE}"
  fi
  if [ -n "${FUGUE_DISCOVERY_K3S_SERVER:-}" ]; then
    FUGUE_JOIN_SERVER="${FUGUE_DISCOVERY_K3S_SERVER}"
  fi
  if [ -n "${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS:-}" ]; then
    FUGUE_JOIN_SERVER_FALLBACKS="${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS}"
  fi
  if [ -n "${FUGUE_DISCOVERY_REGISTRY_PULL_BASE:-}" ]; then
    FUGUE_JOIN_REGISTRY_BASE="${FUGUE_DISCOVERY_REGISTRY_PULL_BASE}"
  fi
  if [ -n "${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}" ]; then
    FUGUE_JOIN_REGISTRY_ENDPOINT="${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT}"
  elif [ -n "${FUGUE_DISCOVERY_REGISTRY_MIRROR:-}" ]; then
    FUGUE_JOIN_REGISTRY_ENDPOINT="${FUGUE_DISCOVERY_REGISTRY_MIRROR}"
  fi
  if [ -n "${FUGUE_DISCOVERY_GENERATION:-}" ]; then
    FUGUE_JOIN_DISCOVERY_GENERATION="${FUGUE_DISCOVERY_GENERATION}"
  fi
}

write_join_discovery_state() {
  local state_tmp=""
  mkdir -p "${FUGUE_DISCOVERY_STATE_DIR}"
  state_tmp="$(mktemp)"
  {
    write_env_var FUGUE_DISCOVERY_GENERATION "${FUGUE_DISCOVERY_GENERATION:-}"
    write_env_var FUGUE_DISCOVERY_BUNDLE_FILE "${FUGUE_DISCOVERY_BUNDLE_FILE}"
    write_env_var FUGUE_DISCOVERY_ENV_FILE "${FUGUE_DISCOVERY_ENV_FILE}"
    write_env_var FUGUE_JOIN_DISCOVERY_GENERATION "${FUGUE_JOIN_DISCOVERY_GENERATION:-}"
  } >"${state_tmp}"
  write_file_if_changed "${state_tmp}" "${FUGUE_DISCOVERY_STATE_ENV_FILE}" || true
}

run_with_heartbeat() {
  local label="$1"
  local expected="$2"
  local started_at=""
  local elapsed=0
  local next_notice="${FUGUE_PROGRESS_HEARTBEAT_SECONDS}"
  local rc=0
  local command_pid=0
  shift 2

  started_at="$(date +%%s)"
  log_step "${label} (expected ${expected})."
  "$@" &
  command_pid=$!

  while kill -0 "${command_pid}" 2>/dev/null; do
    sleep 1
    if ! kill -0 "${command_pid}" 2>/dev/null; then
      break
    fi
    elapsed=$(( $(date +%%s) - started_at ))
    if [ "${elapsed}" -ge "${next_notice}" ]; then
      log_step "${label} is still running after $(format_duration "${elapsed}") (expected ${expected}). This is normal on a first install."
      next_notice=$((next_notice + FUGUE_PROGRESS_HEARTBEAT_SECONDS))
    fi
  done

  if wait "${command_pid}"; then
    rc=0
  else
    rc=$?
  fi
  elapsed=$(( $(date +%%s) - started_at ))
  if [ "${rc}" -ne 0 ]; then
    log_step "${label} failed after $(format_duration "${elapsed}")."
    return "${rc}"
  fi
  log_step "${label} completed in $(format_duration "${elapsed}")."
}

print_install_timeline() {
  log_step "Starting Fugue cluster join for node ${node_name}."
  log_step "Estimated time: usually under 2 minutes on an existing node, 3-12 minutes on a fresh node."
  log_step "Longest steps: first k3s download/install (2-10 minutes) and first k3s-agent startup (30 seconds to 5 minutes)."
  log_step "Heartbeat: long-running steps print progress every ${FUGUE_PROGRESS_HEARTBEAT_SECONDS}s so the install never looks stuck."
}

usage() {
  cat >&2 <<EOF_USAGE
Usage: join-cluster.sh [--cpu LIMIT] [--memory LIMIT] [--disk LIMIT]

Optional resource caps can also be provided as environment variables:
  FUGUE_LIMIT_CPU=2
  FUGUE_LIMIT_MEMORY=4Gi
  FUGUE_LIMIT_DISK=50Gi

Accepted formats:
  CPU: integer cores (2), decimal cores (1.5), or millicores (1500m)
  Memory/Disk: Kubernetes-style sizes such as 4096Mi, 4Gi, 500G, or raw bytes

Examples:
  curl -fsSL ${FUGUE_API_BASE}/install/join-cluster.sh | \\
    sudo FUGUE_NODE_KEY='...' \\
    FUGUE_LIMIT_CPU='2' \\
    FUGUE_LIMIT_MEMORY='4Gi' \\
    FUGUE_LIMIT_DISK='50Gi' \\
    bash

  curl -fsSL ${FUGUE_API_BASE}/install/join-cluster.sh | \\
    sudo FUGUE_NODE_KEY='...' bash -s -- --cpu 2 --memory 4Gi --disk 50Gi
EOF_USAGE
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --cpu|--cpu-limit)
        [ "$#" -ge 2 ] || {
          echo "$1 requires a value" >&2
          usage
          exit 1
        }
        FUGUE_LIMIT_CPU="$2"
        shift 2
        ;;
      --memory|--memory-limit)
        [ "$#" -ge 2 ] || {
          echo "$1 requires a value" >&2
          usage
          exit 1
        }
        FUGUE_LIMIT_MEMORY="$2"
        shift 2
        ;;
      --disk|--disk-limit|--ephemeral-storage)
        [ "$#" -ge 2 ] || {
          echo "$1 requires a value" >&2
          usage
          exit 1
        }
        FUGUE_LIMIT_DISK="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

wait_for_systemd_unit_active() {
  local unit="$1"
  local timeout_seconds="${2:-600}"
  local interval_seconds="${3:-}"
  local expected="${4:-30s-5m}"
  local started_at=""
  local elapsed=0
  local state=""
  local substate=""
  if [ -z "${interval_seconds}" ]; then
    interval_seconds="${FUGUE_PROGRESS_HEARTBEAT_SECONDS}"
  fi
  started_at="$(date +%%s)"
  while [ "${elapsed}" -lt "${timeout_seconds}" ]; do
    elapsed=$(( $(date +%%s) - started_at ))
    if systemctl is-active --quiet "${unit}"; then
      log_step "${unit} is active after $(format_duration "${elapsed}")."
      return 0
    fi
    state="$(systemctl is-active "${unit}" 2>/dev/null || true)"
    substate="$(systemctl show "${unit}" --property=SubState --value 2>/dev/null || true)"
    log_step "Waiting for ${unit} to become active (state=${state:-unknown}, substate=${substate:-unknown}, elapsed=$(format_duration "${elapsed}"), expected ${expected})..."
    sleep "${interval_seconds}"
  done
  log_step "${unit} did not become active within $(format_duration "${timeout_seconds}")."
  systemctl status "${unit}" --no-pager || true
  return 1
}

run_systemd_action_and_wait() {
  local action="$1"
  local unit="$2"
  local timeout_seconds="${3:-600}"
  local expected="${4:-30s-5m}"
  log_step "Requesting ${action} for ${unit} (expected ${expected})..."
  systemctl "${action}" --no-block "${unit}"
  wait_for_systemd_unit_active "${unit}" "${timeout_seconds}" "${FUGUE_PROGRESS_HEARTBEAT_SECONDS}" "${expected}"
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

sanitize_k3s_env_file() {
  local target_path="$1"
  local sanitized_tmp=""
  [ -f "${target_path}" ] || return 1
  sanitized_tmp="$(mktemp)"
  awk '
    /^[[:space:]]*#/ { print; next }
    /^[[:space:]]*$/ { print; next }
    /^[[:space:]]*(export[[:space:]]+|declare[[:space:]]+-x[[:space:]]+)?K3S_[A-Za-z0-9_]*=/ { next }
    { print }
  ' "${target_path}" >"${sanitized_tmp}"
  write_file_if_changed "${sanitized_tmp}" "${target_path}"
}

sanitize_k3s_agent_environment_files() {
  local sanitized_any=1
  local target_path=""
  for target_path in \
    /etc/default/k3s-agent \
    /etc/sysconfig/k3s-agent \
    /etc/systemd/system/k3s-agent.service.env; do
    if sanitize_k3s_env_file "${target_path}"; then
      log_step "Sanitized stale K3S_* environment overrides from ${target_path}."
      sanitized_any=0
    fi
  done
  if [ "${sanitized_any}" -eq 0 ]; then
    return 0
  fi
  log_step "k3s-agent environment files are already free of stale K3S_* overrides."
  return 1
}

ensure_k3s_agent_service_override() {
  local k3s_binary=""
  local override_tmp=""
  k3s_binary="$(command -v k3s 2>/dev/null || true)"
  [ -n "${k3s_binary}" ] || return 1
  mkdir -p /etc/systemd/system/k3s-agent.service.d
  override_tmp="$(mktemp)"
  cat >"${override_tmp}" <<EOF_K3S_AGENT_OVERRIDE
[Service]
# Fugue manages these through /etc/rancher/k3s/config.yaml during cluster join.
Environment="K3S_URL="
Environment="K3S_TOKEN="
Environment="K3S_TOKEN_FILE="
Environment="K3S_CONFIG_FILE="
Environment="K3S_CONFIG_FILE_MODE="
Environment="K3S_CLUSTER_SECRET="
Environment="K3S_AGENT_TOKEN="
Environment="K3S_AGENT_TOKEN_FILE="
Environment="K3S_NODE_NAME="
Environment="K3S_NODE_LABEL="
Environment="K3S_NODE_TAINT="
Environment="K3S_FLANNEL_IFACE="
Environment="K3S_NODE_EXTERNAL_IP="
Environment="K3S_KUBELET_ARG="
ExecStart=
ExecStart=${k3s_binary} agent
EOF_K3S_AGENT_OVERRIDE
  if write_file_if_changed "${override_tmp}" /etc/systemd/system/k3s-agent.service.d/10-fugue-managed.conf; then
    log_step "Updated k3s-agent systemd override to ignore stale installer settings."
    systemctl daemon-reload
    return 0
  fi
  log_step "k3s-agent systemd override is unchanged."
  return 1
}

ensure_k3s_agent_non_stub_resolv_conf() {
  local current_target=""
  local backup_path=""
  if [ ! -L /etc/resolv.conf ]; then
    log_step "/etc/resolv.conf is not a symlink; leaving resolver configuration unchanged."
    return 1
  fi
  current_target="$(readlink /etc/resolv.conf 2>/dev/null || true)"
  case "${current_target}" in
    *stub-resolv.conf)
      ;;
    *)
      log_step "/etc/resolv.conf is not the systemd-resolved stub; leaving resolver configuration unchanged."
      return 1
      ;;
  esac
  if [ ! -s /run/systemd/resolve/resolv.conf ]; then
    log_step "systemd-resolved upstream resolv.conf is unavailable; leaving resolver configuration unchanged."
    return 1
  fi
  backup_path="/etc/resolv.conf.fugue-stub-$(date -u +%%Y%%m%%dT%%H%%M%%SZ)"
  cp -P /etc/resolv.conf "${backup_path}" || true
  ln -sfn ../run/systemd/resolve/resolv.conf /etc/resolv.conf
  log_step "Pointed /etc/resolv.conf at /run/systemd/resolve/resolv.conf for k3s/containerd image pulls; backup is ${backup_path}."
  return 0
}

trim_whitespace() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%%"${value##*[![:space:]]}"}"
  printf '%%s' "${value}"
}

append_csv_value() {
  local csv="$1"
  local entry="$2"
  if [ -z "${entry}" ]; then
    printf '%%s' "${csv}"
    return 0
  fi
  if [ -n "${csv}" ]; then
    printf '%%s,%%s' "${csv}" "${entry}"
    return 0
  fi
  printf '%%s' "${entry}"
}

parse_cpu_millicores() {
  local raw=""
  raw="$(trim_whitespace "$1")"
  [ -n "${raw}" ] || return 1
  awk -v raw="${raw}" '
    BEGIN {
      value = raw
      if (value ~ /m$/) {
        sub(/m$/, "", value)
        if (value !~ /^[0-9]+$/ || value <= 0) {
          exit 1
        }
        printf "%%d", value
        exit 0
      }
      if (value !~ /^[0-9]+([.][0-9]+)?$/) {
        exit 1
      }
      milli = value * 1000
      if (milli <= 0) {
        exit 1
      }
      printf "%%d", milli + 0.5
    }
  '
}

parse_quantity_bytes() {
  local raw=""
  raw="$(trim_whitespace "$1")"
  [ -n "${raw}" ] || return 1
  awk -v raw="${raw}" '
    BEGIN {
      value = tolower(raw)
      if (value !~ /^[0-9]+([.][0-9]+)?([kmgtpe]i?b?)?$/) {
        exit 1
      }
      number = value
      unit = ""
      if (match(value, /[kmgtpe]i?b?$/)) {
        unit = substr(value, RSTART, RLENGTH)
        number = substr(value, 1, RSTART - 1)
      }
      multiplier = 1
      if (unit == "k" || unit == "kb") {
        multiplier = 1000
      } else if (unit == "ki" || unit == "kib") {
        multiplier = 1024
      } else if (unit == "m" || unit == "mb") {
        multiplier = 1000 ^ 2
      } else if (unit == "mi" || unit == "mib") {
        multiplier = 1024 ^ 2
      } else if (unit == "g" || unit == "gb") {
        multiplier = 1000 ^ 3
      } else if (unit == "gi" || unit == "gib") {
        multiplier = 1024 ^ 3
      } else if (unit == "t" || unit == "tb") {
        multiplier = 1000 ^ 4
      } else if (unit == "ti" || unit == "tib") {
        multiplier = 1024 ^ 4
      } else if (unit == "p" || unit == "pb") {
        multiplier = 1000 ^ 5
      } else if (unit == "pi" || unit == "pib") {
        multiplier = 1024 ^ 5
      } else if (unit == "e" || unit == "eb") {
        multiplier = 1000 ^ 6
      } else if (unit == "ei" || unit == "eib") {
        multiplier = 1024 ^ 6
      }
      bytes = number * multiplier
      if (bytes <= 0) {
        exit 1
      }
      printf "%%.0f", bytes
    }
  '
}

format_cpu_millicores() {
  printf '%%sm' "$1"
}

format_bytes_quantity() {
  local bytes="$1"
  if [ "${bytes}" -ge 1125899906842624 ] && [ $((bytes %% 1125899906842624)) -eq 0 ]; then
    printf '%%sPi' "$((bytes / 1125899906842624))"
    return 0
  fi
  if [ "${bytes}" -ge 1099511627776 ] && [ $((bytes %% 1099511627776)) -eq 0 ]; then
    printf '%%sTi' "$((bytes / 1099511627776))"
    return 0
  fi
  if [ "${bytes}" -ge 1073741824 ] && [ $((bytes %% 1073741824)) -eq 0 ]; then
    printf '%%sGi' "$((bytes / 1073741824))"
    return 0
  fi
  if [ "${bytes}" -ge 1048576 ] && [ $((bytes %% 1048576)) -eq 0 ]; then
    printf '%%sMi' "$((bytes / 1048576))"
    return 0
  fi
  if [ "${bytes}" -ge 1024 ] && [ $((bytes %% 1024)) -eq 0 ]; then
    printf '%%sKi' "$((bytes / 1024))"
    return 0
  fi
  printf '%%s' "${bytes}"
}

detect_total_cpu_millicores() {
  local cores=""
  cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
  if [ -z "${cores}" ] && command -v nproc >/dev/null 2>&1; then
    cores="$(nproc 2>/dev/null || true)"
  fi
  case "${cores}" in
    ''|*[!0-9]*)
      return 1
      ;;
  esac
  printf '%%s' "$((cores * 1000))"
}

detect_total_memory_bytes() {
  local kib=""
  kib="$(awk '/MemTotal:/ {print $2; exit}' /proc/meminfo 2>/dev/null || true)"
  case "${kib}" in
    ''|*[!0-9]*)
      return 1
      ;;
  esac
  printf '%%s' "$((kib * 1024))"
}

detect_total_disk_bytes() {
  local path="${FUGUE_LIMIT_DISK_PATH:-/}"
  local bytes=""
  bytes="$(df -B1 -P "${path}" 2>/dev/null | awk 'NR == 2 {print $2; exit}' || true)"
  case "${bytes}" in
    ''|*[!0-9]*)
      return 1
      ;;
  esac
  printf '%%s' "${bytes}"
}

FUGUE_EFFECTIVE_LIMIT_CPU=""
FUGUE_EFFECTIVE_LIMIT_MEMORY=""
FUGUE_EFFECTIVE_LIMIT_DISK=""
FUGUE_KUBELET_SYSTEM_RESERVED=""

configure_resource_limits() {
  local system_reserved=""
  local total=""
  local limit=""
  local reserved=""

  if [ -n "${FUGUE_LIMIT_CPU:-}" ]; then
    total="$(detect_total_cpu_millicores)" || {
      echo "unable to detect total node CPU capacity" >&2
      exit 1
    }
    limit="$(parse_cpu_millicores "${FUGUE_LIMIT_CPU}")" || {
      echo "invalid cpu limit: ${FUGUE_LIMIT_CPU}" >&2
      exit 1
    }
    if [ "${limit}" -gt "${total}" ]; then
      echo "cpu limit ${FUGUE_LIMIT_CPU} exceeds detected node capacity $(format_cpu_millicores "${total}")" >&2
      exit 1
    fi
    FUGUE_EFFECTIVE_LIMIT_CPU="$(format_cpu_millicores "${limit}")"
    reserved=$((total - limit))
    if [ "${reserved}" -gt 0 ]; then
      system_reserved="$(append_csv_value "${system_reserved}" "cpu=$(format_cpu_millicores "${reserved}")")"
    fi
  fi

  if [ -n "${FUGUE_LIMIT_MEMORY:-}" ]; then
    total="$(detect_total_memory_bytes)" || {
      echo "unable to detect total node memory" >&2
      exit 1
    }
    limit="$(parse_quantity_bytes "${FUGUE_LIMIT_MEMORY}")" || {
      echo "invalid memory limit: ${FUGUE_LIMIT_MEMORY}" >&2
      exit 1
    }
    if [ "${limit}" -gt "${total}" ]; then
      echo "memory limit ${FUGUE_LIMIT_MEMORY} exceeds detected node capacity $(format_bytes_quantity "${total}")" >&2
      exit 1
    fi
    FUGUE_EFFECTIVE_LIMIT_MEMORY="$(format_bytes_quantity "${limit}")"
    reserved=$((total - limit))
    if [ "${reserved}" -gt 0 ]; then
      system_reserved="$(append_csv_value "${system_reserved}" "memory=$(format_bytes_quantity "${reserved}")")"
    fi
  fi

  if [ -n "${FUGUE_LIMIT_DISK:-}" ]; then
    total="$(detect_total_disk_bytes)" || {
      echo "unable to detect total node disk capacity from ${FUGUE_LIMIT_DISK_PATH}" >&2
      exit 1
    }
    limit="$(parse_quantity_bytes "${FUGUE_LIMIT_DISK}")" || {
      echo "invalid disk limit: ${FUGUE_LIMIT_DISK}" >&2
      exit 1
    }
    if [ "${limit}" -gt "${total}" ]; then
      echo "disk limit ${FUGUE_LIMIT_DISK} exceeds detected node capacity $(format_bytes_quantity "${total}") on ${FUGUE_LIMIT_DISK_PATH}" >&2
      exit 1
    fi
    FUGUE_EFFECTIVE_LIMIT_DISK="$(format_bytes_quantity "${limit}")"
    reserved=$((total - limit))
    if [ "${reserved}" -gt 0 ]; then
      system_reserved="$(append_csv_value "${system_reserved}" "ephemeral-storage=$(format_bytes_quantity "${reserved}")")"
    fi
  fi

  if [ -n "${system_reserved}" ]; then
    FUGUE_KUBELET_SYSTEM_RESERVED="system-reserved=${system_reserved}"
  fi

  if [ -n "${FUGUE_LIMIT_CPU:-}${FUGUE_LIMIT_MEMORY:-}${FUGUE_LIMIT_DISK:-}" ]; then
    log_step "Applying Fugue resource caps: cpu=${FUGUE_EFFECTIVE_LIMIT_CPU:-unbounded}, memory=${FUGUE_EFFECTIVE_LIMIT_MEMORY:-unbounded}, disk=${FUGUE_EFFECTIVE_LIMIT_DISK:-unbounded}."
  fi
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

install_tailscale_binaries() {
  curl -fsSL https://tailscale.com/install.sh | sh 1>&2
}

install_tailscale() {
  if command -v tailscale >/dev/null 2>&1; then
    return 0
  fi
  run_with_heartbeat "Installing tailscale" "10-60s" install_tailscale_binaries
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
        run_systemd_action_and_wait start tailscaled 60 "5-30s"
      else
        log_step "tailscaled is already active."
      fi
      run_with_heartbeat "Running tailscale up for ${hostname}" "10-60s" tailscale up \
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

install_k3s_agent_binaries() {
  curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL="${FUGUE_K3S_CHANNEL}" INSTALL_K3S_EXEC="agent" INSTALL_K3S_SKIP_START="true" sh -
}

install_nfs_client_tools() {
  if command -v mount.nfs >/dev/null 2>&1; then
    log_step "NFS client tools are already installed."
    return 0
  fi
  if command -v apt-get >/dev/null 2>&1; then
    log_step "Installing NFS client tools required by shared-workspace volumes..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y --no-install-recommends nfs-common
    return 0
  fi
  log_step "Skipping NFS client tools install because apt-get is unavailable; shared-workspace NFS volumes may not mount."
  return 0
}

flannel_mtu() {
  local subnet_file="/run/flannel/subnet.env"
  if [ ! -r "${subnet_file}" ]; then
    return 1
  fi
  awk -F= '$1 == "FLANNEL_MTU" { print $2; exit }' "${subnet_file}"
}

interface_mtu() {
  local iface="$1"
  local path="/sys/class/net/${iface}/mtu"
  if [ ! -r "${path}" ]; then
    return 1
  fi
  cat "${path}"
}

reconcile_cni_bridge_mtu() {
  local target_mtu=""
  local current_mtu=""
  local changed=1
  target_mtu="$(flannel_mtu || true)"
  case "${target_mtu}" in
    ""|*[!0-9]*)
      return 1
      ;;
  esac
  for iface in cni0; do
    current_mtu="$(interface_mtu "${iface}" || true)"
    if [ -z "${current_mtu}" ] || [ "${current_mtu}" = "${target_mtu}" ]; then
      continue
    fi
    if ip link set dev "${iface}" mtu "${target_mtu}"; then
      log_step "Updated ${iface} MTU from ${current_mtu} to ${target_mtu}."
      changed=0
    fi
  done
  return "${changed}"
}

install_dns_escape_hatch_tools() {
  if command -v dnsmasq >/dev/null 2>&1; then
    log_step "Local DNS escape hatch tools are already installed."
    return 0
  fi
  if command -v apt-get >/dev/null 2>&1; then
    log_step "Installing dnsmasq for the local DNS escape hatch..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y --no-install-recommends dnsmasq
    return 0
  fi
  log_step "Skipping local DNS escape hatch because dnsmasq is unavailable and apt-get is missing."
  return 1
}

install_dns_escape_hatch_script() {
  local helper_tmp=""
  helper_tmp="$(mktemp)"
  cat >"${helper_tmp}" <<'EOF_DNS_ESCAPE_HATCH'
#!/usr/bin/env bash
set -euo pipefail

if ! command -v iptables-save >/dev/null 2>&1 || ! command -v iptables >/dev/null 2>&1; then
  echo "Fugue node DNS escape hatch skipping because iptables is unavailable."
  exit 0
fi

ensure_rule() {
  local table="$1"
  local chain="$2"
  shift 2
  if ! iptables -t "${table}" -C "${chain}" "$@" 2>/dev/null; then
    iptables -t "${table}" -I "${chain}" 1 "$@"
  fi
}

detect_cni_bridge_ip() {
  ip -4 addr show dev cni0 2>/dev/null | awk '/inet / {split($2, parts, "/"); print parts[1]; exit}'
}

detect_kube_dns_service_ip() {
  iptables-save 2>/dev/null | awk '
    /-A KUBE-SERVICES/ && /kube-system\/kube-dns:dns/ && /--dport 53/ {
      line = $0
      if (match(line, /-d [0-9.]+\/32/)) {
        ip = substr(line, RSTART + 3, RLENGTH - 3)
        sub(/\/32$/, "", ip)
        print ip
        exit
      }
    }
    /-A KUBE-SERVICES/ && /kube-system\/coredns:dns/ && /--dport 53/ {
      line = $0
      if (match(line, /-d [0-9.]+\/32/)) {
        ip = substr(line, RSTART + 3, RLENGTH - 3)
        sub(/\/32$/, "", ip)
        print ip
        exit
      }
    }
  '
}

render_service_host_records() {
  iptables-save 2>/dev/null | awk '
    function trim(value) {
      gsub(/^[[:space:]]+/, "", value)
      gsub(/[[:space:]]+$/, "", value)
      return value
    }
    function emit(ip, name) {
      if (ip == "" || name == "") {
        return
      }
      key = ip " " name
      if (seen[key]++) {
        return
      }
      print key
    }
    /-A KUBE-SERVICES/ && /--comment "/ {
      line = $0
      ip = ""
      comment = ""
      if (match(line, /-d [0-9.]+\/32/)) {
        ip = substr(line, RSTART + 3, RLENGTH - 3)
        sub(/\/32$/, "", ip)
      }
      if (match(line, /--comment "[^"]+"/)) {
        comment = substr(line, RSTART + 11, RLENGTH - 12)
      }
      if (ip == "" || comment == "") {
        next
      }
      if (split(comment, parts, "/") < 2) {
        next
      }
      namespace = trim(parts[1])
      service_part = trim(parts[2])
      split(service_part, service_bits, ":")
      service = trim(service_bits[1])
      if (namespace == "" || service == "") {
        next
      }
      emit(ip, service)
      emit(ip, service "." namespace ".svc.cluster.local")
    }
  ' | sort -u
}

main() {
  local cni_bridge_ip=""
  local kube_dns_service_ip=""
  local hosts_tmp=""
  local hosts_path="/var/lib/fugue-node-dns/hosts.generated"

  cni_bridge_ip="$(detect_cni_bridge_ip)"
  kube_dns_service_ip="$(detect_kube_dns_service_ip)"
  if [ -z "${cni_bridge_ip}" ] || [ -z "${kube_dns_service_ip}" ]; then
    echo "Fugue node DNS escape hatch waiting for cni0 and kube-dns service endpoints."
    exit 0
  fi

  mkdir -p /var/lib/fugue-node-dns
  hosts_tmp="$(mktemp)"
  render_service_host_records >"${hosts_tmp}"
  if ! cmp -s "${hosts_tmp}" "${hosts_path}" 2>/dev/null; then
    cp "${hosts_tmp}" "${hosts_path}"
  fi
  chmod 0644 "${hosts_path}" 2>/dev/null || true
  rm -f "${hosts_tmp}"

  ensure_rule nat PREROUTING -i cni0 -d "${kube_dns_service_ip}/32" -p udp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53"
  ensure_rule nat PREROUTING -i cni0 -d "${kube_dns_service_ip}/32" -p tcp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53"
  ensure_rule nat OUTPUT -d "${kube_dns_service_ip}/32" -p udp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53"
  ensure_rule nat OUTPUT -d "${kube_dns_service_ip}/32" -p tcp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53"

  systemctl reload-or-restart dnsmasq.service >/dev/null 2>&1 || systemctl restart dnsmasq.service
}

main "$@"
EOF_DNS_ESCAPE_HATCH
  if write_file_if_changed "${helper_tmp}" /usr/local/sbin/fugue-node-dns-escape-hatch; then
    log_step "Updated /usr/local/sbin/fugue-node-dns-escape-hatch."
  else
    log_step "/usr/local/sbin/fugue-node-dns-escape-hatch is unchanged."
  fi
  chmod 0755 /usr/local/sbin/fugue-node-dns-escape-hatch
  rm -f "${helper_tmp}"
}

configure_dns_escape_hatch() {
  local config_tmp=""
  local unit_tmp=""
  local timer_tmp=""
  local config_changed=0
  case "${FUGUE_JOIN_NODE_DNS_ESCAPE_HATCH_ENABLED:-true}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      log_step "Local DNS escape hatch installation disabled."
      return 0
      ;;
  esac

  install_dns_escape_hatch_tools || return 0
  mkdir -p /etc/dnsmasq.d /var/lib/fugue-node-dns
  config_tmp="$(mktemp)"
  cat >"${config_tmp}" <<'EOF_DNSMASQ'
interface=cni0
bind-dynamic
listen-address=127.0.0.1
no-resolv
no-hosts
cache-size=1000
addn-hosts=/var/lib/fugue-node-dns/hosts.generated
server=1.1.1.1
server=8.8.8.8
EOF_DNSMASQ
  if write_file_if_changed "${config_tmp}" /etc/dnsmasq.d/fugue-node-dns-escape-hatch.conf; then
    config_changed=1
    log_step "Updated /etc/dnsmasq.d/fugue-node-dns-escape-hatch.conf."
  else
    log_step "/etc/dnsmasq.d/fugue-node-dns-escape-hatch.conf is unchanged."
  fi

  install_dns_escape_hatch_script

  unit_tmp="$(mktemp)"
  cat >"${unit_tmp}" <<'EOF_DNS_ESCAPE_HATCH_UNIT'
[Unit]
Description=Fugue node DNS escape hatch
Wants=network-online.target k3s-agent.service
After=network-online.target k3s-agent.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/fugue-node-dns-escape-hatch
EOF_DNS_ESCAPE_HATCH_UNIT
  if write_file_if_changed "${unit_tmp}" /etc/systemd/system/fugue-node-dns-escape-hatch.service; then
    config_changed=1
    log_step "Updated fugue-node-dns-escape-hatch.service."
  else
    log_step "fugue-node-dns-escape-hatch.service is unchanged."
  fi

  timer_tmp="$(mktemp)"
  cat >"${timer_tmp}" <<'EOF_DNS_ESCAPE_HATCH_TIMER'
[Unit]
Description=Refresh Fugue node DNS escape hatch

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
RandomizedDelaySec=30s
Persistent=true

[Install]
WantedBy=timers.target
EOF_DNS_ESCAPE_HATCH_TIMER
  if write_file_if_changed "${timer_tmp}" /etc/systemd/system/fugue-node-dns-escape-hatch.timer; then
    config_changed=1
    log_step "Updated fugue-node-dns-escape-hatch.timer."
  else
    log_step "fugue-node-dns-escape-hatch.timer is unchanged."
  fi

  if [ "${config_changed}" -eq 1 ]; then
    systemctl daemon-reload
  fi
  systemctl enable --now dnsmasq.service >/dev/null 2>&1 || true
  systemctl enable --now fugue-node-dns-escape-hatch.timer >/dev/null 2>&1 || true
  systemctl start --no-block fugue-node-dns-escape-hatch.service >/dev/null 2>&1 || true
}

server_endpoint_from_url() {
  local raw="$1"
  raw="${raw#https://}"
  raw="${raw#http://}"
  raw="${raw%%/*}"
  printf '%%s' "${raw}"
}

install_haproxy_if_needed() {
  if command -v haproxy >/dev/null 2>&1; then
    return 0
  fi
  if ! command -v apt-get >/dev/null 2>&1; then
    log_step "Skipping local k3s API load balancer because haproxy is missing and apt-get is unavailable."
    return 1
  fi
  log_step "Installing haproxy for the local k3s API load balancer..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends haproxy
}

configure_k3s_api_load_balancer() {
  local fallback_csv="${FUGUE_JOIN_SERVER_FALLBACKS:-}"
  local endpoint=""
  local server_url=""
  local old_ifs="${IFS}"
  local index=0
  local lb_tmp=""
  local unit_tmp=""
  local config_changed=0

  FUGUE_JOIN_EFFECTIVE_SERVER="${FUGUE_JOIN_SERVER}"
  [ -n "${fallback_csv}" ] || return 0
  install_haproxy_if_needed || return 0

  mkdir -p /etc/fugue
  lb_tmp="$(mktemp)"
  {
    echo "global"
    echo "  log stdout format raw local0"
    echo "  maxconn 128"
    echo "defaults"
    echo "  mode tcp"
    echo "  timeout connect 3s"
    echo "  timeout client 1m"
    echo "  timeout server 1m"
    echo "frontend k3s_api"
    echo "  bind 127.0.0.1:16443"
    echo "  default_backend k3s_servers"
    echo "backend k3s_servers"
    echo "  option tcp-check"
    echo "  balance first"
    for server_url in "${FUGUE_JOIN_SERVER}" ${fallback_csv//,/ }; do
      endpoint="$(server_endpoint_from_url "${server_url}")"
      [ -n "${endpoint}" ] || continue
      index=$((index + 1))
      echo "  server cp${index} ${endpoint} check inter 2s fall 2 rise 1"
    done
  } >"${lb_tmp}"
  if [ "${index}" -lt 2 ]; then
    rm -f "${lb_tmp}"
    log_step "Skipping local k3s API load balancer because fewer than two server endpoints were configured."
    return 0
  fi
  if write_file_if_changed "${lb_tmp}" /etc/fugue/k3s-api-lb.cfg; then
    config_changed=1
    log_step "Updated /etc/fugue/k3s-api-lb.cfg."
  else
    log_step "/etc/fugue/k3s-api-lb.cfg is unchanged."
  fi

  unit_tmp="$(mktemp)"
  cat >"${unit_tmp}" <<EOF_K3S_API_LB_UNIT
[Unit]
Description=Fugue local k3s API load balancer
Wants=network-online.target tailscaled.service
After=network-online.target tailscaled.service

[Service]
ExecStart=/usr/sbin/haproxy -Ws -f /etc/fugue/k3s-api-lb.cfg -p /run/fugue-k3s-api-lb.pid
ExecReload=/bin/kill -USR2 \$MAINPID
Restart=always
RestartSec=2s

[Install]
WantedBy=multi-user.target
EOF_K3S_API_LB_UNIT
  if write_file_if_changed "${unit_tmp}" /etc/systemd/system/fugue-k3s-api-lb.service; then
    config_changed=1
    log_step "Updated fugue-k3s-api-lb.service."
  fi
  if [ "${config_changed}" -eq 1 ]; then
    systemctl daemon-reload
    systemctl restart fugue-k3s-api-lb.service
  fi
  systemctl enable --now fugue-k3s-api-lb.service >/dev/null
  FUGUE_JOIN_EFFECTIVE_SERVER="https://127.0.0.1:16443"
  IFS="${old_ifs}"
  log_step "Using local k3s API load balancer at ${FUGUE_JOIN_EFFECTIVE_SERVER}."
}

shell_quote_for_env() {
  local value="$1"
  value="${value//\'/\'\\\'\'}"
  printf "'%%s'" "${value}"
}

write_env_var() {
  printf '%%s=' "$1"
  shell_quote_for_env "$2"
  printf '\n'
}

install_fugue_node_updater() {
  local updater_tmp=""
  local enroll_env=""
  local env_tmp=""
  local unit_tmp=""
  local timer_tmp=""
  local systemd_changed=0
  case "${FUGUE_NODE_UPDATER_ENABLED}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      log_step "Fugue node updater installation disabled."
      return 0
      ;;
  esac

  log_step "Installing Fugue node updater..."
  mkdir -p /etc/fugue /var/lib/fugue-node-updater
  updater_tmp="$(mktemp)"
  curl -fsSL --retry 3 --retry-delay 2 "${FUGUE_API_BASE}/install/node-updater.sh" -o "${updater_tmp}"
  cp "${updater_tmp}" /usr/local/bin/fugue-node-updater
  chmod 0755 /usr/local/bin/fugue-node-updater
  rm -f "${updater_tmp}"

  enroll_env="$(mktemp)"
  curl -fsSL --retry 3 --retry-delay 2 -X POST "${FUGUE_API_BASE}/v1/node-updater/enroll" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "node_key=${FUGUE_NODE_KEY}" \
    --data-urlencode "node_name=${FUGUE_JOIN_NODE_NAME}" \
    --data-urlencode "machine_name=${machine_name}" \
    --data-urlencode "machine_fingerprint=${machine_fingerprint}" \
    --data-urlencode "endpoint=${node_endpoint}" \
    --data-urlencode "labels=${FUGUE_RUNTIME_LABELS:-}" \
    --data-urlencode "updater_version=v1" \
    --data-urlencode "join_script_version=${FUGUE_JOIN_SCRIPT_VERSION}" \
    --data-urlencode "capabilities=heartbeat,tasks,refresh-join-config,restart-k3s-agent,upgrade-k3s-agent,upgrade-node-updater,diagnose-node,install-nfs-client-tools,prepull-system-images,prepull-app-images,verify-systemd-escape-hatch" \
    >"${enroll_env}"
  # shellcheck disable=SC1090
  . "${enroll_env}"
  rm -f "${enroll_env}"

  env_tmp="$(mktemp)"
  {
    write_env_var FUGUE_API_BASE "${FUGUE_API_BASE}"
    write_env_var FUGUE_NODE_UPDATER_ID "${FUGUE_NODE_UPDATER_ID}"
    write_env_var FUGUE_NODE_UPDATER_TOKEN "${FUGUE_NODE_UPDATER_TOKEN}"
    write_env_var FUGUE_NODE_UPDATER_VERSION "v1"
    write_env_var FUGUE_JOIN_SCRIPT_VERSION "${FUGUE_JOIN_SCRIPT_VERSION}"
    write_env_var FUGUE_K3S_CHANNEL "${FUGUE_K3S_CHANNEL}"
    write_env_var FUGUE_NODE_UPDATER_WORK_DIR "/var/lib/fugue-node-updater"
    write_env_var FUGUE_NODE_UPDATER_STATE_DIR "${FUGUE_DISCOVERY_STATE_DIR}"
    write_env_var FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE "${FUGUE_DISCOVERY_BUNDLE_FILE}"
    write_env_var FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE "${FUGUE_DISCOVERY_ENV_FILE}"
    write_env_var FUGUE_NODE_UPDATER_STATE_ENV_FILE "${FUGUE_DISCOVERY_STATE_ENV_FILE}"
    write_env_var FUGUE_DISCOVERY_GENERATION "${FUGUE_DISCOVERY_GENERATION:-}"
  } >"${env_tmp}"
  chmod 0600 "${env_tmp}"
  if write_file_if_changed "${env_tmp}" /etc/fugue/node-updater.env; then
    log_step "Updated /etc/fugue/node-updater.env."
  else
    log_step "/etc/fugue/node-updater.env is unchanged."
  fi
  chmod 0600 /etc/fugue/node-updater.env

  unit_tmp="$(mktemp)"
  cat >"${unit_tmp}" <<EOF_NODE_UPDATER_SERVICE
[Unit]
Description=Fugue node updater
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
EnvironmentFile=/etc/fugue/node-updater.env
ExecStart=/usr/local/bin/fugue-node-updater run-once
TimeoutStartSec=1800
EOF_NODE_UPDATER_SERVICE
  if write_file_if_changed "${unit_tmp}" /etc/systemd/system/fugue-node-updater.service; then
    systemd_changed=1
    log_step "Updated fugue-node-updater.service."
  fi

  timer_tmp="$(mktemp)"
  cat >"${timer_tmp}" <<EOF_NODE_UPDATER_TIMER
[Unit]
Description=Run Fugue node updater

[Timer]
OnBootSec=2min
OnUnitActiveSec=${FUGUE_NODE_UPDATER_POLL_INTERVAL}
RandomizedDelaySec=30s
Persistent=true

[Install]
WantedBy=timers.target
EOF_NODE_UPDATER_TIMER
  if write_file_if_changed "${timer_tmp}" /etc/systemd/system/fugue-node-updater.timer; then
    systemd_changed=1
    log_step "Updated fugue-node-updater.timer."
  fi

  if [ "${systemd_changed}" -eq 1 ]; then
    systemctl daemon-reload
  fi
  systemctl enable --now fugue-node-updater.timer >/dev/null
  systemctl start --no-block fugue-node-updater.service >/dev/null 2>&1 || true
  log_step "Fugue node updater is installed."
}

restart_k3s_agent_if_needed() {
  local config_changed="$1"
  local state=""
  systemctl enable k3s-agent
  state="$(systemctl is-active k3s-agent 2>/dev/null || true)"
  if [ "${state}" != "active" ]; then
    if [ "${state}" = "activating" ]; then
      log_step "k3s-agent is still activating from a previous attempt; forcing a clean restart so it reloads the latest join configuration."
    elif [ "${config_changed}" -eq 1 ]; then
      log_step "k3s-agent is not active; forcing a clean restart with the updated configuration."
    else
      log_step "k3s-agent is not active; forcing a clean restart."
    fi
    systemctl stop k3s-agent >/dev/null 2>&1 || true
    systemctl reset-failed k3s-agent >/dev/null 2>&1 || true
    run_systemd_action_and_wait start k3s-agent 900 "30s-5m on first startup"
  elif [ "${config_changed}" -eq 1 ]; then
    log_step "k3s agent configuration changed; restarting k3s-agent."
    run_systemd_action_and_wait restart k3s-agent 900 "15s-3m"
  else
    log_step "k3s-agent configuration unchanged; service already active, skipping restart."
  fi
  systemctl is-active --quiet k3s-agent
}

parse_args "$@"

if [ "$(id -u)" -ne 0 ]; then
  echo "run with sudo, for example: curl -fsSL ${FUGUE_API_BASE}/install/join-cluster.sh | sudo FUGUE_NODE_KEY=... bash" >&2
  exit 1
fi

require_cmd curl
require_cmd systemctl
require_cmd ip
require_cmd cmp
require_cmd awk
require_cmd df

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
script_started_at="$(date +%%s)"
print_install_timeline
if fetch_discovery_bundle; then
  log_step "DiscoveryBundle cached before join."
else
  log_step "DiscoveryBundle unavailable before join; falling back to join-env values and any cached bundle."
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
apply_discovery_join_defaults
write_join_discovery_state
FUGUE_JOIN_NODE_LABELS="$(append_location_node_labels)"
log_step "Join parameters received for node ${FUGUE_JOIN_NODE_NAME} (discovery_generation=${FUGUE_JOIN_DISCOVERY_GENERATION:-none})."

mesh_provider="${FUGUE_JOIN_MESH_PROVIDER:-}"
flannel_iface=""
if [ -n "${mesh_provider}" ]; then
  node_external_ip="$(connect_mesh "${mesh_provider}" "${FUGUE_JOIN_NODE_NAME}")"
  flannel_iface="tailscale0"
fi

configure_resource_limits
configure_k3s_api_load_balancer

log_step "Preparing k3s agent configuration..."
mkdir -p /etc/rancher/k3s
k3s_config_tmp="$(mktemp)"
{
  printf 'server: "%%s"\n' "${FUGUE_JOIN_EFFECTIVE_SERVER:-${FUGUE_JOIN_SERVER}}"
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
  if [ -n "${FUGUE_KUBELET_SYSTEM_RESERVED:-}" ]; then
    printf 'kubelet-arg:\n'
    printf '  - "%%s"\n' "${FUGUE_KUBELET_SYSTEM_RESERVED}"
  fi
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
  registry_endpoint_url_value="$(registry_endpoint_url "${FUGUE_JOIN_REGISTRY_ENDPOINT}")"
  cat >"${k3s_registry_tmp}" <<EOF_REG
mirrors:
  "${FUGUE_JOIN_REGISTRY_BASE}":
    endpoint:
      - "${registry_endpoint_url_value}"
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

k3s_installed_now=0
if ! command -v k3s >/dev/null 2>&1; then
  run_with_heartbeat "Downloading and installing k3s agent binaries" "2-10 min on a fresh node" install_k3s_agent_binaries
  log_step "k3s agent install completed; starting service."
  k3s_installed_now=1
fi

k3s_environment_files_changed=0
if sanitize_k3s_agent_environment_files; then
  k3s_environment_files_changed=1
fi

k3s_service_override_changed=0
if ensure_k3s_agent_service_override; then
  k3s_service_override_changed=1
fi

host_resolv_conf_changed=0
if ensure_k3s_agent_non_stub_resolv_conf; then
  host_resolv_conf_changed=1
fi

k3s_restart_needed="${k3s_config_changed}"
if [ "${k3s_installed_now}" -eq 1 ] || [ "${k3s_environment_files_changed}" -eq 1 ] || [ "${k3s_service_override_changed}" -eq 1 ] || [ "${host_resolv_conf_changed}" -eq 1 ]; then
  k3s_restart_needed=1
fi

if ! run_with_heartbeat "Installing NFS client tools" "5-30s" install_nfs_client_tools; then
  log_step "NFS client tools installation failed; shared-workspace NFS mounts may remain unavailable until the host is repaired."
fi

restart_k3s_agent_if_needed "${k3s_restart_needed}"

if ! reconcile_cni_bridge_mtu; then
  log_step "CNI bridge MTU is already aligned or not ready yet."
fi

if ! run_with_heartbeat "Installing local DNS escape hatch" "5-30s" configure_dns_escape_hatch; then
  log_step "Local DNS escape hatch installation failed; orphan workloads may still depend on control-plane DNS."
fi

systemctl is-active --quiet k3s-agent
cleanup_stale_cluster_nodes
if ! install_fugue_node_updater; then
  log_step "Fugue node updater installation failed; cluster join succeeded but automatic host updates are not active yet."
fi
log_step "Cluster node join finished in $(format_duration $(( $(date +%%s) - script_started_at )))."

cat <<EOF
Fugue node joined.
runtime_id=${FUGUE_JOIN_RUNTIME_ID}
node_name=${FUGUE_JOIN_NODE_NAME}
server=${FUGUE_JOIN_EFFECTIVE_SERVER:-${FUGUE_JOIN_SERVER}}
server_fallbacks=${FUGUE_JOIN_SERVER_FALLBACKS:-}
registry_base=${FUGUE_JOIN_REGISTRY_BASE:-}
registry_endpoint=${FUGUE_JOIN_REGISTRY_ENDPOINT:-}
discovery_generation=${FUGUE_JOIN_DISCOVERY_GENERATION:-}
labels=${FUGUE_JOIN_NODE_LABELS}
taints=${FUGUE_JOIN_NODE_TAINTS}
resource_limit_cpu=${FUGUE_EFFECTIVE_LIMIT_CPU:-}
resource_limit_memory=${FUGUE_EFFECTIVE_LIMIT_MEMORY:-}
resource_limit_disk=${FUGUE_EFFECTIVE_LIMIT_DISK:-}
kubelet_system_reserved=${FUGUE_KUBELET_SYSTEM_RESERVED:-}
EOF
`, strconv.Quote(apiBase))
}
