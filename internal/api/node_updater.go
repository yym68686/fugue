package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

const nodeUpdaterScriptVersion = "v1"

func (s *Server) handleNodeUpdaterInstallScript(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
	_, _ = fmt.Fprint(w, s.nodeUpdaterInstallScript(s.publicInstallAPIBaseURL(r)))
}

func (s *Server) handleNodeUpdaterEnroll(w http.ResponseWriter, r *http.Request) {
	req, wantsEnv, err := decodeNodeUpdaterEnrollRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	updater, token, err := s.store.EnrollNodeUpdater(
		req.NodeKey,
		coalesceNodeName(req.NodeName, req.RuntimeName),
		req.Endpoint,
		req.Labels,
		req.MachineName,
		req.MachineFingerprint,
		req.UpdaterVersion,
		req.JoinScriptVersion,
		req.Capabilities,
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   updater.NodeKeyID,
		TenantID:  updater.TenantID,
	}, "node_updater.enroll", "node_updater", updater.ID, updater.TenantID, map[string]string{
		"cluster_node_name": updater.ClusterNodeName,
		"runtime_id":        updater.RuntimeID,
	})
	if wantsEnv {
		writeNodeUpdaterEnrollEnv(w, updater, token)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"node_updater": updater,
		"token":        token,
	})
}

func (s *Server) handleNodeUpdaterHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	req, wantsEnv, err := decodeNodeUpdaterHeartbeatRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	updater, err := s.store.UpdateNodeUpdaterHeartbeat(principal.ActorID, model.NodeUpdater{
		Labels:              req.Labels,
		Capabilities:        req.Capabilities,
		UpdaterVersion:      req.UpdaterVersion,
		JoinScriptVersion:   req.JoinScriptVersion,
		K3SVersion:          req.K3SVersion,
		K3SServer:           req.K3SServer,
		K3SFallbackServers:  req.K3SFallbackServers,
		RegistryMirror:      req.RegistryMirror,
		LabelsHash:          req.LabelsHash,
		TaintsHash:          req.TaintsHash,
		EdgeEnvGeneration:   req.EdgeEnvGeneration,
		DNSEnvGeneration:    req.DNSEnvGeneration,
		ConfigHash:          req.ConfigHash,
		DiscoveryGeneration: req.DiscoveryGeneration,
		OS:                  req.OS,
		Arch:                req.Arch,
		LastError:           req.LastError,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if wantsEnv {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "FUGUE_NODE_UPDATER_ID=%s\n", shellQuote(updater.ID))
		fmt.Fprintf(w, "FUGUE_NODE_UPDATER_STATUS=%s\n", shellQuote(updater.Status))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_updater": updater})
}

func (s *Server) handleGetNodeUpdaterDesiredState(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	state, err := s.nodeUpdaterDesiredState(r.Context(), r, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"desired_state": state})
}

func (s *Server) nodeUpdaterDesiredState(ctx context.Context, r *http.Request, principal model.Principal) (model.NodeUpdaterDesiredState, error) {
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		return model.NodeUpdaterDesiredState{}, err
	}
	discovery, err := s.deriveDiscoveryBundle(r.WithContext(ctx), discoveryBundlePrincipal())
	if err != nil {
		return model.NodeUpdaterDesiredState{}, err
	}
	var nodePolicy *model.ClusterNodePolicyStatus
	warnings := []string{}
	statuses, err := s.loadClusterNodePolicyStatuses(ctx, principal)
	if err != nil {
		warnings = append(warnings, err.Error())
	} else {
		for i := range statuses {
			if strings.EqualFold(strings.TrimSpace(statuses[i].NodeName), strings.TrimSpace(updater.ClusterNodeName)) {
				status := statuses[i]
				nodePolicy = &status
				break
			}
		}
		if nodePolicy == nil {
			warnings = append(warnings, "node policy not found for current cluster node")
		}
	}
	return model.NodeUpdaterDesiredState{
		GeneratedAt:     time.Now().UTC(),
		NodeUpdater:     updater,
		DiscoveryBundle: discovery,
		NodePolicy:      nodePolicy,
		Warnings:        warnings,
	}, nil
}

func (s *Server) nodeUpdaterByPrincipal(principal model.Principal) (model.NodeUpdater, error) {
	updaters, err := s.store.ListNodeUpdaters(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.NodeUpdater{}, err
	}
	for _, updater := range updaters {
		if strings.EqualFold(strings.TrimSpace(updater.ID), strings.TrimSpace(principal.ActorID)) {
			return updater, nil
		}
	}
	return model.NodeUpdater{}, store.ErrNotFound
}

func (s *Server) handleListNodeUpdaters(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	updaters, err := s.store.ListNodeUpdaters(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_updaters": updaters})
}

func (s *Server) handleCreateNodeUpdateTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	var req struct {
		NodeUpdaterID   string            `json:"node_updater_id"`
		ClusterNodeName string            `json:"cluster_node_name"`
		RuntimeID       string            `json:"runtime_id"`
		Type            string            `json:"type"`
		Payload         map[string]string `json:"payload"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	task, err := s.store.CreateNodeUpdateTask(principal, req.NodeUpdaterID, req.ClusterNodeName, req.RuntimeID, req.Type, req.Payload)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "node_update_task.create", "node_update_task", task.ID, task.TenantID, map[string]string{
		"type":              task.Type,
		"node_updater_id":   task.NodeUpdaterID,
		"cluster_node_name": task.ClusterNodeName,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"task": task})
}

func (s *Server) handleListNodeUpdateTasks(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	tasks, err := s.store.ListNodeUpdateTasks(
		principal.TenantID,
		principal.IsPlatformAdmin(),
		r.URL.Query().Get("node_updater_id"),
		r.URL.Query().Get("status"),
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleNodeUpdaterTasks(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	limit := parsePositiveInt(r.URL.Query().Get("limit"), 10)
	tasks, err := s.store.ListPendingNodeUpdateTasks(principal.ActorID, limit)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("format")), "env") {
		writeNodeUpdateTaskEnv(w, firstNodeUpdateTask(tasks))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleNodeUpdaterClaimTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	task, err := s.store.ClaimNodeUpdateTask(r.PathValue("id"), principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) handleNodeUpdaterLogTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	message, err := decodeNodeUpdaterMessage(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	task, err := s.store.AppendNodeUpdateTaskLog(r.PathValue("id"), principal.ActorID, message)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) handleNodeUpdaterCompleteTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	req, err := decodeNodeUpdaterCompleteRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	task, err := s.store.CompleteNodeUpdateTask(r.PathValue("id"), principal.ActorID, req.Status, req.Message, req.ErrorMessage)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

type nodeUpdaterEnrollRequest struct {
	NodeKey            string            `json:"node_key"`
	NodeName           string            `json:"node_name"`
	RuntimeName        string            `json:"runtime_name"`
	MachineName        string            `json:"machine_name"`
	MachineFingerprint string            `json:"machine_fingerprint"`
	Endpoint           string            `json:"endpoint"`
	Labels             map[string]string `json:"labels"`
	UpdaterVersion     string            `json:"updater_version"`
	JoinScriptVersion  string            `json:"join_script_version"`
	Capabilities       []string          `json:"capabilities"`
}

type nodeUpdaterHeartbeatRequest struct {
	Labels              map[string]string `json:"labels"`
	Capabilities        []string          `json:"capabilities"`
	UpdaterVersion      string            `json:"updater_version"`
	JoinScriptVersion   string            `json:"join_script_version"`
	K3SVersion          string            `json:"k3s_version"`
	K3SServer           string            `json:"k3s_server"`
	K3SFallbackServers  string            `json:"k3s_fallback_servers"`
	RegistryMirror      string            `json:"registry_mirror"`
	LabelsHash          string            `json:"labels_hash"`
	TaintsHash          string            `json:"taints_hash"`
	EdgeEnvGeneration   string            `json:"edge_env_generation"`
	DNSEnvGeneration    string            `json:"dns_env_generation"`
	ConfigHash          string            `json:"config_hash"`
	DiscoveryGeneration string            `json:"discovery_generation"`
	OS                  string            `json:"os"`
	Arch                string            `json:"arch"`
	LastError           string            `json:"last_error"`
}

func decodeNodeUpdaterEnrollRequest(r *http.Request) (nodeUpdaterEnrollRequest, bool, error) {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return nodeUpdaterEnrollRequest{}, false, err
		}
		return nodeUpdaterEnrollRequest{
			NodeKey:            r.Form.Get("node_key"),
			NodeName:           r.Form.Get("node_name"),
			RuntimeName:        r.Form.Get("runtime_name"),
			MachineName:        r.Form.Get("machine_name"),
			MachineFingerprint: r.Form.Get("machine_fingerprint"),
			Endpoint:           r.Form.Get("endpoint"),
			Labels:             parseCSVLabels(r.Form.Get("labels")),
			UpdaterVersion:     r.Form.Get("updater_version"),
			JoinScriptVersion:  r.Form.Get("join_script_version"),
			Capabilities:       parseCSVList(r.Form.Get("capabilities")),
		}, true, nil
	}
	var req nodeUpdaterEnrollRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return nodeUpdaterEnrollRequest{}, false, err
	}
	return req, false, nil
}

func decodeNodeUpdaterHeartbeatRequest(r *http.Request) (nodeUpdaterHeartbeatRequest, bool, error) {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return nodeUpdaterHeartbeatRequest{}, false, err
		}
		return nodeUpdaterHeartbeatRequest{
			Labels:              parseCSVLabels(r.Form.Get("labels")),
			Capabilities:        parseCSVList(r.Form.Get("capabilities")),
			UpdaterVersion:      r.Form.Get("updater_version"),
			JoinScriptVersion:   r.Form.Get("join_script_version"),
			K3SVersion:          r.Form.Get("k3s_version"),
			K3SServer:           r.Form.Get("k3s_server"),
			K3SFallbackServers:  r.Form.Get("k3s_fallback_servers"),
			RegistryMirror:      r.Form.Get("registry_mirror"),
			LabelsHash:          r.Form.Get("labels_hash"),
			TaintsHash:          r.Form.Get("taints_hash"),
			EdgeEnvGeneration:   r.Form.Get("edge_env_generation"),
			DNSEnvGeneration:    r.Form.Get("dns_env_generation"),
			ConfigHash:          r.Form.Get("config_hash"),
			DiscoveryGeneration: r.Form.Get("discovery_generation"),
			OS:                  r.Form.Get("os"),
			Arch:                r.Form.Get("arch"),
			LastError:           r.Form.Get("last_error"),
		}, true, nil
	}
	var req nodeUpdaterHeartbeatRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return nodeUpdaterHeartbeatRequest{}, false, err
	}
	return req, false, nil
}

func decodeNodeUpdaterMessage(r *http.Request) (string, error) {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return "", err
		}
		return r.Form.Get("message"), nil
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return "", err
	}
	return req.Message, nil
}

func decodeNodeUpdaterCompleteRequest(r *http.Request) (struct {
	Status       string `json:"status"`
	Message      string `json:"message"`
	ErrorMessage string `json:"error_message"`
}, error) {
	var req struct {
		Status       string `json:"status"`
		Message      string `json:"message"`
		ErrorMessage string `json:"error_message"`
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return req, err
		}
		req.Status = r.Form.Get("status")
		req.Message = r.Form.Get("message")
		req.ErrorMessage = r.Form.Get("error_message")
		return req, nil
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return req, err
	}
	return req, nil
}

func writeNodeUpdaterEnrollEnv(w http.ResponseWriter, updater model.NodeUpdater, token string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_ID=%s\n", shellQuote(updater.ID))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_TOKEN=%s\n", shellQuote(token))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_STATUS=%s\n", shellQuote(updater.Status))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_RUNTIME_ID=%s\n", shellQuote(updater.RuntimeID))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_CLUSTER_NODE_NAME=%s\n", shellQuote(updater.ClusterNodeName))
}

func writeNodeUpdateTaskEnv(w http.ResponseWriter, task *model.NodeUpdateTask) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if task == nil {
		fmt.Fprintf(w, "FUGUE_NODE_UPDATE_TASK_ID=''\n")
		return
	}
	fmt.Fprintf(w, "FUGUE_NODE_UPDATE_TASK_ID=%s\n", shellQuote(task.ID))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATE_TASK_TYPE=%s\n", shellQuote(task.Type))
	keys := make([]string, 0, len(task.Payload))
	for key := range task.Payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		envKey := "FUGUE_NODE_UPDATE_TASK_" + envKeyName(key)
		fmt.Fprintf(w, "%s=%s\n", envKey, shellQuote(task.Payload[key]))
	}
}

func firstNodeUpdateTask(tasks []model.NodeUpdateTask) *model.NodeUpdateTask {
	if len(tasks) == 0 {
		return nil
	}
	return &tasks[0]
}

func parseCSVList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parsePositiveInt(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envKeyName(key string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(key) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - ('a' - 'A'))
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func (s *Server) nodeUpdaterInstallScript(apiBase string) string {
	script := `#!/usr/bin/env bash
set -euo pipefail

FUGUE_API_BASE="${FUGUE_API_BASE:-__FUGUE_API_BASE__}"
FUGUE_NODE_UPDATER_VERSION="${FUGUE_NODE_UPDATER_VERSION:-v1}"
FUGUE_NODE_UPDATER_WORK_DIR="${FUGUE_NODE_UPDATER_WORK_DIR:-/var/lib/fugue-node-updater}"
FUGUE_NODE_UPDATER_LAST_ERROR_FILE="${FUGUE_NODE_UPDATER_LAST_ERROR_FILE:-${FUGUE_NODE_UPDATER_WORK_DIR}/last-error}"
FUGUE_NODE_UPDATER_STATE_DIR="${FUGUE_NODE_UPDATER_STATE_DIR:-${FUGUE_NODE_UPDATER_WORK_DIR}}"
FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE="${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/discovery-bundle.json}"
FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE="${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/discovery.env}"
FUGUE_NODE_UPDATER_DESIRED_STATE_FILE="${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/desired-state.json}"
FUGUE_NODE_UPDATER_STATE_ENV_FILE="${FUGUE_NODE_UPDATER_STATE_ENV_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/state.env}"
FUGUE_NODE_UPDATER_K3S_CONFIG_FILE="${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE:-/etc/rancher/k3s/config.yaml}"
FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE="${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE:-/etc/rancher/k3s/registries.yaml}"
FUGUE_NODE_UPDATER_EDGE_ENV_FILE="${FUGUE_NODE_UPDATER_EDGE_ENV_FILE:-/etc/fugue/fugue-edge.env}"
FUGUE_NODE_UPDATER_DNS_ENV_FILE="${FUGUE_NODE_UPDATER_DNS_ENV_FILE:-/etc/fugue/fugue-dns.env}"

log() {
  printf '[fugue-node-updater] %s\n' "$*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

k3s_version() {
  if command -v k3s >/dev/null 2>&1; then
    k3s --version 2>/dev/null | head -n 1 || true
  fi
}

last_error() {
  if [ -f "${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}" ]; then
    head -c 2000 "${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}" 2>/dev/null || true
  fi
}

clear_last_error() {
  rm -f "${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}" 2>/dev/null || true
}

write_file_if_changed() {
  local source_path="$1"
  local target_path="$2"
  if [ -f "${target_path}" ] && cmp -s "${source_path}" "${target_path}"; then
    rm -f "${source_path}"
    return 1
  fi
  install -m 0644 "${source_path}" "${target_path}"
  rm -f "${source_path}"
  return 0
}

sha256_file() {
  local path="$1"
  if [ ! -r "${path}" ]; then
    return 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${path}" | awk '{print $1}'
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${path}" | awk '{print $1}'
    return 0
  fi
  return 1
}

sha256_text() {
  local value="$1"
  local tmp=""
  tmp="$(mktemp)"
  printf '%s' "${value}" >"${tmp}"
  if sha256_file "${tmp}"; then
    rm -f "${tmp}"
    return 0
  fi
  rm -f "${tmp}"
  return 1
}

load_cached_env_file() {
  local path="$1"
  if [ -r "${path}" ]; then
    # shellcheck disable=SC1090
    . "${path}"
    return 0
  fi
  return 1
}

parse_yaml_list_value_hash() {
  local file="$1"
  local key="$2"
  local tmp=""
  if [ ! -r "${file}" ]; then
    return 1
  fi
  tmp="$(mktemp)"
  awk -v key="${key}" '
    $0 ~ "^[[:space:]]*" key ":[[:space:]]*$" { in_block = 1; next }
    in_block && $0 ~ "^[^[:space:]]" { exit }
    in_block && $0 ~ "^[[:space:]]*-[[:space:]]*" {
      line = $0
      sub(/^[[:space:]]*-[[:space:]]*"/, "", line)
      sub(/"$/, "", line)
      print line
    }
  ' "${file}" | sort >"${tmp}"
  if [ ! -s "${tmp}" ]; then
    rm -f "${tmp}"
    return 1
  fi
  if sha256_file "${tmp}"; then
    rm -f "${tmp}"
    return 0
  fi
  rm -f "${tmp}"
  return 1
}

json_quote_env() {
  local value="$1"
  value="${value//\'/\'\\\'\'}"
  printf "'%s'" "${value}"
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
emit("FUGUE_DISCOVERY_ISSUER", bundle.get("issuer", ""))
emit("FUGUE_DISCOVERY_API_URL", runtime.get("FUGUE_API_URL") or first_named(bundle.get("api_endpoints"), "public").get("url", ""))
emit("FUGUE_DISCOVERY_APP_BASE_DOMAIN", runtime.get("FUGUE_APP_BASE_DOMAIN", ""))
emit("FUGUE_DISCOVERY_REGISTRY_PUSH_BASE", runtime.get("FUGUE_REGISTRY_PUSH_BASE") or registry.get("push_base", ""))
emit("FUGUE_DISCOVERY_REGISTRY_PULL_BASE", runtime.get("FUGUE_REGISTRY_PULL_BASE") or registry.get("pull_base", ""))
emit("FUGUE_DISCOVERY_REGISTRY_MIRROR", runtime.get("FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT") or registry.get("mirror", ""))
emit("FUGUE_DISCOVERY_K3S_SERVER", kube.get("server", ""))
emit("FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS", ",".join(kube.get("fallback_servers") or []))
emit("FUGUE_DISCOVERY_K3S_CA_HASH", kube.get("ca_hash", ""))
emit("FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT", kube.get("registry_endpoint", ""))
emit("FUGUE_DISCOVERY_SMOKE_URL", runtime.get("FUGUE_SMOKE_URL", ""))
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
  printf 'FUGUE_DISCOVERY_SCHEMA_VERSION=%s\n' "$(json_quote_env "${schema_version}")"
  printf 'FUGUE_DISCOVERY_GENERATION=%s\n' "$(json_quote_env "${generation}")"
}

fetch_discovery_bundle() {
  local tmp=""
  local env_tmp=""
  mkdir -p "${FUGUE_NODE_UPDATER_STATE_DIR}"
  tmp="$(mktemp)"
  if ! curl -fsSL --retry 3 --retry-delay 2 "${FUGUE_API_BASE}/v1/discovery/bundle" -o "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE}" || true
  env_tmp="$(mktemp)"
  if render_discovery_env "${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE}" >"${env_tmp}"; then
    write_file_if_changed "${env_tmp}" "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" || true
  else
    rm -f "${env_tmp}"
    return 1
  fi
}

render_node_updater_state_env() {
  local tmp=""
  tmp="$(mktemp)"
  {
    printf 'FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE}")"
    printf 'FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}")"
    printf 'FUGUE_NODE_UPDATER_DESIRED_STATE_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}")"
    printf 'FUGUE_NODE_UPDATER_STATE_ENV_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_STATE_ENV_FILE}")"
  } >"${tmp}"
  write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_STATE_ENV_FILE}" || true
}

fetch_node_policy_desired_state() {
  local tmp=""
  mkdir -p "${FUGUE_NODE_UPDATER_STATE_DIR}"
  tmp="$(mktemp)"
  if ! curl -fsSL --retry 3 --retry-delay 2 \
    -H "Authorization: Bearer ${FUGUE_NODE_UPDATER_TOKEN:?FUGUE_NODE_UPDATER_TOKEN is required}" \
    "${FUGUE_API_BASE}/v1/node-updater/desired-state" \
    -o "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}" || true
  return 0
}

yaml_update_scalar() {
  local file="$1"
  local key="$2"
  local value="$3"
  local tmp=""
  local changed=1
  tmp="$(mktemp)"
  if [ ! -r "${file}" ]; then
    printf '%s: "%s"\n' "${key}" "${value}" >"${tmp}"
    write_file_if_changed "${tmp}" "${file}"
    return $?
  fi
  awk -v key="${key}" -v value="${value}" '
    BEGIN { done = 0 }
    $0 ~ "^[[:space:]]*" key ":[[:space:]]*" {
      print key ": \"" value "\""
      done = 1
      next
    }
    { print }
    END {
      if (!done) {
        print key ": \"" value "\""
      }
    }
  ' "${file}" >"${tmp}"
  write_file_if_changed "${tmp}" "${file}"
  changed=$?
  return "${changed}"
}

yaml_list_block_hash() {
  parse_yaml_list_value_hash "$1" "$2" || return 1
}

current_file_hash() {
  local path="$1"
  sha256_file "${path}" || true
}

reconcile_k3s_config() {
  local server="${FUGUE_DISCOVERY_K3S_SERVER:-}"
  local fallback_servers="${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS:-}"
  local lb_cfg="/etc/fugue/k3s-api-lb.cfg"

  if [ -z "${server}" ]; then
    log "no discovery server available for k3s reconciliation"
    return 1
  fi

  mkdir -p /etc/rancher/k3s /etc/fugue
  if [ -n "${fallback_servers}" ] && command -v haproxy >/dev/null 2>&1; then
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
      echo "  server cp1 ${server#*://} check inter 2s fall 2 rise 1"
      local index=1
      local fallback=""
      for fallback in ${fallback_servers//,/ }; do
        index=$((index + 1))
        fallback="${fallback#*://}"
        echo "  server cp${index} ${fallback} check inter 2s fall 2 rise 1"
      done
    } >"${lb_cfg}.tmp"
    write_file_if_changed "${lb_cfg}.tmp" "${lb_cfg}" || true
    server="https://127.0.0.1:16443"
    if systemctl list-unit-files 2>/dev/null | grep -q '^fugue-k3s-api-lb\.service'; then
      systemctl daemon-reload >/dev/null 2>&1 || true
      systemctl restart fugue-k3s-api-lb.service >/dev/null 2>&1 || true
    fi
  fi
  yaml_update_scalar "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" server "${server}" || true
  return 0
}

reconcile_registry_mirror() {
  local registry_base="${FUGUE_DISCOVERY_REGISTRY_PULL_BASE:-}"
  local endpoint="${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT:-${FUGUE_DISCOVERY_REGISTRY_MIRROR:-}}"
  local tmp=""
  if [ -z "${registry_base}" ] || [ -z "${endpoint}" ]; then
    return 1
  fi
  mkdir -p /etc/rancher/k3s
  tmp="$(mktemp)"
  {
    printf 'mirrors:\n'
    printf '  "%s":\n' "${registry_base}"
    printf '    endpoint:\n'
    printf '      - "%s"\n' "${endpoint}"
    printf 'configs:\n'
    printf '  "%s":\n' "${registry_base}"
    printf '    tls:\n'
    printf '      insecure_skip_verify: true\n'
  } >"${tmp}"
  write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE}" || true
  return 0
}

render_lkg_service_env() {
  local target="$1"
  local generation_key="$2"
  local tmp=""
  local api_url="${FUGUE_DISCOVERY_API_URL:-}"
  if [ -z "${api_url}" ]; then
    return 1
  fi
  mkdir -p "$(dirname "${target}")"
  tmp="$(mktemp)"
  if [ -r "${target}" ]; then
    grep -Ev '^(FUGUE_API_URL|FUGUE_EDGE_DISCOVERY_GENERATION|FUGUE_DNS_DISCOVERY_GENERATION|FUGUE_EDGE_TOKEN|FUGUE_DNS_TOKEN|FUGUE_BUNDLE_SIGNING_KEY|FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY)=' "${target}" >"${tmp}" || true
  fi
  {
    printf 'FUGUE_API_URL=%s\n' "$(json_quote_env "${api_url}")"
    printf '%s=%s\n' "${generation_key}" "$(json_quote_env "${FUGUE_DISCOVERY_GENERATION:-}")"
    if [ -n "${FUGUE_BUNDLE_SIGNING_KEY_ID:-}" ]; then
      printf 'FUGUE_BUNDLE_SIGNING_KEY_ID=%s\n' "$(json_quote_env "${FUGUE_BUNDLE_SIGNING_KEY_ID}")"
    fi
    if [ -n "${FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID:-}" ]; then
      printf 'FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID=%s\n' "$(json_quote_env "${FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID}")"
    fi
    if [ -n "${FUGUE_BUNDLE_REVOKED_KEY_IDS:-}" ]; then
      printf 'FUGUE_BUNDLE_REVOKED_KEY_IDS=%s\n' "$(json_quote_env "${FUGUE_BUNDLE_REVOKED_KEY_IDS}")"
    fi
  } >>"${tmp}"
  write_file_if_changed "${tmp}" "${target}" || true
}

reconcile_lkg_service_envs() {
  local changed=0
  if render_lkg_service_env "${FUGUE_NODE_UPDATER_EDGE_ENV_FILE}" FUGUE_EDGE_DISCOVERY_GENERATION; then
    changed=1
  fi
  if render_lkg_service_env "${FUGUE_NODE_UPDATER_DNS_ENV_FILE}" FUGUE_DNS_DISCOVERY_GENERATION; then
    changed=1
  fi
  [ "${changed}" -eq 1 ]
}

reconcile_node_state() {
  mkdir -p "${FUGUE_NODE_UPDATER_STATE_DIR}"
  if fetch_discovery_bundle; then
    log "refreshed discovery bundle cache"
  elif [ -r "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" ]; then
    log "using cached discovery bundle state"
    # shellcheck disable=SC1090
    . "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"
  fi
  if fetch_node_policy_desired_state; then
    log "refreshed desired node policy cache"
  fi
  if [ -r "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" ]; then
    # shellcheck disable=SC1090
    . "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"
  fi
  render_node_updater_state_env
  if reconcile_registry_mirror; then
    log "updated k3s registry mirror configuration"
  fi
  if reconcile_k3s_config; then
    log "updated k3s join configuration"
  fi
  if reconcile_lkg_service_envs; then
    log "updated edge/dns non-secret environment generation"
  fi
  return 0
}

current_k3s_server() {
  if [ -n "${FUGUE_DISCOVERY_K3S_SERVER:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_K3S_SERVER}"
    return 0
  fi
  if [ -r "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" ]; then
    awk '
      $1 == "server:" {
        value = $2
        gsub(/"/, "", value)
        print value
        exit
      }
    ' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
  fi
}

current_k3s_fallback_servers() {
  if [ -n "${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS}"
    return 0
  fi
  if [ -r "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" ]; then
    # shellcheck disable=SC1090
    . "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"
    printf '%s' "${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS:-}"
  fi
}

current_registry_mirror() {
  if [ -n "${FUGUE_DISCOVERY_REGISTRY_MIRROR:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_REGISTRY_MIRROR}"
    return 0
  fi
  if [ -n "${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT}"
    return 0
  fi
  if [ -r "${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE}" ]; then
    awk '
      $1 == "endpoint:" { in_endpoints = 1; next }
      in_endpoints && $0 ~ /^[[:space:]]*-/ {
        value = $0
        sub(/^[[:space:]]*-[[:space:]]*"/, "", value)
        sub(/"$/, "", value)
        print value
        exit
      }
    ' "${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE}"
  fi
}

current_labels_hash() {
  yaml_list_block_hash "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" node-label || true
}

current_taints_hash() {
  yaml_list_block_hash "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" node-taint || true
}

current_edge_env_generation() {
  current_file_hash "${FUGUE_NODE_UPDATER_EDGE_ENV_FILE}"
}

current_dns_env_generation() {
  current_file_hash "${FUGUE_NODE_UPDATER_DNS_ENV_FILE}"
}

current_config_hash() {
  local tmp=""
  tmp="$(mktemp)"
  {
    printf 'server=%s\n' "$(current_k3s_server)"
    printf 'fallbacks=%s\n' "$(current_k3s_fallback_servers)"
    printf 'registry_mirror=%s\n' "$(current_registry_mirror)"
    printf 'labels_hash=%s\n' "$(current_labels_hash)"
    printf 'taints_hash=%s\n' "$(current_taints_hash)"
    printf 'edge_env_generation=%s\n' "$(current_edge_env_generation)"
    printf 'dns_env_generation=%s\n' "$(current_dns_env_generation)"
    printf 'discovery_generation=%s\n' "${FUGUE_DISCOVERY_GENERATION:-}"
  } >"${tmp}"
  sha256_file "${tmp}" || true
  rm -f "${tmp}"
}

record_last_error() {
  mkdir -p "${FUGUE_NODE_UPDATER_WORK_DIR}"
  printf '%s\n' "$*" >"${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}"
}

api_form() {
  local method="$1"
  local path="$2"
  shift 2
  curl -fsSL --retry 3 --retry-delay 2 -X "${method}" "${FUGUE_API_BASE}${path}" \
    -H "Authorization: Bearer ${FUGUE_NODE_UPDATER_TOKEN:?FUGUE_NODE_UPDATER_TOKEN is required}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    "$@"
}

heartbeat() {
  local current_k3s=""
  current_k3s="$(k3s_version)"
  api_form POST /v1/node-updater/heartbeat \
    --data-urlencode "updater_version=${FUGUE_NODE_UPDATER_VERSION}" \
    --data-urlencode "join_script_version=${FUGUE_JOIN_SCRIPT_VERSION:-}" \
    --data-urlencode "k3s_version=${current_k3s}" \
    --data-urlencode "k3s_server=$(current_k3s_server)" \
    --data-urlencode "k3s_fallback_servers=$(current_k3s_fallback_servers)" \
    --data-urlencode "registry_mirror=$(current_registry_mirror)" \
    --data-urlencode "labels_hash=$(current_labels_hash)" \
    --data-urlencode "taints_hash=$(current_taints_hash)" \
    --data-urlencode "edge_env_generation=$(current_edge_env_generation)" \
    --data-urlencode "dns_env_generation=$(current_dns_env_generation)" \
    --data-urlencode "config_hash=$(current_config_hash)" \
    --data-urlencode "discovery_generation=${FUGUE_DISCOVERY_GENERATION:-}" \
    --data-urlencode "os=$(uname -s 2>/dev/null || true)" \
    --data-urlencode "arch=$(uname -m 2>/dev/null || true)" \
    --data-urlencode "last_error=$(last_error)" \
    >/dev/null
}

claim_task() {
  api_form POST "/v1/node-updater/tasks/${FUGUE_NODE_UPDATE_TASK_ID}/claim" >/dev/null
}

log_task() {
  local message="$1"
  api_form POST "/v1/node-updater/tasks/${FUGUE_NODE_UPDATE_TASK_ID}/log" \
    --data-urlencode "message=${message}" >/dev/null || true
}

complete_task() {
  local status="$1"
  local message="$2"
  local error_message="${3:-}"
  api_form POST "/v1/node-updater/tasks/${FUGUE_NODE_UPDATE_TASK_ID}/complete" \
    --data-urlencode "status=${status}" \
    --data-urlencode "message=${message}" \
    --data-urlencode "error_message=${error_message}" >/dev/null || true
}

wait_for_unit_active() {
  local unit="$1"
  local timeout_seconds="${2:-900}"
  local started_at=""
  local elapsed=0
  started_at="$(date +%s)"
  while [ "${elapsed}" -lt "${timeout_seconds}" ]; do
    if systemctl is-active --quiet "${unit}"; then
      return 0
    fi
    sleep 5
    elapsed=$(( $(date +%s) - started_at ))
  done
  systemctl status "${unit}" --no-pager || true
  return 1
}

restart_k3s_agent() {
  log_task "restarting k3s-agent"
  systemctl restart k3s-agent
  wait_for_unit_active k3s-agent 900
  log_task "k3s-agent is active"
}

upgrade_k3s_agent() {
  local channel="${FUGUE_NODE_UPDATE_TASK_K3S_CHANNEL:-${FUGUE_K3S_CHANNEL:-stable}}"
  local target_version="${FUGUE_NODE_UPDATE_TASK_TARGET_K3S_VERSION:-}"
  local install_env=(INSTALL_K3S_EXEC=agent INSTALL_K3S_SKIP_START=true INSTALL_K3S_CHANNEL="${channel}")
  if [ -n "${target_version}" ]; then
    install_env+=(INSTALL_K3S_VERSION="${target_version}")
  fi
  log_task "installing k3s agent channel=${channel} target=${target_version:-latest}"
  curl -sfL https://get.k3s.io | env "${install_env[@]}" sh -
  restart_k3s_agent
  log_task "k3s agent version after upgrade: $(k3s_version)"
}

upgrade_node_updater() {
  local script_url="${FUGUE_NODE_UPDATE_TASK_NODE_UPDATER_SCRIPT_URL:-${FUGUE_API_BASE}/install/node-updater.sh}"
  local tmp=""
  tmp="$(mktemp)"
  log_task "downloading node updater script from ${script_url}"
  curl -fsSL "${script_url}" -o "${tmp}"
  install -m 0755 "${tmp}" /usr/local/bin/fugue-node-updater
  rm -f "${tmp}"
  log_task "node updater script installed"
}

diagnose_node() {
  log_task "hostname=$(hostname 2>/dev/null || true)"
  log_task "kernel=$(uname -a 2>/dev/null || true)"
  log_task "k3s=$(k3s_version)"
  log_task "k3s-agent=$(systemctl is-active k3s-agent 2>/dev/null || true)"
}

install_nfs_client_tools() {
  if command -v mount.nfs >/dev/null 2>&1; then
    log_task "NFS client tools are already installed"
    return 0
  fi
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "apt-get is unavailable; cannot install nfs-common automatically" >&2
    return 2
  fi
  log_task "installing NFS client tools"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends nfs-common
  log_task "NFS client tools installed: $(command -v mount.nfs 2>/dev/null || true)"
}

run_task() {
  case "${FUGUE_NODE_UPDATE_TASK_TYPE}" in
    refresh-join-config)
      log_task "refreshing join configuration from discovery bundle"
      if reconcile_node_state; then
        log_task "join configuration refreshed"
        return 0
      fi
      return 1
      ;;
    restart-k3s-agent)
      restart_k3s_agent
      ;;
    upgrade-k3s-agent)
      upgrade_k3s_agent
      ;;
    upgrade-node-updater)
      upgrade_node_updater
      ;;
    diagnose-node)
      diagnose_node
      ;;
    install-nfs-client-tools)
      install_nfs_client_tools
      ;;
    *)
      echo "unsupported node update task type: ${FUGUE_NODE_UPDATE_TASK_TYPE}" >&2
      return 2
      ;;
  esac
}

run_once() {
  local task_env=""
  local rc=0
  mkdir -p "${FUGUE_NODE_UPDATER_WORK_DIR}"
  if load_cached_env_file "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"; then
    log "loaded cached discovery env"
  fi
  if load_cached_env_file "${FUGUE_NODE_UPDATER_STATE_ENV_FILE}"; then
    log "loaded cached state env"
  fi
  if reconcile_node_state; then
    log "node state reconciled"
  else
    log "node state reconciliation did not make changes or could not complete"
  fi
  heartbeat || log "heartbeat failed"
  task_env="$(mktemp)"
  api_form GET "/v1/node-updater/tasks?format=env&limit=1" >"${task_env}"
  # shellcheck disable=SC1090
  . "${task_env}"
  rm -f "${task_env}"
  if [ -z "${FUGUE_NODE_UPDATE_TASK_ID:-}" ]; then
    log "no pending task"
    return 0
  fi
  log "claiming task ${FUGUE_NODE_UPDATE_TASK_ID} (${FUGUE_NODE_UPDATE_TASK_TYPE})"
  claim_task
  if run_task; then
    clear_last_error
    complete_task completed "node update task completed"
    heartbeat || true
    return 0
  else
    rc=$?
    record_last_error "task ${FUGUE_NODE_UPDATE_TASK_ID} failed with exit code ${rc}"
    complete_task failed "node update task failed" "$(last_error)"
    heartbeat || true
    return "${rc}"
  fi
}

case "${1:-run-once}" in
  run-once)
    require_cmd curl
    require_cmd systemctl
    require_cmd cmp
    require_cmd awk
    require_cmd sed
    run_once
    ;;
  heartbeat)
    require_cmd curl
    heartbeat
    ;;
  version)
    printf '%s\n' "${FUGUE_NODE_UPDATER_VERSION}"
    ;;
  *)
    echo "usage: fugue-node-updater [run-once|heartbeat|version]" >&2
    exit 2
    ;;
esac
`
	return strings.ReplaceAll(script, "__FUGUE_API_BASE__", apiBase)
}
