package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
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
		Labels:            req.Labels,
		Capabilities:      req.Capabilities,
		UpdaterVersion:    req.UpdaterVersion,
		JoinScriptVersion: req.JoinScriptVersion,
		K3SVersion:        req.K3SVersion,
		OS:                req.OS,
		Arch:              req.Arch,
		LastError:         req.LastError,
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
	Labels            map[string]string `json:"labels"`
	Capabilities      []string          `json:"capabilities"`
	UpdaterVersion    string            `json:"updater_version"`
	JoinScriptVersion string            `json:"join_script_version"`
	K3SVersion        string            `json:"k3s_version"`
	OS                string            `json:"os"`
	Arch              string            `json:"arch"`
	LastError         string            `json:"last_error"`
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
			Labels:            parseCSVLabels(r.Form.Get("labels")),
			Capabilities:      parseCSVList(r.Form.Get("capabilities")),
			UpdaterVersion:    r.Form.Get("updater_version"),
			JoinScriptVersion: r.Form.Get("join_script_version"),
			K3SVersion:        r.Form.Get("k3s_version"),
			OS:                r.Form.Get("os"),
			Arch:              r.Form.Get("arch"),
			LastError:         r.Form.Get("last_error"),
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

run_task() {
  case "${FUGUE_NODE_UPDATE_TASK_TYPE}" in
    refresh-join-config)
      echo "refresh-join-config is not supported by fugue-node-updater v1; rerun join-cluster.sh on the node" >&2
      return 2
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
