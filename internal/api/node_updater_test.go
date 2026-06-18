package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestNodeUpdaterAPILifecycle(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Node Updater API Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, apiSecret, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"runtime.attach"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	enrollForm := url.Values{}
	enrollForm.Set("node_key", nodeSecret)
	enrollForm.Set("node_name", "worker-1")
	enrollForm.Set("machine_name", "worker-1")
	enrollForm.Set("machine_fingerprint", "machine-1")
	enrollForm.Set("endpoint", "https://worker-1.example.com")
	enrollForm.Set("labels", "zone=test-a,tier=edge")
	enrollForm.Set("updater_version", "v1")
	enrollForm.Set("join_script_version", "join-v1")
	enrollForm.Set("capabilities", "heartbeat,tasks,upgrade-k3s-agent")
	enrollRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/enroll", "", enrollForm)
	if enrollRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, enrollRecorder.Code, enrollRecorder.Body.String())
	}
	enrollEnv := parseEnvResponse(enrollRecorder.Body.String())
	updaterID := enrollEnv["FUGUE_NODE_UPDATER_ID"]
	updaterToken := enrollEnv["FUGUE_NODE_UPDATER_TOKEN"]
	if updaterID == "" || !strings.HasPrefix(updaterToken, "fugue_nu") {
		t.Fatalf("expected updater id and token in env response, got %q", enrollRecorder.Body.String())
	}

	heartbeatForm := url.Values{}
	heartbeatForm.Set("updater_version", "v2")
	heartbeatForm.Set("join_script_version", "join-v2")
	heartbeatForm.Set("capabilities", "heartbeat,tasks,upgrade-k3s-agent")
	heartbeatForm.Set("k3s_version", "k3s version v1.32.0+k3s1")
	heartbeatForm.Set("os", "linux")
	heartbeatForm.Set("arch", "amd64")
	heartbeatRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/heartbeat", updaterToken, heartbeatForm)
	if heartbeatRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, heartbeatRecorder.Code, heartbeatRecorder.Body.String())
	}

	desiredRecorder := performFormRequest(t, server, http.MethodGet, "/v1/node-updater/desired-state", updaterToken, nil)
	if desiredRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, desiredRecorder.Code, desiredRecorder.Body.String())
	}
	var desired struct {
		DesiredState model.NodeUpdaterDesiredState `json:"desired_state"`
	}
	mustDecodeJSON(t, desiredRecorder, &desired)
	if desired.DesiredState.NodeUpdater.ID != updaterID || desired.DesiredState.DiscoveryBundle.Generation == "" {
		t.Fatalf("unexpected desired state: %+v", desired.DesiredState)
	}

	forbiddenRecorder := performFormRequest(t, server, http.MethodGet, "/v1/node-updaters", updaterToken, nil)
	if forbiddenRecorder.Code != http.StatusForbidden {
		t.Fatalf("expected node updater token to be forbidden on API endpoints, got %d body=%s", forbiddenRecorder.Code, forbiddenRecorder.Body.String())
	}
	forbiddenRecorder = performFormRequest(t, server, http.MethodGet, "/v1/node-updater/tasks", apiSecret, nil)
	if forbiddenRecorder.Code != http.StatusForbidden {
		t.Fatalf("expected api key to be forbidden on updater endpoints, got %d body=%s", forbiddenRecorder.Code, forbiddenRecorder.Body.String())
	}

	createRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/node-update-tasks", apiSecret, map[string]any{
		"node_updater_id": updaterID,
		"type":            model.NodeUpdateTaskTypeUpgradeK3SAgent,
		"payload": map[string]string{
			"k3s_channel":        "stable",
			"target_k3s_version": "v1.32.0+k3s1",
		},
	})
	if createRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, createRecorder.Code, createRecorder.Body.String())
	}
	var created struct {
		Task model.NodeUpdateTask `json:"task"`
	}
	mustDecodeJSON(t, createRecorder, &created)
	if created.Task.ID == "" || created.Task.NodeUpdaterID != updaterID {
		t.Fatalf("unexpected created task: %+v", created.Task)
	}

	pollRecorder := performFormRequest(t, server, http.MethodGet, "/v1/node-updater/tasks?format=env&limit=1", updaterToken, nil)
	if pollRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, pollRecorder.Code, pollRecorder.Body.String())
	}
	taskEnv := parseEnvResponse(pollRecorder.Body.String())
	if taskEnv["FUGUE_NODE_UPDATE_TASK_ID"] != created.Task.ID || taskEnv["FUGUE_NODE_UPDATE_TASK_TYPE"] != model.NodeUpdateTaskTypeUpgradeK3SAgent {
		t.Fatalf("unexpected task env: %q", pollRecorder.Body.String())
	}
	if taskEnv["FUGUE_NODE_UPDATE_TASK_TARGET_K3S_VERSION"] != "v1.32.0+k3s1" {
		t.Fatalf("expected target version payload in task env, got %q", pollRecorder.Body.String())
	}

	claimRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+created.Task.ID+"/claim", updaterToken, nil)
	if claimRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, claimRecorder.Code, claimRecorder.Body.String())
	}
	logForm := url.Values{}
	logForm.Set("message", "k3s upgrade started")
	logRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+created.Task.ID+"/log", updaterToken, logForm)
	if logRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, logRecorder.Code, logRecorder.Body.String())
	}
	completeForm := url.Values{}
	completeForm.Set("status", model.NodeUpdateTaskStatusCompleted)
	completeForm.Set("message", "k3s upgraded")
	completeRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+created.Task.ID+"/complete", updaterToken, completeForm)
	if completeRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, completeRecorder.Code, completeRecorder.Body.String())
	}

	listRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/node-update-tasks?status=completed", apiSecret, nil)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, listRecorder.Code, listRecorder.Body.String())
	}
	var listed struct {
		Tasks []model.NodeUpdateTask `json:"tasks"`
	}
	mustDecodeJSON(t, listRecorder, &listed)
	if len(listed.Tasks) != 1 || listed.Tasks[0].ID != created.Task.ID || len(listed.Tasks[0].Logs) != 1 {
		t.Fatalf("expected completed task with log, got %+v", listed.Tasks)
	}
}

func TestNodeUpdaterTaskPollExpiresStaleRunningTasks(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	s := store.New(storePath)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Node Updater Stale API Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	enrollForm := url.Values{}
	enrollForm.Set("node_key", nodeSecret)
	enrollForm.Set("node_name", "worker-stale")
	enrollForm.Set("machine_name", "worker-stale")
	enrollForm.Set("machine_fingerprint", "machine-stale")
	enrollForm.Set("endpoint", "https://worker-stale.example.com")
	enrollForm.Set("updater_version", "v1")
	enrollForm.Set("join_script_version", "join-v1")
	enrollForm.Set("capabilities", "heartbeat,tasks,diagnose-node")
	enrollRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/enroll", "", enrollForm)
	if enrollRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, enrollRecorder.Code, enrollRecorder.Body.String())
	}
	enrollEnv := parseEnvResponse(enrollRecorder.Body.String())
	updaterID := enrollEnv["FUGUE_NODE_UPDATER_ID"]
	updaterToken := enrollEnv["FUGUE_NODE_UPDATER_TOKEN"]
	requester := model.Principal{
		ActorType: model.ActorTypeAPIKey,
		ActorID:   "apikey_test",
		TenantID:  tenant.ID,
	}
	staleTask, err := s.CreateNodeUpdateTask(requester, updaterID, "", "", model.NodeUpdateTaskTypeDiagnoseNode, map[string]string{"reason": "stale"})
	if err != nil {
		t.Fatalf("create stale task: %v", err)
	}
	nextTask, err := s.CreateNodeUpdateTask(requester, updaterID, "", "", model.NodeUpdateTaskTypeDiagnoseNode, map[string]string{"reason": "next"})
	if err != nil {
		t.Fatalf("create next task: %v", err)
	}
	if _, err := s.ClaimNodeUpdateTask(staleTask.ID, updaterID); err != nil {
		t.Fatalf("claim stale task: %v", err)
	}
	ageTaskInStoreFile(t, storePath, staleTask.ID, time.Now().UTC().Add(-staleNodeUpdateTaskTimeout-time.Minute))

	pollRecorder := performFormRequest(t, server, http.MethodGet, "/v1/node-updater/tasks?format=env&limit=1", updaterToken, nil)
	if pollRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, pollRecorder.Code, pollRecorder.Body.String())
	}
	taskEnv := parseEnvResponse(pollRecorder.Body.String())
	if taskEnv["FUGUE_NODE_UPDATE_TASK_ID"] != nextTask.ID {
		t.Fatalf("expected next pending task after stale task cleanup, got %q", pollRecorder.Body.String())
	}
	tasks, err := s.ListNodeUpdateTasks(tenant.ID, false, updaterID, "")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	byID := map[string]model.NodeUpdateTask{}
	for _, task := range tasks {
		byID[task.ID] = task
	}
	if byID[staleTask.ID].Status != model.NodeUpdateTaskStatusFailed || byID[staleTask.ID].CompletedAt == nil {
		t.Fatalf("expected stale task failed, got %+v", byID[staleTask.ID])
	}
	if byID[nextTask.ID].Status != model.NodeUpdateTaskStatusPending {
		t.Fatalf("expected next task to remain pending until claimed, got %+v", byID[nextTask.ID])
	}
}

func TestNodeUpdaterCanReportImageLocationForAppTenant(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	nodeTenant, err := s.CreateTenant("Node Tenant")
	if err != nil {
		t.Fatalf("create node tenant: %v", err)
	}
	appTenant, err := s.CreateTenant("App Tenant")
	if err != nil {
		t.Fatalf("create app tenant: %v", err)
	}
	project, err := s.CreateProject(appTenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(appTenant.ID, project.ID, "web", "", model.AppSpec{
		Image:     "registry.fugue.internal:5000/fugue-apps/web:test",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(nodeTenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{RegistryPullBase: "registry.fugue.internal:5000"})

	enrollForm := url.Values{}
	enrollForm.Set("node_key", nodeSecret)
	enrollForm.Set("node_name", "worker-image")
	enrollForm.Set("machine_name", "worker-image")
	enrollForm.Set("machine_fingerprint", "machine-image")
	enrollForm.Set("endpoint", "https://worker-image.example.com")
	enrollForm.Set("updater_version", "v1")
	enrollForm.Set("join_script_version", "join-v1")
	enrollForm.Set("capabilities", "heartbeat,tasks,prepull-app-images")
	enrollRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/enroll", "", enrollForm)
	if enrollRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, enrollRecorder.Code, enrollRecorder.Body.String())
	}
	updaterToken := parseEnvResponse(enrollRecorder.Body.String())["FUGUE_NODE_UPDATER_TOKEN"]

	reportForm := url.Values{}
	reportForm.Set("app_id", app.ID)
	reportForm.Set("image_ref", "registry.fugue.internal:5000/fugue-apps/web:test")
	reportForm.Set("status", model.ImageLocationStatusPulling)
	reportForm.Set("cache_endpoint", "http://127.0.0.1:5000")
	reportRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/image-locations", updaterToken, reportForm)
	if reportRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, reportRecorder.Code, reportRecorder.Body.String())
	}
	var response struct {
		ImageLocation model.ImageLocation `json:"image_location"`
	}
	mustDecodeJSON(t, reportRecorder, &response)
	if response.ImageLocation.TenantID != appTenant.ID || response.ImageLocation.AppID != app.ID {
		t.Fatalf("expected app tenant image location, got %+v", response.ImageLocation)
	}
	if response.ImageLocation.ClusterNodeName != "worker-image" || response.ImageLocation.Status != model.ImageLocationStatusPulling {
		t.Fatalf("expected node metadata and pulling status, got %+v", response.ImageLocation)
	}

	presentForm := url.Values{}
	presentForm.Set("app_id", app.ID)
	presentForm.Set("image_ref", "registry.fugue.internal:5000/fugue-apps/web:test")
	presentForm.Set("status", model.ImageLocationStatusPresent)
	presentRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/image-locations", updaterToken, presentForm)
	if presentRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, presentRecorder.Code, presentRecorder.Body.String())
	}
	mustDecodeJSON(t, presentRecorder, &response)
	if response.ImageLocation.CacheEndpoint != "http://worker-image.example.com:5000" {
		t.Fatalf("expected inferred cache endpoint, got %+v", response.ImageLocation)
	}

	listRecorder := performFormRequest(t, server, http.MethodGet, "/v1/node-updater/image-locations?image_ref=registry.fugue.internal:5000/fugue-apps/web:test&status=present", updaterToken, nil)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, listRecorder.Code, listRecorder.Body.String())
	}
	var listResponse struct {
		ImageLocations []model.ImageLocation `json:"image_locations"`
	}
	mustDecodeJSON(t, listRecorder, &listResponse)
	if len(listResponse.ImageLocations) != 1 || listResponse.ImageLocations[0].TenantID != appTenant.ID {
		t.Fatalf("expected cross-tenant app image location, got %+v", listResponse.ImageLocations)
	}

	unfilteredRecorder := performFormRequest(t, server, http.MethodGet, "/v1/node-updater/image-locations", updaterToken, nil)
	if unfilteredRecorder.Code != http.StatusBadRequest {
		t.Fatalf("expected unfiltered node updater list to be rejected, got %d body=%s", unfilteredRecorder.Code, unfilteredRecorder.Body.String())
	}
}

func TestNodeUpdaterInstallScriptHasValidBashSyntax(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}

	var server Server
	script := server.nodeUpdaterInstallScript("https://api.fugue.pro")
	for _, want := range []string{
		`/v1/discovery/bundle`,
		`/v1/node-updater/desired-state`,
		`refresh-join-config`,
		`prepull-app-images`,
		`FUGUE_NODE_UPDATER_SCRIPT_VERSION="v9"`,
		`FUGUE_NODE_UPDATER_CAPABILITIES=`,
		`verify_image_cache_manifest`,
		`pre-pull succeeded but node image cache does not serve registry manifest`,
		`restart_k3s_agent_for_config_reload`,
		`restarting k3s-agent so containerd reloads updated join/registry configuration`,
		`time-sync`,
		`render_desired_k3s_policy_lists`,
		`reconcile_node_policy_k3s_config`,
		`node-external-ip`,
		`flannel-iface`,
		`--data-urlencode "capabilities=${FUGUE_NODE_UPDATER_CAPABILITIES}"`,
		`capabilities)`,
		`/etc/rancher/k3s/config.yaml`,
		`/etc/rancher/k3s/registries.yaml`,
		`/etc/systemd/timesyncd.conf.d/10-fugue-managed.conf`,
		`PollIntervalMaxSec=%ss`,
		`control_plane_date_epoch`,
		`reconcile_cni_bridge_mtu`,
		`FLANNEL_MTU`,
		`fugue-edge.env`,
		`fugue-dns.env`,
		`reconcile_node_dns_escape_hatch`,
		`bind-interfaces`,
		`discovery_generation=`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected node-updater script to contain %q", want)
		}
	}
	scriptPath := filepath.Join(t.TempDir(), "node-updater.sh")
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write node-updater script: %v", err)
	}

	cmd := exec.Command("bash", "-n", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n %s: %v\n%s", scriptPath, err, output)
	}
}

func ageTaskInStoreFile(t *testing.T, storePath, taskID string, updatedAt time.Time) {
	t.Helper()
	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode store: %v", err)
	}
	for i := range state.NodeUpdateTasks {
		if state.NodeUpdateTasks[i].ID == taskID {
			state.NodeUpdateTasks[i].UpdatedAt = updatedAt
			state.NodeUpdateTasks[i].ClaimedAt = &updatedAt
		}
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("encode store: %v", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(storePath, encoded, 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}
}

func TestNodeUpdaterK3sConfigReconcileRefreshesNodePolicyLabelsAndTaints(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}

	var server Server
	script := server.nodeUpdaterInstallScript("https://api.fugue.pro")
	prefix, _, ok := strings.Cut(script, "\ncase \"${1:-run-once}\" in")
	if !ok {
		t.Fatalf("node updater script missing command dispatch")
	}

	harness := prefix + `
tmpdir="$(mktemp -d)"
FUGUE_NODE_UPDATER_K3S_CONFIG_FILE="${tmpdir}/config.yaml"
FUGUE_NODE_UPDATER_DESIRED_STATE_FILE="${tmpdir}/desired-state.json"
FUGUE_DISCOVERY_K3S_SERVER="https://cp.example:6443"
cat >"${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" <<'YAML'
server: "https://cp.example:6443"
node-external-ip: "100.64.0.13"
flannel-iface: "tailscale0"
node-label:
  - "fugue.io/machine-id=machine_edge"
  - "fugue.io/machine-scope=tenant-runtime"
  - "fugue.io/node-key-id=nodekey_edge"
  - "fugue.io/node-mode=managed-owned"
  - "fugue.io/role.app-runtime=true"
  - "fugue.io/role.edge=true"
  - "fugue.io/runtime-id=runtime_edge"
  - "fugue.io/tenant-id=tenant_edge"
  - "fugue.io/location-country-code=us"
  - "fugue.io/public-ip=203.0.113.10"
node-taint:
  - "fugue.io/tenant=tenant_edge:NoSchedule"
YAML
cat >"${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}" <<'JSON'
{
  "desired_state": {
    "node_updater": {
      "node_key_id": "nodekey_edge",
      "machine_id": "machine_edge",
      "runtime_id": "runtime_edge",
      "tenant_id": "tenant_edge"
    },
    "node_policy": {
      "node_name": "edge-1",
      "runtime_id": "runtime_edge",
      "tenant_id": "tenant_edge",
      "machine_id": "machine_edge",
      "policy": {
        "allow_app_runtime": false,
        "allow_builds": false,
        "allow_shared_pool": false,
        "allow_edge": true,
        "allow_dns": false,
        "allow_internal_maintenance": false,
        "dedicated_mode": "edge",
        "node_mode": "managed-owned",
        "node_health": "ready",
        "desired_control_plane_role": "none"
      },
      "labels": {
        "fugue.io/machine-id": "machine_edge",
        "fugue.io/machine-scope": "tenant-runtime",
        "fugue.io/node-key-id": "nodekey_edge",
        "fugue.io/node-mode": "managed-owned",
        "fugue.io/role.app-runtime": "true",
        "fugue.io/role.edge": "true",
        "fugue.io/runtime-id": "runtime_edge",
        "fugue.io/tenant-id": "tenant_edge",
        "fugue.io/location-country-code": "us",
        "fugue.io/public-ip": "203.0.113.10"
      }
    }
  }
}
JSON
if ! reconcile_k3s_config; then
  echo "first reconcile should report a write"
  exit 1
fi
if grep -q 'fugue.io/role.app-runtime=true' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"; then
  echo "stale app-runtime label was not removed"
  cat "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
  exit 1
fi
grep -q 'node-external-ip: "203.0.113.10"' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
if grep -q '^flannel-iface:' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"; then
  echo "stale flannel iface was not removed"
  cat "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
  exit 1
fi
grep -q 'fugue.io/role.edge=true' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/dedicated=edge:NoSchedule' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/tenant=tenant_edge:NoSchedule' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
if reconcile_k3s_config; then
  echo "second reconcile should not report a write"
  exit 1
fi
`
	scriptPath := filepath.Join(t.TempDir(), "node-updater-policy-reconcile-test.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node updater policy reconcile harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node updater policy reconcile harness failed: %v\n%s", err, output)
	}
}

func TestNodeUpdaterK3sConfigReconcileOnlyReportsRealChanges(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}

	var server Server
	script := server.nodeUpdaterInstallScript("https://api.fugue.pro")
	prefix, _, ok := strings.Cut(script, "\ncase \"${1:-run-once}\" in")
	if !ok {
		t.Fatalf("node updater script missing command dispatch")
	}

	harness := prefix + `
tmpdir="$(mktemp -d)"
FUGUE_NODE_UPDATER_K3S_CONFIG_FILE="${tmpdir}/config.yaml"
FUGUE_DISCOVERY_K3S_SERVER="https://cp.example:6443"
FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS=""
mkdir() {
  local args=()
  local arg=""
  for arg in "$@"; do
    case "${arg}" in
      /etc/rancher/k3s) arg="${tmpdir}/etc/rancher/k3s" ;;
      /etc/fugue) arg="${tmpdir}/etc/fugue" ;;
    esac
    args+=("${arg}")
  done
  command mkdir "${args[@]}"
}
if ! reconcile_k3s_config; then
  echo "first reconcile should report a write"
  exit 1
fi
if reconcile_k3s_config; then
  echo "second reconcile should not report a write"
  exit 1
fi
grep -q 'server: "https://cp.example:6443"' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
`
	scriptPath := filepath.Join(t.TempDir(), "node-updater-reconcile-test.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node-updater reconcile harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node updater reconcile harness failed: %v\n%s", err, output)
	}
}

func TestNodeUpdaterTaskEnvNormalizesLegacyManagedPrepullImageRefs(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Node Updater API Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, apiSecret, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"runtime.attach"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		RegistryPushBase: "fugue-fugue-registry.fugue-system.svc.cluster.local:5000",
		RegistryPullBase: "registry.fugue.internal:5000",
	})

	enrollForm := url.Values{}
	enrollForm.Set("node_key", nodeSecret)
	enrollForm.Set("node_name", "worker-1")
	enrollForm.Set("machine_name", "worker-1")
	enrollForm.Set("machine_fingerprint", "machine-1")
	enrollForm.Set("endpoint", "https://worker-1.example.com")
	enrollForm.Set("capabilities", "heartbeat,tasks,prepull-app-images")
	enrollRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/enroll", "", enrollForm)
	if enrollRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, enrollRecorder.Code, enrollRecorder.Body.String())
	}
	enrollEnv := parseEnvResponse(enrollRecorder.Body.String())
	updaterToken := enrollEnv["FUGUE_NODE_UPDATER_TOKEN"]
	updaterID := enrollEnv["FUGUE_NODE_UPDATER_ID"]
	if updaterID == "" || updaterToken == "" {
		t.Fatalf("expected updater enrollment env, got %q", enrollRecorder.Body.String())
	}

	legacyRef := "fugue-fugue-registry.fugue-system.svc.cluster.local:5000/fugue-apps/demo:git-abc"
	createRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/node-update-tasks", apiSecret, map[string]any{
		"node_updater_id": updaterID,
		"type":            model.NodeUpdateTaskTypePrepullAppImages,
		"payload": map[string]string{
			"images":    legacyRef,
			"image_ref": legacyRef,
		},
	})
	if createRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, createRecorder.Code, createRecorder.Body.String())
	}

	pollRecorder := performFormRequest(t, server, http.MethodGet, "/v1/node-updater/tasks?format=env&limit=1", updaterToken, nil)
	if pollRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, pollRecorder.Code, pollRecorder.Body.String())
	}
	taskEnv := parseEnvResponse(pollRecorder.Body.String())
	wantRef := "registry.fugue.internal:5000/fugue-apps/demo:git-abc"
	if taskEnv["FUGUE_NODE_UPDATE_TASK_IMAGES"] != wantRef || taskEnv["FUGUE_NODE_UPDATE_TASK_IMAGE_REF"] != wantRef {
		t.Fatalf("expected normalized prepull refs %q, got %q", wantRef, pollRecorder.Body.String())
	}
}

func TestJoinClusterInstallScriptIncludesNodeUpdaterInstaller(t *testing.T) {
	t.Parallel()

	var server Server
	script := server.joinClusterInstallScript("https://api.fugue.pro")
	for _, want := range []string{
		`FUGUE_NODE_UPDATER_ENABLED="${FUGUE_NODE_UPDATER_ENABLED:-true}"`,
		`/install/node-updater.sh`,
		`/v1/node-updater/enroll`,
		`fugue-node-updater.service`,
		`fugue-node-updater.timer`,
		`Installing NFS client tools`,
		`install-nfs-client-tools`,
		`time-sync`,
		`local updater_version="v6"`,
		`reconcile_cni_bridge_mtu`,
		`FLANNEL_MTU`,
		`/v1/discovery/bundle`,
		`FUGUE_DISCOVERY_GENERATION`,
		`refresh-join-config`,
		`updater_capabilities="$(/usr/local/bin/fugue-node-updater capabilities`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected join-cluster install script to contain %q", want)
		}
	}
}

func performFormRequest(t *testing.T, server *Server, method, target, token string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	var body *strings.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	} else {
		body = strings.NewReader("")
	}
	req := httptest.NewRequest(method, target, body)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func parseEnvResponse(body string) map[string]string {
	values := map[string]string{}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		values[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "'")
	}
	return values
}
