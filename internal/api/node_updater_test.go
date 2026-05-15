package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

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
		`/etc/rancher/k3s/config.yaml`,
		`/etc/rancher/k3s/registries.yaml`,
		`reconcile_cni_bridge_mtu`,
		`FLANNEL_MTU`,
		`fugue-edge.env`,
		`fugue-dns.env`,
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
		`reconcile_cni_bridge_mtu`,
		`FLANNEL_MTU`,
		`/v1/discovery/bundle`,
		`FUGUE_DISCOVERY_GENERATION`,
		`refresh-join-config`,
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
