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
	_, platformAdminKey, err := s.CreateAPIKey(tenant.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create platform admin api key: %v", err)
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
	deepHealthRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/node-updater/heartbeat", updaterToken, map[string]any{
		"updater_version": "v2",
		"capabilities":    []string{"heartbeat", "tasks", "diagnose-node", "upgrade-k3s-agent"},
		"deep_health": map[string]any{
			"reported_at": time.Now().UTC(),
			"checks": []map[string]any{
				{
					"name":      model.NodeDeepHealthCheckPodDNSToKubeDNSService,
					"category":  "dns",
					"status":    model.NodeDeepHealthStatusFail,
					"hard_fail": true,
					"observed":  "timeout to 10.43.0.10:53",
					"expected":  "DNS response",
				},
			},
		},
	})
	if deepHealthRecorder.Code != http.StatusOK {
		t.Fatalf("expected deep health heartbeat status %d, got %d body=%s", http.StatusOK, deepHealthRecorder.Code, deepHealthRecorder.Body.String())
	}
	var heartbeatResponse struct {
		DeepHealth model.NodeDeepHealthResult `json:"deep_health"`
	}
	mustDecodeJSON(t, deepHealthRecorder, &heartbeatResponse)
	if heartbeatResponse.DeepHealth.QuarantineState != model.NodeQuarantineStateQuarantined ||
		heartbeatResponse.DeepHealth.QuarantineReason != model.NodeQuarantineReasonDNSHardFail ||
		!heartbeatResponse.DeepHealth.ObservedOnly {
		t.Fatalf("expected observe-only DNS quarantine, got %+v", heartbeatResponse.DeepHealth)
	}
	healthRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/node-health/"+updaterID, platformAdminKey, nil)
	if healthRecorder.Code != http.StatusOK {
		t.Fatalf("expected admin health status %d, got %d body=%s", http.StatusOK, healthRecorder.Code, healthRecorder.Body.String())
	}
	var healthResponse model.NodeDeepHealthResponse
	mustDecodeJSON(t, healthRecorder, &healthResponse)
	if healthResponse.Result.QuarantineState != model.NodeQuarantineStateQuarantined ||
		healthResponse.Result.QuarantineReason != model.NodeQuarantineReasonDNSHardFail {
		t.Fatalf("unexpected stored deep health response: %+v", healthResponse.Result)
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

func TestNodeUpdaterEdgeCredentialInfersCountryFromPublicIP(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	geoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","countryCode":"US"}`))
	}))
	defer geoServer.Close()
	oldClient := geoIPHTTPClient
	oldEndpoints := geoIPLookupEndpoints
	defer func() {
		geoIPHTTPClient = oldClient
		geoIPLookupEndpoints = oldEndpoints
	}()
	geoIPHTTPClient = geoServer.Client()
	geoIPLookupEndpoints = []geoIPLookupEndpoint{
		{
			Name:   "fixture",
			URL:    func(string) string { return geoServer.URL },
			Decode: decodeIPAPIComCountryCode,
		},
	}

	req := httptest.NewRequest(http.MethodGet, "https://api.fugue.pro/v1/node-updater/desired-state", nil)
	credential, warnings, err := server.nodeUpdaterEdgeCredential(req, model.NodeUpdater{
		ClusterNodeName: "dmit",
		Labels: map[string]string{
			"fugue.io/public-ip": "191.222.213.223",
			"fugue.io/role.edge": "true",
		},
	}, &model.ClusterNodePolicyStatus{
		NodeName: "dmit",
		Policy: &model.ClusterNodePolicy{
			AllowEdge:     true,
			AllowDNS:      false,
			DedicatedMode: "edge",
		},
		Labels: map[string]string{
			"fugue.io/public-ip": "191.222.213.223",
			"fugue.io/role.edge": "true",
		},
	})
	if err != nil {
		t.Fatalf("issue edge credential: %v", err)
	}
	if credential == nil {
		t.Fatalf("expected edge credential, warnings=%v", warnings)
	}
	if credential.Token == "" || credential.TokenPrefix == "" {
		t.Fatalf("expected first credential response to include token, got %+v", credential)
	}
	if credential.EdgeGroupID != "edge-group-country-us" || credential.Country != "us" || credential.WorkloadMode != "dynamic" {
		t.Fatalf("unexpected inferred credential: %+v warnings=%v", credential, warnings)
	}
	reportedCredential, _, err := server.nodeUpdaterEdgeCredential(req, model.NodeUpdater{
		ClusterNodeName:   "dmit",
		EdgeEnvGeneration: "v2:" + credential.TokenPrefix + ":already-installed",
		Labels: map[string]string{
			"fugue.io/public-ip": "191.222.213.223",
			"fugue.io/role.edge": "true",
		},
	}, &model.ClusterNodePolicyStatus{
		NodeName: "dmit",
		Policy: &model.ClusterNodePolicy{
			AllowEdge:     true,
			AllowDNS:      false,
			DedicatedMode: "edge",
		},
		Labels: map[string]string{
			"fugue.io/public-ip": "191.222.213.223",
			"fugue.io/role.edge": "true",
		},
	})
	if err != nil {
		t.Fatalf("issue reported edge credential: %v", err)
	}
	if reportedCredential == nil || reportedCredential.Token != "" || reportedCredential.TokenPrefix != credential.TokenPrefix {
		t.Fatalf("expected installed token prefix to avoid reissue, got %+v", reportedCredential)
	}
	reissuedCredential, _, err := server.nodeUpdaterEdgeCredential(req, model.NodeUpdater{
		ClusterNodeName:   "dmit",
		EdgeEnvGeneration: "v2:missing:empty-file",
		Labels: map[string]string{
			"fugue.io/public-ip": "191.222.213.223",
			"fugue.io/role.edge": "true",
		},
	}, &model.ClusterNodePolicyStatus{
		NodeName: "dmit",
		Policy: &model.ClusterNodePolicy{
			AllowEdge:     true,
			AllowDNS:      false,
			DedicatedMode: "edge",
		},
		Labels: map[string]string{
			"fugue.io/public-ip": "191.222.213.223",
			"fugue.io/role.edge": "true",
		},
	})
	if err != nil {
		t.Fatalf("reissue edge credential: %v", err)
	}
	if reissuedCredential == nil || reissuedCredential.Token == "" || reissuedCredential.TokenPrefix == credential.TokenPrefix {
		t.Fatalf("expected missing token prefix to reissue credential, got %+v", reissuedCredential)
	}
	policy := nodeUpdaterPolicyWithEdgeCredentialLabels(&model.ClusterNodePolicyStatus{
		NodeName: "dmit",
		Policy: &model.ClusterNodePolicy{
			AllowEdge:     true,
			AllowDNS:      false,
			DedicatedMode: "edge",
		},
		Labels: map[string]string{
			"fugue.io/public-ip": "191.222.213.223",
			"fugue.io/role.edge": "true",
		},
	}, credential)
	if policy == nil {
		t.Fatalf("expected policy labels to be augmented")
	}
	for key, want := range map[string]string{
		"fugue.io/location-country-code": "us",
		"fugue.io/edge-group-id":         "edge-group-country-us",
		"fugue.io/edge-workload":         "dynamic",
		"fugue.io/edge-location-status":  "ready",
	} {
		if got := policy.Labels[key]; got != want {
			t.Fatalf("expected augmented label %s=%s, got %q labels=%v", key, want, got, policy.Labels)
		}
	}
	if !strings.Contains(strings.Join(warnings, "\n"), "inferred from public IP") {
		t.Fatalf("expected inference warning, got %v", warnings)
	}
	node, _, err := s.GetEdgeNode("dmit")
	if err != nil {
		t.Fatalf("get created edge node: %v", err)
	}
	if node.EdgeGroupID != "edge-group-country-us" || node.Country != "us" || node.WorkloadMode != "dynamic" {
		t.Fatalf("unexpected stored edge node: %+v", node)
	}
}

func TestNodeUpdaterEdgeCredentialDefaultsLegacyDNSNodeToStatic(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	existingNode, _, err := s.CreateEdgeNodeToken(model.EdgeNode{
		ID:           "vps-591f4447",
		EdgeGroupID:  "edge-group-country-us",
		WorkloadMode: "static",
		CanaryState:  model.EdgeCanaryStateJoined,
		CanaryWeight: 1,
		Country:      "us",
		PublicIPv4:   "15.204.94.71",
		Status:       model.EdgeHealthUnknown,
	})
	if err != nil {
		t.Fatalf("seed legacy static edge node: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "https://api.fugue.pro/v1/node-updater/desired-state", nil)
	legacyLabels := map[string]string{
		"fugue.io/location-country-code": "us",
		"fugue.io/public-ip":             "15.204.94.71",
		"fugue.io/role.dns":              "true",
		"fugue.io/role.edge":             "true",
	}
	credential, warnings, err := server.nodeUpdaterEdgeCredential(req, model.NodeUpdater{
		ClusterNodeName: "vps-591f4447",
		Labels:          legacyLabels,
	}, &model.ClusterNodePolicyStatus{
		NodeName: "vps-591f4447",
		Policy: &model.ClusterNodePolicy{
			AllowAppRuntime: true,
			AllowEdge:       true,
			AllowDNS:        true,
			DedicatedMode:   "none",
		},
		Labels: legacyLabels,
	})
	if err != nil {
		t.Fatalf("issue legacy edge credential: %v", err)
	}
	if credential == nil {
		t.Fatalf("expected edge credential, warnings=%v", warnings)
	}
	if credential.WorkloadMode != "static" {
		t.Fatalf("expected legacy edge/DNS node to default static, got %+v", credential)
	}
	if credential.Token != "" {
		t.Fatalf("expected node-updater not to rotate static edge token, got %+v", credential)
	}
	if credential.TokenPrefix != existingNode.TokenPrefix {
		t.Fatalf("expected static edge token prefix to stay %q, got %+v", existingNode.TokenPrefix, credential)
	}
	if credential.EdgeGroupID != "edge-group-country-us" || credential.Country != "us" {
		t.Fatalf("unexpected legacy credential location: %+v", credential)
	}

	policy := nodeUpdaterPolicyWithEdgeCredentialLabels(&model.ClusterNodePolicyStatus{
		NodeName: "vps-591f4447",
		Policy: &model.ClusterNodePolicy{
			AllowAppRuntime: true,
			AllowEdge:       true,
			AllowDNS:        true,
			DedicatedMode:   "none",
		},
		Labels: legacyLabels,
	}, credential)
	if policy == nil || policy.Labels["fugue.io/edge-workload"] != "static" {
		t.Fatalf("expected legacy node policy to be augmented as static, got %+v", policy)
	}
	node, _, err := s.GetEdgeNode("vps-591f4447")
	if err != nil {
		t.Fatalf("get stored legacy edge node: %v", err)
	}
	if node.WorkloadMode != "static" {
		t.Fatalf("expected stored legacy edge node to be static, got %+v", node)
	}
	if node.TokenPrefix != existingNode.TokenPrefix {
		t.Fatalf("expected stored legacy edge token to be preserved, got %+v", node)
	}
}

func TestNodeUpdaterClaimRefusesProtectedImageCacheDeleteTask(t *testing.T) {
	t.Parallel()

	stateStore, _, updaterToken, server := newImageCacheAdminAPITest(t, "Node Updater Image Cache Claim Tenant")
	updaters, err := stateStore.ListNodeUpdaters("", true)
	if err != nil {
		t.Fatalf("list updaters: %v", err)
	}
	if len(updaters) != 1 {
		t.Fatalf("expected one updater, got %+v", updaters)
	}
	updater := updaters[0]
	digest := "sha256:570d3b2870631111111111111111111111111111111111111111111111111111"
	reportImageCacheTestManifest(t, server, updaterToken, digest)
	if _, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant_1",
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo@" + digest,
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	}); err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	rawTargets, err := json.Marshal([]map[string]string{{
		"repo":   "fugue-apps/demo",
		"target": "old",
		"digest": digest,
	}})
	if err != nil {
		t.Fatalf("marshal targets: %v", err)
	}
	task, err := stateStore.CreateNodeUpdateTask(model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "test",
		TenantID:  "tenant_1",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache, map[string]string{
		"dry_run":          "false",
		"allow_delete":     "true",
		"prune_reason":     "image-cache-orphan",
		"targets_json":     string(rawTargets),
		"min_manifest_age": "24h",
	})
	if err != nil {
		t.Fatalf("create prune task: %v", err)
	}
	claim := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+task.ID+"/claim", updaterToken, nil)
	if claim.Code != http.StatusConflict {
		t.Fatalf("expected conflict, got %d body=%s", claim.Code, claim.Body.String())
	}
	if !strings.Contains(claim.Body.String(), "not present in the latest prune plan") {
		t.Fatalf("expected latest plan refusal, got %s", claim.Body.String())
	}
	failed, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusFailed)
	if err != nil {
		t.Fatalf("list failed tasks: %v", err)
	}
	if len(failed) != 1 || failed[0].ID != task.ID || !strings.Contains(failed[0].ErrorMessage, "latest prune plan") {
		t.Fatalf("expected task to be failed by claim guard, got %+v", failed)
	}
}

func TestNodeUpdaterLocalPVDecommissionCompletionWritesAudit(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Node Updater LocalPV Audit Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, updaterToken, err := s.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://worker-1.example.com",
		nil,
		"machine-1",
		"fingerprint-worker-1",
		"v10",
		"join-v10",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypeDecommissionLocalPV},
	)
	if err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	task, err := s.CreateNodeUpdateTask(model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "test",
		TenantID:  tenant.ID,
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeDecommissionLocalPV, map[string]string{
		"dry_run":                    "false",
		"allow_delete":               "true",
		"allow_localpv_decommission": "true",
		"expected_image_size_bytes":  "1024",
		"expected_lv_count":          "0",
		"expected_bound_pv_count":    "0",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := s.ClaimNodeUpdateTask(task.ID, updater.ID); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	completeForm := url.Values{}
	completeForm.Set("status", model.NodeUpdateTaskStatusCompleted)
	completeForm.Set("message", `LocalPV decommission completed {"expected_freed_bytes":1024}`)
	completeRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+task.ID+"/complete", updaterToken, completeForm)
	if completeRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, completeRecorder.Code, completeRecorder.Body.String())
	}
	events, err := s.ListAuditEvents("", true, 10)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	for _, event := range events {
		if event.Action != "localpv_decommission_completed" {
			continue
		}
		if event.TargetID != task.ID || event.Metadata["allow_delete"] != "true" || event.Metadata["expected_lv_count"] != "0" {
			t.Fatalf("unexpected audit event: %+v", event)
		}
		return
	}
	t.Fatalf("expected localpv decommission audit event, got %+v", events)
}

func TestNodeRepairTaskGuardsUnsafeExecutionAndWritesAudit(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Node Repair Guard Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, updaterToken, err := s.EnrollNodeUpdater(
		nodeSecret,
		"repair-node-1",
		"https://repair-node-1.example.com",
		nil,
		"machine-repair-1",
		"fingerprint-repair-1",
		"v20",
		"join-v20",
		[]string{
			"heartbeat",
			"tasks",
			model.NodeUpdateTaskTypeRepairManagedIPTables,
			model.NodeUpdateTaskTypeRestartStatelessNodeService,
		},
	)
	if err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	requester := model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "repair-guardian",
		TenantID:  tenant.ID,
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}

	unsafeRepair, err := s.CreateNodeUpdateTask(requester, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeRepairManagedIPTables, map[string]string{
		"dry_run":      "false",
		"allow_delete": "false",
	})
	if err != nil {
		t.Fatalf("create unsafe repair task: %v", err)
	}
	claimUnsafe := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+unsafeRepair.ID+"/claim", updaterToken, nil)
	if claimUnsafe.Code != http.StatusConflict || !strings.Contains(claimUnsafe.Body.String(), "allow_delete=true") {
		t.Fatalf("expected unsafe repair refusal, code=%d body=%s", claimUnsafe.Code, claimUnsafe.Body.String())
	}

	unsafeRestart, err := s.CreateNodeUpdateTask(requester, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeRestartStatelessNodeService, map[string]string{
		"service": "postgresql.service",
	})
	if err != nil {
		t.Fatalf("create unsafe restart task: %v", err)
	}
	claimRestart := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+unsafeRestart.ID+"/claim", updaterToken, nil)
	if claimRestart.Code != http.StatusConflict || !strings.Contains(claimRestart.Body.String(), "allowlist") {
		t.Fatalf("expected unsafe restart refusal, code=%d body=%s", claimRestart.Code, claimRestart.Body.String())
	}

	safePayload := map[string]string{
		"dry_run":       "true",
		"repair_id":     "repair-dns-dnat",
		"repair_action": "stale-managed-dnat-dry-run",
		"safety_class":  model.NodeRepairSafetyDryRun,
	}
	safeRepair, err := s.CreateNodeUpdateTask(requester, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeRepairManagedIPTables, safePayload)
	if err != nil {
		t.Fatalf("create safe repair task: %v", err)
	}
	duplicate, err := s.CreateNodeUpdateTask(requester, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeRepairManagedIPTables, safePayload)
	if err != nil {
		t.Fatalf("create duplicate repair task: %v", err)
	}
	if duplicate.ID != safeRepair.ID {
		t.Fatalf("expected duplicate pending repair task to reuse lock/lease task id, first=%s duplicate=%s", safeRepair.ID, duplicate.ID)
	}
	claimSafe := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+safeRepair.ID+"/claim", updaterToken, nil)
	if claimSafe.Code != http.StatusOK {
		t.Fatalf("expected safe repair claim status %d, got %d body=%s", http.StatusOK, claimSafe.Code, claimSafe.Body.String())
	}
	completeForm := url.Values{}
	completeForm.Set("status", model.NodeUpdateTaskStatusCompleted)
	completeForm.Set("message", "dry-run managed iptables repair completed")
	completeRecorder := performFormRequest(t, server, http.MethodPost, "/v1/node-updater/tasks/"+safeRepair.ID+"/complete", updaterToken, completeForm)
	if completeRecorder.Code != http.StatusOK {
		t.Fatalf("expected safe repair completion status %d, got %d body=%s", http.StatusOK, completeRecorder.Code, completeRecorder.Body.String())
	}
	events, err := s.ListAuditEvents("", true, 20)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	for _, event := range events {
		if event.Action != "node_repair_dry_run_completed" {
			continue
		}
		if event.TargetID != safeRepair.ID || event.Metadata["repair_id"] != "repair-dns-dnat" || event.Metadata["dry_run"] != "true" {
			t.Fatalf("unexpected repair audit event: %+v", event)
		}
		return
	}
	t.Fatalf("expected node repair dry-run audit event, got %+v", events)
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
		`FUGUE_NODE_UPDATER_SCRIPT_VERSION="v20"`,
		`FUGUE_NODE_UPDATER_CAPABILITIES=`,
		`FUGUE_NODE_GUARDIAN_AUTONOMY_WAL_PATH=`,
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
		`FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_ENABLED="${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_ENABLED:-false}"`,
		`disable_node_dns_escape_hatch`,
		`disabled local DNS escape hatch so pod DNS uses Kubernetes CoreDNS`,
		`discovery_generation=`,
		`import os`,
		`image-cache inventory produced no chunks`,
		`image-cache inventory chunk list count ${chunk_file_count} did not match expected ${expected_chunks}`,
		`--data-binary @"${chunk_file}"`,
		`image-cache inventory POST failed for chunk ${next_chunk_number}/${expected_chunks}`,
		`image-cache inventory posted ${posted_chunks} chunks, expected ${expected_chunks}`,
		`raw_unreferenced_blobs = inventory.get("unreferenced_blobs") or []`,
		`base["unreferenced_blobs"] = unreferenced_blobs`,
		`"unreferenced_blob_count": unreferenced_blob_count`,
		`"unreferenced_blob_bytes": unreferenced_blob_bytes`,
		`"planned_delete_bytes"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\)`,
		`image-cache prune delete completed; reporting post-prune inventory`,
		`repair-managed-iptables`,
		`refresh-desired-state`,
		`reload-lkg-bundle`,
		`restart-stateless-node-service`,
		`run-deep-health`,
		`record_node_guardian_wal "deep_health_heartbeat"`,
		`write_file_hash_sidecar`,
		`verify_file_hash_sidecar`,
		`cached env hash verification failed`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected node-updater script to contain %q", want)
		}
	}
	if strings.Contains(script, `api_json POST /v1/node-updater/image-cache/inventory "$(cat "${chunk_file}")"`) {
		t.Fatalf("node-updater script must not pass image-cache inventory chunks through argv")
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

func TestNodeUpdaterCachedLKGEnvRejectsHashCorruption(t *testing.T) {
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
source_file="${tmpdir}/source.env"
target_file="${tmpdir}/cached.env"
printf 'FUGUE_TEST_LKG_VALUE=good\n' >"${source_file}"
write_file_if_changed "${source_file}" "${target_file}" >/dev/null
if ! load_cached_env_file "${target_file}"; then
  echo "expected hash-verified cache load to pass" >&2
  exit 1
fi
if [ "${FUGUE_TEST_LKG_VALUE:-}" != "good" ]; then
  echo "expected good cached value" >&2
  exit 1
fi
printf 'FUGUE_TEST_LKG_VALUE=corrupt\n' >"${target_file}"
unset FUGUE_TEST_LKG_VALUE
if load_cached_env_file "${target_file}"; then
  echo "corrupt cached env should not load" >&2
  exit 1
fi
if [ -n "${FUGUE_TEST_LKG_VALUE:-}" ]; then
  echo "corrupt cached env leaked variables" >&2
  exit 1
fi
`

	scriptPath := filepath.Join(t.TempDir(), "node-updater-lkg-cache-corruption.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node-updater harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run node-updater LKG corruption harness: %v\n%s", err, output)
	}
}

func TestNodeUpdaterEdgeEnvGenerationUsesSecretPrefix(t *testing.T) {
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
FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE="${tmpdir}/edge-node.env"
cat >"${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}" <<'EOF_EDGE_NODE_ENV'
FUGUE_EDGE_NODE_TOKEN='fugue_edge_abcd1234_0123456789abcdef0123456789abcdef'
EOF_EDGE_NODE_ENV

got="$(current_edge_node_token_prefix)"
if [ "${got}" != "abcd1234" ]; then
  printf 'expected secret prefix abcd1234, got %s\n' "${got}" >&2
  exit 1
fi
`

	scriptPath := filepath.Join(t.TempDir(), "node-updater-prefix.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node-updater harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run node-updater prefix harness: %v\n%s", err, output)
	}
}

func TestNodeUpdaterDisablesDNSEscapeHatchByDefault(t *testing.T) {
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
actions="${tmpdir}/actions.log"
iptables_rules="${tmpdir}/iptables.rules"
FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_CONFIG_FILE="${tmpdir}/fugue-node-dns-escape-hatch.conf"
FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE="fugue-node-dns-escape-hatch.service"
FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_TIMER="fugue-node-dns-escape-hatch.timer"
: >"${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_CONFIG_FILE}"
cat >"${iptables_rules}" <<'EOF_IPTABLES_RULES'
-A KUBE-SERVICES -d 10.43.0.10/32 -p udp -m comment --comment "kube-system/kube-dns:dns cluster IP" -m udp --dport 53 -j KUBE-SVC-TCOU7JCQXEZGVUNU
-A PREROUTING -d 10.43.0.10/32 -i cni0 -p udp --dport 53 -j DNAT --to-destination 10.42.8.1:53
-A PREROUTING -d 10.43.0.10/32 -i cni0 -p tcp --dport 53 -j DNAT --to-destination 10.42.8.1:53
-A OUTPUT -d 10.43.0.10/32 -p udp --dport 53 -j DNAT --to-destination 10.42.8.1:53
-A OUTPUT -d 10.43.0.10/32 -p tcp --dport 53 -j DNAT --to-destination 10.42.8.1:53
-A PREROUTING -i cni0 -d 10.43.0.10/32 -p udp --dport 53 -j DNAT --to-destination 10.42.7.1:53
-A PREROUTING -i cni0 -d 10.43.0.10/32 -p tcp --dport 53 -j DNAT --to-destination 10.42.7.1:53
-A OUTPUT -d 10.43.0.10/32 -p udp --dport 53 -j DNAT --to-destination 10.42.7.1:53
-A OUTPUT -d 10.43.0.10/32 -p tcp --dport 53 -j DNAT --to-destination 10.42.7.1:53
EOF_IPTABLES_RULES

systemctl() {
  printf 'systemctl %s\n' "$*" >>"${actions}"
  case "$1" in
    is-active|is-enabled|list-unit-files)
      return 0
      ;;
    disable)
      return 0
      ;;
  esac
  return 0
}

ip() {
  if [ "$1" = "-4" ]; then
    printf '3: cni0: <BROADCAST> mtu 1450\n    inet 10.42.7.1/24 scope global cni0\n'
    return 0
  fi
  return 1
}

iptables_save_called=0
iptables-save() {
  iptables_save_called=$((iptables_save_called + 1))
  cat "${iptables_rules}"
}

iptables() {
  printf 'iptables %s\n' "$*" >>"${actions}"
  if [ "${1:-}" = "-t" ]; then
    shift 2
  fi
  local op="${1:-}"
  local chain="${2:-}"
  shift 2 || true
  local rule="-A ${chain}"
  local arg=""
  for arg in "$@"; do
    rule="${rule} ${arg}"
  done
  case "${op}" in
    -C)
      grep -Fxq -- "${rule}" "${iptables_rules}"
      return $?
      ;;
    -D)
      if ! grep -Fxq -- "${rule}" "${iptables_rules}"; then
        return 1
      fi
      awk -v target="${rule}" '
        $0 == target && !removed {
          removed = 1
          next
        }
        { print }
      ' "${iptables_rules}" >"${iptables_rules}.tmp"
      mv "${iptables_rules}.tmp" "${iptables_rules}"
      return 0
      ;;
  esac
  return 1
}

dnsmasq() {
  echo "dnsmasq must not run when escape hatch is disabled" >&2
  return 1
}

if ! reconcile_node_dns_escape_hatch; then
  echo "expected DNS escape hatch reconciliation to report a change" >&2
  cat "${actions}" >&2
  exit 1
fi
grep -q 'systemctl disable --now fugue-node-dns-escape-hatch.timer' "${actions}"
grep -q 'systemctl disable --now fugue-node-dns-escape-hatch.service' "${actions}"
grep -q 'iptables -t nat -D PREROUTING -d 10.43.0.10/32 -i cni0 -p udp --dport 53 -j DNAT --to-destination 10.42.8.1:53' "${actions}"
grep -q 'iptables -t nat -D OUTPUT -d 10.43.0.10/32 -p tcp --dport 53 -j DNAT --to-destination 10.42.8.1:53' "${actions}"
grep -q 'iptables -t nat -D PREROUTING -i cni0 -d 10.43.0.10/32 -p udp --dport 53 -j DNAT --to-destination 10.42.7.1:53' "${actions}"
if grep -q '10.42.8.1:53' "${iptables_rules}"; then
  echo "stale DNS escape hatch redirect rules were not removed" >&2
  cat "${iptables_rules}" >&2
  cat "${actions}" >&2
  exit 1
fi
if grep -q 'dnsmasq.service' "${actions}"; then
  echo "dnsmasq should not be restarted while disabling the escape hatch" >&2
  cat "${actions}" >&2
  exit 1
fi
`
	scriptPath := filepath.Join(t.TempDir(), "node-updater-disable-dns-escape-hatch.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node-updater DNS escape hatch harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node updater DNS escape hatch harness failed: %v\n%s", err, output)
	}
}

func TestPrepareLocalPVNodeRolePolicyDryRun(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	scriptPath := filepath.Join("..", "..", "scripts", "prepare_fugue_lvm_localpv_node.sh")
	for _, tt := range []struct {
		name     string
		args     []string
		wantOK   bool
		wantText string
		envRoles string
	}{
		{
			name:     "storage agent allowed",
			args:     []string{"--size-gib", "1", "--node-role", "storage-agent", "--dry-run"},
			wantOK:   true,
			wantText: "LocalPV preallocation dry-run",
		},
		{
			name:     "edge refused",
			args:     []string{"--size-gib", "1", "--node-role", "edge", "--dry-run"},
			wantText: "disabled for edge, DNS, and control-plane-only roles",
		},
		{
			name:     "control plane refused",
			args:     []string{"--size-gib", "1", "--node-role", "control-plane-only", "--dry-run"},
			wantText: "disabled for edge, DNS, and control-plane-only roles",
		},
		{
			name:     "env dns refused",
			args:     []string{"--size-gib", "1", "--dry-run"},
			envRoles: "dns",
			wantText: "disabled for edge, DNS, and control-plane-only roles",
		},
		{
			name:     "explicit maintenance override allowed",
			args:     []string{"--size-gib", "1", "--node-role", "edge", "--allow-localpv", "--dry-run"},
			wantOK:   true,
			wantText: "allow_localpv=true",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command("bash", append([]string{scriptPath}, tt.args...)...)
			if tt.envRoles != "" {
				cmd.Env = append(os.Environ(), "FUGUE_NODE_ROLES="+tt.envRoles)
			}
			output, err := cmd.CombinedOutput()
			if tt.wantOK && err != nil {
				t.Fatalf("expected success, got %v\n%s", err, output)
			}
			if !tt.wantOK && err == nil {
				t.Fatalf("expected failure, got success\n%s", output)
			}
			if !strings.Contains(string(output), tt.wantText) {
				t.Fatalf("expected output to contain %q, got\n%s", tt.wantText, output)
			}
		})
	}
}

func TestNodeUpdaterPrepullAppImagesSkipsMissingManifestRefs(t *testing.T) {
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
reports="${tmpdir}/reports"
logs="${tmpdir}/logs"
log_task() {
  printf '%s\n' "$*" >>"${logs}"
}
report_image_location() {
  printf '%s\t%s\n' "$1" "$2" >>"${reports}"
}
verify_image_cache_manifest() {
  return 0
}
pull_container_image() {
  case "$1" in
    *missing*)
      printf 'rpc error: code = NotFound desc = failed to resolve reference "registry.example/app:missing": registry.example/app:missing: not found'
      return 1
      ;;
    *retryable*)
      printf 'rpc error: code = Unavailable desc = connection refused'
      return 1
      ;;
  esac
  return 0
}

FUGUE_NODE_UPDATE_TASK_IMAGES="registry.example/app:present,registry.example/app:missing"
if ! prepull_app_images; then
  echo "missing manifest should not fail non-blocking pre-pull"
  cat "${logs}" || true
  exit 1
fi
grep -q $'registry.example/app:present\tpresent' "${reports}"
grep -q $'registry.example/app:missing\tmissing' "${reports}"
grep -q 'skipping stale app image registry.example/app:missing' "${logs}"

: >"${reports}"
: >"${logs}"
FUGUE_NODE_UPDATE_TASK_IMAGES="registry.example/app:retryable"
if prepull_app_images; then
  echo "retryable pull failure should still fail"
  exit 1
fi
grep -q $'registry.example/app:retryable\tfailed' "${reports}"
`
	scriptPath := filepath.Join(t.TempDir(), "node-updater-prepull-missing-test.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node-updater prepull harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node updater prepull harness failed: %v\n%s", err, output)
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
FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE="${tmpdir}/edge-node.env"
FUGUE_DISCOVERY_K3S_SERVER="https://cp.example:6443"
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
        "fugue.io/tenant-id": "tenant_edge"
      }
    },
    "edge_credential": {
      "edge_id": "edge-1",
      "edge_group_id": "edge-group-country-us",
      "workload_mode": "dynamic",
      "country": "us",
      "region": "north-america",
      "public_ipv4": "203.0.113.10",
      "token": "fugue_edge_test_secret",
      "desired_state_url": "https://api.fugue.pro/v1/edge/nodes/edge-1/desired-state"
      }
  }
}
JSON
if ! reconcile_k3s_config; then
  echo "first reconcile should report a write"
  exit 1
fi
if ! reconcile_edge_node_env; then
  echo "edge credential reconcile should report a write"
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
grep -q 'fugue.io/edge-workload=dynamic' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/edge-group-id=edge-group-country-us' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/edge-location-status=ready' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/dedicated=edge:NoSchedule' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/tenant=tenant_edge:NoSchedule' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q "FUGUE_EDGE_NODE_ID=edge-1" "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}"
grep -q "FUGUE_EDGE_GROUP_ID=edge-group-country-us" "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}"
grep -q "FUGUE_EDGE_NODE_TOKEN=fugue_edge_test_secret" "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}"
grep -q "FUGUE_EDGE_DESIRED_STATE_URL=https://api.fugue.pro/v1/edge/nodes/edge-1/desired-state" "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}"
if [ "$(stat -c '%a' "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}" 2>/dev/null || stat -f '%Lp' "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}")" != "600" ]; then
  echo "edge node env file is not 0600"
  ls -l "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}"
  exit 1
fi
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

func TestNodeUpdaterK3sConfigKeepsLegacyDNSEdgeStatic(t *testing.T) {
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
cat >"${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" <<'YAML'
server: "https://cp.example:6443"
node-external-ip: "15.204.94.71"
node-label:
  - "fugue.io/role.edge=true"
  - "fugue.io/role.dns=true"
  - "fugue.io/location-country-code=us"
  - "fugue.io/public-ip=15.204.94.71"
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
      "node_name": "vps-591f4447",
      "runtime_id": "runtime_edge",
      "tenant_id": "tenant_edge",
      "machine_id": "machine_edge",
      "policy": {
        "allow_app_runtime": true,
        "allow_builds": false,
        "allow_shared_pool": false,
        "allow_edge": true,
        "allow_dns": true,
        "allow_internal_maintenance": false,
        "dedicated_mode": "none",
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
        "fugue.io/role.dns": "true",
        "fugue.io/runtime-id": "runtime_edge",
        "fugue.io/tenant-id": "tenant_edge",
        "fugue.io/location-country-code": "us",
        "fugue.io/public-ip": "15.204.94.71"
      }
    },
    "edge_credential": {
      "edge_id": "vps-591f4447",
      "edge_group_id": "edge-group-country-us",
      "workload_mode": "dynamic",
      "country": "us",
      "public_ipv4": "15.204.94.71",
      "token": "fugue_edge_test_secret",
      "desired_state_url": "https://api.fugue.pro/v1/edge/nodes/vps-591f4447/desired-state"
    }
  }
}
JSON
if ! reconcile_k3s_config; then
  echo "legacy DNS edge reconcile should report a write"
  exit 1
fi
grep -q 'fugue.io/role.edge=true' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/role.dns=true' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q 'fugue.io/edge-workload=static' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
if grep -q 'fugue.io/edge-workload=dynamic' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"; then
  echo "legacy DNS edge was incorrectly rendered as dynamic"
  cat "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
  exit 1
fi
`
	scriptPath := filepath.Join(t.TempDir(), "node-updater-legacy-dns-edge-static-test.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node updater legacy DNS edge harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node updater legacy DNS edge harness failed: %v\n%s", err, output)
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
