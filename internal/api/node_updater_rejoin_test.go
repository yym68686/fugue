package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestValidKubernetesBootstrapTokenComponent(t *testing.T) {
	for _, test := range []struct {
		value  string
		length int
		valid  bool
	}{
		{value: "abc123", length: 6, valid: true},
		{value: "0123456789abcdef", length: 16, valid: true},
		{value: "ABC123", length: 6, valid: false},
		{value: "abc12-", length: 6, valid: false},
		{value: "abc12", length: 6, valid: false},
	} {
		if got := validKubernetesBootstrapTokenComponent(test.value, test.length); got != test.valid {
			t.Fatalf("validKubernetesBootstrapTokenComponent(%q, %d) = %t, want %t", test.value, test.length, got, test.valid)
		}
	}
}

func TestNodeUpdaterClusterRejoinIssuesReusesAndRevokesBoundedCredential(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, nodeKeySecret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform node key: %v", err)
	}
	updater, updaterToken, err := stateStore.EnrollNodeUpdater(
		nodeKeySecret,
		"worker-rejoin",
		"198.51.100.10",
		map[string]string{"fugue.io/public-ip": "198.51.100.10"},
		"worker-rejoin",
		"machine-fingerprint",
		model.NodeUpdaterCurrentVersion,
		"join-v1",
		[]string{"heartbeat", model.NodeUpdaterCapabilityRejoinK3SNode, model.NodeUpdaterCapabilitySafeK3SNodeRejoin},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}
	_, principal, err := stateStore.AuthenticateNodeUpdater(updaterToken)
	if err != nil {
		t.Fatalf("authenticate node updater: %v", err)
	}

	var mu sync.Mutex
	nodePresent := false
	postCount := 0
	deleteCount := 0
	var bootstrapSecret map[string]any
	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/worker-rejoin":
			if !nodePresent {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "worker-rejoin"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/kube-system/secrets":
			items := []any{}
			if bootstrapSecret != nil {
				items = append(items, bootstrapSecret)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/kube-system/secrets":
			var payload struct {
				Metadata   map[string]any    `json:"metadata"`
				StringData map[string]string `json:"stringData"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode bootstrap Secret: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			data := map[string]string{}
			for key, value := range payload.StringData {
				data[key] = base64.StdEncoding.EncodeToString([]byte(value))
			}
			bootstrapSecret = map[string]any{
				"metadata": payload.Metadata,
				"data":     data,
			}
			postCount++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/kube-system/secrets/bootstrap-token-"):
			bootstrapSecret = nil
			deleteCount++
			_, _ = w.Write([]byte(`{}`))
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		RegistryPullBase:             "registry.example.internal:5000",
		ClusterJoinRegistryEndpoint:  "https://registry.example.internal:5000",
		ClusterJoinServer:            "https://control-plane.example:6443",
		ClusterJoinCAHash:            strings.Repeat("a", 64),
		ClusterJoinBootstrapTokenTTL: 15 * time.Minute,
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	first, warnings := server.nodeUpdaterClusterRejoin(context.Background(), principal, updater)
	if len(warnings) != 0 {
		t.Fatalf("unexpected first rejoin warnings: %v", warnings)
	}
	if first.Status != model.NodeUpdaterClusterRejoinStatusCredentialReady ||
		first.Reason != "kubernetes_node_not_found" ||
		first.Credential == nil ||
		first.Credential.Class != nodeUpdaterRejoinCredentialClass ||
		!strings.HasPrefix(first.Credential.Token, "K10"+strings.Repeat("a", 64)+"::") {
		t.Fatalf("unexpected first rejoin plan: %+v", first)
	}
	if postCount != 1 {
		t.Fatalf("bootstrap Secret posts = %d, want 1", postCount)
	}
	if first.Credential.ExpiresAt.Nanosecond() != 0 {
		t.Fatalf("first credential expiry has sub-second precision: %s", first.Credential.ExpiresAt.Format(time.RFC3339Nano))
	}
	secretData, ok := bootstrapSecret["data"].(map[string]string)
	if !ok {
		t.Fatalf("bootstrap Secret data has unexpected shape: %#v", bootstrapSecret["data"])
	}
	encodedExpiration := secretData["expiration"]
	persistedExpiration, err := base64.StdEncoding.DecodeString(encodedExpiration)
	if err != nil {
		t.Fatalf("decode persisted expiration: %v", err)
	}
	if got, want := string(persistedExpiration), first.Credential.ExpiresAt.Format(time.RFC3339); got != want {
		t.Fatalf("persisted expiration = %q, credential expiration = %q", got, want)
	}

	second, warnings := server.nodeUpdaterClusterRejoin(context.Background(), principal, updater)
	if len(warnings) != 0 {
		t.Fatalf("unexpected reused rejoin warnings: %v", warnings)
	}
	if second.Credential == nil || second.Credential.Token != first.Credential.Token || postCount != 1 {
		t.Fatalf("expected the active bounded credential to be reused: first=%+v second=%+v posts=%d", first, second, postCount)
	}

	mu.Lock()
	nodePresent = true
	mu.Unlock()
	third, warnings := server.nodeUpdaterClusterRejoin(context.Background(), principal, updater)
	if len(warnings) != 0 {
		t.Fatalf("unexpected cleanup warnings: %v", warnings)
	}
	if third.Status != model.NodeUpdaterClusterRejoinStatusNotRequired || third.Reason != "node_present" {
		t.Fatalf("unexpected node-present plan: %+v", third)
	}
	if deleteCount != 1 {
		t.Fatalf("bootstrap Secret deletes = %d, want 1", deleteCount)
	}

	events, err := stateStore.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	actionCounts := map[string]int{}
	for _, event := range events {
		actionCounts[event.Action]++
		for _, value := range event.Metadata {
			if strings.Contains(value, first.Credential.Token) {
				t.Fatalf("audit event leaked bootstrap token: %+v", event)
			}
		}
	}
	if actionCounts["node_updater.cluster_rejoin.credential_issued"] != 1 {
		t.Fatalf("credential issuance audit count = %d, want 1", actionCounts["node_updater.cluster_rejoin.credential_issued"])
	}
	if actionCounts["node_updater.cluster_rejoin.credentials_revoked"] != 1 {
		t.Fatalf("credential revocation audit count = %d, want 1", actionCounts["node_updater.cluster_rejoin.credentials_revoked"])
	}
}

func TestNodeUpdaterClusterRejoinSuppressesBindingMismatchWithoutKubernetesMutation(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, nodeKeySecret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform node key: %v", err)
	}
	updater, updaterToken, err := stateStore.EnrollNodeUpdater(
		nodeKeySecret,
		"worker-mismatch",
		"198.51.100.11",
		nil,
		"worker-mismatch",
		"machine-fingerprint",
		model.NodeUpdaterCurrentVersion,
		"join-v1",
		[]string{model.NodeUpdaterCapabilityRejoinK3SNode, model.NodeUpdaterCapabilitySafeK3SNodeRejoin},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}
	_, principal, err := stateStore.AuthenticateNodeUpdater(updaterToken)
	if err != nil {
		t.Fatalf("authenticate node updater: %v", err)
	}
	updater.MachineID = "different-machine"

	kubernetesClientCalls := 0
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		RegistryPullBase:             "registry.example.internal:5000",
		ClusterJoinRegistryEndpoint:  "https://registry.example.internal:5000",
		ClusterJoinServer:            "https://control-plane.example:6443",
		ClusterJoinBootstrapTokenTTL: 15 * time.Minute,
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		kubernetesClientCalls++
		return nil, nil
	}

	plan, _ := server.nodeUpdaterClusterRejoin(context.Background(), principal, updater)
	if plan.Status != model.NodeUpdaterClusterRejoinStatusSuppressed || plan.Reason != "machine_binding_mismatch" {
		t.Fatalf("unexpected suppressed plan: %+v", plan)
	}
	if kubernetesClientCalls != 0 {
		t.Fatalf("Kubernetes client calls = %d, want 0 for a mismatched binding", kubernetesClientCalls)
	}
}

func TestNodeUpdaterClusterRejoinSuppressesLegacyUpdaterBeforeKubernetesAccess(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, nodeKeySecret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform node key: %v", err)
	}
	updater, updaterToken, err := stateStore.EnrollNodeUpdater(
		nodeKeySecret,
		"worker-legacy-rejoin",
		"198.51.100.12",
		nil,
		"worker-legacy-rejoin",
		"machine-fingerprint",
		"v28",
		"join-v1",
		[]string{model.NodeUpdaterCapabilityRejoinK3SNode},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}
	_, principal, err := stateStore.AuthenticateNodeUpdater(updaterToken)
	if err != nil {
		t.Fatalf("authenticate node updater: %v", err)
	}

	kubernetesClientCalls := 0
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		ClusterJoinServer:            "https://control-plane.example:6443",
		ClusterJoinBootstrapTokenTTL: 15 * time.Minute,
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		kubernetesClientCalls++
		return nil, nil
	}

	plan, _ := server.nodeUpdaterClusterRejoin(context.Background(), principal, updater)
	if plan.Status != model.NodeUpdaterClusterRejoinStatusSuppressed || plan.Reason != "safe_rejoin_capability_missing" {
		t.Fatalf("unexpected legacy updater plan: %+v", plan)
	}
	if kubernetesClientCalls != 0 {
		t.Fatalf("Kubernetes client calls = %d, want 0 for a legacy updater", kubernetesClientCalls)
	}
}

func TestNodeUpdaterScriptAppliesRejoinCredentialOnceWithoutLeakingMetadata(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not available")
	}

	var server Server
	script := server.nodeUpdaterInstallScript("https://api.fugue.pro")
	prefix, _, ok := strings.Cut(script, "\ncase \"${1:-run-once}\" in")
	if !ok {
		t.Fatalf("node updater script missing command dispatch")
	}

	token := "abcdef.0123456789abcdef"
	expiresAt := time.Now().UTC().Truncate(time.Second).Add(10*time.Minute + 123456789*time.Nanosecond).Format(time.RFC3339Nano)
	harness := prefix + `
tmpdir="$(mktemp -d)"
FUGUE_NODE_UPDATER_STATE_DIR="${tmpdir}"
FUGUE_NODE_UPDATER_DESIRED_STATE_FILE="${tmpdir}/desired-state.json"
FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE="${tmpdir}/cluster-rejoin.env"
FUGUE_NODE_UPDATER_K3S_CONFIG_FILE="${tmpdir}/config.yaml"
FUGUE_NODE_UPDATER_K3S_CLIENT_KUBELET_CERT_FILE="${tmpdir}/client-kubelet.crt"
FUGUE_NODE_GUARDIAN_AUTONOMY_WAL_PATH="${tmpdir}/autonomy.wal"
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${tmpdir}/client-kubelet.key" \
  -out "${FUGUE_NODE_UPDATER_K3S_CLIENT_KUBELET_CERT_FILE}" \
  -days 1 \
  -subj '/O=system:nodes/CN=system:node:worker-rejoin' >/dev/null 2>&1
cat >"${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" <<'YAML'
server: "https://control-plane.example:6443"
token: "aaaaaa.aaaaaaaaaaaaaaaa"
YAML
printf '%s\n' 'sha256:stale' >"${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}.sha256"
cat >"${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}" <<'JSON'
{
  "desired_state": {
    "node_updater": {"cluster_node_name": "worker-rejoin"},
    "cluster_rejoin": {
      "status": "credential_ready",
      "reason": "kubernetes_node_not_found",
      "node_name": "worker-rejoin",
      "observed_at": "2026-07-27T00:00:00Z",
      "credential": {
        "class": "short_lived_kubernetes_bootstrap_token",
        "token": "` + token + `",
        "token_id": "abcdef",
        "generation": "bootstrap-token/abcdef",
        "expires_at": "` + expiresAt + `"
      }
    }
  }
}
JSON
if ! reconcile_cluster_rejoin_credential; then
  echo "first cluster rejoin credential reconcile did not report a change" >&2
  exit 1
fi
grep -q '^token: "abcdef.0123456789abcdef"$' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
grep -q '^FUGUE_K3S_REJOIN_GENERATION=bootstrap-token/abcdef$' "${FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE}"
grep -q '^FUGUE_K3S_REJOIN_CLIENT_IDENTITY_STATE=quarantined$' "${FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE}"
grep -q '^FUGUE_K3S_REJOIN_AUTH_FALLBACK_MODE=bootstrap_without_stale_client_certificate$' "${FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE}"
if [ -e "${FUGUE_NODE_UPDATER_K3S_CLIENT_KUBELET_CERT_FILE}" ]; then
  echo "stale kubelet client certificate was not quarantined" >&2
  exit 1
fi
if [ ! -e "${tmpdir}/client-kubelet.key" ]; then
  echo "kubelet client private key was removed instead of retained for the CSR" >&2
  exit 1
fi
quarantine_count="$(find "${tmpdir}" -maxdepth 1 -type f -name 'client-kubelet.crt.fugue-rejoin-abcdef*' | wc -l | tr -d ' ')"
if [ "${quarantine_count}" != "1" ]; then
  echo "expected one quarantined client certificate, found ${quarantine_count}" >&2
  exit 1
fi
if grep -q '0123456789abcdef' "${FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE}"; then
  echo "cluster rejoin metadata leaked the credential secret" >&2
  exit 1
fi
config_mode="$(stat -c '%a' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" 2>/dev/null || stat -f '%Lp' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}")"
metadata_mode="$(stat -c '%a' "${FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE}" 2>/dev/null || stat -f '%Lp' "${FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE}")"
rollback_mode="$(stat -c '%a' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}.rollback" 2>/dev/null || stat -f '%Lp' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}.rollback")"
if [ "${config_mode}" != "600" ] || [ "${metadata_mode}" != "600" ] || [ "${rollback_mode}" != "600" ]; then
  echo "credential files are not mode 0600: config=${config_mode} metadata=${metadata_mode} rollback=${rollback_mode}" >&2
  exit 1
fi
if [ -e "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}.sha256" ]; then
  echo "stale non-secret hash sidecar survived a secret config write" >&2
  exit 1
fi
if reconcile_cluster_rejoin_credential; then
  echo "identical cluster rejoin credential triggered a second config change" >&2
  exit 1
fi
grep -q '^FUGUE_K3S_REJOIN_CLIENT_IDENTITY_STATE=quarantined$' "${FUGUE_NODE_UPDATER_REJOIN_METADATA_FILE}"
cat >"${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}" <<'JSON'
{
  "desired_state": {
    "node_updater": {"cluster_node_name": "worker-rejoin"},
    "cluster_rejoin": {
      "status": "credential_ready",
      "reason": "kubernetes_node_not_found",
      "node_name": "worker-rejoin",
      "observed_at": "2026-07-27T00:00:00Z",
      "credential": {
        "class": "short_lived_kubernetes_bootstrap_token",
        "token": "fedcba.fedcba9876543210",
        "token_id": "fedcba",
        "generation": "bootstrap-token/fedcba",
        "expires_at": "2020-01-01T00:00:00Z"
      }
    }
  }
}
JSON
if reconcile_cluster_rejoin_credential; then
  echo "expired cluster rejoin credential was accepted" >&2
  exit 1
fi
grep -q '^token: "abcdef.0123456789abcdef"$' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
rm -f "${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}"
if reconcile_cluster_rejoin_credential 2>"${tmpdir}/missing-state.err"; then
  echo "missing desired state was accepted" >&2
  exit 1
fi
if grep -q 'missing.*]' "${tmpdir}/missing-state.err"; then
  echo "missing desired-state guard has malformed bracket syntax" >&2
  exit 1
fi
`
	scriptPath := filepath.Join(t.TempDir(), "node-updater-rejoin-test.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node updater rejoin harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node updater rejoin harness failed: %v\n%s", err, output)
	}
}

func TestNodeUpdaterClusterRejoinRestartUsesNonBlockingSystemdRequest(t *testing.T) {
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
mkdir -p "${tmpdir}/bin"
export FUGUE_TEST_SYSTEMCTL_LOG="${tmpdir}/systemctl.log"
export FUGUE_NODE_UPDATER_CLUSTER_NODE_NAME="worker-rejoin"
export FUGUE_NODE_GUARDIAN_AUTONOMY_WAL_PATH="${tmpdir}/autonomy.wal"
cat >"${tmpdir}/bin/systemctl" <<'SYSTEMCTL'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${FUGUE_TEST_SYSTEMCTL_LOG}"
case "$*" in
  "list-unit-files k3s-agent.service"|"--no-block restart k3s-agent") exit 0 ;;
  *) exit 91 ;;
esac
SYSTEMCTL
chmod 0755 "${tmpdir}/bin/systemctl"
PATH="${tmpdir}/bin:${PATH}"
if ! request_k3s_agent_restart_for_cluster_rejoin; then
  echo "non-blocking rejoin restart request failed" >&2
  exit 1
fi
grep -q '^list-unit-files k3s-agent.service$' "${FUGUE_TEST_SYSTEMCTL_LOG}"
grep -q '^--no-block restart k3s-agent$' "${FUGUE_TEST_SYSTEMCTL_LOG}"
grep -q '"action":"cluster_rejoin_restart_requested"' "${FUGUE_NODE_GUARDIAN_AUTONOMY_WAL_PATH}"
`
	scriptPath := filepath.Join(t.TempDir(), "node-updater-rejoin-restart-test.sh")
	if err := os.WriteFile(scriptPath, []byte(harness), 0o700); err != nil {
		t.Fatalf("write node updater restart harness: %v", err)
	}
	cmd := exec.Command("bash", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("node updater restart harness failed: %v\n%s", err, output)
	}
}

func TestClusterNodeDeletionAuditRecordsExactActorReasonAndOutcome(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/nodes/worker-delete" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	principal := model.Principal{ActorType: model.ActorTypeAPIKey, ActorID: "operator-key"}
	outcome, err := server.deleteClusterNodeWithAudit(
		context.Background(),
		&clusterNodeClient{client: kubeServer.Client(), baseURL: kubeServer.URL, bearerToken: "test-token"},
		principal,
		"worker-delete",
		"tenant-1",
		"test_explicit_deletion",
		map[string]string{"runtime_id": "runtime-1"},
	)
	if err != nil {
		t.Fatalf("delete cluster node: %v", err)
	}
	if outcome != clusterNodeDeleteOutcomeDeleted {
		t.Fatalf("deletion outcome = %q", outcome)
	}
	events, err := stateStore.ListAuditEvents("", true, 10)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("audit event count = %d, want 2: %+v", len(events), events)
	}
	var operationID string
	for _, event := range events {
		if event.ActorType != principal.ActorType || event.ActorID != principal.ActorID ||
			event.Metadata["reason"] != "test_explicit_deletion" ||
			event.Metadata["runtime_id"] != "runtime-1" {
			t.Fatalf("incomplete deletion audit event: %+v", event)
		}
		if operationID == "" {
			operationID = event.Metadata["operation_id"]
		} else if event.Metadata["operation_id"] != operationID {
			t.Fatalf("deletion audit operation IDs do not match: %+v", events)
		}
		switch event.Action {
		case "cluster.node.delete.requested":
			if _, exists := event.Metadata["outcome"]; exists {
				t.Fatalf("deletion intent was mutated after persistence: %+v", event)
			}
		case "cluster.node.delete.succeeded":
			if event.Metadata["outcome"] != clusterNodeDeleteOutcomeDeleted {
				t.Fatalf("deletion result is missing its exact outcome: %+v", event)
			}
		default:
			t.Fatalf("unexpected deletion audit action: %+v", event)
		}
	}
}
