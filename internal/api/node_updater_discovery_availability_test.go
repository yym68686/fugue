package api

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestNodeUpdaterDesiredStateRetainsConfigurationWhenDiscoveryUnavailable(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	state := store.New(path)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := state.CreateTenant("synthetic updater tenant")
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := state.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := state.EnrollNodeUpdater(key, "worker-a", "https://worker.example.test", nil, "worker-a", "synthetic-machine", "v1", "v1", []string{"heartbeat"})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(state, auth.New(state, ""), nil, ServerConfig{BundleSigningKey: "synthetic-discovery-key", BundleSigningKeyID: "discovery-key"})
	now := time.Now().UTC()
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8", LastSeenAt: &now}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, server, "example.test")
	before := performJSONRequest(t, server, http.MethodGet, "/v1/node-updater/desired-state", token, nil)
	if before.Code != http.StatusOK {
		t.Fatalf("verified baseline: %d %s", before.Code, before.Body.String())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var original model.State
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	if len(original.PlatformLKGSnapshots) == 0 {
		t.Fatal("fixture requires a verified LKG")
	}
	original.PlatformLKGSnapshots[0].SnapshotProvenance.Signature = "corrupt"
	damaged, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, damaged, 0600); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"/v1/discovery/bundle", "/v1/node-updater/desired-state"} {
		response := performJSONRequest(t, server, http.MethodGet, route, token, nil)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s returned %d instead of temporary unavailability: %s", route, response.Code, response.Body.String())
		}
		if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("ETag") != "" {
			t.Fatalf("unavailable discovery may not become cacheable configuration: %v", response.Header())
		}
		if !strings.Contains(response.Body.String(), errDiscoveryRoutesUnavailable.Error()) || strings.Contains(response.Body.String(), "desired_state") || strings.Contains(response.Body.String(), "platform_routes") {
			t.Fatalf("unexpected unavailable response: %s", response.Body.String())
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var persisted model.State
		if err := json.Unmarshal(after, &persisted); err != nil {
			t.Fatal(err)
		}
		// Authentication may update credential last-use metadata. Configuration
		// artifacts and their LKG verification must remain byte-for-byte equal
		// after decoding the independent store snapshot.
		if !reflect.DeepEqual(persisted.PlatformArtifacts, original.PlatformArtifacts) ||
			!reflect.DeepEqual(persisted.PlatformLKGSnapshots, original.PlatformLKGSnapshots) {
			t.Fatal("read rewrote configuration or its failed verification")
		}
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	recovered := performJSONRequest(t, server, http.MethodGet, "/v1/node-updater/desired-state", token, nil)
	if recovered.Code != http.StatusOK {
		t.Fatalf("verified configuration recovery: %d %s", recovered.Code, recovered.Body.String())
	}
}
