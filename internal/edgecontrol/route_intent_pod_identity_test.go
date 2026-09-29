package edgecontrol

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficbinding"
)

func TestNeutralControlUsesOnlyBoundPodExchange(t *testing.T) {
	for _, mode := range []string{"valid", "foreign-cell", "foreign-node", "wrong-kind", "long-lived", "expired", "unknown-field", "duplicate-field", "redirect", "no-store-missing", "trailing-json", "no-release"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			snapshot := routeIntentFixture()
			d := "sha256:" + strings.Repeat("a", 64)
			snapshot.TrafficRelease = &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: "parent", ReleaseSetDigest: d, ReleaseSetGeneration: "parent-gen", RouteArtifactID: "route", RouteArtifactDigest: d, RouteArtifactGeneration: snapshot.Generation, RouteArtifactSequence: 1, ReleaseID: "release", ReleaseChannel: "gray", FencingToken: 1, ScopeKey: "global", IntentDigest: d, PolicyDigest: d, InputSnapshotDigest: d, CompilerVersion: "compiler", ProjectionDigest: trafficbinding.ProjectionDigest(snapshot), CanaryRuleRef: "cohort=first", EdgeGroupIDs: []string{"cell-a"}}
			if mode == "no-release" {
				snapshot.TrafficRelease = nil
			}
			var exchangeCalls, routeCalls atomic.Int32
			server, ca, name, address := newRouteIntentTLSServer(t, now, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.TLS == nil || r.TLS.ServerName != routeIntentTestServerName || r.ProtoMajor != 1 {
					t.Error("identity lost private TLS boundary")
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case routeIntentIdentityPath:
					exchangeCalls.Add(1)
					if r.Method != "POST" || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer pod-bound-token" {
						t.Error("unbound credential exchange")
					}
					w.Header().Set("Cache-Control", "no-store")
					id := map[string]any{"token": "cell-component-token", "component": "edge-control", "node_id": "node-a", "authority_id": "cell-a", "consumer_id": "edge-control:cell-a:node-a", "scope_key": "global", "artifact_kinds": []string{"edge_route_intent"}, "expires_at": now.Add(2 * time.Minute)}
					switch mode {
					case "foreign-cell":
						id["authority_id"] = "cell-b"
					case "foreign-node":
						id["node_id"] = "node-b"
					case "wrong-kind":
						id["artifact_kinds"] = []string{"edge_route_bundle"}
					case "long-lived":
						id["expires_at"] = now.Add(time.Hour)
					case "expired":
						id["expires_at"] = now
					case "unknown-field":
						id["unrecognized"] = true
					case "no-store-missing":
						w.Header().Del("Cache-Control")
					case "redirect":
						http.Redirect(w, r, "https://foreign.example.test/credential", 307)
						return
					}
					body, _ := json.Marshal(id)
					if mode == "duplicate-field" {
						body = append([]byte(`{"authority_id":"cell-foreign",`), body[1:]...)
					}
					w.Write(body)
					if mode == "trailing-json" {
						w.Write([]byte(`{}`))
					}
				case RouteIntentPathV1:
					routeCalls.Add(1)
					if r.Method != "GET" || r.URL.RawQuery != "edge_group_id=cell-a" || r.Header.Get("Authorization") != "Bearer cell-component-token" {
						t.Error("Pod credential reached route source or authority changed")
					}
					w.Header().Set("ETag", strconv.Quote(snapshot.Generation))
					w.Header().Set(RouteIntentGenerationHeader, snapshot.Generation)
					json.NewEncoder(w).Encode(snapshot)
				default:
					t.Error("unexpected endpoint")
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			file := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(file, []byte("pod-bound-token"), 0600); err != nil {
				t.Fatal(err)
			}
			config := RouteIntentClientConfig{Endpoint: server.URL + RouteIntentPathV1, EdgeGroupID: "cell-a", PodTokenFile: file, IdentityNodeID: "node-a", CAFile: ca, ServerName: name, Now: func() time.Time { return now }}
			client, err := NewRouteIntentClient(config)
			if err != nil {
				t.Fatal(err)
			}
			bindRouteIntentTestDialer(t, client, address)
			_, err = client.FetchRouteIntents(context.Background())
			if (err == nil) != (mode == "valid") {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
			wantRoutes := int32(0)
			if mode == "valid" || mode == "no-release" {
				wantRoutes = 1
			}
			if exchangeCalls.Load() != 1 || routeCalls.Load() != wantRoutes {
				t.Fatal("failed exchange reached route authority", exchangeCalls.Load(), routeCalls.Load())
			}
			if mode == "valid" {
				config.IssuerFile = file
				if ValidateRouteIntentClientConfig(config) == nil {
					t.Fatal("mixed issuer and Pod credentials accepted")
				}
				config.IssuerFile = ""
				config.EdgeGroupID = "edge-group-old"
				if ValidateRouteIntentClientConfig(config) == nil {
					t.Fatal("Pod credential entered a legacy authority")
				}
			}
		})
	}
}
