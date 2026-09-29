package edge

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edgecontrol"
	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestInitialInventoryBootstrapRequiresExactLeaseAndSignedGrayPublication(t *testing.T) {
	for _, scenario := range []string{"valid", "expired", "future", "unbounded lease", "other Pod", "other source", "other cell", "other parent", "untrusted parent", "shadow", "publication changed", "permission changed", "activation corrupt", "activation appeared", "unknown field", "duplicate field"} {
		t.Run(scenario, func(t *testing.T) {
			_, route := tlsShadowFixtures(t, platformconfig.PublicationRoleCellRoutes)
			route.Assignment.ReleaseChannel, route.Release.ReleaseChannel, route.Release.CanaryRuleRef = "gray", "gray", "cohort=initial"
			dir := t.TempDir()
			permissionPath := filepath.Join(dir, "bootstrap.json")
			activationPath := filepath.Join(dir, "activation.json")
			now := time.Now().UTC()
			cfg := config.EdgeConfig{EdgeID: "node-a", EdgeGroupID: "cell-a", PlatformScopeKey: "authority-cell:cell-a", EdgeSlot: "a", EdgeInstanceUID: "pod-a", EdgeReleaseEpoch: strings.Repeat("a", 40), FaultDomainID: "host-a", EdgePoolID: "pool-a", CachePath: filepath.Join(dir, "routes.json"), CaddyEnabled: true, BundleSigningKey: "synthetic-tls-key", BundleSigningKeyID: "signer"}
			a := inventoryBootstrapAuthorization{Schema: "fugue.cell-inventory-bootstrap/v1", AuthorityCellID: cfg.EdgeGroupID, EdgeID: cfg.EdgeID, InstanceUID: cfg.EdgeInstanceUID, Slot: cfg.EdgeSlot, SourceSHA: cfg.EdgeReleaseEpoch, ReleaseSetID: route.ReleaseSet.ID, ReleaseSetDigest: route.ReleaseSet.ContentHash, ReleaseID: route.Release.ID, IssuedAt: now, ExpiresAt: now.Add(40 * time.Second)}
			switch scenario {
			case "expired":
				a.IssuedAt = now.Add(-time.Hour)
				a.ExpiresAt = now.Add(-time.Second)
			case "future":
				a.IssuedAt = now.Add(time.Minute)
				a.ExpiresAt = now.Add(2 * time.Minute)
			case "unbounded lease":
				a.ExpiresAt = now.Add(time.Hour)
			case "other Pod":
				a.InstanceUID = "other-pod"
			case "other source":
				a.SourceSHA = strings.Repeat("b", 40)
			case "other cell":
				a.AuthorityCellID = "cell-b"
			case "other parent":
				a.ReleaseSetDigest = "sha256:" + strings.Repeat("b", 64)
			case "untrusted parent":
				route.ReleaseSet.Provenance.Signature = "invalid"
			case "shadow":
				route.Assignment.ReleaseChannel, route.Release.ReleaseChannel, route.Release.CanaryRuleRef = "shadow", "shadow", ""
			case "activation corrupt":
				os.WriteFile(activationPath, []byte("corrupt"), 0600)
			}
			raw, _ := json.Marshal(a)
			if scenario == "unknown field" {
				raw = append(raw[:len(raw)-1], []byte(",\"extra\":true}")...)
			}
			if scenario == "duplicate field" {
				raw = append(raw[:len(raw)-1], []byte(",\"slot\":\"a\"}")...)
			}
			if err := os.WriteFile(permissionPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			tokenPath := filepath.Join(dir, "pod-token")
			os.WriteFile(tokenPath, []byte("pod-token"), 0600)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					json.NewEncoder(w).Encode(map[string]any{"token": "token", "expires_at": time.Now().Add(time.Minute), "component": "edge-worker", "node_id": cfg.EdgeID, "scope_key": cfg.PlatformScopeKey, "authority_id": cfg.EdgeGroupID, "consumer_id": "edge-worker:cell-a:node-a", "artifact_kinds": []string{route.Artifact.ArtifactKind}})
				case "/v1/platform-state/consumers/assignment":
					if r.URL.Query().Get("serving_only") != "true" {
						t.Error("bootstrap consulted non-serving permission")
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{route.Assignment}})
				case "/v1/platform-state/consumers/artifacts/" + route.Artifact.ID:
					json.NewEncoder(w).Encode(route)
				case "/v1/platform-state/consumers/artifacts/" + route.ReleaseSet.ID:
					json.NewEncoder(w).Encode(map[string]any{"artifact": route.ReleaseSet, "assignment": route.Assignment, "release": route.Release})
				default:
					t.Error("unexpected request", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			cfg.APIURL = server.URL
			keyring, _ := writeInventoryProducerKeyringFixture(t, cfg.EdgeGroupID)
			s := NewServiceWithEdgeSources(cfg, RouteBundleSourceConfig{}, InventoryProducerConfig{URL: "http://control-a.fugue-system.svc:8092" + edgecontrol.GroupAuthorityInventoryHeartbeatPathV1, AuthorityService: "control-a", IdentityKeyringFile: keyring, ActivationStateFile: activationPath, BootstrapFile: permissionPath, Interval: 30 * time.Second}, log.New(io.Discard, "", 0))
			s.PlatformTokenFile = tokenPath
			s.snapshot = Status{Status: "unhealthy", CaddyEnabled: true, LastError: `edge routes returned status 503: {"error":"group_bundle_unavailable"}`}
			writes := 0
			s.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method == "GET" {
					switch scenario {
					case "publication changed":
						route.Release.ID = "next"
						route.Assignment.ArtifactReleaseID = "next"
					case "permission changed":
						a.ExpiresAt = a.ExpiresAt.Add(time.Second)
						b, _ := json.Marshal(a)
						os.WriteFile(permissionPath, b, 0600)
					case "activation appeared":
						writeInventoryActivationAt(t, activationPath, cfg, now)
					}
					return inventoryJSONResponse(503, edgecontrol.AuthorityGroupStatus{GroupID: cfg.EdgeGroupID}), nil
				}
				writes++
				var h edgecontrol.GroupInventoryHeartbeat
				if json.NewDecoder(r.Body).Decode(&h) != nil {
					t.Fatal("heartbeat invalid")
				}
				if len(h.Inventory.Instances) != 1 {
					t.Fatal("membership changed")
				}
				instance := h.Inventory.Instances[0]
				if instance.BootstrapEligibility == nil || instance.ServingHealthy == nil || *instance.ServingHealthy || instance.EffectiveHealthy || h.Inventory.ActiveEpoch.FenceSequence != 1 || h.ExpiresAtUnix > a.ExpiresAt.Unix() || instance.InstanceUID != cfg.EdgeInstanceUID {
					t.Fatal("bootstrap claimed serving or changed lease/identity", h)
				}
				return inventoryJSONResponse(201, edgecontrol.GroupInventoryHeartbeatReceipt{Schema: edgecontrol.GroupInventoryHeartbeatReceiptSchemaV1, GroupID: cfg.EdgeGroupID, Sequence: h.Inventory.Sequence, Generation: "inventory", Authority: "edge-control", Publication: true, ProducerNodeID: cfg.EdgeID, ProducerGeneration: h.ProducerGeneration}), nil
			})}
			err := s.InventoryHeartbeatOnce(context.Background())
			if scenario != "valid" {
				if err == nil || writes != 0 {
					t.Fatal("invalid bootstrap wrote inventory", err, writes)
				}
				return
			}
			if err != nil || writes != 1 {
				t.Fatal("valid initial inventory failed", err, writes)
			}
			if _, err := os.Stat(activationPath); !os.IsNotExist(err) {
				t.Fatal("bootstrap invented Front activation")
			}
			if _, err := os.Stat(cfg.CachePath); !os.IsNotExist(err) {
				t.Fatal("bootstrap invented a serving cache")
			}
			writeInventoryActivationAt(t, activationPath, cfg, now)
			os.WriteFile(permissionPath, []byte("corrupt"), 0600)
			selected, err := s.selectInventoryEpoch(context.Background(), cfg)
			if err != nil || selected.Bootstrap != nil || selected.Activation.BundleGeneration != "real-observed-bundle" {
				t.Fatal("positive activation depends on bootstrap configuration", err)
			}
		})
	}
}

func writeInventoryActivationAt(t *testing.T, path string, cfg config.EdgeConfig, now time.Time) {
	t.Helper()
	_, err := edgegroupfront.ApplyActivationCAS(path, edgegroupfront.ActivationCASRequest{Operation: edgegroupfront.ActivationOperationInit, GroupID: cfg.EdgeGroupID, ExpectedSlot: cfg.EdgeSlot, TargetSlot: cfg.EdgeSlot, BundleGeneration: "real-observed-bundle", WorkerSourceCommit: cfg.EdgeReleaseEpoch, WorkerImageDigest: "sha256:" + strings.Repeat("b", 64), Reason: "verified initial bundle fixture"}, now)
	if err != nil {
		t.Fatal(err)
	}
}
