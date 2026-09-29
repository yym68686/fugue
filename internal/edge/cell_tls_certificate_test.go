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
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCellCertificateUsesExactSignedServingAssignmentWithoutLegacyCredential(t *testing.T) {
	for _, scenario := range []string{"valid", "shadow", "foreign hostname", "bad parent", "bad TLS", "foreign certificate owner", "assignment replaced", "missing certificate", "different scope", "missing credential"} {
		t.Run(scenario, func(t *testing.T) {
			tls, route := tlsShadowFixtures(t, platformconfig.PublicationRoleCellRoutes)
			for _, candidate := range []*edgePlatformCandidate{&tls, &route} {
				candidate.Assignment.ReleaseChannel, candidate.Release.ReleaseChannel, candidate.Release.CanaryRuleRef = "gray", "gray", "cohort=initial"
			}
			if scenario == "shadow" {
				tls.Assignment.ReleaseChannel, tls.Release.ReleaseChannel = "shadow", "shadow"
			}
			if scenario == "bad parent" {
				route.ReleaseSet.Provenance.Signature = "untrusted"
			}
			if scenario == "bad TLS" {
				tls.Artifact.Provenance.Signature = "untrusted"
			}
			certificateReads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("token") != "" || strings.HasPrefix(r.URL.Path, "/v1/edge/") {
					t.Error("cell certificate request used legacy credentials")
				}
				switch r.URL.Path {
				case "/v1/platform-state/consumers/identity":
					json.NewEncoder(w).Encode(map[string]any{"token": "component", "expires_at": time.Now().Add(time.Minute), "component": "edge-worker", "node_id": "node-a", "scope_key": "authority-cell:cell-a", "authority_id": "cell-a", "consumer_id": "edge-worker:cell-a:node-a", "artifact_kinds": []string{"edge_route_bundle", "caddy_route_config"}})
				case "/v1/platform-state/consumers/assignment":
					if r.URL.Query().Get("serving_only") != "true" {
						t.Error("certificate discovery used non-serving assignments")
					}
					assigned := tls.Assignment
					if scenario == "assignment replaced" && certificateReads > 0 {
						assigned.ArtifactReleaseID = "new-release"
					}
					json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{assigned, route.Assignment}})
				case "/v1/platform-state/consumers/artifacts/" + tls.Artifact.ID:
					json.NewEncoder(w).Encode(tls)
				case "/v1/platform-state/consumers/artifacts/" + route.Artifact.ID:
					json.NewEncoder(w).Encode(route)
				case "/v1/platform-state/consumers/artifacts/" + route.ReleaseSet.ID:
					json.NewEncoder(w).Encode(map[string]any{"artifact": route.ReleaseSet, "assignment": route.Assignment, "release": route.Release})
				case "/v1/platform-state/consumers/artifacts/" + tls.Artifact.ID + "/tls/app.example.test":
					certificateReads++
					if r.Header.Get("Authorization") != "Bearer component" || r.URL.Query().Get("expected_consumer_set_id") != tls.Assignment.ExpectedConsumerSetID {
						t.Error("certificate pull is not bound to component and expectation")
					}
					if scenario == "missing certificate" {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					cert := model.EdgeTLSCertificate{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", CertificatePEM: "synthetic-cert", PrivateKeyPEM: "synthetic-key", IssuerStorage: "imported"}
					if scenario == "foreign certificate owner" {
						cert.TenantID = "foreign"
					}
					json.NewEncoder(w).Encode(map[string]any{"certificate": cert})
				default:
					t.Error("unexpected certificate request", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			s := NewService(config.EdgeConfig{APIURL: server.URL, EdgeID: "node-a", EdgeGroupID: "cell-a", PlatformScopeKey: "authority-cell:cell-a", BundleSigningKey: "synthetic-tls-key", BundleSigningKeyID: "signer", EdgeToken: "legacy-must-not-be-used"}, log.New(io.Discard, "", 0))
			s.PlatformTokenFile = filepath.Join(t.TempDir(), "pod-token")
			if err := os.WriteFile(s.PlatformTokenFile, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			host := "app.example.test"
			if scenario == "foreign hostname" {
				host = "foreign.example.test"
			}
			if scenario == "different scope" {
				s.Config.PlatformScopeKey = "global"
			}
			if scenario == "missing credential" {
				s.PlatformTokenFile = ""
			}
			cert, err := s.fetchSharedCaddyTLSCertificate(context.Background(), host)
			if scenario == "valid" {
				if err != nil || cert == nil || cert.CertificatePEM != "synthetic-cert" || certificateReads != 1 {
					t.Fatal("scoped certificate unavailable", err)
				}
			} else if err == nil || cert != nil {
				t.Fatal("invalid certificate authorization succeeded")
			}
			if rejectedBeforeCertificate(scenario) && certificateReads != 0 {
				t.Fatal("unverified assignment requested private certificate")
			}
		})
	}
}

func rejectedBeforeCertificate(scenario string) bool {
	return scenario == "shadow" || scenario == "foreign hostname" || scenario == "bad parent" || scenario == "bad TLS" || scenario == "different scope" || scenario == "missing credential"
}
