package api

import (
	"context"
	"database/sql"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	runtimepkg "fugue/internal/runtime"

	"k8s.io/utils/ptr"
)

func TestLiveDNSConvergenceRejectsHistoricalBackendWithoutChangingFacts(t *testing.T) {
	const fixture = "live-dns-convergence"
	state := newStoreTestDatabase(t, fixture)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(state, auth.New(state, "backend-admin"), nil, ServerConfig{BundleSigningKey: "synthetic-convergence", BundleSigningKeyID: "key"})
	f := newDNSBackendFixture(t)
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: f.claims.NodeID, EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8"}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test", platformconfig.RouteIntent{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true})
	s.clusterNodeInventoryCache = newExpiringResponseCache[[]clusterNodeSnapshot](time.Hour)
	s.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{
		{node: model.ClusterNode{Name: f.claims.NodeID}, labels: map[string]string{runtimepkg.DNSRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
		{node: model.ClusterNode{Name: "delegation-edge-0"}, labels: map[string]string{runtimepkg.EdgeRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
	})
	parent, release, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "gray")
	if err != nil || !found {
		t.Fatal(err)
	}
	sets, err := s.currentReleaseSetExpectations(parent)
	if err != nil {
		t.Fatal(err)
	}
	var set model.PlatformExpectedConsumerSet
	for _, item := range sets {
		if item.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
			set = item
		}
	}
	child, err := state.GetPlatformArtifactByIdentity(set.ArtifactKind, set.ScopeKey, set.ExpectedGeneration)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "test", Keys: map[string]string{"test": "synthetic-identity"}}
	token, err := platformcontrol.IssuePlatformComponentIdentity(ring, f.claims, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
	if err != nil {
		t.Fatal(err)
	}
	h := trustedPlatformHeartbeatRequest(t, claims, set, now, 20, child.GenerationSequence, release.FencingToken, "current-pod-receipt-nonce")
	h.LKGGeneration = child.Generation
	h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
	h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if _, err = state.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.install(t, s)
	consumers, err := state.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil {
		t.Fatal(err)
	}
	binding := s.platformConvergenceBinding(set)
	if got := s.evaluateLiveConsumerConvergence(context.Background(), set, consumers, binding); !got.Pass {
		t.Fatal(got)
	}
	db := openStoreTestSQL(t, fixture)
	before := convergenceStoreFacts(t, db)
	baseline := *f.slices.DeepCopy()
	for _, scenario := range []string{"replaced", "unready", "unavailable", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f.slices = *baseline.DeepCopy()
			f.failPath = ""
			ctx := context.Background()
			reason, wantState := "dns_public_backend_mismatch", model.InvariantEvidenceStateFail
			switch scenario {
			case "replaced":
				f.slices.Items[0].Endpoints[0].TargetRef.UID = "replacement-pod"
			case "unready":
				f.slices.Items[0].Endpoints[0].Conditions.Ready = ptr.To(false)
			case "unavailable":
				f.failPath = "/endpointslices"
				reason, wantState = "dns_public_backend_unavailable", model.InvariantEvidenceStateUnknown
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				reason, wantState = "dns_public_backend_unavailable", model.InvariantEvidenceStateUnknown
			}
			// Same exact stored receipt is still fresh and valid for the release.
			if got := platformcontrol.EvaluateConsumerConvergence(set, consumers, time.Now(), binding); !got.Pass {
				t.Fatal("fixture no longer has a fresh positive receipt", got)
			}
			got := s.evaluateLiveConsumerConvergence(ctx, set, consumers, binding)
			if got.Pass || got.State != wantState || got.RequiredObserved != 1 || got.RequiredPassing != 0 || len(got.Assessments) != 1 || !strings.Contains(strings.Join(got.Assessments[0].Reasons, ","), reason) {
				t.Fatal(got)
			}
			if !reflect.DeepEqual(got.Assessments[0].Observed, &consumers[0]) {
				t.Fatal("live assessment rewrote original evidence")
			}
			if scenario != "canceled" {
				response := performJSONRequest(t, s, "GET", "/v1/admin/platform-state/convergence?release_set_id="+parent.ID+"&artifact_kind=dns_answer_bundle", "backend-admin", nil)
				if response.Code != 200 || !strings.Contains(response.Body.String(), reason) {
					t.Fatal(response.Code, response.Body.String())
				}
				_, healthy, err := s.publishedRouteDiagnostics(ctx, "edge-group-a")
				if err != nil || healthy["edge-group-a"] {
					t.Fatal("route diagnostics accepted historical backend", healthy, err)
				}
				if _, err := s.inspectPublishedTrafficArtifacts(ctx, "example.test", map[string]string{f.claims.NodeID: "edge-group-a"}); err == nil {
					t.Fatal("traffic diagnostics accepted historical backend")
				}
				full := performJSONRequest(t, s, "POST", "/v1/admin/artifacts/"+parent.ID+"/release", "backend-admin", model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", Reason: "must retain baseline"})
				if full.Code != http.StatusConflict {
					t.Fatal("full promotion accepted historical backend", full.Code, full.Body.String())
				}
				verify := performJSONRequest(t, s, "POST", "/v1/admin/artifact-releases/"+release.ID+"/verify-lkg", "backend-admin", platformArtifactVerifyLKGHTTPRequest{FencingToken: release.FencingToken, Reason: "must retain LKG", AllowInitialLKG: true, Evidence: passingPlatformVerificationEvidenceHTTPRequest()})
				if verify.Code != http.StatusConflict {
					t.Fatal("LKG verification accepted historical backend", verify.Code, verify.Body.String())
				}
			}
			for table, after := range convergenceStoreFacts(t, db) {
				if before[table] != after {
					t.Fatal("convergence changed persistent facts in", table)
				}
			}
		})
	}
	f.slices = baseline
	f.failPath = ""
	if got := s.evaluateLiveConsumerConvergence(context.Background(), set, consumers, binding); !got.Pass {
		t.Fatal("recovered observation remained cached as failed", got)
	}
	full := performJSONRequest(t, s, "POST", "/v1/admin/artifacts/"+parent.ID+"/release", "backend-admin", model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", Reason: "current backend passed"})
	if full.Code != http.StatusOK {
		t.Fatal("valid backend could not promote", full.Code, full.Body.String())
	}
	var promoted model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, full, &promoted)
	reportServingProducerAPI(t, s, parent, promoted.Release)
	currentSets, err := s.currentReleaseSetExpectations(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range currentSets {
		if current.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		at := time.Now().UTC()
		h := trustedPlatformHeartbeatRequest(t, claims, current, at, 40, child.GenerationSequence, promoted.Release.FencingToken, "current-full-backend-nonce")
		h.LKGGeneration = child.Generation
		h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
		h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
		if _, err := state.AcceptTrustedPlatformConsumerHeartbeat(claims, current.ID, h, at, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	verifyPlatformArtifactReleaseAPI(t, s, "backend-admin", promoted.Release, false)
	// A matching backend cannot renew an old heartbeat while metadata is read.
	set.Consumers[0].HeartbeatFreshnessSeconds = 1
	consumers[0].LastHeartbeatAt = time.Now().Add(-800 * time.Millisecond)
	f.podListDelay = 400 * time.Millisecond
	if got := s.evaluateLiveConsumerConvergence(context.Background(), set, consumers, binding); got.Pass || got.State != model.InvariantEvidenceStateStale {
		t.Fatal("lookup extended heartbeat freshness", got)
	}
}

func convergenceStoreFacts(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	facts := make(map[string]string)
	for _, table := range []string{
		"fugue_platform_artifacts", "fugue_platform_artifact_releases", "fugue_platform_release_lanes",
		"fugue_platform_release_messages", "fugue_platform_lkg_snapshots", "fugue_platform_lkg_snapshot_history",
		"fugue_platform_consumer_instances", "fugue_platform_expected_consumer_sets", "fugue_dns_nodes", "fugue_audit_events",
		"fugue_platform_input_manifests", "fugue_platform_input_hot_chunks",
	} {
		var value string
		if err := db.QueryRow(`SELECT coalesce(jsonb_agg(to_jsonb(f) ORDER BY to_jsonb(f)), '[]')::text FROM ` + table + ` f`).Scan(&value); err != nil {
			t.Fatalf("read stored %s facts: %v", table, err)
		}
		facts[table] = value
	}
	return facts
}

// Used in the actual producer state-machine regression, at gray->full and at
// full->verified. Only a disposable fixture's credential is changed; original
// artifact lineage, fresh apply/probe evidence and producer policy remain valid.
func assertProducerRejectsRetiredDNSBackend(t *testing.T, s *Server, fixture string, policyAuthority model.PlatformArtifactRelease) {
	t.Helper()
	db := openStoreTestSQL(t, fixture)
	rows, err := db.Query(`SELECT id, credential_id FROM fugue_platform_consumer_instances WHERE component=$1`, model.PlatformConsumerComponentDNSServer)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	credentials := make(map[string]string)
	for rows.Next() {
		var id, credential string
		if err := rows.Scan(&id, &credential); err != nil {
			t.Fatal(err)
		}
		credentials[id] = credential
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(credentials) == 0 {
		t.Fatal("missing stored DNS consumer evidence")
	}
	f := newDNSBackendFixture(t)
	f.claims.NodeID = "dns-a"
	f.pod.Spec.NodeName = "dns-a"
	f.node.Name = "dns-a"
	f.slices.Items[0].Endpoints = nil
	factory, namespace := s.newClusterNodeClient, s.controlPlaneNamespace
	f.install(t, s)
	defer func() {
		s.newClusterNodeClient = factory
		s.controlPlaneNamespace = namespace
		for id, credential := range credentials {
			if _, err := db.Exec(`UPDATE fugue_platform_consumer_instances SET credential_id=$2 WHERE id=$1`, id, credential); err != nil {
				t.Error(err)
			}
		}
	}()
	if _, err := db.Exec(`UPDATE fugue_platform_consumer_instances SET credential_id=$2 WHERE component=$1`, model.PlatformConsumerComponentDNSServer, f.claims.CredentialID); err != nil {
		t.Fatal(err)
	}
	before := convergenceStoreFacts(t, db)
	policyArtifact, err := s.store.GetPlatformArtifact(policyAuthority.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := platformproducer.Decode(policyArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := s.reconcilePendingProducedTraffic(context.Background(), policy, policyAuthority); err != nil || !handled {
		t.Fatal(handled, err)
	}
	after := convergenceStoreFacts(t, db)
	if before["fugue_platform_artifact_releases"] != after["fugue_platform_artifact_releases"] ||
		before["fugue_platform_lkg_snapshots"] != after["fugue_platform_lkg_snapshots"] {
		t.Fatal("producer advanced with retired DNS backend")
	}
}
