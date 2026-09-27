package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/agentedge"
	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routebinding"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

type agentAuthorityFixture struct {
	s                                    *Server
	admin, tenant, runtimeID, runtimeKey string
	policy                               agentedge.AuthorityPolicy
	keys                                 map[string]agentedge.TrustKey
	topology                             edgetopology.Intent
	compiled                             platformConfigCompileResponse
}

func setupAgentAuthorityFixture(t *testing.T) agentAuthorityFixture {
	t.Helper()
	st, s, tenant, admin, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	_, secret, err := st.CreateEnrollmentToken(app.TenantID, "agent-test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	runtime, runtimeKey, err := st.ConsumeEnrollmentToken(secret, "agent-test", "https://runtime.example.test", nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	topology := edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion,
		Cells: []edgetopology.AuthorityCell{{ID: "cell-a", LegacyGroupID: "edge-group-a"}, {ID: "cell-b", LegacyGroupID: "edge-group-b"}},
		Pools: []edgetopology.ServingPool{{ID: "pool-public"}},
		Edges: []edgetopology.Edge{
			{ID: "edge-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "host-a"}},
			{ID: "edge-b", AuthorityCellID: "cell-b", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "host-b"}},
		}}
	addresses := map[string]string{"edge-a": "8.8.8.8", "edge-b": "9.9.9.9"}
	for i, edge := range topology.Edges {
		_, _, err := st.UpdateEdgeHeartbeat(model.EdgeNode{ID: edge.ID, EdgeGroupID: topology.Cells[i].ServingGroupID(), PublicIPv4: addresses[edge.ID], Healthy: true, Status: model.EdgeHealthHealthy, CaddyRouteCount: 2})
		if err != nil {
			t.Fatal(err)
		}
	}
	intent := platformconfig.PlatformIntent{Scope: "global", Generation: "agent-traffic-input", EdgeTopology: &topology,
		Routes: []platformconfig.RouteIntent{
			{Hostname: "api.example.test", PathPrefix: "/", Kind: model.EdgeRouteKindControlPlaneAPI, UpstreamURL: "http://origin:8080", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled},
			{Hostname: "api.example.test", PathPrefix: "/v1", Kind: model.EdgeRouteKindControlPlaneAPI, UpstreamURL: "http://origin:8081", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled},
		},
		DNS: []platformconfig.DNSIntent{{Hostname: "api.example.test", Type: "FUGUE_ROUTE", TTL: 60, Values: []string{}, Route: &platformconfig.DNSRouteIntent{Hostnames: []string{"api.example.test"}, DNSApplicationIntent: platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}},
	}
	policy := platformconfig.PolicySnapshot{Scope: "global", Generation: "traffic-policy", MinimumHealthyEdges: 1, MaxStaleSeconds: 120,
		DNSReadiness:          &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 10, ProbeTimeoutSeconds: 2, FactFreshnessSeconds: 120, MaxConcurrency: 4, MaxProbes: 64},
		TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "both", EdgeGroupIDs: []string{"edge-group-a", "edge-group-b"}}}}
	intent = platformconfig.NormalizePlatformIntent(intent)
	policy = platformconfig.NormalizePolicySnapshot(policy)
	routes, err := platformconfig.ResolveRouteOrigins(intent.Routes, platformconfig.RuntimeSnapshot{CapturedAt: &now}, policy)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := platformconfig.DNSPlacementInputDigest(intent.DNS[0], routes, policy)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now,
		DNSPlacements: []platformconfig.DNSPlacementObservation{{InputDigest: digest, CheckedAt: now, Status: "resolved", TargetTTL: 60, Candidates: []platformconfig.DNSPlacementCandidate{
			{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", ServingGeneration: "serving-a", ObservedAt: now, ValidUntil: now.Add(90 * time.Second), Healthy: true, RouteReady: true, TLSReady: true, A: []string{"8.8.8.8"}},
			{EdgeID: "edge-b", EdgeGroupID: "edge-group-b", ServingGeneration: "serving-b", ObservedAt: now, ValidUntil: now.Add(90 * time.Second), Healthy: true, RouteReady: true, TLSReady: true, A: []string{"9.9.9.9"}},
		}}}, DNSEdgeEndpoints: []platformconfig.DNSEdgeEndpoint{
			{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", ObservedAt: now, A: []string{"8.8.8.8"}},
			{EdgeID: "edge-b", EdgeGroupID: "edge-group-b", ObservedAt: now, A: []string{"9.9.9.9"}},
		}}})
	if err != nil {
		t.Fatal("compile traffic", err)
	}
	c, err := s.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal("store traffic", err)
	}
	for phase, channel := range []string{"shadow", "gray"} {
		request := model.PlatformArtifactReleaseRequest{ReleaseChannel: channel}
		if channel == "gray" {
			request.CanaryRuleRef = "cohort=both"
		}
		_, release, _, _, err := st.ReleasePlatformArtifact(c.ReleaseArtifact.ID, request, platformProducerPrincipal())
		if err != nil {
			t.Fatal("publish traffic", err)
		}
		for i, child := range []model.PlatformArtifact{c.RouteArtifact, c.DNSArtifact, c.TLSArtifact} {
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: c.ReleaseArtifact.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Generation: child.Generation, Revision: int64(phase*3 + i + 1), Topology: platformcontrol.ExpectedConsumerTopology{
				EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a"}, {ID: "edge-b", EdgeGroupID: "edge-group-b"}},
				DNSNodes:  []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test"}, {ID: "dns-b", PhysicalNodeID: "dns-b", EdgeGroupID: "edge-group-b", Zone: "example.test"}},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = st.CreatePlatformExpectedConsumerSet(set); err != nil {
				t.Fatal(err)
			}
			for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
				keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-agent-edge-capability"}}
				claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "test", Component: member.Component, NodeID: member.NodeID, ScopeKey: "global", ArtifactKinds: []string{child.ArtifactKind}}
				token, e := platformcontrol.IssuePlatformComponentIdentity(keys, claims, time.Now(), time.Minute)
				if e != nil {
					t.Fatal(e)
				}
				claims, e = platformcontrol.ParsePlatformComponentIdentity(keys, token, time.Now())
				if e != nil {
					t.Fatal(e)
				}
				h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", ReleaseSetID: c.ReleaseArtifact.ID, ExpectedConsumerSetID: set.ID, FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: int64(phase + 1), IssuedAt: time.Now().UTC(), Nonce: model.NewID("nonce"), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, ActualGeneration: child.Generation, LKGGeneration: child.Generation, ApplyStatus: "applied", ProbeStatus: "passed", CompatibilityCapabilities: []string{platformcontrol.TrafficReleaseCapabilityV1}}
				if channel == "shadow" {
					h.ActualGeneration = ""
					h.LKGGeneration = ""
					h.CandidateGeneration = child.Generation
					h.ApplyStatus = "staged"
					h.ProbeStatus = "shadow_validated"
				}
				h.EvidenceHash, e = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = st.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, time.Now(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ring := agentedge.PrivateKeyring{Schema: agentedge.PrivateKeyringSchema, Generation: 1, Keys: []agentedge.PrivateKeyConfig{{PublicKeyConfig: agentedge.PublicKeyConfig{KeyID: "key-a", PublicKey: base64.RawURLEncoding.EncodeToString(public), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}, PrivateKey: base64.RawURLEncoding.EncodeToString(private)}}}
	raw, _ := json.Marshal(ring)
	s.agentEdgeSigningKeyFile = filepath.Join(t.TempDir(), "signing.json")
	if err = os.WriteFile(s.agentEdgeSigningKeyFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	keys, err := ring.Public().PublicKeys()
	if err != nil {
		t.Fatal(err)
	}
	p := agentedge.AuthorityPolicy{SchemaVersion: agentedge.AuthorityPolicySchema, Generation: "agent-authority", Scope: agentedge.PolicyScope, Mode: "shadow", Origin: "https://api.example.test", SigningKeyID: "key-a", TopologyIntentArtifactID: c.IntentArtifact.ID, TopologyIntentDigest: c.IntentArtifact.ContentHash,
		Constraint: platformconfig.EdgeSelectionConstraint{OwnerKind: "platform", Hostname: "api.example.test", AllowedPoolIDs: []string{"pool-public"}, RequiredCapabilities: []string{"http", "tls"}, MinCandidates: 1, MinDistinctCells: 1, MinDistinctDomains: map[string]int{"host": 1}, FactMaxAgeSeconds: 120},
		Selection:  agentedge.Policy{ProbeIntervalSeconds: 10, ProbeTimeoutMilliseconds: 500, FactMaxAgeSeconds: 120, FailureThreshold: 3, BetterSampleThreshold: 3, SwitchImprovementPercent: 15, SwitchCooldownSeconds: 60, StandbyCount: 1, DesiredDistinctCells: 2, MaxCandidates: 8},
		Capacity:   agentedge.CapacityPolicy{MaxNodeCPUPercent: 85, MaxNodeMemoryPercent: 85, FactMaxAgeSeconds: 120}, GrantTTLSeconds: 60, MinimumLeaseSeconds: 20}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	f := agentAuthorityFixture{s: s, admin: admin, tenant: tenant, runtimeID: runtime.ID, runtimeKey: runtimeKey, policy: p, keys: keys, topology: topology, compiled: c}
	f.publishPolicy(t, p)
	kube := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer capacity-token" {
			w.WriteHeader(401)
			return
		}
		for id, address := range addresses {
			node, summary := agentCapacityFixture(id, address, now)
			switch r.URL.Path {
			case "/api/v1/nodes/" + id:
				json.NewEncoder(w).Encode(node)
				return
			case "/api/v1/nodes/" + id + "/proxy/stats/summary":
				json.NewEncoder(w).Encode(summary)
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(kube.Close)
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{baseURL: kube.URL, client: kube.Client(), bearerToken: "capacity-token"}, nil
	}
	snapshots := map[string]model.EdgeRouteIntentSnapshot{}
	for _, cell := range topology.Cells {
		snapshot, found, err := s.edgeRouteIntentSnapshotFromTrafficRelease(cell.ServingGroupID())
		if err != nil || !found {
			t.Fatal("source unavailable", err)
		}
		snapshots[cell.ServingGroupID()] = snapshot
	}
	s.agentEdgeProbe = func(_ context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		for i, edge := range topology.Edges {
			if address != addresses[edge.ID] {
				continue
			}
			group := topology.Cells[i].ServingGroupID()
			snapshot := snapshots[group]
			for _, route := range snapshot.Routes {
				if route.Hostname == host && route.PathPrefix == path {
					digest, err := routeproof.Digest(routebinding.FromIntent(route, group))
					if err != nil {
						return routeprobe.Proof{}, err
					}
					return routeprobe.Proof{Version: "proof-v1", Digest: digest, EdgeID: edge.ID, GroupID: group, CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(90 * time.Second), TrafficRelease: snapshot.TrafficRelease}, nil
				}
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected probe")
	}
	return f
}

func (f agentAuthorityFixture) publishPolicy(t *testing.T, p agentedge.AuthorityPolicy) {
	t.Helper()
	raw, _ := json.Marshal(p)
	var content map[string]any
	json.Unmarshal(raw, &content)
	a, err := f.s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: agentedge.PolicyScope}, Generation: p.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePlatformPolicyArtifact(a); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.store.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "agent-policy", Pass: true}}); err != nil {
		t.Fatal(err)
	}
	_, _, found, err := f.s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, agentedge.PolicyScope, "full")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		seedVerifiedPlatformArtifactAPI(t, f.s, f.admin, a.ID)
	}
	releaseAndVerifyFullPlatformArtifactAPI(t, f.s, f.admin, a.ID)
}

func TestAgentGrantBindsRuntimeAndEveryPathWithoutChangingTraffic(t *testing.T) {
	f := setupAgentAuthorityFixture(t)
	before, beforeRelease, _, _ := f.s.selectTrafficRouteRelease("edge-group-a")
	r := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	v, err := agentedge.Verify(r.Body.Bytes(), f.keys, f.runtimeID, f.policy.Origin, nil, time.Now())
	if err != nil {
		t.Fatal("issued grant did not verify", err)
	}
	g := v.View()
	if g.Mode != "shadow" || len(g.Candidates) != 2 || g.MinimumCandidates != 1 || g.MinDistinctCells != 1 || g.Policy.DesiredDistinctCells != 2 {
		t.Fatalf("wrong grant: %+v", g)
	}
	for _, c := range g.Candidates {
		if len(c.RouteDigests) != 2 || !slices.IsSorted(c.RouteDigests) || c.Publication.ReleaseSetID != f.compiled.ReleaseArtifact.ID {
			t.Fatal("missing path/publication binding")
		}
	}
	if _, err = agentedge.Verify(r.Body.Bytes(), f.keys, "runtime-foreign", f.policy.Origin, nil, time.Now()); err == nil {
		t.Fatal("foreign runtime accepted grant")
	}
	after, afterRelease, _, _ := f.s.selectTrafficRouteRelease("edge-group-a")
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeRelease, afterRelease) {
		t.Fatal("read-only grant issuance changed traffic authority")
	}
	preview := performJSONRequest(t, f.s, http.MethodGet, "/v1/admin/agent-edge/preview?runtime_id="+f.runtimeID, f.admin, nil)
	var result agentEdgePreview
	mustDecodeJSON(t, preview, &result)
	if preview.Code != 200 || !result.Ready || result.AuthorizesTraffic || !result.SigningKeyReady || strings.Contains(preview.Body.String(), "signature") {
		t.Fatal("preview signed or authorized traffic", preview.Body.String())
	}
	trust := performJSONRequest(t, f.s, http.MethodGet, "/v1/admin/agent-edge/trust", f.admin, nil)
	if trust.Code != 200 || strings.Contains(trust.Body.String(), "private_key") || !strings.Contains(trust.Body.String(), "public_key") {
		t.Fatal("private material exposed", trust.Code)
	}
}

func TestAgentGrantRejectsCallerChosenIdentityAndUnprivilegedPreview(t *testing.T) {
	f := setupAgentAuthorityFixture(t)
	for _, tc := range []struct {
		path, key string
		code      int
	}{
		{"/v1/agent/edge-candidates", f.admin, 403},
		{"/v1/agent/edge-candidates?runtime_id=foreign", f.runtimeKey, 400},
		{"/v1/admin/agent-edge/preview?runtime_id=" + f.runtimeID, f.tenant, 403},
		{"/v1/admin/agent-edge/trust", f.tenant, 403},
		{"/v1/admin/agent-edge/preview?runtime_id=" + f.runtimeID + "&runtime_id=other", f.admin, 400},
	} {
		r := performJSONRequest(t, f.s, http.MethodGet, tc.path, tc.key, nil)
		if r.Code != tc.code {
			t.Fatalf("%s: got %d want %d: %s", tc.path, r.Code, tc.code, r.Body.String())
		}
	}
	if err := os.Remove(f.s.agentEdgeSigningKeyFile); err != nil {
		t.Fatal(err)
	}
	r := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
	if r.Code != 503 {
		t.Fatal("missing independent key did not fail closed", r.Code)
	}
	if _, err := os.Stat(f.s.agentEdgeSigningKeyFile); !os.IsNotExist(err) {
		t.Fatal("request generated a new trust root")
	}
}

func TestAgentGrantRejectsStaleForeignAndIncompleteEvidence(t *testing.T) {
	f := setupAgentAuthorityFixture(t)
	original := f.s.agentEdgeProbe
	for _, scenario := range []string{"stale", "foreign edge", "foreign release", "foreign route", "negative state", "missing path", "short lease"} {
		t.Run(scenario, func(t *testing.T) {
			f.s.agentEdgeProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
				p, err := original(ctx, host, path, address, state, timeout)
				if err != nil {
					return p, err
				}
				switch scenario {
				case "stale":
					p.CheckedAt = time.Now().Add(-3 * time.Minute)
				case "foreign edge":
					p.EdgeID = "foreign"
				case "foreign release":
					b := *p.TrafficRelease
					b.ReleaseID = "foreign"
					p.TrafficRelease = &b
				case "foreign route":
					p.Digest = "sha256:" + strings.Repeat("b", 64)
				case "negative state":
					p.State = "disabled"
				case "missing path":
					if path == "/v1" {
						return routeprobe.Proof{}, errors.New("not serving")
					}
				case "short lease":
					p.ValidUntil = time.Now().Add(5 * time.Second)
				}
				return p, nil
			}
			r := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
			if r.Code != 503 {
				t.Fatal("unsafe evidence signed", r.Code, r.Body.String())
			}
		})
	}
}

func TestAgentGrantRefusesAuthorityChangesDuringObservation(t *testing.T) {
	f := setupAgentAuthorityFixture(t)
	original := f.s.agentEdgeProbe
	var once sync.Once
	f.s.agentEdgeProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		once.Do(func() { p := f.policy; p.Generation = "new-agent-authority"; p.Mode = "active"; f.publishPolicy(t, p) })
		return original(ctx, host, path, address, state, timeout)
	}
	r := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
	if r.Code != 503 {
		t.Fatal("mixed authority observation was signed", r.Code, r.Body.String())
	}
}

func TestAgentGrantPreservesExplicitSurvivorFloorAndRechecksAuthority(t *testing.T) {
	f := setupAgentAuthorityFixture(t)
	original := f.s.agentEdgeProbe
	f.s.agentEdgeProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		if address == "9.9.9.9" {
			return routeprobe.Proof{}, routeprobe.ErrUnavailable
		}
		return original(ctx, host, path, address, state, timeout)
	}
	grant, _, diagnostics, err := f.s.captureAgentEdgeGrant(context.Background(), f.runtimeID, "edge-foreign")
	if err != nil || len(grant.Candidates) != 1 || grant.Candidates[0].EdgeID != "edge-a" || grant.MinimumCandidates != 1 || grant.Policy.DesiredDistinctCells != 2 || diagnostics["edge-b"] == "" {
		t.Fatal("desired redundancy became an implicit availability veto", grant, diagnostics, err)
	}
	if !f.s.agentGrantAuthorityUnchanged(grant) {
		t.Fatal("current authority rejected")
	}
	p := f.policy
	p.Generation = "stricter-agent-policy"
	p.Constraint.MinCandidates = 2
	f.publishPolicy(t, p)
	if f.s.agentGrantAuthorityUnchanged(grant) {
		t.Fatal("superseded policy remained valid for signing")
	}
	if _, _, _, err = f.s.captureAgentEdgeGrant(context.Background(), f.runtimeID, "edge-a"); err == nil {
		t.Fatal("explicit hard minimum was relaxed during outage")
	}
}

func TestAgentConstraintIntersectionNeverWidensExistingRoutePolicy(t *testing.T) {
	base := platformconfig.EdgeSelectionConstraint{OwnerKind: "platform", Hostname: "api.example.test", AllowedPoolIDs: []string{"pool-a", "pool-b"}, RequiredCapabilities: []string{"http", "tls"}, MinCandidates: 1, MinDistinctCells: 1, MinDistinctDomains: map[string]int{"host": 1}, FactMaxAgeSeconds: 120}
	existing := base
	existing.AllowedPoolIDs = []string{"pool-b"}
	existing.AllowedCountries = []string{"de"}
	existing.MinCandidates = 2
	existing.MinDistinctCells = 2
	existing.MinDistinctDomains = map[string]int{"provider": 2}
	existing.FactMaxAgeSeconds = 60
	combined, err := combineAgentConstraints(base, []platformconfig.EdgeSelectionConstraint{existing})
	if err != nil || !reflect.DeepEqual(combined.AllowedPoolIDs, []string{"pool-b"}) || !reflect.DeepEqual(combined.AllowedCountries, []string{"de"}) || combined.MinCandidates != 2 || combined.MinDistinctCells != 2 || combined.MinDistinctDomains["provider"] != 2 || combined.FactMaxAgeSeconds != 60 {
		t.Fatal("signed route constraint weakened", combined, err)
	}
	if len(base.MinDistinctDomains) != 1 {
		t.Fatal("authority policy mutated")
	}
	existing.AllowedPoolIDs = []string{"pool-foreign"}
	if _, err = combineAgentConstraints(base, []platformconfig.EdgeSelectionConstraint{existing}); err == nil {
		t.Fatal("disjoint pools accepted")
	}
}

func TestInitialAgentShadowPolicyCannotAuthorizeActiveTraffic(t *testing.T) {
	f := setupAgentAuthorityFixture(t)
	st, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	p := f.policy
	for _, mode := range []string{"shadow", "active"} {
		p.Mode = mode
		p.Generation = "initial-agent-" + mode
		raw, _ := json.Marshal(p)
		var content map[string]any
		json.Unmarshal(raw, &content)
		a, err := st.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: agentedge.PolicyScope}, Generation: p.Generation, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = st.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "test", Pass: true}}); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err = st.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal()); err != nil {
			t.Fatal(err)
		}
		got, _, release, err := s.currentAgentAuthority()
		if mode == "shadow" && (err != nil || got.Mode != "shadow" || release.ReleaseChannel != "shadow") {
			t.Fatal("initial observational policy unavailable", err)
		}
		if mode == "active" && err == nil {
			t.Fatal("shadow publication authorized active requests")
		}
	}
}
