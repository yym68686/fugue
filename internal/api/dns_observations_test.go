package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSObservationsUseVerifiedPolicyAndNeverPublishArtifacts(t *testing.T) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	server.edgeQualityRankingMode = "legacy" // Ambient disabled mode cannot disable signed policy.
	now := time.Now().UTC()
	host := "rank.example.test"
	samples := []model.EdgePerformanceSample{}
	for i := 0; i < 2; i++ {
		samples = append(samples, model.EdgePerformanceSample{ID: fmt.Sprintf("sample-%d", i), Hostname: host, EdgeGroupID: fmt.Sprintf("edge-group-%d", i), ClientCountry: "aa", TTFBMS: int64(100 + i*600), UpstreamMS: 50, TotalMS: int64(150 + i*600), StatusCode: 200, SampleCount: 50, SampledAt: now.Add(-time.Minute)})
	}
	if err := state.RecordEdgePerformanceSamples(samples, time.Time{}); err != nil {
		t.Fatal(err)
	}
	seed := model.EdgeDNSRoutingDecision{Hostname: host, ScopeKey: "global", SelectedEdgeGroupID: "edge-group-1", SwitchedAt: now.Add(-120 * time.Second), CooldownUntil: now.Add(time.Hour), CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Minute)}
	if err := state.UpsertEdgeDNSRoutingDecisions([]model.EdgeDNSRoutingDecision{seed}); err != nil {
		t.Fatal(err)
	}
	original, err := state.ListEdgeDNSRoutingDecisions("")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.reconcileDNSObservations(context.Background(), now); err == nil {
		t.Fatal("ambient settings replaced missing verified policy")
	}
	unchanged, _ := state.ListEdgeDNSRoutingDecisions("")
	if !reflect.DeepEqual(unchanged, original) {
		t.Fatal("missing policy changed ranking facts")
	}
	makePolicy := func(gen, mode string, cooldown int, verified bool) model.PlatformArtifact {
		t.Helper()
		p := platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{Generation: gen, Scope: "global", DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: mode, PreferenceMode: "runtime_locality", SwitchCooldownSeconds: cooldown, MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}})
		b, _ := json.Marshal(p)
		content := map[string]any{}
		json.Unmarshal(b, &content)
		a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: gen, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		a, err = state.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "synthetic policy", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		if verified {
			if prior, _ := state.GetPlatformLKG(model.PlatformArtifactKindPolicySnapshot, "global"); prior == nil {
				seedVerifiedPlatformArtifactAPI(t, server, admin, a.ID)
			} else {
				releaseAndVerifyFullPlatformArtifactAPI(t, server, admin, a.ID)
			}
		}
		return a
	}
	makePolicy("draft", "active", 0, false)
	if err := server.reconcileDNSObservations(context.Background(), now); err == nil {
		t.Fatal("unverified draft authorized observations")
	}
	makePolicy("active", "active", 180, true)
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.reconcileDNSObservations(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	decisions, _ := state.ListEdgeDNSRoutingDecisions(host)
	var global model.EdgeDNSRoutingDecision
	for _, d := range decisions {
		if d.ScopeKey == "global" {
			global = d
		}
	}
	if global.SelectedEdgeGroupID != "edge-group-1" || !global.CooldownUntil.Equal(seed.SwitchedAt.Add(180*time.Second)) {
		t.Fatal("signed cooldown ignored original switch time", global)
	}
	after, _ := state.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 1000})
	if !reflect.DeepEqual(after, before) {
		t.Fatal("observation created or mutated artifacts")
	}
	for _, kind := range []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindDNSAnswerBundle} {
		a, r, found, err := state.GetActivePlatformArtifact(kind, "global", "full")
		if err != nil || found || a.ID != "" || r.ID != "" {
			t.Fatal("observer published serving state", kind, err)
		}
	}
	makePolicy("shorter", "active", 60, true)
	if err := server.reconcileDNSObservations(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	decisions, _ = state.ListEdgeDNSRoutingDecisions(host)
	for _, d := range decisions {
		if d.ScopeKey == "global" && d.SelectedEdgeGroupID != "edge-group-0" {
			t.Fatal("new verified policy did not shorten cooldown", d)
		}
	}
	makePolicy("disabled", "disabled", 0, true)
	beforeDecisions, _ := state.ListEdgeDNSRoutingDecisions("")
	server.edgeQualityRankingMode = "active"
	if err := server.reconcileDNSObservations(context.Background(), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	afterDecisions, _ := state.ListEdgeDNSRoutingDecisions("")
	if !reflect.DeepEqual(beforeDecisions, afterDecisions) {
		t.Fatal("ambient ranking activated disabled policy")
	}
	server.bundleSigningKey = "wrong-key"
	if _, _, err := server.verifiedDNSObservationPolicy(); err == nil {
		t.Fatal("invalid policy/LKG signature accepted")
	}
	// Serving endpoint remains retired after observation refreshes.
	response := performJSONRequest(t, server, http.MethodGet, "/v1/edge/dns?token=edge-secret&answer_ip=192.0.2.1", "", nil)
	if response.Code != http.StatusGone {
		t.Fatal(response.Code, response.Body.String())
	}
}

func TestDNSObservationLoopHonorsCancellation(t *testing.T) {
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { s.StartBackgroundDNSObservations(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled observation loop blocked")
	}
	if s.dnsObservation.runs != 0 {
		t.Fatal("canceled observer wrote facts")
	}
}
