package platformconfig

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/model"
)

func cachedCellPublicationFixture(t testing.TB, padding int) CellRoutePublicationInput {
	t.Helper()
	req := declaredCellCompileFixture(t)
	req.Intent.PublicationRole, req.Policy.PublicationRole = PublicationRoleCellRoutes, PublicationRoleCellRoutes
	req.Intent.DNSConsumers, req.RuntimeSnapshot.DNSConsumers = nil, nil
	req.Policy.TLSReadiness = &ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxProbes: 4096, MaxConcurrency: 8}
	topology, err := TrafficConsumerTopologyFromIntent(req.Intent)
	if err != nil {
		t.Fatal(err)
	}
	req.Policy.ConsumerTopologyDigest, _ = Digest(topology)
	out, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	parent := BuildReleaseSetArtifact(out.ReleaseSet, []string{"route", "tls"}, req.CreatedAt)
	p := CellRoutePublicationInput{Parent: parent, Route: out.RouteArtifact, TLS: out.TLSArtifact}
	for i, a := range []*model.PlatformArtifact{&p.Parent, &p.Route, &p.TLS} {
		a.ID = []string{"parent", "route", "tls"}[i]
		a.ScopeKey, a.Status, a.GenerationSequence = a.Scope.Key, model.PlatformArtifactStatusValidated, 1
		if i != 0 {
			a.Content["padding"] = strings.Repeat("x", padding)
		}
		a.ContentHash, _ = Digest(a.Content)
	}
	p.Reference = CellRoutePublicationReference{AuthorityCellID: req.Intent.AuthorityCellID, ReleaseSetID: p.Parent.ID, ReleaseSetDigest: p.Parent.ContentHash, ReleaseID: "release", ReleaseChannel: "full", FencingToken: 1, RouteArtifactID: p.Route.ID, RouteArtifactDigest: p.Route.ContentHash, TLSArtifactID: p.TLS.ID, TLSArtifactDigest: p.TLS.ContentHash}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCellRoutePublication(p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCellPublicationCacheRechecksCompleteInput(t *testing.T) {
	p := cachedCellPublicationFixture(t, 32<<10)
	raw, _ := json.Marshal(p)
	if err := validateDNSCellRoutePublication(p, len(raw)); err != nil {
		t.Fatal(err)
	}
	key, ok := dnsCellPublicationValidationKey(p)
	if !ok || !dnsCellPublicationValidations.contains(key) {
		t.Fatal("valid large publication not remembered")
	}
	for _, member := range []string{"parent", "route", "tls"} {
		for _, change := range []string{"content", "digest", "id", "kind", "scope", "scope object", "status", "schema", "sequence", "generation", "lineage", "release metadata"} {
			t.Run(member+"/"+change, func(t *testing.T) {
				var v CellRoutePublicationInput
				if err := json.Unmarshal(raw, &v); err != nil {
					t.Fatal(err)
				}
				a := map[string]*model.PlatformArtifact{"parent": &v.Parent, "route": &v.Route, "tls": &v.TLS}[member]
				switch change {
				case "content":
					a.Content["padding"] = "changed"
				case "digest":
					a.ContentHash = "sha256:" + strings.Repeat("f", 64)
				case "id":
					a.ID = "foreign"
				case "kind":
					a.ArtifactKind = "foreign"
				case "scope":
					a.ScopeKey = "foreign"
				case "scope object":
					a.Scope.Key = "foreign"
				case "status":
					a.Status = "rejected"
				case "schema":
					a.SchemaVersion = "foreign"
				case "sequence":
					a.GenerationSequence = 0
				case "generation":
					a.Generation += "-changed"
				case "lineage":
					a.Metadata["policy_digest"] = "changed"
				case "release metadata":
					a.Metadata["release_set_generation"] = "changed"
				}
				before, _ := json.Marshal(v)
				got, want := validateDNSCellRoutePublication(v, len(raw)), ValidateCellRoutePublication(v)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("cache changed rejection: got=%v want=%v", got, want)
				}
				next, ok := dnsCellPublicationValidationKey(v)
				if ok && next == key {
					t.Fatal("mutation reused original identity")
				}
				if want != nil && ok && dnsCellPublicationValidations.contains(next) {
					t.Fatal("rejected publication was cached")
				}
				after, _ := json.Marshal(v)
				if string(before) != string(after) {
					t.Fatal("validation mutated input")
				}
			})
		}
	}
	for _, change := range []func(*CellRoutePublicationInput){
		func(v *CellRoutePublicationInput) { v.Reference.FencingToken = 0 },
		func(v *CellRoutePublicationInput) { v.Reference.RouteArtifactDigest = "invalid" },
		func(v *CellRoutePublicationInput) {
			v.ProducerPolicy = &model.PlatformPublicationPrecondition{ArtifactID: "policy"}
		},
	} {
		var v CellRoutePublicationInput
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		change(&v)
		got, want := validateDNSCellRoutePublication(v, len(raw)), ValidateCellRoutePublication(v)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("reference validation changed: %v %v", got, want)
		}
		if next, ok := dnsCellPublicationValidationKey(v); ok && next == key {
			t.Fatal("reference or producer policy omitted")
		}
	}
	// A claim-preserving custom marshaler must execute only in the original
	// validator. Fingerprinting must not invoke it or remember its result.
	calls := 0
	p.Route.Content["padding"] = countedDNSPlanJSON{raw: []byte(`"changed"`), calls: &calls}
	if _, ok := dnsCellPublicationValidationKey(p); ok || calls != 0 {
		t.Fatal("cache probe invoked custom marshaler")
	}
	if err := validateDNSCellRoutePublication(p, len(raw)); err == nil || calls != 1 {
		t.Fatalf("custom content did not use original validation: %v calls=%d", err, calls)
	}
}

func TestCellPublicationCacheSmallAndThresholdBoundary(t *testing.T) {
	p := cachedCellPublicationFixture(t, 32<<10)
	for _, size := range []int{0, (64 << 10) - 1, 64 << 10} {
		p.Reference.ReleaseID = fmt.Sprintf("boundary-%d", size)
		key, ok := dnsCellPublicationValidationKey(p)
		if !ok || dnsCellPublicationValidations.contains(key) {
			t.Fatal("test identity already remembered")
		}
		if err := validateDNSCellRoutePublication(p, size); err != nil {
			t.Fatal(err)
		}
		if dnsCellPublicationValidations.contains(key) != (size >= 64<<10) {
			t.Fatal("small envelope entered cache")
		}
	}
	// The full actual tree is always re-keyed even if its claimed hash and ID
	// are unchanged; a coherent changed tree may pass its own ordinary checks.
	p.Route.Content["padding"] = strings.Repeat("y", 32<<10)
	p.Route.ContentHash, _ = Digest(p.Route.Content)
	p.Reference.RouteArtifactDigest = p.Route.ContentHash
	if err := validateDNSCellRoutePublication(p, 128<<10); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCellPublicationValidationReuse(b *testing.B) {
	for _, padding := range []int{0, 32 << 10, 256 << 10} {
		p := cachedCellPublicationFixture(b, padding)
		raw, _ := json.Marshal(p)
		for _, mode := range []string{"baseline", "cold", "warm"} {
			b.Run(fmt.Sprintf("padding-%d/%s", padding, mode), func(b *testing.B) {
				v := p
				if err := validateDNSCellRoutePublication(v, len(raw)); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if mode == "cold" {
						v.Reference.ReleaseID = fmt.Sprintf("release-%d", i)
					}
					var err error
					if mode == "baseline" {
						err = ValidateCellRoutePublication(v)
					} else {
						err = validateDNSCellRoutePublication(v, len(raw))
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
