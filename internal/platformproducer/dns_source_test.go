package platformproducer_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

func dnsProducerFixture(t *testing.T) (platformproducer.Policy, platformconfig.CompileRequest, platformconfig.CompileRequest) {
	t.Helper()
	req, _, _ := celldns.SourceAuthorizedRequest(t, true)
	_, source := celldns.TransitionRequestWithSource(t)
	celldns.RetainGlobalOnly(&req)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-producer-one", Mode: "serving", InputSource: platformproducer.DNSProducerInputSource,
		PublicationRole: platformconfig.PublicationRoleCellDNS, AuthorityCellID: "cell-dns", TargetScope: "authority-cell:cell-dns", IntervalSeconds: 30, RefreshSeconds: 120,
		Serving:   &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 60, FullMinSeconds: 60, RolloutTimeoutSeconds: 300},
		DNSSource: &platformproducer.DNSProducerSource{ProducerPolicy: *req.PreviousTrafficPublication.ProducerPolicy, PreviousTopology: req.Intent.RouteAuthorityTransition.PreviousTopology, DNSNodeIDs: []string{"dns-a"}}}
	return p, req, source
}

func TestDNSProducerPreservesCompleteVerifiedBehavior(t *testing.T) {
	p, req, source := dnsProducerFixture(t)
	before, _ := json.Marshal(source)
	projected, err := platformproducer.ComposeDNSProjection(p, *req.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	compiled := celldns.Compile(t, projected)
	if err := platformproducer.ValidateDNSProducerOutput(p, compiled.DNSArtifact); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(compiled.ReleaseSet.ArtifactKinds, []string{model.PlatformArtifactKindDNSAnswerBundle}) {
		t.Fatal("producer emitted route/TLS execution")
	}
	if !reflect.DeepEqual(source.RuntimeSnapshot.CapturedAt, projected.RuntimeSnapshot.CapturedAt) {
		t.Fatal("renewed source observation")
	}
	after, _ := json.Marshal(source)
	if string(before) != string(after) {
		t.Fatal("mutated signed source inputs")
	}
	replay, err := platformproducer.ComposeDNSProjection(p, *req.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
	if err != nil || !reflect.DeepEqual(projected, replay) {
		t.Fatal("projection is not deterministic", err)
	}
	for name, mutate := range map[string]func(*platformconfig.CompileRequest){
		"stale deadline":  func(r *platformconfig.CompileRequest) { r.Policy.MaxStaleSeconds-- },
		"proof freshness": func(r *platformconfig.CompileRequest) { r.Policy.DNSReadiness.FactFreshnessSeconds-- },
		"TTL":             func(r *platformconfig.CompileRequest) { r.Policy.DNSAnswerRules[0].TTLSeconds++ },
		"unrelated record": func(r *platformconfig.CompileRequest) {
			r.Intent.DNS = append(r.Intent.DNS, platformconfig.DNSIntent{Hostname: "other.example.test", Type: "TXT", Values: []string{"unexpected"}, TTL: 60})
		},
		"query policy": func(r *platformconfig.CompileRequest) { r.Policy.DNSQueryPolicy.MinimumTTLSeconds++ },
		"nameserver": func(r *platformconfig.CompileRequest) {
			r.Policy.DNSAuthorities[0].Nameservers = []string{"wrong.example.test"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fresh, err := platformproducer.ComposeDNSProjection(p, *req.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&fresh)
			c := celldns.Compile(t, fresh)
			if platformproducer.ValidateDNSProducerOutput(p, c.DNSArtifact) == nil {
				t.Fatal("accepted unauthorized behavior change")
			}
		})
	}
}

func TestDNSProducerPolicyRequiresExplicitSourceAndBounds(t *testing.T) {
	for _, scenario := range []string{"valid", "paused", "shadow", "global", "missing source", "other source", "business inputs", "membership", "alias", "mixed authority", "missing serving", "single", "interval", "false pin", "empty nodes", "source null"} {
		t.Run(scenario, func(t *testing.T) {
			p, _, _ := dnsProducerFixture(t)
			switch scenario {
			case "paused", "shadow":
				p.Mode = scenario
			case "global":
				p.TargetScope, p.AuthorityCellID = "global", ""
			case "missing source":
				p.DNSSource = nil
			case "other source":
				p.InputSource = "business-static-intent"
			case "business inputs":
				p.StaticIntentArtifactID = "foreign"
			case "membership":
				p.DNSSource.DNSNodeIDs = []string{"dns-a", "dns-a"}
			case "alias":
				p.DNSSource.PreviousTopology.Cells[0].LegacyGroupID = ""
			case "mixed authority":
				p.DNSSource.PreviousTopology.Cells[0].ID = p.AuthorityCellID
			case "missing serving":
				p.Serving = nil
			case "single":
				p.Serving.SinglePublication = true
			case "interval":
				p.IntervalSeconds = 1
			case "false pin":
				p.DNSSource.ProducerPolicy.FencingToken = 0
			case "empty nodes":
				p.DNSSource.DNSNodeIDs = nil
			}
			raw, _ := json.Marshal(p)
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: "platform-config-producer:cell-dns", Generation: p.Generation}
			json.Unmarshal(raw, &a.Content)
			if scenario == "source null" {
				a.Content["dns_source"] = nil
			}
			_, err := platformproducer.Decode(a)
			if (err == nil) != (scenario == "valid" || scenario == "paused" || scenario == "shadow") {
				t.Fatal("unexpected admission", err)
			}
		})
	}
}

func TestDNSProducerRejectsChangedSourceLineageAndMembership(t *testing.T) {
	for _, scenario := range []string{"intent", "policy", "producer", "node", "endpoint", "candidate"} {
		t.Run(scenario, func(t *testing.T) {
			p, req, source := dnsProducerFixture(t)
			switch scenario {
			case "intent":
				source.Intent.DNS[0].TTL++
			case "policy":
				source.Policy.MaxStaleSeconds++
			case "producer":
				req.PreviousTrafficPublication.ProducerPolicy.ReleaseID = "unapproved"
			case "node":
				p.DNSSource.DNSNodeIDs = []string{"other"}
			case "endpoint":
				source.RuntimeSnapshot.DNSEdgeEndpoints[0].EdgeID = "other"
			case "candidate":
				source.RuntimeSnapshot.DNSSelections[0].Candidates[0].EdgeID = "other"
			}
			if _, err := platformproducer.ComposeDNSProjection(p, *req.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot); err == nil {
				t.Fatal("changed source accepted")
			}
		})
	}
}

func TestDNSProducerFollowsNewVerifiedRecordsWithoutEditingRoutes(t *testing.T) {
	p, req, source := dnsProducerFixture(t)
	first, err := platformproducer.ComposeDNSProjection(p, *req.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	// The source publisher owns this change. The DNS producer may project it
	// only after the resulting complete source has passed its own verification.
	source.Intent.Generation = "new-record-source"
	source.Intent.DNS = append(source.Intent.DNS, platformconfig.DNSIntent{Hostname: "new.example.test", Type: "TXT", Values: []string{"verified-value"}, TTL: 120})
	source.RuntimeSnapshot.IntentGeneration = source.Intent.Generation
	c, err := platformconfig.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	previous := *req.PreviousTrafficPublication
	previous.Route = celldns.Sign(t, c.RouteArtifact, "new-route", 2)
	previous.TLS = celldns.Sign(t, c.TLSArtifact, "new-tls", 2)
	previous.DNS = celldns.Sign(t, c.DNSArtifact, "new-dns", 2)
	parent := platformconfig.BuildReleaseSetArtifact(c.ReleaseSet, []string{previous.Route.ID, previous.DNS.ID, previous.TLS.ID}, source.CreatedAt)
	parent.Metadata[platformproducer.PolicyReleaseMetadata] = previous.ProducerPolicy.ReleaseID
	previous.Parent = celldns.Sign(t, parent, "new-parent", 2)
	previous.Reference = platformconfig.PreviousTrafficPublicationReference{ReleaseSetID: previous.Parent.ID, ReleaseSetDigest: previous.Parent.ContentHash, ReleaseID: "new-full", FencingToken: 2, RouteArtifactID: previous.Route.ID, RouteArtifactDigest: previous.Route.ContentHash, TLSArtifactID: previous.TLS.ID, TLSArtifactDigest: previous.TLS.ContentHash, DNSArtifactID: previous.DNS.ID, DNSArtifactDigest: previous.DNS.ContentHash}
	next, err := platformproducer.ComposeDNSProjection(p, previous, source.Intent, source.Policy, source.RuntimeSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if first.Intent.Generation == next.Intent.Generation {
		t.Fatal("new source did not produce a new immutable identity")
	}
	compiled := celldns.Compile(t, next)
	if err := platformproducer.ValidateDNSProducerOutput(p, compiled.DNSArtifact); err != nil {
		t.Fatal(err)
	}
	payload, err := platformconfig.DecodeDNSArtifact(compiled.DNSArtifact)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range payload.Records {
		if record.Hostname == "new.example.test" && record.TTL == 120 && reflect.DeepEqual(record.Values, []string{"verified-value"}) {
			found = true
		}
	}
	if !found || len(next.Intent.Routes) != 0 || len(next.Intent.TLS) != 0 {
		t.Fatal("successor lost new DNS intent or acquired route execution")
	}
}
