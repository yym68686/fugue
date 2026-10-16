package api

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
	"reflect"
	"testing"
	"time"
)

func TestDynamicQualityRestoreRejectsUnneededEvidence(t *testing.T) {
	now := time.Now().UTC()
	base := store.CompilerDNSSelectionSummary{HasSelection: true, HasEvidence: true, CapturedAt: now.Add(-time.Minute)}
	job := dynamicQualityJob{Route: platformconfig.PhysicalQualityRoute{Policy: model.PhysicalEdgeQualityPolicy{EvidenceMaxAgeSeconds: 120}}}
	for _, tc := range []struct {
		name    string
		summary store.CompilerDNSSelectionSummary
		entry   dynamicQualityEntry
		want    bool
	}{
		{name: "cold", summary: base, want: true},
		{name: "older", summary: base, entry: dynamicQualityEntry{Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now.Add(-90 * time.Second)}}}, want: true},
		{name: "same", summary: base, entry: dynamicQualityEntry{Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: base.CapturedAt}}}},
		{name: "newer", summary: base, entry: dynamicQualityEntry{Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now}}}},
		{name: "no receipt", summary: store.CompilerDNSSelectionSummary{HasSelection: true, CapturedAt: base.CapturedAt}},
		{name: "no selection", summary: store.CompilerDNSSelectionSummary{HasEvidence: true, CapturedAt: base.CapturedAt}},
		{name: "expired", summary: store.CompilerDNSSelectionSummary{HasSelection: true, HasEvidence: true, CapturedAt: now.Add(-120*time.Second - time.Nanosecond)}},
		{name: "expiry boundary", summary: store.CompilerDNSSelectionSummary{HasSelection: true, HasEvidence: true, CapturedAt: now.Add(-120 * time.Second)}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dynamicQualityRestoreNeeded(tc.summary, job, tc.entry, now); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
func TestDynamicQualitySelectiveRestoreRetainsEvidenceGuards(t *testing.T) {
	for _, scenario := range []string{"valid", "intent changed", "lineage changed", "candidates changed", "receipt changed", "selection changed", "policy changed", "expired", "newer entry", "entry cleared during read"} {
		t.Run(scenario, func(t *testing.T) {
			receipt, route := businessPublicationFixture(t)
			route.BusinessExploration = nil
			route.PathPrefix = ""
			captured := receipt.Snapshot.CapturedAt
			selection, err := compileBoundPhysicalQualitySelection(receipt, captured)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(receipt)
			projection, _, _, _ := physicalQueryProjectionFixture(t)
			projection.Intent.Generation = "intent-a"
			fact := projection.RuntimeSnapshot.DNSSelections[0]
			fact.PhysicalEvidence = raw
			fact.PhysicalSelection = selection
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "policy-a", DNSSelections: []platformconfig.DNSSelectionObservation{fact}}
			now := captured.Add(time.Second)
			job := dynamicQualityJob{NodeID: fact.NodeID, Key: fact.NodeID + "\x00" + fact.Hostname, EvidenceHostname: route.Hostname, Route: route}
			switch scenario {
			case "intent changed":
				projection.Intent.Generation = "other-intent"
			case "lineage changed":
				snapshot.PolicyGeneration = "other-policy"
			case "candidates changed":
				projection.RuntimeSnapshot.DNSSelections[0].Candidates = nil
			case "receipt changed":
				receipt.Snapshot.Hostname = "foreign.example.test"
				snapshot.DNSSelections[0].PhysicalEvidence, _ = json.Marshal(receipt)
			case "selection changed":
				selection.PrimaryEdgeID = "foreign-edge"
			case "policy changed":
				job.Route.Policy.EvidenceMaxAgeSeconds++
			case "expired":
				now = captured.Add(time.Duration(route.Policy.EvidenceMaxAgeSeconds+1) * time.Second)
			}
			state := store.New(t.TempDir() + "/state.json")
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Fatal(err)
			}
			server := &Server{store: state}
			var previous dynamicQualityEntry
			if scenario == "newer entry" || scenario == "entry cleared during read" {
				previous = dynamicQualityEntry{ContextDigest: "new-context", Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now}, Evidence: json.RawMessage(`{"current":true}`)}}
				server.dynamicQuality.entries = map[string]dynamicQualityEntry{job.Key: previous}
			}
			parent := model.PlatformArtifact{Metadata: map[string]string{"input_snapshot_digest": digest, "intent_generation": "intent-a", "policy_generation": "policy-a"}}
			clockCalls := 0
			server.restorePublishedQualityEvidenceAt(projection, []dynamicQualityJob{job}, parent, func() time.Time {
				clockCalls++
				if scenario == "entry cleared during read" && clockCalls == 1 {
					server.dynamicQuality.entries[job.Key] = dynamicQualityEntry{}
				}
				return now
			})
			got := server.dynamicQuality.entries[job.Key]
			if scenario == "valid" || scenario == "entry cleared during read" {
				if got.Compiled.Selection == nil || got.ContextDigest == "" || !got.Compiled.Selection.CapturedAt.Equal(captured) {
					t.Fatal("valid receipt was not restored")
				}
				return
			}
			if scenario == "newer entry" {
				if !reflect.DeepEqual(got, previous) {
					t.Fatal("newer evidence replaced")
				}
				return
			}
			if got.Compiled.Selection != nil {
				t.Fatalf("invalid evidence restored: %s", scenario)
			}
		})
	}
}
