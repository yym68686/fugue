package api

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
	"reflect"
	"testing"
	"time"
)

// Actual original input and a signed typed dependency graph with a positive
// full-release baseline. This is a restoration-boundary fixture, not a complete
// publication E2E. Receipt clocks remain historical; authority uses real time.
func qualityRestoreInputFixture(t *testing.T, snapshot platformconfig.RuntimeSnapshot) (*Server, model.PlatformArtifact, string) {
	t.Helper()
	path := t.TempDir() + "/state.json"
	st := store.New(path)
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(st, auth.New(st, ""), nil, ServerConfig{BundleSigningKey: "synthetic-quality-input", BundleSigningKeyID: "quality-input"})
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []model.PlatformArtifact{
		{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Generation: "policy-a", Content: map[string]any{"generation": "policy-a", "scope": "global"}},
		{ID: "quality-child", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Generation: "quality-child", Content: map[string]any{"lineage": map[string]any{"input_snapshot_digest": digest}}},
	} {
		artifact.Status = model.PlatformArtifactStatusValidated
		if _, err := st.CreatePlatformArtifact(artifact); err != nil {
			t.Fatal(err)
		}
	}
	parent, err := st.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindReleaseSet,
		Generation: "quality-parent", Status: model.PlatformArtifactStatusValidated, Content: map[string]any{"artifact_ids": []string{"quality-child"}},
		Metadata: map[string]string{"input_snapshot_digest": digest, "intent_generation": "intent-a", "policy_generation": "policy-a"}})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	release := model.PlatformArtifactRelease{ID: "quality-full", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind,
		Scope: parent.Scope, ScopeKey: parent.ScopeKey, Generation: parent.Generation, ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1, VerificationState: model.PlatformArtifactVerificationStateVerified,
		VerifiedLKGGeneration: parent.Generation, VerifiedAt: &at, ReleasedAt: at, UpdatedAt: at}
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "quality-lkg", ArtifactID: parent.ID,
		ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, SchemaVersion: parent.SchemaVersion,
		Generation: parent.Generation, GenerationSequence: parent.GenerationSequence, ContentHash: parent.ContentHash, ArtifactProvenance: parent.Provenance,
		VerifiedByReleaseID: release.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64),
		ExpiresAt: at.Add(time.Hour), CreatedAt: at, UpdatedAt: at}, s.bundleKeyring())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, release)
	state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, lkg)
	raw, _ = json.Marshal(state)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return s, parent, path
}

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
	for _, scenario := range []string{"valid", "intent changed", "lineage changed", "candidates changed", "receipt changed", "selection changed", "policy changed", "expired", "newer entry", "entry cleared during read", "lkg changed during read", "expires before installation", "caller canceled before read", "caller canceled during read", "cold history preserves live entry"} {
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
			server, parent, path := qualityRestoreInputFixture(t, snapshot)
			var previous dynamicQualityEntry
			if scenario == "newer entry" || scenario == "entry cleared during read" || scenario == "cold history preserves live entry" {
				previous = dynamicQualityEntry{ContextDigest: "new-context", Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now}, Evidence: json.RawMessage(`{"current":true}`)}}
				server.dynamicQuality.entries = map[string]dynamicQualityEntry{job.Key: previous}
			}
			if scenario == "cold history preserves live entry" {
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var state model.State
				if err := json.Unmarshal(body, &state); err != nil {
					t.Fatal(err)
				}
				for i, content := range state.PlatformArtifactContents {
					if content.ContentHash == parent.Metadata["input_snapshot_digest"] {
						state.PlatformArtifactContents = append(state.PlatformArtifactContents[:i], state.PlatformArtifactContents[i+1:]...)
						break
					}
				}
				body, _ = json.Marshal(state)
				if err := os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			clockCalls := 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "caller canceled before read" {
				cancel()
			}
			server.restorePublishedQualityEvidenceContext(ctx, projection, []dynamicQualityJob{job}, parent, func() time.Time {
				clockCalls++
				if scenario == "caller canceled during read" && clockCalls == 1 {
					cancel()
				}
				if scenario == "entry cleared during read" && clockCalls == 1 {
					server.dynamicQuality.entries[job.Key] = dynamicQualityEntry{}
				}
				if scenario == "lkg changed during read" && clockCalls == 1 {
					body, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					var state model.State
					if err := json.Unmarshal(body, &state); err != nil {
						t.Fatal(err)
					}
					state.PlatformLKGSnapshots[0].VerificationEvidenceHash = "sha256:" + strings.Repeat("b", 64)
					body, _ = json.Marshal(state)
					if err := os.WriteFile(path, body, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "expires before installation" && clockCalls >= 3 {
					return captured.Add(time.Duration(job.Route.Policy.EvidenceMaxAgeSeconds+1) * time.Second)
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
			if scenario == "newer entry" || scenario == "cold history preserves live entry" {
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
