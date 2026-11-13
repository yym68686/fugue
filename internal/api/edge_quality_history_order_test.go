package api

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func TestHistoricalPublishedOrderRequiresExactActualPredecessorAndOriginalTime(t *testing.T) {
	receipt := historicalBoundReceiptFixture(t)
	previous, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	at := previous.CapturedAt.Add(-48 * time.Hour)
	previous.QualityState, previous.QualityObservedAt = "retained", &at
	for _, scenario := range []string{"match", "order changed", "quality time changed", "missing time", "business", "scope changed"} {
		t.Run(scenario, func(t *testing.T) {
			order := &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: append([]string(nil), previous.OrderedEdgeIDs...), QualityObservedAt: &at}
			current := model.CloneDNSPhysicalSelection(previous)
			switch scenario {
			case "order changed":
				order.OrderedEdgeIDs = []string{"different"}
			case "quality time changed":
				changed := at.Add(time.Second)
				order.QualityObservedAt = &changed
			case "missing time":
				order.QualityObservedAt = nil
			case "business":
				current.BusinessAllocation = &model.DNSBusinessAllocation{}
			case "scope changed":
				current.Scope = "tcp_peer:198.51.100.0/24"
			}
			record := model.EdgeDNSRecord{AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: order}}
			if got := publishedQualityOrderMatches(current, record); got != (scenario == "match") {
				t.Fatal("unrelated order lent history", got)
			}
		})
	}
}

func TestHistoricalPublishedOrderBindsVerifiedDNSMemberWithoutProbeTranscripts(t *testing.T) {
	receipt := historicalBoundReceiptFixture(t)
	previous, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	at := previous.CapturedAt
	previous.QualityState, previous.QualityObservedAt = "retained", &at
	s, old, path := qualityRestoreInputFixture(t, platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "policy-a"})
	metadata := map[string]string{}
	for k, v := range old.Metadata {
		metadata[k] = v
	}
	metadata["compiler_version"], metadata["intent_digest"], metadata["policy_digest"] = platformconfig.CompilerVersion, previous.LoadedDigest, previous.PolicyDigest
	metadata["release_set_generation"] = "order-parent"
	order := &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: append([]string(nil), previous.OrderedEdgeIDs...), QualityObservedAt: &at}
	record := model.EdgeDNSRecord{Name: receipt.Snapshot.Hostname, Type: "A", AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: order}}
	ids, kinds := []string{}, platformconfig.PublicationArtifactKinds("")
	for _, kind := range kinds {
		content := map[string]any{"lineage": map[string]any{"input_snapshot_digest": metadata["input_snapshot_digest"]}}
		if kind == model.PlatformArtifactKindDNSAnswerBundle {
			content["query_views"] = []platformconfig.DNSQueryView{{NodeID: "dns-a", Records: []model.EdgeDNSRecord{record}}}
		}
		member, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Generation: "order-" + kind, Status: model.PlatformArtifactStatusValidated, Metadata: metadata, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, member.ID)
	}
	parent, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: old.ArtifactKind, Generation: "order-parent", Status: model.PlatformArtifactStatusValidated, Metadata: metadata, Content: map[string]any{"artifact_ids": ids, "artifact_kinds": kinds}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state model.State
	if json.Unmarshal(body, &state) != nil {
		t.Fatal("state")
	}
	release := &state.PlatformArtifactReleases[0]
	release.ArtifactID, release.Generation, release.VerifiedLKGGeneration = parent.ID, parent.Generation, parent.Generation
	lkg := state.PlatformLKGSnapshots[0]
	lkg.ArtifactID, lkg.Generation, lkg.GenerationSequence, lkg.ContentHash, lkg.ArtifactProvenance = parent.ID, parent.Generation, parent.GenerationSequence, parent.ContentHash, parent.Provenance
	lkg, err = platformsafety.SignPlatformLKGSnapshot(lkg, s.bundleKeyring())
	if err != nil {
		t.Fatal(err)
	}
	state.PlatformLKGSnapshots = []model.PlatformLKGSnapshot{lkg}
	body, _ = json.Marshal(state)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	current := receipt.Snapshot
	current.Policy = edgequality.DefaultPairedContinuityPolicy()
	current.PreviousSelection = model.CloneDNSPhysicalSelection(previous)
	current.CurrentEdgeID = previous.PrimaryEdgeID
	current.CapturedAt = current.CapturedAt.Add(48 * time.Hour)
	s.bindHistoricalQualityEvidence(t.Context(), &current, "dns-a")
	if current.HistoricalEvidence != nil || current.HistoricalOrder == nil || current.HistoricalOrder.SourceArtifactID != parent.ID {
		t.Fatal("verified order lost when original input contains no measurements", current.HistoricalOrder)
	}
	current.HistoricalOrder = nil
	current.PreviousSelection.OrderedEdgeIDs = []string{previous.PrimaryEdgeID}
	s.bindHistoricalQualityEvidence(t.Context(), &current, "dns-a")
	if current.HistoricalOrder != nil {
		t.Fatal("different actual predecessor borrowed published order")
	}
}

func TestHistoricalPublishedOrderFallbackPreservesOldClockOnlyFromVerifiedBaseline(t *testing.T) {
	for _, verified := range []bool{false, true} {
		projection, nodes, policy, now := directQueryFixture()
		at := time.Now().UTC().Add(-48 * time.Hour)
		policy.ECSEnabled, policy.ExplorationPercent = false, 0
		policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}, QualityObservedAt: &at}}
		policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultPairedContinuityPolicy(), RefreshQueriesPerCycle: 4, RefreshConcurrency: 2}
		availability := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
		policy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: availability}}
		if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
			t.Fatal(err)
		}
		for i := range projection.Policy.DNSAnswerRules {
			if rule := &projection.Policy.DNSAnswerRules[i]; rule.Hostname == "app.example.test" {
				rule.PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}, QualityObservedAt: &at}
			}
		}
		if verified {
			for i := range projection.RuntimeSnapshot.DNSSelections {
				projection.RuntimeSnapshot.DNSSelections[i].Reason = "physical_quality_learning_preserved_verified_order"
			}
		}
		if err := applyDNSAvailabilityBaseline(&projection, "app.example.test", &availability); err != nil {
			t.Fatal(err)
		}
		for _, rule := range projection.Policy.DNSAnswerRules {
			if rule.Hostname != "app.example.test" {
				continue
			}
			want := "edge-a"
			if verified {
				want = "edge-b"
			}
			if rule.PhysicalOrder == nil || rule.PhysicalOrder.OrderedEdgeIDs[0] != want || (rule.PhysicalOrder.QualityObservedAt != nil) != verified {
				t.Fatal("old verified order expired or unauthenticated timestamp bypassed retention", verified, rule.PhysicalOrder)
			}
			if verified && !rule.PhysicalOrder.QualityObservedAt.Equal(at) {
				t.Fatal("fallback renewed old timestamp")
			}
		}
	}
}
