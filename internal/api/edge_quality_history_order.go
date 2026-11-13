package api

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func publishedQualityOrderMatches(previous *model.DNSPhysicalSelection, record model.EdgeDNSRecord) bool {
	if previous == nil || previous.BusinessAllocation != nil || previous.Scope != "global" || model.ValidateDNSPhysicalSelection(previous) != nil {
		return false
	}
	var order []string
	var observed *time.Time
	switch record.AnswerPolicy.PolicyKind {
	case model.DNSAnswerPolicyKindPhysicalQuality:
		selection := record.AnswerPolicy.PhysicalSelection
		if model.ValidateDNSPhysicalSelection(selection) != nil || selection.BusinessAllocation != nil || selection.Scope != previous.Scope {
			return false
		}
		order, observed = selection.OrderedEdgeIDs, selection.QualityObservedAt
		if observed == nil && selection.QualityState == "measured" {
			observed = &selection.CapturedAt
		}
	case model.DNSAnswerPolicyKindPhysicalOrder:
		physical := record.AnswerPolicy.PhysicalOrder
		if model.ValidateDNSPhysicalOrder(physical) != nil {
			return false
		}
		order, observed = physical.OrderedEdgeIDs, physical.QualityObservedAt
	default:
		return false
	}
	return observed != nil && previous.QualityObservedAt != nil && observed.Equal(*previous.QualityObservedAt) && slices.Equal(order, previous.OrderedEdgeIDs)
}

func (s *Server) bindPublishedQualityOrder(ctx context.Context, snapshot *edgequality.Snapshot, nodeID string, source qualityInputSource) {
	if snapshot.PreviousSelection == nil || snapshot.Scope != "global" {
		return
	}
	parent := source.parent
	if _, err := platformconfig.ValidateReleaseComposition(parent); err != nil {
		return
	}
	var set platformconfig.ReleaseSet
	raw, _ := json.Marshal(parent.Content)
	if json.Unmarshal(raw, &set) != nil {
		return
	}
	for i, kind := range set.ArtifactKinds {
		if kind != model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		member, err := s.store.GetPlatformArtifact(set.ArtifactIDs[i])
		if err != nil || member.ArtifactKind != kind || member.ScopeKey != parent.ScopeKey || member.Metadata["release_set_generation"] != parent.Generation || s.store.VerifyPlatformArtifactIntegrity(member) != nil {
			return
		}
		for _, key := range []string{"input_snapshot_digest", "intent_generation", "policy_generation", "compiler_version", "intent_digest", "policy_digest"} {
			if parent.Metadata[key] == "" || member.Metadata[key] != parent.Metadata[key] {
				return
			}
		}
		var payload struct {
			Views []platformconfig.DNSQueryView `json:"query_views"`
		}
		raw, _ = json.Marshal(member.Content)
		if json.Unmarshal(raw, &payload) != nil {
			return
		}
		matches := 0
		for _, view := range payload.Views {
			if view.NodeID != nodeID {
				continue
			}
			for _, record := range view.Records {
				if record.Name == edgequality.DNSHostname(*snapshot) && record.Type == "A" && publishedQualityOrderMatches(snapshot.PreviousSelection, record) {
					matches++
				}
			}
		}
		if matches == 1 && s.qualityInputSourceUnchanged(ctx, source) && s.checkQualityInputSource(ctx, source) == nil {
			snapshot.HistoricalOrder = &edgequality.PublishedOrderEvidence{SourceArtifactID: parent.ID, InputDigest: parent.Metadata["input_snapshot_digest"], EvidenceDigest: member.ContentHash,
				Selection: model.CloneDNSPhysicalSelection(snapshot.PreviousSelection)}
		}
		return
	}
}
