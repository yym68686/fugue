package api

import (
	"context"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

// Migration fallback only: inspect a bounded recent verified history when a
// serving publication has already discarded its raw comparison. Never search
// arbitrary artifacts, restore cold storage, or borrow an old serving lease.
// Once carried forward, the independent audit field avoids this search.
func (s *Server) preparePriorQualityHistory(ctx context.Context, p *historicalQualityPreparation, requested map[string]bool, remaining *int) {
	missing := map[string]bool{}
	for _, summary := range p.input.Omitted {
		key := summary.NodeID + "\x00" + summary.Hostname
		if summary.Type == "A" && (len(requested) == 0 || requested[key]) {
			missing[key] = true
		}
	}
	for key, facts := range p.facts {
		missing[key] = true
		for _, fact := range facts {
			if len(retainedPhysicalHistory(fact.fact, nil)) > 0 {
				delete(missing, key)
				break
			}
		}
	}
	if len(missing) == 0 || *remaining <= 0 {
		return
	}
	// This optional migration cannot consume the main preparation's entire
	// deadline or repeatedly stall a normal live refresh on unavailable history.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	history, err := s.store.ListPlatformLKGHistory(model.PlatformArtifactKindReleaseSet, p.source.parent.ScopeKey, 8)
	if err != nil {
		return
	}
	for _, lkg := range history {
		if ctx.Err() != nil || len(missing) == 0 || *remaining <= 0 {
			return
		}
		if lkg.ArtifactID == p.source.parent.ID || lkg.UpdatedAt.After(p.source.lkg.UpdatedAt) {
			continue
		}
		parent, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
		if err != nil || parent.ScopeKey != p.source.parent.ScopeKey || parent.ArtifactKind != model.PlatformArtifactKindReleaseSet ||
			s.store.VerifyPlatformArtifactIntegrity(parent) != nil || lkg.SnapshotProvenance.SignedAt.IsZero() ||
			lkg.SnapshotProvenance.SignedAt.After(time.Now()) ||
			!platformsafety.EvaluatePlatformLKGSnapshot(lkg, parent, s.bundleKeyring(), lkg.SnapshotProvenance.SignedAt).Pass {
			continue
		}
		release, err := s.store.GetPlatformArtifactRelease(lkg.VerifiedByReleaseID)
		if err != nil || release.ArtifactID != parent.ID || release.Generation != parent.Generation || release.ScopeKey != parent.ScopeKey ||
			release.ArtifactKind != parent.ArtifactKind || release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull ||
			release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.VerifiedLKGGeneration != parent.Generation ||
			release.VerifiedAt == nil || release.VerifiedAt.After(time.Now()) ||
			(release.Status != model.PlatformArtifactReleaseStatusActive && release.Status != model.PlatformArtifactReleaseStatusSuperseded) {
			continue
		}
		// Pin the CURRENT positive authority separately from the historical
		// immutable source. Expired history can explain an order, never serve it.
		if s.store.CheckPlatformInputRead(ctx, []store.PlatformInputReadSelection{
			{ArtifactID: p.source.parent.ID, ContentHash: p.source.parent.ContentHash, ReleaseID: p.source.release.ID,
				LKGKind: p.source.lkg.ArtifactKind, LKGScope: p.source.lkg.ScopeKey, LKG: &p.source.lkg},
			{ArtifactID: parent.ID, ContentHash: parent.ContentHash, ReleaseID: release.ID, OriginalInput: true},
		}) != nil {
			continue
		}
		digest, err := originalQualityInputDigest(parent)
		if err != nil {
			continue
		}
		input, err := s.store.GetPlatformCompilerInputDNSSelectionsContext(ctx, digest, boundedHistoricalQualitySelector(missing, remaining))
		if err != nil || input.IntentGeneration != parent.Metadata["intent_generation"] || input.PolicyGeneration != parent.Metadata["policy_generation"] ||
			!s.qualityInputSourceUnchanged(ctx, p.source) {
			continue
		}
		for _, fact := range input.DNSSelections {
			if len(retainedPhysicalHistory(fact, nil)) == 0 {
				continue
			}
			key := fact.NodeID + "\x00" + fact.Hostname
			p.facts[key] = append(p.facts[key], historicalQualityFact{fact: fact, artifactID: parent.ID, inputDigest: digest,
				priorSource: &qualityInputSource{parent: parent, lkg: lkg, release: release}})
		}
	}
}

func (s *Server) historicalQualityFactUnchanged(ctx context.Context, fact historicalQualityFact) bool {
	if ctx.Err() != nil {
		return false
	}
	if fact.priorSource == nil {
		return true // Current source is independently rechecked by the caller.
	}
	source := fact.priorSource
	parent, err := s.store.GetPlatformArtifact(source.parent.ID)
	if err != nil || !reflect.DeepEqual(parent, source.parent) {
		return false
	}
	release, err := s.store.GetPlatformArtifactRelease(source.release.ID)
	return err == nil && reflect.DeepEqual(release, source.release) && ctx.Err() == nil
}
