package api

import (
	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Replaying a DNS compilation may read immutable artifacts, but may not endorse
// an obsolete, shadow or foreign Cell publication as its serving dependency.
func (s *Server) validateCellDNSCompilation(compiled platformconfig.CompileResult) error {
	pubs, err := platformconfig.DecodeCellRoutePublications(compiled.DNSArtifact)
	if err != nil {
		return &platformConfigReferenceError{err.Error()}
	}
	for _, p := range pubs {
		if err := cellpublication.VerifyInput(p, s.bundleKeyring()); err != nil {
			return &platformConfigReferenceError{err.Error()}
		}
		r := p.Reference
		parent, release, found, err := s.selectTrafficRouteRelease(r.AuthorityCellID)
		if err != nil || !found || parent.ID != r.ReleaseSetID || parent.ContentHash != r.ReleaseSetDigest || release.ID != r.ReleaseID || release.FencingToken != r.FencingToken || release.ReleaseChannel != r.ReleaseChannel || release.CanaryRuleRef != r.CanaryRuleRef {
			return &platformConfigReferenceError{"DNS reference is not the currently selected Cell publication"}
		}
		for _, embedded := range []model.PlatformArtifact{p.Parent, p.Route, p.TLS} {
			stored, err := s.store.GetPlatformArtifact(embedded.ID)
			if err != nil || stored.ID != embedded.ID || stored.ContentHash != embedded.ContentHash || stored.GenerationSequence != embedded.GenerationSequence || stored.ScopeKey != embedded.ScopeKey || stored.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(stored) != nil {
				return &platformConfigReferenceError{"DNS reference differs from retained validated Cell artifacts"}
			}
		}
	}
	return nil
}
