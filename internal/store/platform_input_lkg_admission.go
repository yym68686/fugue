package store

import (
	"context"

	"fugue/internal/inputarchive"
)

func compilerInputLKGPromotionRoots(releaseID string) []inputarchive.HotRoot {
	return []inputarchive.HotRoot{{Source: "lkg-promotion", ID: releaseID, Reference: inputarchive.HotReference{Kind: "release", ID: releaseID}}}
}

func (s *Store) prepareCompilerInputLKGPromotion(ctx context.Context, releaseID string, guard *platformProducerReleaseGuard) (*compilerInputActivationProof, error) {
	if guard == nil {
		return s.prepareOrdinaryCompilerInputProof(ctx, compilerInputLKGPromotionRoots(releaseID), nil, nil, nil, nil)
	}
	return s.prepareOrdinaryCompilerInputProof(ctx, nil, nil, &compilerInputPublicationIntent{lkgReleaseID: releaseID, hasGuard: true, guard: *guard}, nil, nil)
}

func compilerInputProofHasLKGPromotionRoot(proof *compilerInputActivationProof, releaseID string, guard *platformProducerReleaseGuard) bool {
	if guard != nil {
		return proof != nil && proof.publication != nil && releaseID != "" && proof.publication.lkgReleaseID == releaseID && proof.publication.hasGuard && proof.publication.guard == *guard
	}
	return proof != nil && proof.publication == nil && releaseID != "" && len(proof.closure.Roots) == 1 &&
		proof.closure.Roots[0] == compilerInputLKGPromotionRoots(releaseID)[0]
}
