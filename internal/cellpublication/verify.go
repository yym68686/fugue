// Package cellpublication verifies embedded route/TLS authorities independently
// of DNS's own signed parent. It does not select or publish a release.
package cellpublication

import (
	"fmt"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func VerifyInput(p platformconfig.CellRoutePublicationInput, keys bundleauth.Keyring) error {
	if err := platformconfig.ValidateCellRoutePublication(p); err != nil {
		return err
	}
	for _, a := range []model.PlatformArtifact{p.Parent, p.Route, p.TLS} {
		if !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
			return fmt.Errorf("referenced Cell artifact signature rejected")
		}
	}
	return nil
}

func VerifyDNSArtifact(a model.PlatformArtifact, keys bundleauth.Keyring) ([]platformconfig.CellRoutePublicationInput, error) {
	if a.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
		return nil, fmt.Errorf("DNS artifact required")
	}
	if err := platformconfig.ValidateDNSCellPlan(a); err != nil {
		return nil, err
	}
	pubs, err := platformconfig.DecodeCellRoutePublications(a)
	if err != nil {
		return nil, err
	}
	for _, p := range pubs {
		if err := VerifyInput(p, keys); err != nil {
			return nil, err
		}
	}
	previous, err := platformconfig.DecodePreviousTrafficPublication(a)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		if err := VerifyPreviousInput(*previous, keys); err != nil {
			return nil, err
		}
	}
	return pubs, nil
}

func VerifyPreviousInput(p platformconfig.PreviousTrafficPublicationInput, keys bundleauth.Keyring) error {
	if err := platformconfig.ValidatePreviousTrafficPublication(p); err != nil {
		return err
	}
	for _, a := range []model.PlatformArtifact{p.Parent, p.Route, p.TLS, p.DNS} {
		if !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
			return fmt.Errorf("previous traffic artifact signature rejected")
		}
	}
	return nil
}

// VerifyDNSAuthorityHandoff authenticates the artifacts before comparing one
// node's complete DNS behavior. It is only the immutable-input gate; fresh
// serving snapshots and transport identity CAS remain separate requirements.
func VerifyDNSAuthorityHandoff(previous, next model.PlatformArtifact, node string, keys bundleauth.Keyring) error {
	for _, artifact := range []model.PlatformArtifact{previous, next} {
		if !platformsafety.EvaluateArtifactIntegrity(artifact, keys).Pass {
			return fmt.Errorf("DNS handoff artifact signature rejected")
		}
	}
	if _, err := VerifyDNSArtifact(next, keys); err != nil {
		return err
	}
	return platformconfig.ValidateDNSAuthorityHandoff(previous, next, node)
}
