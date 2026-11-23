package platformconfig

import (
	"encoding/json"
	"errors"

	"fugue/internal/model"
)

const HostedAddressSourceMetadata = "hosted_address_reservations_source"

// Both keys are part of the artifact signature. This only decodes the
// narrow read set; the serving transaction authenticates each named object.
func HostedAddressAuthority(a model.PlatformArtifact) (*PreviousTrafficPublicationReference, string, error) {
	raw, named := a.Metadata[HostedAddressSourceMetadata]
	digest, marked := a.Metadata[HostedAddressReservationsDigestMetadata]
	if !named && !marked {
		return nil, "", nil
	}
	bad := errors.New("invalid hosted address reservation source")
	scope := a.ScopeKey
	if scope == "" && a.Scope.ScopeType == "global" {
		scope = a.Scope.Key // pre-materialization compiler output
	}
	if !named || !marked || !trafficConsumerDigest.MatchString(digest) || len(raw) > 4096 ||
		scope != GlobalScopeKey || a.Scope.Key != "" && a.Scope.Key != scope || a.ArtifactKind != model.PlatformArtifactKindReleaseSet && a.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
		return nil, "", bad
	}
	var ref PreviousTrafficPublicationReference
	if json.Unmarshal([]byte(raw), &ref) != nil || ref.Validate() != nil {
		return nil, "", bad
	}
	canonical, err := json.Marshal(ref)
	if err != nil || string(canonical) != raw {
		return nil, "", bad
	}
	return &ref, digest, nil
}
