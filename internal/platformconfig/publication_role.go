package platformconfig

import (
	"encoding/json"
	"fmt"
	"slices"

	"fugue/internal/model"
)

const PublicationRoleCellRoutes = "cell-routes"

// An explicit role changes the signed publication boundary, never the meaning
// of an existing release. Empty retains the original complete traffic set.
func ValidatePublicationRole(role, cell, scope string) error {
	if role == "" {
		return nil
	}
	if role != PublicationRoleCellRoutes || !validTrafficConsumerCell(cell) || scope != AuthorityCellScope(cell) {
		return fmt.Errorf("publication role requires an explicit neutral cell")
	}
	return nil
}

func PublicationArtifactKinds(role string) []string {
	if role == PublicationRoleCellRoutes {
		return []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig}
	}
	return []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig}
}

// ValidateReleaseComposition is shared by consumers and publication/LKG
// transactions. Dropping DNS from an ordinary traffic set never makes it a
// route-only release; the signed role and exact topology must authorize it.
func ValidateReleaseComposition(parent model.PlatformArtifact) ([]string, error) {
	var set ReleaseSet
	raw, err := json.Marshal(parent.Content)
	if err != nil || json.Unmarshal(raw, &set) != nil || parent.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		return nil, fmt.Errorf("release composition invalid")
	}
	cell := ""
	if set.ConsumerTopology != nil {
		cell = set.ConsumerTopology.AuthorityCellID
		if set.ConsumerTopology.PublicationRole != set.PublicationRole || set.ConsumerTopology.Validate(parent.ScopeKey) != nil {
			return nil, fmt.Errorf("release role differs from declared membership")
		}
	}
	if err := ValidatePublicationRole(set.PublicationRole, cell, parent.ScopeKey); err != nil {
		return nil, err
	}
	kinds := PublicationArtifactKinds(set.PublicationRole)
	if len(set.ArtifactIDs) != len(kinds) || len(set.ArtifactKinds) != len(kinds) {
		return nil, fmt.Errorf("release membership is incomplete for publication role")
	}
	seenIDs, seenKinds := map[string]bool{}, map[string]bool{}
	for i, kind := range set.ArtifactKinds {
		id := set.ArtifactIDs[i]
		if !slices.Contains(kinds, kind) || seenKinds[kind] || id == "" || id == parent.ID || seenIDs[id] {
			return nil, fmt.Errorf("release member identity invalid")
		}
		seenIDs[id], seenKinds[kind] = true, true
	}
	return kinds, nil
}

func validateRouteOnlyIntent(in PlatformIntent) error {
	if err := ValidatePublicationRole(in.PublicationRole, in.AuthorityCellID, in.Scope); err != nil {
		return err
	}
	if in.PublicationRole == PublicationRoleCellRoutes && (len(in.DNS) != 0 || len(in.DNSConsumers) != 0 || len(in.ACMEChallenges) != 0) {
		return fmt.Errorf("cell route intent cannot own DNS configuration")
	}
	return nil
}

func validateRouteOnlyPolicy(in PolicySnapshot) error {
	if err := ValidatePublicationRole(in.PublicationRole, in.AuthorityCellID, in.Scope); err != nil {
		return err
	}
	if in.PublicationRole == PublicationRoleCellRoutes && in.TLSReadiness == nil {
		return fmt.Errorf("cell route publication requires explicit TLS readiness policy")
	}
	if in.PublicationRole == PublicationRoleCellRoutes && (in.DNSPlacementMode != "" || in.DNSQueryPolicy != nil || in.DNSReadiness != nil || len(in.DNSAuthorities) != 0 || len(in.DNSClientPolicies) != 0 || len(in.DNSAnswerRules) != 0 || len(in.EdgeSelectionConstraints) != 0) {
		return fmt.Errorf("cell route policy cannot own DNS placement or candidate selection")
	}
	return nil
}
