package platformconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/model"
	"fugue/internal/trafficbinding"
)

// CellRoutePublicationReference is configuration intent, not a live selection.
// Each reference pins both immutable content and the exact serving publication.
type CellRoutePublicationReference struct {
	AuthorityCellID     string `json:"authority_cell_id"`
	ReleaseSetID        string `json:"release_set_id"`
	ReleaseSetDigest    string `json:"release_set_digest"`
	ReleaseID           string `json:"release_id"`
	ReleaseChannel      string `json:"release_channel"`
	FencingToken        int64  `json:"fencing_token"`
	CanaryRuleRef       string `json:"canary_rule_ref,omitempty"`
	RouteArtifactID     string `json:"route_artifact_id"`
	RouteArtifactDigest string `json:"route_artifact_digest"`
	TLSArtifactID       string `json:"tls_artifact_id"`
	TLSArtifactDigest   string `json:"tls_artifact_digest"`
}

// These inputs are embedded in the signed DNS artifact for independent replay
// and recovery. Callers verify every embedded signature with their current
// keyring. A publisher additionally checks current Cell authority transactionally.
type CellRoutePublicationInput struct {
	ProducerPolicy *model.PlatformPublicationPrecondition `json:"producer_policy,omitempty"`
	Reference      CellRoutePublicationReference          `json:"reference"`
	Parent         model.PlatformArtifact                 `json:"parent"`
	Route          model.PlatformArtifact                 `json:"route"`
	TLS            model.PlatformArtifact                 `json:"tls"`
}

func (r CellRoutePublicationReference) Validate() error {
	if !validTrafficConsumerCell(r.AuthorityCellID) || r.FencingToken <= 0 ||
		(r.ReleaseChannel != "gray" && r.ReleaseChannel != "full") ||
		(r.ReleaseChannel == "full" && r.CanaryRuleRef != "") ||
		(r.ReleaseChannel == "gray" && (!strings.HasPrefix(r.CanaryRuleRef, "cohort=") || len(r.CanaryRuleRef) > 135)) {
		return fmt.Errorf("Cell reference requires exact neutral serving authority")
	}
	for _, id := range []string{r.ReleaseSetID, r.ReleaseID, r.RouteArtifactID, r.TLSArtifactID} {
		if !validDNSConsumerIdentity(id) || strings.ContainsAny(id, " /") {
			return fmt.Errorf("Cell reference identity invalid")
		}
	}
	if r.ReleaseSetID == r.RouteArtifactID || r.ReleaseSetID == r.TLSArtifactID || r.RouteArtifactID == r.TLSArtifactID {
		return fmt.Errorf("Cell reference identities overlap")
	}
	for _, digest := range []string{r.ReleaseSetDigest, r.RouteArtifactDigest, r.TLSArtifactDigest} {
		if !trafficConsumerDigest.MatchString(digest) {
			return fmt.Errorf("Cell reference digest invalid")
		}
	}
	return nil
}

// ValidateCellRoutePublication checks content and membership without pretending
// to authenticate a signature or the mutable release transport.
func ValidateCellRoutePublication(p CellRoutePublicationInput) error {
	r, parent := p.Reference, p.Parent
	if err := r.Validate(); err != nil {
		return err
	}
	fail := func() error {
		return fmt.Errorf("Cell route publication differs from exact reference or signed composition")
	}
	if parent.ID != r.ReleaseSetID || parent.ContentHash != r.ReleaseSetDigest || p.Route.ID != r.RouteArtifactID || p.Route.ContentHash != r.RouteArtifactDigest || p.TLS.ID != r.TLSArtifactID || p.TLS.ContentHash != r.TLSArtifactDigest || parent.Content["publication_role"] != PublicationRoleCellRoutes {
		return fail()
	}
	if _, err := ValidateReleaseComposition(parent); err != nil {
		return err
	}
	topology, err := TrafficConsumersFromRelease(parent)
	if err != nil || topology == nil || topology.AuthorityCellID != r.AuthorityCellID {
		return fail()
	}
	var set ReleaseSet
	raw, _ := json.Marshal(parent.Content)
	if json.Unmarshal(raw, &set) != nil || set.SchemaVersion != SchemaVersion || set.Scope != parent.ScopeKey || set.Generation != parent.Generation || !reflect.DeepEqual(set.Lineage, LineageFromArtifact(parent)) {
		return fail()
	}
	for i, a := range []model.PlatformArtifact{parent, p.Route, p.TLS} {
		kind := []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig}[i]
		digest, err := Digest(a.Content)
		if err != nil || digest != a.ContentHash || a.ArtifactKind != kind || a.Status != model.PlatformArtifactStatusValidated || a.SchemaVersion != model.PlatformArtifactSchemaVersionV1 || a.GenerationSequence <= 0 || a.ScopeKey != AuthorityCellScope(r.AuthorityCellID) || a.Scope.Key != a.ScopeKey {
			return fail()
		}
		if i == 0 {
			continue
		}
		index := slices.Index(set.ArtifactKinds, kind)
		if index < 0 || set.ArtifactIDs[index] != a.ID || a.Metadata["release_set_generation"] != parent.Generation || !reflect.DeepEqual(set.Lineage, LineageFromArtifact(a)) || ValidateTrafficCohortProjection(parent, a) != nil {
			return fail()
		}
		var payload struct {
			Policy  PolicySnapshot `json:"policy"`
			Lineage Lineage        `json:"lineage"`
		}
		raw, _ = json.Marshal(a.Content)
		if json.Unmarshal(raw, &payload) != nil || !reflect.DeepEqual(payload.Lineage, set.Lineage) || ValidatePolicySnapshot(payload.Policy) != nil {
			return fail()
		}
		digest, err = Digest(payload.Policy)
		if err != nil || digest != set.Lineage.PolicyDigest {
			return fail()
		}
	}
	if r.ReleaseChannel == "gray" {
		groups, err := ResolveTrafficCanary(parent, r.CanaryRuleRef)
		if err != nil || !TrafficCanaryContains(groups, r.AuthorityCellID) {
			return fail()
		}
	}
	return nil
}

// CellRoutePublicationBinding matches the ordinary route executor projection
// byte for byte. A DNS parent is never substituted into this Cell binding.
func CellRoutePublicationBinding(p CellRoutePublicationInput) (*model.TrafficReleaseBinding, error) {
	if err := ValidateCellRoutePublication(p); err != nil {
		return nil, err
	}
	projection, err := ProjectRouteArtifact(p.Route)
	if err != nil {
		return nil, err
	}
	r, parent, route := p.Reference, p.Parent, p.Route
	lineage := LineageFromArtifact(parent)
	b := &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, ReleaseSetGeneration: parent.Generation, RouteArtifactID: route.ID, RouteArtifactDigest: route.ContentHash, RouteArtifactGeneration: route.Generation, RouteArtifactSequence: route.GenerationSequence, ReleaseID: r.ReleaseID, ReleaseChannel: r.ReleaseChannel, FencingToken: r.FencingToken, ScopeKey: parent.ScopeKey, IntentDigest: lineage.IntentDigest, PolicyDigest: lineage.PolicyDigest, InputSnapshotDigest: lineage.InputSnapshotDigest, CompilerVersion: lineage.CompilerVersion, ProjectionDigest: trafficbinding.ProjectionDigest(projection), CanaryRuleRef: r.CanaryRuleRef}
	if r.ReleaseChannel == "gray" {
		b.EdgeGroupIDs, err = ResolveTrafficCanary(parent, r.CanaryRuleRef)
	}
	if err != nil {
		return nil, err
	}
	return b, trafficbinding.ValidateGroup(b, r.AuthorityCellID, true)
}

func DecodeCellRoutePublications(artifact model.PlatformArtifact) ([]CellRoutePublicationInput, error) {
	var payload struct {
		Publications []CellRoutePublicationInput `json:"cell_route_publications"`
		Policy       PolicySnapshot              `json:"policy"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil || json.Unmarshal(raw, &payload) != nil {
		return nil, fmt.Errorf("DNS Cell references cannot be decoded")
	}
	if payload.Policy.PublicationRole != PublicationRoleCellDNS {
		if len(payload.Publications) != 0 {
			return nil, fmt.Errorf("Cell references require DNS publication authority")
		}
		return nil, nil
	}
	if len(payload.Publications) < 1 || len(payload.Publications) > 16 {
		return nil, fmt.Errorf("DNS Cell references must be nonempty and bounded")
	}
	for i, p := range payload.Publications {
		if p.Reference.AuthorityCellID == payload.Policy.AuthorityCellID || i > 0 && payload.Publications[i-1].Reference.AuthorityCellID >= p.Reference.AuthorityCellID {
			return nil, fmt.Errorf("DNS Cell references must be independent, sorted and unique")
		}
		if err := ValidateCellRoutePublication(p); err != nil {
			return nil, err
		}
	}
	return payload.Publications, nil
}
