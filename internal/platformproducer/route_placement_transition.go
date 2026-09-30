package platformproducer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/platformconfig"
)

// RoutePlacementTransition is signed configuration, never a serving grant.
// Its only operation is replacing explicitly declared authority aliases while
// retaining the same physical placement and every other constraint field.
type RoutePlacementTransition struct {
	PreviousTopology edgetopology.Intent         `json:"previous_topology"`
	NextTopology     edgetopology.Intent         `json:"next_topology"`
	Constraints      []RouteConstraintTransition `json:"constraints"`
}

type RouteConstraintTransition struct {
	Source       platformconfig.RoutePolicyConstraint `json:"source"`
	SourceDigest string                               `json:"source_digest"`
}

// RouteConstraintDigest uses the normalized JSON object representation, so
// declaration tooling and the artifact's decoded content hash the same bytes.
func RouteConstraintDigest(rule platformconfig.RoutePolicyConstraint) (string, error) {
	rule = normalizedRouteConstraint(rule)
	raw, err := json.Marshal(rule)
	if err != nil {
		return "", err
	}
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return "", err
	}
	return platformconfig.Digest(object)
}

func normalizedRouteConstraint(rule platformconfig.RoutePolicyConstraint) platformconfig.RoutePolicyConstraint {
	return platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{RouteConstraints: []platformconfig.RoutePolicyConstraint{rule}}).RouteConstraints[0]
}

func (t *RoutePlacementTransition) aliases() (map[string]string, error) {
	if t == nil || t.PreviousTopology.Validate() != nil || t.NextTopology.Validate() != nil || len(t.PreviousTopology.Cells) != len(t.NextTopology.Cells) {
		return nil, fmt.Errorf("route placement transition requires complete valid topology inputs")
	}
	expected := t.PreviousTopology.Clone()
	aliases := map[string]string{}
	for i, next := range t.NextTopology.Cells {
		previous := expected.Cells[i]
		if previous == next {
			continue
		}
		if previous.ID != next.ID || previous.LegacyGroupID == "" || next.LegacyGroupID != "" || !strings.HasPrefix(next.ID, "cell-") || !edgetopology.ValidAuthorityID(next.ID) {
			return nil, fmt.Errorf("route placement transition may only retire explicit authority aliases")
		}
		aliases[previous.LegacyGroupID] = next.ID
		expected.Cells[i].LegacyGroupID = ""
	}
	if len(aliases) == 0 || !reflect.DeepEqual(expected, t.NextTopology.Clone()) {
		return nil, fmt.Errorf("route placement transition changes membership, pools, capabilities, labels or failure domains")
	}
	return aliases, nil
}

func replaceConstraintAliases(rule platformconfig.RoutePolicyConstraint, aliases map[string]string) (platformconfig.RoutePolicyConstraint, bool) {
	rule = normalizedRouteConstraint(rule)
	changed := false
	if target, ok := aliases[rule.EdgeGroupID]; ok {
		rule.EdgeGroupID, changed = target, true
	}
	for i, group := range rule.ExcludedEdgeGroupIDs {
		if target, ok := aliases[group]; ok {
			rule.ExcludedEdgeGroupIDs[i], changed = target, true
		}
	}
	return normalizedRouteConstraint(rule), changed
}

func (t *RoutePlacementTransition) Validate() error {
	aliases, err := t.aliases()
	if err != nil {
		return err
	}
	if len(t.Constraints) == 0 || len(t.Constraints) > 4096 {
		return fmt.Errorf("route placement transition requires bounded source constraints")
	}
	for i, pin := range t.Constraints {
		rule := normalizedRouteConstraint(pin.Source)
		canonical, canonicalErr := json.Marshal(rule)
		declared, declaredErr := json.Marshal(pin.Source)
		validation := platformconfig.PolicySnapshot{SchemaVersion: platformconfig.SchemaVersion, Generation: "transition-validation", Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 60, RouteConstraints: []platformconfig.RoutePolicyConstraint{rule}}
		if canonicalErr != nil || declaredErr != nil || !bytes.Equal(canonical, declared) || platformconfig.ValidatePolicySnapshot(validation) != nil || i > 0 && t.Constraints[i-1].Source.Hostname >= rule.Hostname {
			return fmt.Errorf("route placement sources must be valid, canonical and ordered by unique hostname")
		}
		digest, err := RouteConstraintDigest(rule)
		if err != nil || !ValidDigest(pin.SourceDigest) || digest != pin.SourceDigest {
			return fmt.Errorf("route placement source digest differs")
		}
		if _, changed := replaceConstraintAliases(rule, aliases); !changed {
			return fmt.Errorf("route placement source has no declared authority alias")
		}
	}
	return nil
}

func validatePlacementTransitionPolicy(p Policy) error {
	if p.RoutePlacementTransition == nil {
		return nil
	}
	if p.PublicationRole != platformconfig.PublicationRoleCellRoutes || p.AuthorityCellID == "" || p.TargetScope != platformconfig.AuthorityCellScope(p.AuthorityCellID) {
		return fmt.Errorf("route placement transition requires a neutral route-only producer")
	}
	return p.RoutePlacementTransition.Validate()
}

func validatePlacementTransitionTopology(p Policy, static StaticIntentInput) error {
	if err := validatePlacementTransitionPolicy(p); err != nil {
		return err
	}
	if p.RoutePlacementTransition == nil {
		return nil
	}
	if static.EdgeTopology == nil || static.EdgeTopology.Validate() != nil || len(static.EdgeTopology.Cells) != 1 || static.EdgeTopology.Cells[0] != (edgetopology.AuthorityCell{ID: p.AuthorityCellID}) {
		return fmt.Errorf("route placement transition lacks exact neutral static membership")
	}
	next := p.RoutePlacementTransition.NextTopology
	byEdge := make(map[string]edgetopology.Edge, len(next.Edges))
	for _, edge := range next.Edges {
		byEdge[edge.ID] = edge
	}
	// A private initial executor cohort may be smaller than the complete
	// desired topology. Every enrolled member must still match it exactly;
	// this declaration cannot invent runtime membership or grant capability.
	for _, edge := range static.EdgeTopology.Edges {
		if edge.AuthorityCellID != p.AuthorityCellID || !reflect.DeepEqual(edge, byEdge[edge.ID]) {
			return fmt.Errorf("route placement topology differs from pinned static Edge identity")
		}
	}
	return nil
}

// transitionRouteConstraints is shared by capture and the publication guard.
// Both reconstruct output from the complete signed source, never by trusting
// an inverse alias rewrite or a claimed source digest on the compiled artifact.
func transitionRouteConstraints(p Policy, policy platformconfig.PolicySnapshot, output bool) (platformconfig.PolicySnapshot, error) {
	if err := validatePlacementTransitionPolicy(p); err != nil {
		return platformconfig.PolicySnapshot{}, err
	}
	if p.RoutePlacementTransition == nil {
		return policy, nil
	}
	if policy.PublicationRole != p.PublicationRole || policy.Scope != p.TargetScope || policy.AuthorityCellID != p.AuthorityCellID {
		return platformconfig.PolicySnapshot{}, fmt.Errorf("route placement output belongs to another authority")
	}
	t := p.RoutePlacementTransition
	aliases, _ := t.aliases()
	pins := make(map[string]RouteConstraintTransition, len(t.Constraints))
	for _, pin := range t.Constraints {
		pins[pin.Source.Hostname] = pin
	}
	result := platformconfig.NormalizePolicySnapshot(policy)
	matched := 0
	for i, rule := range result.RouteConstraints {
		pin, exists := pins[rule.Hostname]
		if !exists {
			if _, affected := replaceConstraintAliases(rule, aliases); affected {
				return platformconfig.PolicySnapshot{}, fmt.Errorf("route placement contains an unpinned affected constraint")
			}
			continue
		}
		expected := pin.Source
		transformed, _ := replaceConstraintAliases(expected, aliases)
		if output {
			expected = transformed
		}
		actualDigest, err := RouteConstraintDigest(rule)
		expectedDigest, expectedErr := RouteConstraintDigest(expected)
		if err != nil || expectedErr != nil || actualDigest != expectedDigest {
			return platformconfig.PolicySnapshot{}, fmt.Errorf("route placement constraint differs from exact signed source or output")
		}
		matched++
		result.RouteConstraints[i] = transformed
	}
	if matched != len(pins) {
		return platformconfig.PolicySnapshot{}, fmt.Errorf("route placement source constraint is absent")
	}
	return result, nil
}

func ApplyRoutePlacementTransition(p Policy, policy platformconfig.PolicySnapshot) (platformconfig.PolicySnapshot, error) {
	result, err := transitionRouteConstraints(p, policy, false)
	if err != nil || p.RoutePlacementTransition == nil {
		return result, err
	}
	if err := platformconfig.ValidatePolicySnapshot(result); err != nil {
		return platformconfig.PolicySnapshot{}, err
	}
	result.Generation, err = platformconfig.PolicySnapshotGeneration(result)
	return result, err
}

func ValidateRoutePlacementOutput(p Policy, policy platformconfig.PolicySnapshot) error {
	if p.RoutePlacementTransition != nil {
		if err := platformconfig.ValidatePolicySnapshot(policy); err != nil {
			return err
		}
	}
	_, err := transitionRouteConstraints(p, policy, true)
	return err
}
