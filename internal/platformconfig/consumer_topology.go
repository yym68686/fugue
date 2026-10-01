package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
)

const TrafficConsumerTopologySchema = "fugue.traffic-consumer-topology/v1"
const authorityCellScopePrefix = "authority-cell:"

var trafficConsumerNodeID = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)
var trafficConsumerDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type TrafficConsumerTopology struct {
	PublicationRole string   `json:"publication_role,omitempty"`
	SchemaVersion   string   `json:"schema_version"`
	AuthorityCellID string   `json:"authority_cell_id"`
	EdgeNodeIDs     []string `json:"edge_node_ids"`
	DNSNodeIDs      []string `json:"dns_node_ids"`
}

func AuthorityCellScope(cell string) string { return authorityCellScopePrefix + cell }

func validTrafficConsumerCell(cell string) bool {
	return strings.HasPrefix(cell, "cell-") && edgetopology.ValidAuthorityID(cell)
}

func (topology TrafficConsumerTopology) Validate(scope string) error {
	if err := ValidatePublicationRole(topology.PublicationRole, topology.AuthorityCellID, scope); err != nil {
		return err
	}
	if topology.SchemaVersion != TrafficConsumerTopologySchema || !validTrafficConsumerCell(topology.AuthorityCellID) || scope != AuthorityCellScope(topology.AuthorityCellID) {
		return fmt.Errorf("traffic consumer authority or scope invalid")
	}
	for _, list := range []struct {
		ids      []string
		min, max int
	}{{topology.EdgeNodeIDs, 0, 10000}, {topology.DNSNodeIDs, 0, 4096}} {
		if len(list.ids) < list.min || len(list.ids) > list.max {
			return fmt.Errorf("traffic consumer membership must be nonempty and bounded")
		}
		for i, id := range list.ids {
			if !trafficConsumerNodeID.MatchString(id) || i > 0 && list.ids[i-1] >= id {
				return fmt.Errorf("traffic consumer nodes must be canonical, sorted and unique")
			}
		}
	}
	if topology.PublicationRole == PublicationRoleCellDNS && (len(topology.EdgeNodeIDs) != 0 || len(topology.DNSNodeIDs) == 0) || topology.PublicationRole != PublicationRoleCellDNS && len(topology.EdgeNodeIDs) == 0 || topology.PublicationRole == PublicationRoleCellRoutes && len(topology.DNSNodeIDs) != 0 || topology.PublicationRole == "" && len(topology.DNSNodeIDs) == 0 {
		return fmt.Errorf("DNS membership differs from publication role")
	}
	return nil
}

// Only explicit intent enrolls a cell. Inventory health, aliases and process
// observations cannot add or remove a required execution identity.
func TrafficConsumerTopologyFromIntent(intent PlatformIntent) (*TrafficConsumerTopology, error) {
	if err := validateRouteOnlyIntent(intent); err != nil {
		return nil, err
	}
	if intent.AuthorityCellID == "" {
		if strings.HasPrefix(intent.Scope, authorityCellScopePrefix) {
			return nil, fmt.Errorf("cell scope requires explicit consumer authority")
		}
		return nil, nil
	}
	if intent.PublicationRole == PublicationRoleCellDNS {
		t := &TrafficConsumerTopology{PublicationRole: intent.PublicationRole, SchemaVersion: TrafficConsumerTopologySchema, AuthorityCellID: intent.AuthorityCellID, EdgeNodeIDs: []string{}, DNSNodeIDs: []string{}}
		for _, c := range intent.DNSConsumers {
			if c.EdgeGroupID != intent.AuthorityCellID {
				return nil, fmt.Errorf("foreign DNS authority")
			}
			t.DNSNodeIDs = append(t.DNSNodeIDs, c.NodeID)
		}
		sort.Strings(t.DNSNodeIDs)
		return t, t.Validate(intent.Scope)
	}
	cell := intent.AuthorityCellID
	if !validTrafficConsumerCell(cell) || intent.Scope != AuthorityCellScope(cell) || intent.EdgeTopology == nil || intent.EdgeTopology.Validate() != nil || len(intent.EdgeTopology.Cells) != 1 || intent.EdgeTopology.Cells[0].ID != cell || intent.EdgeTopology.Cells[0].LegacyGroupID != "" || ValidateDNSConsumers(intent.DNSConsumers) != nil {
		return nil, fmt.Errorf("cell publication requires one explicit neutral topology and DNS ownership")
	}
	out := &TrafficConsumerTopology{PublicationRole: intent.PublicationRole, SchemaVersion: TrafficConsumerTopologySchema, AuthorityCellID: cell, EdgeNodeIDs: []string{}, DNSNodeIDs: []string{}}
	for _, edge := range intent.EdgeTopology.Edges {
		if edge.AuthorityCellID != cell {
			return nil, fmt.Errorf("foreign Edge authority")
		}
		out.EdgeNodeIDs = append(out.EdgeNodeIDs, edge.ID)
	}
	for _, dns := range intent.DNSConsumers {
		if dns.EdgeGroupID != cell {
			return nil, fmt.Errorf("foreign DNS authority")
		}
		out.DNSNodeIDs = append(out.DNSNodeIDs, dns.NodeID)
	}
	sort.Strings(out.EdgeNodeIDs)
	sort.Strings(out.DNSNodeIDs)
	if err := out.Validate(intent.Scope); err != nil {
		return nil, err
	}
	return out, nil
}

func validateConsumerTopologyPolicy(policy PolicySnapshot) error {
	if err := validateRouteOnlyPolicy(policy); err != nil {
		return err
	}
	if policy.AuthorityCellID == "" && policy.ConsumerTopologyDigest == "" {
		if strings.HasPrefix(policy.Scope, authorityCellScopePrefix) {
			return fmt.Errorf("cell policy requires authority and topology digest")
		}
		return nil
	}
	if !validTrafficConsumerCell(policy.AuthorityCellID) || policy.Scope != AuthorityCellScope(policy.AuthorityCellID) || !trafficConsumerDigest.MatchString(policy.ConsumerTopologyDigest) {
		return fmt.Errorf("cell policy authority, scope or membership digest invalid")
	}
	for _, cohort := range policy.TrafficRolloutCohorts {
		for _, group := range cohort.EdgeGroupIDs {
			if group != policy.AuthorityCellID {
				return fmt.Errorf("cell rollout cohort contains foreign authority")
			}
		}
	}
	return nil
}

func CompileTrafficConsumerTopology(intent PlatformIntent, policy PolicySnapshot) (*TrafficConsumerTopology, error) {
	topology, err := TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		return nil, err
	}
	if err = validateConsumerTopologyPolicy(policy); err != nil {
		return nil, err
	}
	if topology == nil {
		if policy.AuthorityCellID != "" || policy.ConsumerTopologyDigest != "" {
			return nil, fmt.Errorf("consumer policy has no declared intent")
		}
		return nil, nil
	}
	digest, err := Digest(topology)
	if err != nil || policy.PublicationRole != topology.PublicationRole || policy.Scope != intent.Scope || policy.AuthorityCellID != topology.AuthorityCellID || policy.ConsumerTopologyDigest != digest {
		return nil, fmt.Errorf("consumer topology differs from pinned policy")
	}
	return topology, nil
}

// Callers must verify artifact integrity before trusting this projection.
func TrafficConsumersFromRelease(parent model.PlatformArtifact) (*TrafficConsumerTopology, error) {
	value, present := parent.Content["consumer_topology"]
	if !present {
		if strings.HasPrefix(parent.ScopeKey, authorityCellScopePrefix) || parent.Metadata["consumer_topology_digest"] != "" || parent.Content["publication_role"] != nil {
			return nil, fmt.Errorf("release consumer topology missing")
		}
		return nil, nil
	}
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || value == nil {
		return nil, fmt.Errorf("release consumer topology invalid")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var topology TrafficConsumerTopology
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&topology) != nil || d.Decode(&struct{}{}) != io.EOF || topology.Validate(parent.ScopeKey) != nil {
		return nil, fmt.Errorf("release consumer topology invalid")
	}
	role, _ := parent.Content["publication_role"].(string)
	if role != topology.PublicationRole {
		return nil, fmt.Errorf("release publication role differs from membership")
	}
	digest, err := Digest(topology)
	if err != nil || parent.Metadata["consumer_topology_digest"] != digest {
		return nil, fmt.Errorf("release consumer topology digest differs")
	}
	return &topology, nil
}

func ValidateTrafficConsumerTopologyProjection(parent, child model.PlatformArtifact) error {
	var policy PolicySnapshot
	// Only the policy participates in this projection. Route, DNS and TLS
	// payloads can be megabytes and are verified separately by the caller.
	raw, err := json.Marshal(child.Content["policy"])
	if err != nil || json.Unmarshal(raw, &policy) != nil {
		return fmt.Errorf("child consumer policy unavailable")
	}
	return validateTrafficConsumerTopologyPolicy(parent, child, policy)
}

func validateTrafficConsumerTopologyPolicy(parent, child model.PlatformArtifact, policy PolicySnapshot) error {
	topology, err := TrafficConsumersFromRelease(parent)
	if err != nil {
		return err
	}
	if topology == nil {
		if policy.PublicationRole != "" || policy.AuthorityCellID != "" || policy.ConsumerTopologyDigest != "" || child.Metadata["consumer_topology_digest"] != "" {
			return fmt.Errorf("child consumer topology has no parent authority")
		}
		return nil
	}
	if child.ScopeKey != parent.ScopeKey || validateConsumerTopologyPolicy(policy) != nil || policy.PublicationRole != topology.PublicationRole || policy.AuthorityCellID != topology.AuthorityCellID || policy.ConsumerTopologyDigest != parent.Metadata["consumer_topology_digest"] || child.Metadata["consumer_topology_digest"] != policy.ConsumerTopologyDigest {
		return fmt.Errorf("child consumer topology differs from parent or policy")
	}
	return nil
}
