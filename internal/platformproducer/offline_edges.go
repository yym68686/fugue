package platformproducer

import (
	"fmt"
	"net/netip"
	"reflect"
	"regexp"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type OfflineEdge struct {
	EdgeID               string                     `json:"edge_id"`
	EdgeGroupID          string                     `json:"edge_group_id"`
	PublicIPv4           string                     `json:"public_ipv4"`
	Reason               string                     `json:"reason"`
	ExpectedRecoveryDate string                     `json:"expected_recovery_date,omitempty"`
	Recovery             *OfflineEdgeRecoveryPolicy `json:"recovery,omitempty"`
}

var offlineIdentity = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)
var offlineGroup = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)

func ValidateOfflineEdges(holds []OfflineEdge) error {
	if len(holds) > 64 {
		return fmt.Errorf("offline endpoint declaration exceeds bound")
	}
	for index, hold := range holds {
		address, err := netip.ParseAddr(hold.PublicIPv4)
		if !offlineIdentity.MatchString(hold.EdgeID) || !offlineGroup.MatchString(hold.EdgeGroupID) || err != nil || !address.Is4() || !platformconfig.PublicDNSFlattenIP(address) || address.String() != hold.PublicIPv4 || index > 0 && holds[index-1].EdgeID >= hold.EdgeID {
			return fmt.Errorf("offline endpoint identity must be canonical and unique")
		}
		if hold.Reason != "quota_exhausted" && hold.Reason != "maintenance" && hold.Reason != "network_unavailable" {
			return fmt.Errorf("offline endpoint reason invalid")
		}
		if hold.ExpectedRecoveryDate != "" {
			if _, err := time.Parse(time.DateOnly, hold.ExpectedRecoveryDate); err != nil {
				return fmt.Errorf("offline recovery date invalid")
			}
		}
		if hold.Recovery != nil {
			if err := hold.Recovery.Validate(); err != nil {
				return err
			}
			if p := hold.Recovery.Capacity; p != nil && slices.Contains(p.ReferenceEdgeIDs, hold.EdgeID) {
				return fmt.Errorf("capacity reference must be another physical Edge")
			}
		}
	}
	return nil
}

func SelectDNSCandidateEndpoints(nodes []model.EdgeNode, holds []OfflineEdge) ([]model.EdgeNode, error) {
	if err := ValidateOfflineEdges(holds); err != nil {
		return nil, err
	}
	byID := make(map[string]OfflineEdge, len(holds))
	for _, hold := range holds {
		byID[hold.EdgeID] = hold
	}
	selected := make([]model.EdgeNode, 0, len(nodes))
	seen := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		if seen[node.ID] {
			return nil, fmt.Errorf("ambiguous endpoint inventory")
		}
		seen[node.ID] = true
		if hold, exists := byID[node.ID]; exists {
			if hold.EdgeGroupID != node.EdgeGroupID || hold.PublicIPv4 != node.PublicIPv4 {
				return nil, fmt.Errorf("offline endpoint identity changed")
			}
			continue
		}
		selected = append(selected, node)
	}
	for _, hold := range holds {
		if !seen[hold.EdgeID] {
			return nil, fmt.Errorf("offline endpoint absent from declared inventory")
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("offline intent removes every DNS candidate")
	}
	return selected, nil
}

func ValidateOfflineEdgeUpdate(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
	return validatePhysicalDNSUpdate(previous, next, static, oldDNS, newDNS, "edge_offline")
}

func validateOfflinePolicyDelta(previous, next ProjectionPolicyInput) error {
	if ValidateOfflineEdges(previous.OfflineEdges) != nil || ValidateOfflineEdges(next.OfflineEdges) != nil || reflect.DeepEqual(previous.OfflineEdges, next.OfflineEdges) {
		return fmt.Errorf("explicit changed offline intent required")
	}
	before := make(map[string]OfflineEdge, len(previous.OfflineEdges))
	for _, hold := range previous.OfflineEdges {
		before[hold.EdgeID] = hold
	}
	changes := 0
	for _, hold := range next.OfflineEdges {
		if old, exists := before[hold.EdgeID]; !exists || !reflect.DeepEqual(old, hold) {
			changes++
		}
		delete(before, hold.EdgeID)
	}
	changes += len(before)
	previous.Generation, next.Generation = "", ""
	previous.OfflineEdges, next.OfflineEdges = nil, nil
	if changes != 1 || !reflect.DeepEqual(previous, next) {
		return fmt.Errorf("offline transition must change one hold and preserve all other constraints")
	}
	return nil
}
