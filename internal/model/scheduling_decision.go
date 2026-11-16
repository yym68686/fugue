package model

import (
	"fmt"
	"strings"
	"time"
)

const OperationEvidenceTypeSchedulingDecision = "scheduling_decision"

// SchedulingDecision is deliberately tenant-safe at creation. Physical identities
// belong in the separate, administrator-only scheduling_diagnostics payload.
type SchedulingDecision struct {
	ObservedAt           time.Time                  `json:"observed_at"`
	Stage                string                     `json:"stage"`
	Outcome              string                     `json:"outcome"`
	Demand               SchedulingResources        `json:"demand"`
	MemoryRequestRatio   float64                    `json:"memory_request_ratio"`
	CPUOvercommitRatio   float64                    `json:"cpu_overcommit_ratio"`
	CPUCapacityEnforced  bool                       `json:"cpu_capacity_enforced"`
	RequiredPodAddresses int64                      `json:"required_pod_addresses"`
	SelectedCandidate    string                     `json:"selected_candidate,omitempty"`
	Nodes                []SchedulingNodeDecision   `json:"nodes,omitempty"`
	Volumes              []SchedulingVolumeDecision `json:"volumes,omitempty"`
	MissingEvidence      []string                   `json:"missing_evidence,omitempty"`
	Rejections           []SchedulingRejection      `json:"rejections,omitempty"`
}

type SchedulingResources struct {
	CPUMilli       int64 `json:"cpu_milli"`
	MemoryBytes    int64 `json:"memory_bytes"`
	EphemeralBytes int64 `json:"ephemeral_bytes"`
}

type SchedulingRejection struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type SchedulingNodeDecision struct {
	CandidateID               string                  `json:"candidate_id"`
	RuntimeMatches            bool                    `json:"runtime_matches"`
	Eligible                  bool                    `json:"eligible"`
	Preserved                 bool                    `json:"preserved"`
	ResourceInventoryObserved bool                    `json:"resource_inventory_observed"`
	Allocatable               SchedulingResources     `json:"allocatable"`
	Requested                 SchedulingResources     `json:"requested"`
	ExistingApp               SchedulingResources     `json:"existing_app"`
	EffectiveRequested        SchedulingResources     `json:"effective_requested"`
	Budget                    SchedulingResources     `json:"budget"`
	AvailablePodAddresses     *int64                  `json:"available_pod_addresses,omitempty"`
	Rejections                []SchedulingRejection   `json:"rejections,omitempty"`
	Volumes                   []SchedulingVolumeMatch `json:"volumes,omitempty"`
}

type SchedulingVolumeDecision struct {
	VolumeID             string `json:"volume_id"`
	Bound                bool   `json:"bound"`
	SingleNode           bool   `json:"single_node"`
	RequestedBytes       int64  `json:"requested_bytes"`
	NodeAffinityRequired bool   `json:"node_affinity_required"`
}

type SchedulingVolumeMatch struct {
	VolumeID        string                `json:"volume_id"`
	AffinityMatches *bool                 `json:"affinity_matches,omitempty"`
	Rejections      []SchedulingRejection `json:"rejections,omitempty"`
}

func (d SchedulingDecision) Summary() string {
	if d.Outcome != "rejected" {
		return fmt.Sprintf("scheduling %s: %s", d.Stage, d.Outcome)
	}
	var parts []string
	for _, r := range d.Rejections {
		parts = append(parts, r.Code+": "+r.Message)
	}
	matching := false
	for _, n := range d.Nodes {
		matching = matching || n.RuntimeMatches
	}
	for _, n := range d.Nodes {
		if matching && !n.RuntimeMatches {
			continue
		}
		for _, r := range n.Rejections {
			if len(parts) == 8 {
				return "scheduling " + d.Stage + " rejected: " + strings.Join(parts, "; ") + "; see scheduling_decision for all candidates"
			}
			parts = append(parts, n.CandidateID+": "+r.Code+": "+r.Message)
		}
	}
	if len(parts) == 0 {
		parts = append(parts, "no candidate nodes were returned by the scheduling inventory")
	}
	return "scheduling " + d.Stage + " rejected: " + strings.Join(parts, "; ")
}
