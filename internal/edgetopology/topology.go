package edgetopology

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const SchemaVersion = "edge-topology/v1"

var identityPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)

// Intent describes placement and shared risk. Endpoints, health, loaded
// artifacts, and release state are runtime facts and do not belong here.
type Intent struct {
	SchemaVersion string          `json:"schema_version"`
	Cells         []AuthorityCell `json:"authority_cells"`
	Pools         []ServingPool   `json:"serving_pools"`
	Edges         []Edge          `json:"edges"`
}

type AuthorityCell struct {
	ID            string `json:"id"`
	LegacyGroupID string `json:"legacy_group_id,omitempty"`
}

type ServingPool struct {
	ID string `json:"id"`
}

type Edge struct {
	ID              string            `json:"id"`
	AuthorityCellID string            `json:"authority_cell_id"`
	ServingPoolIDs  []string          `json:"serving_pool_ids"`
	Capabilities    []string          `json:"capabilities"`
	FailureDomains  map[string]string `json:"failure_domains"`
	Labels          map[string]string `json:"labels,omitempty"`
}

func (intent Intent) Clone() Intent {
	out := Intent{SchemaVersion: intent.SchemaVersion}
	out.Cells = append([]AuthorityCell(nil), intent.Cells...)
	out.Pools = append([]ServingPool(nil), intent.Pools...)
	out.Edges = make([]Edge, len(intent.Edges))
	for i, edge := range intent.Edges {
		out.Edges[i] = edge
		out.Edges[i].ServingPoolIDs = append([]string(nil), edge.ServingPoolIDs...)
		out.Edges[i].Capabilities = append([]string(nil), edge.Capabilities...)
		out.Edges[i].FailureDomains = cloneDomains(edge.FailureDomains)
		out.Edges[i].Labels = cloneDomains(edge.Labels)
	}
	return out
}

func Decode(reader io.Reader) (Intent, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var intent Intent
	if err := decoder.Decode(&intent); err != nil {
		return Intent{}, fmt.Errorf("decode edge topology: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Intent{}, errors.New("edge topology contains trailing data")
	}
	if err := intent.Validate(); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

func (intent Intent) Validate() error {
	if intent.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported edge topology schema %q", intent.SchemaVersion)
	}
	if len(intent.Cells) == 0 || len(intent.Pools) == 0 || len(intent.Edges) == 0 ||
		len(intent.Cells) > 100 || len(intent.Pools) > 100 || len(intent.Edges) > 10000 {
		return errors.New("edge topology contains an invalid number of cells, pools, or edges")
	}
	cells := make(map[string]string, len(intent.Cells))
	legacyGroups := make(map[string]struct{}, len(intent.Cells))
	for i, cell := range intent.Cells {
		if !validID(cell.ID) || (cell.LegacyGroupID != "" && (!validID(cell.LegacyGroupID) || !strings.HasPrefix(cell.LegacyGroupID, "edge-group-"))) {
			return fmt.Errorf("authority cell %d has an invalid identity", i)
		}
		if i > 0 && intent.Cells[i-1].ID >= cell.ID {
			return errors.New("authority cells must be ordered by unique id")
		}
		groupID := cell.ServingGroupID()
		if _, exists := legacyGroups[groupID]; exists {
			return fmt.Errorf("serving edge group %q maps to multiple authority cells", groupID)
		}
		cells[cell.ID] = groupID
		legacyGroups[groupID] = struct{}{}
	}
	pools := make(map[string]struct{}, len(intent.Pools))
	for i, pool := range intent.Pools {
		if !validID(pool.ID) {
			return fmt.Errorf("serving pool %d has an invalid identity", i)
		}
		if i > 0 && intent.Pools[i-1].ID >= pool.ID {
			return errors.New("serving pools must be ordered by unique id")
		}
		pools[pool.ID] = struct{}{}
	}
	usedCells := make(map[string]struct{}, len(cells))
	usedPools := make(map[string]struct{}, len(pools))
	for i, edge := range intent.Edges {
		if !validID(edge.ID) {
			return fmt.Errorf("edge %d has an invalid identity", i)
		}
		if i > 0 && intent.Edges[i-1].ID >= edge.ID {
			return errors.New("edges must be ordered by unique id")
		}
		if _, ok := cells[edge.AuthorityCellID]; !ok {
			return fmt.Errorf("edge %q references unknown authority cell %q", edge.ID, edge.AuthorityCellID)
		}
		usedCells[edge.AuthorityCellID] = struct{}{}
		if len(edge.ServingPoolIDs) == 0 {
			return fmt.Errorf("edge %q has no serving pool", edge.ID)
		}
		for j, poolID := range edge.ServingPoolIDs {
			if _, ok := pools[poolID]; !ok {
				return fmt.Errorf("edge %q references unknown serving pool %q", edge.ID, poolID)
			}
			if j > 0 && edge.ServingPoolIDs[j-1] >= poolID {
				return fmt.Errorf("edge %q serving pools must be ordered and unique", edge.ID)
			}
			usedPools[poolID] = struct{}{}
		}
		if len(edge.Capabilities) == 0 {
			return fmt.Errorf("edge %q has no declared capabilities", edge.ID)
		}
		for j, capability := range edge.Capabilities {
			if !validID(capability) || (j > 0 && edge.Capabilities[j-1] >= capability) {
				return fmt.Errorf("edge %q capabilities must be ordered and unique", edge.ID)
			}
		}
		if len(edge.FailureDomains) == 0 {
			return fmt.Errorf("edge %q needs explicit failure domains", edge.ID)
		}
		for dimension, value := range edge.FailureDomains {
			if !validID(dimension) || !validID(value) || dimension == "country" || dimension == "region" {
				return fmt.Errorf("edge %q has invalid failure domain %q=%q", edge.ID, dimension, value)
			}
		}
		for label, value := range edge.Labels {
			if !validID(label) || strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n") {
				return fmt.Errorf("edge %q has invalid label %q", edge.ID, label)
			}
		}
	}
	if len(usedCells) != len(cells) || len(usedPools) != len(pools) {
		return errors.New("edge topology contains an unused authority cell or serving pool")
	}
	return nil
}

func validID(value string) bool { return identityPattern.MatchString(value) && len(value) <= 128 }

func (cell AuthorityCell) ServingGroupID() string {
	if cell.LegacyGroupID != "" {
		return cell.LegacyGroupID
	}
	return cell.ID
}

// Audit compares explicit intent with a read-only discovery snapshot. Discovery
// eligibility is global; a clean audit never authorizes a tenant route or DNS.
type Audit struct {
	UnknownEdges        []string `json:"unknown_edges,omitempty"`
	MismatchedGroups    []string `json:"mismatched_groups,omitempty"`
	UnobservedEdges     []string `json:"unobserved_edges,omitempty"`
	ObservedEdgeCount   int      `json:"observed_edge_count"`
	ConfiguredEdgeCount int      `json:"configured_edge_count"`
}

type ObservedEdge struct {
	ID            string
	LegacyGroupID string
}

func (intent Intent) Audit(observed []ObservedEdge) Audit {
	cells := make(map[string]string, len(intent.Cells))
	for _, cell := range intent.Cells {
		cells[cell.ID] = cell.ServingGroupID()
	}
	edges := make(map[string]Edge, len(intent.Edges))
	for _, edge := range intent.Edges {
		edges[edge.ID] = edge
	}
	seen := make(map[string]struct{}, len(observed))
	result := Audit{ObservedEdgeCount: len(observed), ConfiguredEdgeCount: len(intent.Edges)}
	for _, node := range observed {
		if _, duplicate := seen[node.ID]; duplicate {
			result.MismatchedGroups = append(result.MismatchedGroups, node.ID)
			continue
		}
		seen[node.ID] = struct{}{}
		edge, known := edges[node.ID]
		if !known {
			result.UnknownEdges = append(result.UnknownEdges, node.ID)
		} else if cells[edge.AuthorityCellID] != node.LegacyGroupID {
			result.MismatchedGroups = append(result.MismatchedGroups, node.ID)
		}
	}
	for _, edge := range intent.Edges {
		if _, ok := seen[edge.ID]; !ok {
			result.UnobservedEdges = append(result.UnobservedEdges, edge.ID)
		}
	}
	sort.Strings(result.UnknownEdges)
	sort.Strings(result.MismatchedGroups)
	sort.Strings(result.UnobservedEdges)
	return result
}
