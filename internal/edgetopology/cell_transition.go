package edgetopology

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
)

// CellTransition is a non-serving description of a single identity change.
// It binds both complete topology intents but contains no health, leases,
// artifact signatures or permission to publish or switch public transport.
type CellTransition struct {
	Schema            string   `json:"schema"`
	AuthorizesTraffic bool     `json:"authorizes_traffic"`
	PreviousDigest    string   `json:"previous_topology_digest"`
	NextDigest        string   `json:"next_topology_digest"`
	CellID            string   `json:"cell_id"`
	PreviousAuthority string   `json:"previous_authority_id"`
	NextAuthority     string   `json:"next_authority_id"`
	EdgeIDs           []string `json:"edge_ids"`
}

// PlanCellTransition prevents a neutral identity migration from silently
// changing serving membership, locality labels, capabilities or shared risk.
// Runtime authority and recovery evidence must be obtained independently.
func PlanCellTransition(previous, next Intent) (CellTransition, error) {
	if err := previous.Validate(); err != nil {
		return CellTransition{}, err
	}
	if err := next.Validate(); err != nil {
		return CellTransition{}, err
	}
	if len(previous.Cells) != len(next.Cells) {
		return CellTransition{}, errors.New("cell identity transition cannot change the cell set")
	}
	matched := -1
	for i, old := range previous.Cells {
		candidate := next.Cells[i]
		if old.ID != candidate.ID {
			return CellTransition{}, errors.New("cell identity transition cannot rename a topology cell")
		}
		if old == candidate {
			continue
		}
		if matched != -1 || old.LegacyGroupID == "" || candidate.LegacyGroupID != "" || !strings.HasPrefix(old.ID, "cell-") {
			return CellTransition{}, errors.New("exactly one legacy authority may transition to its neutral cell identity")
		}
		matched = i
	}
	if matched == -1 {
		return CellTransition{}, errors.New("cell identity transition has no changed authority")
	}
	expected := previous.Clone()
	expected.Cells[matched].LegacyGroupID = ""
	if !reflect.DeepEqual(expected, next.Clone()) {
		return CellTransition{}, errors.New("cell identity transition changes placement, membership, capabilities, labels or failure domains")
	}
	old := previous.Cells[matched]
	plan := CellTransition{
		Schema: "fugue.cell-identity-transition-plan/v1", PreviousDigest: topologyDigest(previous), NextDigest: topologyDigest(next),
		CellID: old.ID, PreviousAuthority: old.ServingGroupID(), NextAuthority: next.Cells[matched].ServingGroupID(),
	}
	for _, edge := range previous.Edges {
		if edge.AuthorityCellID == old.ID {
			plan.EdgeIDs = append(plan.EdgeIDs, edge.ID)
		}
	}
	return plan, nil
}

func topologyDigest(intent Intent) string {
	raw, _ := json.Marshal(intent)
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])
}
