package platformconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/model"
)

// ValidateDNSAuthorityHandoff compares the complete configured DNS behavior for
// one physical DNS node. It grants no runtime readiness or transport mutation.
// Callers must authenticate both artifacts and separately verify fresh complete
// runtime snapshots, selected publication fences and Service/Pod identities.
func ValidateDNSAuthorityHandoff(previous, next model.PlatformArtifact, node string) error {
	if node == "" || previous.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || next.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
		return fmt.Errorf("DNS handoff requires two DNS artifacts and a physical node")
	}
	if err := ValidateDNSCellPlan(next); err != nil {
		return err
	}
	retained, err := DecodePreviousTrafficPublication(next)
	if err != nil {
		return err
	}
	if retained == nil || retained.DNS.ID != previous.ID || retained.DNS.ContentHash != previous.ContentHash {
		return fmt.Errorf("DNS handoff predecessor differs from signed transition")
	}
	hash, err := Digest(previous.Content)
	if err != nil || hash != previous.ContentHash {
		return fmt.Errorf("DNS handoff predecessor content invalid")
	}
	var source struct {
		Source *CellDNSPlanSource `json:"cell_dns_source"`
	}
	raw, _ := json.Marshal(next.Content)
	if json.Unmarshal(raw, &source) != nil || source.Source == nil {
		return fmt.Errorf("DNS handoff transition source missing")
	}
	aliases, err := RouteAuthorityAliases(source.Source.Intent)
	if err != nil {
		return err
	}
	old, err := dnsHandoffBehavior(previous, node, source.Source.Intent.AuthorityCellID, aliases)
	if err != nil {
		return err
	}
	current, err := dnsHandoffBehavior(next, node, source.Source.Intent.AuthorityCellID, nil)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(old, current) {
		for key, before := range old {
			if !reflect.DeepEqual(before, current[key]) {
				return fmt.Errorf("DNS handoff changes %s", key)
			}
		}
		return fmt.Errorf("DNS handoff changes node behavior")
	}
	return nil
}

// Retain unknown fields in the comparison: adding a future behavior field must
// not accidentally make old and new answers look equivalent. Rewrite only the
// finite list of explicitly declared routing authority aliases.
func dnsHandoffBehavior(a model.PlatformArtifact, node, authority string, aliases map[string]string) (map[string]any, error) {
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return nil, err
	}
	var content map[string]any
	if json.Unmarshal(raw, &content) != nil {
		return nil, fmt.Errorf("DNS handoff content invalid")
	}
	policy, ok := content["policy"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("DNS handoff policy missing")
	}
	out := map[string]any{}
	for _, key := range []string{"dns_query_policy", "dns_route_state_constraints", "dns_placement_mode"} {
		out[key] = policy[key]
	}
	for _, key := range []string{"dns_authorities", "dns_client_policies", "dns_answer_rules"} {
		rows, _ := policy[key].([]any)
		selected := []any{}
		for _, value := range rows {
			row, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("DNS handoff policy row invalid")
			}
			if row["node_id"] == node {
				selected = append(selected, row)
			}
		}
		if key != "dns_answer_rules" && len(selected) == 0 {
			return nil, fmt.Errorf("DNS handoff physical node has no complete zone/client policy")
		}
		sortDNSHandoffRows(selected, "zone", "hostname", "type")
		out[key] = selected
	}
	for _, key := range []string{"consumer_views", "query_views"} {
		rows, _ := content[key].([]any)
		selected := []any{}
		for _, value := range rows {
			row, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("DNS handoff node view invalid")
			}
			if row["node_id"] != node {
				continue
			}
			viewGroup, ok := row["edge_group_id"].(string)
			if !ok || viewGroup == "" {
				return nil, fmt.Errorf("DNS handoff node authority missing")
			}
			row["edge_group_id"] = authority
			if records, ok := row["records"].([]any); ok {
				for _, value := range records {
					record, ok := value.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("DNS handoff record invalid")
					}
					if record["record_kind"] == model.EdgeDNSRecordKindProbe {
						if record["edge_group_id"] != viewGroup {
							return nil, fmt.Errorf("DNS probe does not belong to its physical listener")
						}
						record["edge_group_id"] = authority
					}
				}
				sortDNSHandoffRows(records, "name", "hostname", "type")
			}
			selected = append(selected, row)
		}
		if len(selected) == 0 {
			return nil, fmt.Errorf("DNS handoff node has no complete views")
		}
		sortDNSHandoffRows(selected, "zone")
		out[key] = selected
	}
	rewriteDNSAuthorityAliases(out, aliases)
	return out, nil
}

func rewriteDNSAuthorityAliases(value any, aliases map[string]string) {
	switch row := value.(type) {
	case map[string]any:
		for key, value := range row {
			switch key {
			case "edge_group_id", "fallback_edge_group_id", "selected_edge_group_id", "shadow_selected_edge_group_id":
				if text, ok := value.(string); ok && aliases[text] != "" {
					row[key] = aliases[text]
				}
			case "allowed_edge_groups", "preferred_edge_groups", "fallback_edge_groups":
				if values, ok := value.([]any); ok {
					for i, value := range values {
						if text, ok := value.(string); ok && aliases[text] != "" {
							values[i] = aliases[text]
						}
					}
				}
			default:
				rewriteDNSAuthorityAliases(value, aliases)
			}
		}
	case []any:
		for _, item := range row {
			rewriteDNSAuthorityAliases(item, aliases)
		}
	}
}

// Reordering records or zones is not an answer-policy change. Candidate and
// preference ordering deliberately remains significant for deterministic ties.
func sortDNSHandoffRows(rows []any, keys ...string) {
	slices.SortFunc(rows, func(a, b any) int {
		left, right := a.(map[string]any), b.(map[string]any)
		for _, key := range keys {
			if order := strings.Compare(fmt.Sprint(left[key]), fmt.Sprint(right[key])); order != 0 {
				return order
			}
		}
		return 0
	})
}
