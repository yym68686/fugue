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
	return validateDNSAuthorityHandoff(previous, next, node, nil)
}

// ValidateDNSAuthorityHandoffs checks each physical node against the same
// predecessor and transition. Like the single-node check, it only compares
// configured behavior and does not authorize publication or serving.
func ValidateDNSAuthorityHandoffs(previous, next model.PlatformArtifact, nodes []string) error {
	return validateDNSAuthorityHandoffNodes(previous, next, nodes, nil)
}

// An availability handoff permits exactly one explicitly declared host policy
// addition, while preserving every other configured answer behavior. The new
// policy intersects the original signed candidate/route proof requirements.
// Authentication, current publication and fresh runtime checks remain separate.
func ValidateDNSAvailabilityHandoff(previous, next model.PlatformArtifact, node string, route DNSAvailabilityRoute) error {
	if !validDNSConsumerZone(route.Hostname) || model.ValidateDNSAvailabilityPolicy(&route.Policy) != nil {
		return fmt.Errorf("DNS availability handoff requires one exact host policy")
	}
	return validateDNSAuthorityHandoff(previous, next, node, &route)
}

func validateDNSAuthorityHandoff(previous, next model.PlatformArtifact, node string, availability *DNSAvailabilityRoute) error {
	return validateDNSAuthorityHandoffNodes(previous, next, []string{node}, availability)
}

func validateDNSAuthorityHandoffNodes(previous, next model.PlatformArtifact, nodes []string, availability *DNSAvailabilityRoute) error {
	if len(nodes) == 0 || nodes[0] == "" || previous.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || next.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
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
	raw, _ := marshalDNSHandoffFields(next.Content, true)
	if json.Unmarshal(raw, &source) != nil || source.Source == nil {
		return fmt.Errorf("DNS handoff transition source missing")
	}
	aliases, err := RouteAuthorityAliases(source.Source.Intent)
	if err != nil {
		return err
	}
	var previousDocument, nextDocument *dnsHandoffDocument
	var reuseDocuments bool
	if len(nodes) > 1 {
		previousDocument, nextDocument, reuseDocuments = decodeDNSHandoffDocumentsOnce(previous, next)
	}
	for _, node := range nodes {
		if node == "" {
			return fmt.Errorf("DNS handoff requires two DNS artifacts and a physical node")
		}
		var err error
		if reuseDocuments {
			err = compareDNSHandoffDocuments(previousDocument, nextDocument, node, source.Source.Intent, aliases, availability)
		} else {
			err = compareDNSHandoffNode(previous, next, node, source.Source.Intent, aliases, availability)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func compareDNSHandoffNode(previous, next model.PlatformArtifact, node string, intent PlatformIntent, aliases map[string]string, availability *DNSAvailabilityRoute) error {
	old, err := dnsHandoffBehavior(previous, node, intent.AuthorityCellID, aliases)
	if err != nil {
		return err
	}
	current, err := dnsHandoffBehavior(next, node, intent.AuthorityCellID, nil)
	if err != nil {
		return err
	}
	return compareDNSHandoffBehaviors(old, current, intent, availability)
}

func compareDNSHandoffDocuments(previous, next *dnsHandoffDocument, node string, intent PlatformIntent, aliases map[string]string, availability *DNSAvailabilityRoute) error {
	old, err := previous.behavior(node, intent.AuthorityCellID, aliases)
	if err != nil {
		return err
	}
	current, err := next.behavior(node, intent.AuthorityCellID, nil)
	if err != nil {
		return err
	}
	return compareDNSHandoffBehaviors(old, current, intent, availability)
}

func compareDNSHandoffBehaviors(old, current map[string]any, intent PlatformIntent, availability *DNSAvailabilityRoute) error {
	if availability != nil {
		if !DNSUsesRetainedRoutes(intent) {
			return fmt.Errorf("availability handoff requires retained routing authority")
		}
		if err := removeDeclaredAvailabilityDelta(old, current, *availability); err != nil {
			return err
		}
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

func removeDeclaredAvailabilityDelta(previous, next map[string]any, route DNSAvailabilityRoute) error {
	oldQuery, oldOK := previous["dns_query_policy"].(map[string]any)
	newQuery, newOK := next["dns_query_policy"].(map[string]any)
	if !oldOK || !newOK {
		return fmt.Errorf("availability handoff lacks DNS policy")
	}
	before, _ := oldQuery["availability_routes"].([]any)
	after, _ := newQuery["availability_routes"].([]any)
	raw, _ := json.Marshal(route)
	var declared map[string]any
	if json.Unmarshal(raw, &declared) != nil {
		return fmt.Errorf("availability handoff policy invalid")
	}
	for _, item := range before {
		if row, ok := item.(map[string]any); !ok || row["hostname"] == route.Hostname {
			return fmt.Errorf("availability handoff cannot replace an existing policy")
		}
	}
	remaining, count := []any{}, 0
	for _, item := range after {
		row, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("availability handoff policy invalid")
		}
		if row["hostname"] == route.Hostname {
			if !reflect.DeepEqual(row, declared) {
				return fmt.Errorf("availability handoff differs from declared host policy")
			}
			count++
		} else {
			remaining = append(remaining, row)
		}
	}
	if count != 1 || len(remaining) != len(before) {
		return fmt.Errorf("availability handoff changes unrelated host policy")
	}
	for i := range before {
		if !reflect.DeepEqual(before[i], remaining[i]) {
			return fmt.Errorf("availability handoff changes unrelated host policy")
		}
	}
	if v, ok := oldQuery["availability_routes"]; ok {
		newQuery["availability_routes"] = v
	} else {
		delete(newQuery, "availability_routes")
	}
	matched := 0
	views, ok := next["query_views"].([]any)
	if !ok {
		return fmt.Errorf("availability handoff query views missing")
	}
	for _, item := range views {
		view, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("availability handoff view invalid")
		}
		records, ok := view["records"].([]any)
		if !ok {
			return fmt.Errorf("availability handoff records missing")
		}
		for _, item := range records {
			record := item.(map[string]any)
			if record["name"] != route.Hostname {
				continue
			}
			policy, ok := record["answer_policy"].(map[string]any)
			if !ok || !reflect.DeepEqual(policy["availability_policy"], declared["policy"]) {
				return fmt.Errorf("availability handoff record lacks exact policy")
			}
			delete(policy, "availability_policy")
			matched++
		}
	}
	if matched == 0 {
		return fmt.Errorf("availability handoff has no owned node query")
	}
	return nil
}

// Retain unknown fields in the comparison: adding a future behavior field must
// not accidentally make old and new answers look equivalent. Rewrite only the
// finite list of explicitly declared routing authority aliases.
func dnsHandoffBehavior(a model.PlatformArtifact, node, authority string, aliases map[string]string) (map[string]any, error) {
	document, err := decodeDNSHandoffDocument(a)
	if err != nil {
		return nil, err
	}
	return document.behavior(node, authority, aliases)
}

type dnsHandoffDocument struct {
	content map[string]any
	shared  bool
}

// Custom JSON trees keep the original per-node encoder calls and effects.
// Ordinary signed JSON trees can be decoded once and projected independently
// for each physical node.
func decodeDNSHandoffDocumentsOnce(previous, next model.PlatformArtifact) (*dnsHandoffDocument, *dnsHandoffDocument, bool) {
	remaining := 262144
	if !dnsValidationNativeTree(previous.Content, 0, &remaining) {
		return nil, nil, false
	}
	remaining = 262144
	if !dnsValidationNativeTree(next.Content, 0, &remaining) {
		return nil, nil, false
	}
	old, err := decodeDNSHandoffDocument(previous)
	if err != nil {
		return nil, nil, false
	}
	current, err := decodeDNSHandoffDocument(next)
	if err != nil {
		return nil, nil, false
	}
	old.shared, current.shared = true, true
	return old, current, true
}

func decodeDNSHandoffDocument(a model.PlatformArtifact) (*dnsHandoffDocument, error) {
	raw, err := marshalDNSHandoffFields(a.Content, false)
	if err != nil {
		return nil, err
	}
	var content map[string]any
	if json.Unmarshal(raw, &content) != nil {
		return nil, fmt.Errorf("DNS handoff content invalid")
	}
	return &dnsHandoffDocument{content: content}, nil
}

func (d *dnsHandoffDocument) behavior(node, authority string, aliases map[string]string) (map[string]any, error) {
	policy, ok := d.content["policy"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("DNS handoff policy missing")
	}
	out := map[string]any{}
	for _, key := range []string{"dns_query_policy", "dns_route_state_constraints", "dns_placement_mode"} {
		out[key] = d.own(policy[key])
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
				selected = append(selected, d.own(row))
			}
		}
		if key != "dns_answer_rules" && len(selected) == 0 {
			return nil, fmt.Errorf("DNS handoff physical node has no complete zone/client policy")
		}
		sortDNSHandoffRows(selected, "zone", "hostname", "type")
		out[key] = selected
	}
	for _, key := range []string{"consumer_views", "query_views"} {
		rows, _ := d.content[key].([]any)
		selected := []any{}
		for _, value := range rows {
			row, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("DNS handoff node view invalid")
			}
			if row["node_id"] != node {
				continue
			}
			row = d.own(row).(map[string]any)
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

func (d *dnsHandoffDocument) own(value any) any {
	if d.shared {
		return cloneDNSHandoffValue(value)
	}
	return value
}

func cloneDNSHandoffValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(value))
		for key, child := range value {
			cloned[key] = cloneDNSHandoffValue(child)
		}
		return cloned
	case []any:
		cloned := make([]any, len(value))
		for i, child := range value {
			cloned[i] = cloneDNSHandoffValue(child)
		}
		return cloned
	default:
		return value
	}
}

// These decoders inspect either the typed source or the complete generic
// policy/views. Do not repeatedly encode retained artifacts and readiness data
// that neither decoder reads. First establish that the whole tree is ordinary
// encodable JSON without invoking methods or scanning string bodies. Invalid,
// custom or over-budget trees retain the original complete encoder and its
// errors/effects. This does not replace any full content-integrity check.
func marshalDNSHandoffFields(content map[string]any, source bool) ([]byte, error) {
	remaining := 262144
	if !dnsValidationNativeTree(content, 0, &remaining) {
		return json.Marshal(content)
	}
	fields := make(map[string]any, 3)
	for key, value := range content {
		// Preserve all typed source aliases in encoding/json's sorted order.
		// The behavior decoder is a generic map: its keys are case-sensitive.
		if source && strings.EqualFold(key, "cell_dns_source") ||
			!source && (key == "policy" || key == "consumer_views" || key == "query_views") {
			fields[key] = value
		}
	}
	return json.Marshal(fields)
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
