package api

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// This is a migration capture of existing DNS declarations, not a consumer
// serving read. Desired zone/identity and endpoint observations are separated
// before compilation and get independent intent/input digests.
func projectDNSConsumerDeclarations(result *platformIntentProjectionResponse, nodes []model.DNSNode, ttl int, captured time.Time) error {
	original := result
	draft := *result
	result = &draft
	type declaration struct {
		intent platformconfig.DNSConsumerIntent
		fact   platformconfig.DNSConsumerObservation
		zones  map[string]bool
	}
	consumers := map[string]*declaration{}
	aliases := map[string]string{}
	for _, node := range nodes {
		id := firstNonEmpty(strings.TrimSpace(node.PhysicalNodeID), strings.TrimSpace(node.ID))
		if id == "" || node.ID == "" || node.EdgeGroupID == "" || node.Zone == "" {
			return fmt.Errorf("DNS migration declaration is incomplete")
		}
		if prior, ok := aliases[node.ID]; ok && prior != id {
			return fmt.Errorf("DNS migration alias has conflicting owners")
		}
		aliases[node.ID] = id
		fact := platformconfig.DNSConsumerObservation{NodeID: id, EdgeGroupID: node.EdgeGroupID, ObservedAt: captured}
		if node.PublicIPv4 != "" {
			fact.A = []string{node.PublicIPv4}
		}
		if node.PublicIPv6 != "" {
			fact.AAAA = []string{node.PublicIPv6}
		}
		c := consumers[id]
		if c == nil {
			c = &declaration{intent: platformconfig.DNSConsumerIntent{NodeID: id, EdgeGroupID: node.EdgeGroupID, ProbeLabel: defaultEdgeDNSProbeLabel, ProbeTTL: ttl}, fact: fact, zones: map[string]bool{}}
			consumers[id] = c
		} else if !reflect.DeepEqual(c.fact, fact) {
			return fmt.Errorf("DNS zone aliases disagree on physical endpoint ownership")
		}
		if c.zones[node.Zone] {
			return fmt.Errorf("duplicate DNS process zone declaration")
		}
		c.zones[node.Zone] = true
		c.intent.Zones = append(c.intent.Zones, node.Zone)
	}
	for id := range consumers {
		if owner, ok := aliases[id]; ok && owner != id {
			return fmt.Errorf("DNS physical process is also an alias")
		}
	}
	result.Intent.DNSConsumers = nil
	result.RuntimeSnapshot.DNSConsumers = nil
	ids := make([]string, 0, len(consumers))
	for id := range consumers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		c := consumers[id]
		sort.Strings(c.intent.Zones)
		result.Intent.DNSConsumers = append(result.Intent.DNSConsumers, c.intent)
		result.RuntimeSnapshot.DNSConsumers = append(result.RuntimeSnapshot.DNSConsumers, c.fact)
	}
	result.CapturedAt = captured
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	if _, err := platformconfig.CompileDNSConsumerViews(result.Intent.DNSConsumers, result.RuntimeSnapshot, result.Intent.DNS); err != nil {
		return err
	}
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	result.Intent.Generation = ""
	generation, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation, result.RuntimeSnapshot.IntentGeneration = generation, generation
	*original = *result
	return nil
}
