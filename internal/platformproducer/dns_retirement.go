package platformproducer

import (
	"encoding/json"
	"fmt"
	"reflect"

	"fugue/internal/dnslegacy"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func ValidateDNSSelectorRetirement(previous, next Policy, static, oldDNS, newDNS, baseline model.PlatformArtifact) error {
	if previous.Mode != "serving" || next.Mode != "serving" || previous.Serving == nil || next.Serving == nil || previous.Serving.SinglePublication || next.Serving.SinglePublication || previous.TargetScope != "global" || next.TargetScope != "global" || previous.PublicationRole != "" || next.PublicationRole != "" || previous.RoutePlacementTransition != nil || next.RoutePlacementTransition != nil || !previous.RequireDNSQueryPolicy || !next.RequireDNSQueryPolicy || previous.Generation == next.Generation || previous.StaticIntentArtifactID != static.ID || previous.StaticIntentDigest != static.ContentHash || previous.DNSPolicyArtifactID != oldDNS.ID || previous.DNSPolicyDigest != oldDNS.ContentHash || next.DNSPolicyArtifactID != newDNS.ID || next.DNSPolicyDigest != newDNS.ContentHash || newDNS.ID == oldDNS.ID || newDNS.GenerationSequence <= oldDNS.GenerationSequence {
		return fmt.Errorf("exact continuous global predecessor required")
	}
	previous.Generation, next.Generation = "", ""
	previous.DNSPolicyArtifactID, next.DNSPolicyArtifactID = "", ""
	previous.DNSPolicyDigest, next.DNSPolicyDigest = "", ""
	if !reflect.DeepEqual(previous, next) {
		return fmt.Errorf("retirement cannot change serving controls or static intent")
	}
	intent, err := DecodeStaticIntent(static)
	if err != nil {
		return err
	}
	before, err := DecodeProjectionPolicy(oldDNS, intent.Consumers, previous.HostedZoneTemplates)
	if err != nil {
		return err
	}
	after, err := DecodeProjectionPolicy(newDNS, intent.Consumers, next.HostedZoneTemplates)
	if err != nil {
		return err
	}
	if before.DNSQueryPolicy == nil || after.DNSQueryPolicy == nil || before.DNSQueryPolicy.OrderedProjection != nil || after.DNSQueryPolicy.OrderedProjection == nil || before.Generation == after.Generation {
		return fmt.Errorf("one explicit legacy retirement required")
	}
	for _, client := range before.Clients {
		if len(client.Rules) != 0 {
			return fmt.Errorf("client scope must be migrated separately")
		}
	}
	ordered := platformconfig.CloneDNSOrderedProjection(after.DNSQueryPolicy.OrderedProjection)
	before.Generation, after.Generation = "", ""
	after.DNSQueryPolicy.OrderedProjection = nil
	after.DNSQueryPolicy.ECSEnabled = before.DNSQueryPolicy.ECSEnabled
	after.DNSQueryPolicy.ExplorationPercent = before.DNSQueryPolicy.ExplorationPercent
	if !reflect.DeepEqual(before, after) {
		return fmt.Errorf("retirement cannot alter constraints, network policy or TTL")
	}
	var payload struct {
		QueryViews []platformconfig.DNSQueryView `json:"query_views"`
	}
	raw, err := json.Marshal(baseline.Content)
	if err != nil || json.Unmarshal(raw, &payload) != nil || baseline.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || len(payload.QueryViews) == 0 {
		return fmt.Errorf("signed DNS baseline required")
	}
	overrides := map[string]model.DNSPhysicalOrder{}
	for _, entry := range ordered.Overrides {
		overrides[entry.NodeID+"\x00"+entry.Hostname+"\x00"+entry.Type] = entry.Order
	}
	allowed := map[string]bool{}
	for _, view := range payload.QueryViews {
		for _, record := range view.Records {
			if len(record.Candidates) == 0 {
				continue
			}
			for _, candidate := range record.Candidates {
				allowed[candidate.EdgeID] = true
			}
			if record.AnswerPolicy.PolicyKind == model.DNSAnswerPolicyKindPhysicalQuality {
				continue
			}
			key := view.NodeID + "\x00" + record.Name + "\x00" + record.Type
			declared, found := overrides[key]
			order, err := dnslegacy.GlobalOrder(record)
			if !found || err != nil || !reflect.DeepEqual(order, declared) {
				return fmt.Errorf("retirement must preserve the signed normal order and candidate set")
			}
			delete(overrides, key)
		}
	}
	if len(overrides) != 0 {
		return fmt.Errorf("retirement overrides target unknown or static records")
	}
	for _, edgeID := range ordered.DefaultOrder.OrderedEdgeIDs {
		if !allowed[edgeID] {
			return fmt.Errorf("default order contains an unknown physical edge")
		}
		delete(allowed, edgeID)
	}
	if len(allowed) != 0 {
		return fmt.Errorf("default order omits an existing candidate")
	}
	return nil
}
