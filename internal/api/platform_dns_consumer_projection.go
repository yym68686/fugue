package api

import (
	"context"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"net/http"
	"net/url"
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
func projectDNSConsumerDeclarations(result *platformIntentProjectionResponse, nodes []model.DNSNode, declaredZones map[string][]string, ttl int, captured time.Time) error {
	original := result
	draft := *result
	result = &draft
	type declaration struct {
		intent platformconfig.DNSConsumerIntent
		fact   platformconfig.DNSConsumerObservation
		zones  map[string]bool
	}
	consumers := map[string]*declaration{}
	wanted := map[string]map[string]bool{}
	for group, zones := range declaredZones {
		wanted[group] = map[string]bool{}
		for _, zone := range zones {
			wanted[group][zone] = true
		}
	}
	aliases := map[string]string{}
	required := map[string]string{}
	for _, node := range nodes {
		id := firstNonEmpty(strings.TrimSpace(node.PhysicalNodeID), strings.TrimSpace(node.ID))
		if group, exists := required[id]; exists && group != node.EdgeGroupID {
			return fmt.Errorf("DNS inventory disagrees on physical group ownership")
		}
		required[id] = node.EdgeGroupID
	}
	for _, node := range nodes {
		if len(wanted[node.EdgeGroupID]) == 0 {
			return fmt.Errorf("DNS consumer has no declared workload zones")
		}
		// Old dynamic child heartbeats remain in inventory after zone removal.
		// They cannot recreate desired zones or provide endpoint observations.
		if !wanted[node.EdgeGroupID][node.Zone] {
			continue
		}
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
	for id := range required {
		if consumers[id] == nil {
			return fmt.Errorf("required DNS consumer has no declared endpoint observation")
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
		c.intent.Zones = append([]string(nil), declaredZones[c.intent.EdgeGroupID]...)
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

// Workload configuration is read only by this bounded migration capture. The
// deterministic compiler and DNS consumers never query Kubernetes or business
// state. Hosted zones come from the same frozen business snapshot as records.
func (s *Server) captureDNSConsumerZones(ctx context.Context, hosted []model.HostedZone) (map[string][]string, error) {
	client, err := s.requireClusterNodeClient()
	if err != nil {
		return nil, err
	}
	defer client.closeIdleConnections()
	if s.controlPlaneNamespace == "" {
		return nil, fmt.Errorf("DNS workload namespace is unavailable")
	}
	var workloads appsv1.DaemonSetList
	if err = client.doJSON(ctx, http.MethodGet, "/apis/apps/v1/namespaces/"+url.PathEscape(s.controlPlaneNamespace)+"/daemonsets", &workloads); err != nil {
		return nil, err
	}
	return dnsConsumerZonesFromWorkloads(workloads.Items, hosted)
}

func dnsConsumerZonesFromWorkloads(workloads []appsv1.DaemonSet, hosted []model.HostedZone) (map[string][]string, error) {
	if len(workloads) > 4096 {
		return nil, fmt.Errorf("too many DNS workload declarations")
	}
	out := map[string][]string{}
	for _, workload := range workloads {
		if workload.DeletionTimestamp != nil {
			continue
		}
		for _, container := range workload.Spec.Template.Spec.Containers {
			isDNS := false
			for _, v := range container.Env {
				if v.Name == "FUGUE_DNS_ZONE" {
					isDNS = true
				}
			}
			if !isDNS {
				continue
			}
			env := map[string]corev1.EnvVar{}
			for _, v := range container.Env {
				switch v.Name {
				case "FUGUE_DNS_ZONE", "FUGUE_DNS_ZONES", "FUGUE_DNS_EXTRA_ZONES", "FUGUE_EDGE_GROUP_ID":
					if _, exists := env[v.Name]; exists {
						return nil, fmt.Errorf("ambiguous DNS workload environment")
					}
					env[v.Name] = v
				}
			}
			if _, ok := env["FUGUE_DNS_ZONE"]; !ok {
				continue
			}
			for _, v := range env {
				if v.ValueFrom != nil {
					return nil, fmt.Errorf("DNS zone capture requires explicit workload values")
				}
			}
			group := strings.TrimSpace(env["FUGUE_EDGE_GROUP_ID"].Value)
			primary := normalizeExternalAppDomain(env["FUGUE_DNS_ZONE"].Value)
			if group == "" || primary == "" {
				return nil, fmt.Errorf("DNS workload group or primary zone is missing")
			}
			if _, exists := out[group]; exists {
				return nil, fmt.Errorf("multiple DNS workloads claim one group")
			}
			zones := []string{primary}
			for _, key := range []string{"FUGUE_DNS_ZONES", "FUGUE_DNS_EXTRA_ZONES"} {
				for _, value := range strings.Split(env[key].Value, ",") {
					if value = strings.TrimSpace(value); value != "" {
						zones = append(zones, normalizeExternalAppDomain(value))
					}
				}
			}
			zones = append(zones, edgeDNSPublishableHostedZoneNames(hosted)...)
			out[group] = uniqueSortedStrings(zones)
		}
	}
	return out, nil
}
