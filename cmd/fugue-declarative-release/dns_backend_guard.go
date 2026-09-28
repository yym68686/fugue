package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
)

// Transport selection is independently configured. A code rollout cannot
// restart a selected DNS or independent Front executor. Publish a verified
// transport handoff before replacing code.
func (cluster *kubectlCluster) requireUnselectedDNSBackend(ctx context.Context, identity declarativerelease.ResourceIdentity, desired map[string]any) error {
	if identity.Kind != "DaemonSet" && identity.Kind != "Deployment" {
		return nil
	}
	template := mapField(mapField(desired, "spec"), "template")
	annotations := mapField(mapField(template, "metadata"), "annotations")
	var policy struct {
		Component string `json:"component"`
	}
	_ = json.Unmarshal([]byte(stringValue(annotations["fugue.pro/consumer-identity"])), &policy)
	front := annotations["fugue.pro/edge-front-transport"] == "independent/v1"
	if policy.Component != "dns-server" && !front {
		return nil
	}
	raw, err := cluster.getResource(ctx, identity)
	if err != nil {
		return fmt.Errorf("observe DNS workload before release: %w", err)
	}
	var current map[string]any
	if json.Unmarshal(raw, &current) != nil {
		return errors.New("DNS workload observation is invalid")
	}
	if current == nil {
		return nil
	}
	liveTemplate := mapField(mapField(current, "spec"), "template")
	if declarativerelease.ResourceDesiredSubset(template, liveTemplate) {
		return nil
	}
	labels := mapField(mapField(liveTemplate, "metadata"), "labels")
	if len(labels) == 0 {
		return errors.New("DNS workload has no observed selection labels")
	}
	raw, err = cluster.kubectlRead(ctx, nil, "get", "services", "-n", identity.Namespace, "-o", "json")
	if err != nil {
		return fmt.Errorf("observe DNS public transport before release: %w", err)
	}
	var services struct {
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
		Items []map[string]any `json:"items"`
	}
	if json.Unmarshal(raw, &services) != nil || services.Metadata.Continue != "" || services.Items == nil {
		return errors.New("DNS public transport observation is incomplete")
	}
	for _, service := range services.Items {
		if (!front && dnsPublicServiceSelects(service, labels)) || (front && frontPublicServiceSelects(service, labels)) {
			return fmt.Errorf("workload %s/%s is selected by public Service %s; a verified transport handoff is required before code replacement", identity.Kind, identity.Name, stringValue(mapField(service, "metadata")["name"]))
		}
	}
	return nil
}

func dnsPublicServiceSelects(service map[string]any, labels map[string]any) bool {
	spec := mapField(service, "spec")
	addresses, _ := spec["externalIPs"].([]any)
	if len(addresses) == 0 && spec["type"] != "LoadBalancer" && spec["type"] != "NodePort" {
		return false
	}
	publicDNS := false
	ports, _ := spec["ports"].([]any)
	for _, raw := range ports {
		port, _ := raw.(map[string]any)
		publicDNS = publicDNS || fmt.Sprint(port["port"]) == "53"
	}
	selector := mapField(spec, "selector")
	if !publicDNS || len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func frontPublicServiceSelects(service map[string]any, labels map[string]any) bool {
	spec := mapField(service, "spec")
	addresses, _ := spec["externalIPs"].([]any)
	if len(addresses) == 0 && spec["type"] != "LoadBalancer" && spec["type"] != "NodePort" {
		return false
	}
	ports, _ := spec["ports"].([]any)
	public := false
	for _, raw := range ports {
		port, _ := raw.(map[string]any)
		protocol := stringValue(port["protocol"])
		public = public || (protocol == "" || protocol == "TCP") && (fmt.Sprint(port["port"]) == "80" || fmt.Sprint(port["port"]) == "443")
	}
	selector := mapField(spec, "selector")
	if !public || len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}
