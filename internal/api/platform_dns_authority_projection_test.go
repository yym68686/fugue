package api

import (
	"fugue/internal/platformconfig"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestDNSAuthorityCaptureMakesLegacyDefaultsExplicit(t *testing.T) {
	w := appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "dns", Env: []corev1.EnvVar{{Name: "FUGUE_DNS_ZONE", Value: "example.test"}, {Name: "FUGUE_EDGE_GROUP_ID", Value: "edge-group-a"}, {Name: "FUGUE_DNS_TTL", Value: "45"}}}}}}}}
	groups, err := dnsAuthorityFromWorkloads([]appsv1.DaemonSet{w})
	if err != nil {
		t.Fatal(err)
	}
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test", "other.test"}}}}, Policy: platformconfig.PolicySnapshot{Generation: "p"}}
	if err = projectDNSAuthorityPolicies(&result, groups); err != nil {
		t.Fatal(err)
	}
	if len(result.Policy.DNSAuthorities) != 2 || result.Policy.DNSAuthorities[0].Nameservers[0] != "ns1.example.test" || result.Policy.DNSAuthorities[1].Nameservers[0] != "ns1.other.test" || result.Policy.DNSAuthorities[0].TTLSeconds != 45 || result.RuntimeSnapshot.PolicyGeneration != result.Policy.Generation {
		t.Fatal("defaults not captured", result.Policy.DNSAuthorities)
	}
	w.Spec.Template.Spec.Containers[0].Env = append(w.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "FUGUE_DNS_TTL", Value: "90"})
	if _, err = dnsAuthorityFromWorkloads([]appsv1.DaemonSet{w}); err == nil {
		t.Fatal("ambiguous authority accepted")
	}
}
