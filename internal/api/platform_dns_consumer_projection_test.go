package api

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDNSConsumerMigrationCapturesDeclarationsWithoutHealth(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	nodes := []model.DNSNode{
		{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "example.test", PublicIPv4: "8.8.8.8"},
		{ID: "dns-a-zone", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "second.test", PublicIPv4: "8.8.8.8"},
		{ID: "dns-b", PhysicalNodeID: "dns-b", EdgeGroupID: "group-b", Zone: "example.test", PublicIPv4: "9.9.9.9"},
	}
	makeDraft := func() platformIntentProjectionResponse {
		return platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Scope: "global", Generation: "original"}}
	}
	first := makeDraft()
	if err := projectDNSConsumerDeclarations(&first, nodes, map[string][]string{"group-a": {"example.test", "second.test"}, "group-b": {"example.test"}}, 60, now); err != nil {
		t.Fatal(err)
	}
	if len(first.Intent.DNSConsumers) != 2 || len(first.RuntimeSnapshot.DNSConsumers) != 2 || len(first.Intent.DNSConsumers[0].Zones) != 2 {
		t.Fatalf("alias became a physical process: %+v", first.Intent.DNSConsumers)
	}
	slices.Reverse(nodes)
	second := makeDraft()
	if err := projectDNSConsumerDeclarations(&second, nodes, map[string][]string{"group-a": {"example.test", "second.test"}, "group-b": {"example.test"}}, 60, now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("inventory enumeration changes desired state")
	}
	for i := range nodes {
		if nodes[i].PhysicalNodeID == "dns-a" {
			nodes[i].PublicIPv4 = "1.1.1.1"
		}
	}
	third := makeDraft()
	if err := projectDNSConsumerDeclarations(&third, nodes, map[string][]string{"group-a": {"example.test", "second.test"}, "group-b": {"example.test"}}, 60, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Intent, third.Intent) || reflect.DeepEqual(first.RuntimeSnapshot, third.RuntimeSnapshot) {
		t.Fatal("endpoint observations or capture time changed desired intent")
	}
}

func TestDNSConsumerMigrationRejectsConflictingOwnershipAtomically(t *testing.T) {
	now := time.Now().UTC()
	base := []model.DNSNode{
		{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "example.test", PublicIPv4: "8.8.8.8"},
		{ID: "dns-a-zone", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "second.test", PublicIPv4: "8.8.8.8"},
	}
	for name, mutate := range map[string]func([]model.DNSNode){
		"alias group":     func(n []model.DNSNode) { n[1].EdgeGroupID = "group-b" },
		"alias address":   func(n []model.DNSNode) { n[1].PublicIPv4 = "9.9.9.9" },
		"duplicate zone":  func(n []model.DNSNode) { n[1].Zone = "example.test" },
		"alias cycle":     func(n []model.DNSNode) { n[0].PhysicalNodeID = "dns-a-zone" },
		"missing address": func(n []model.DNSNode) { n[0].PublicIPv4 = ""; n[1].PublicIPv4 = "" },
	} {
		t.Run(name, func(t *testing.T) {
			nodes := append([]model.DNSNode(nil), base...)
			mutate(nodes)
			draft := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "original", Scope: "global"}}
			before, _ := json.Marshal(draft)
			if err := projectDNSConsumerDeclarations(&draft, nodes, map[string][]string{"group-a": {"example.test", "second.test"}, "group-b": {"example.test"}}, 60, now); err == nil {
				t.Fatal("invalid ownership accepted")
			}
			after, _ := json.Marshal(draft)
			if string(before) != string(after) {
				t.Fatal("failed capture partially updated projection")
			}
		})
	}
}

func TestDNSConsumerDeclaredZonesIgnoreHistoricalHeartbeats(t *testing.T) {
	now := time.Now().UTC()
	workload := appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "dns", Env: []corev1.EnvVar{
		{Name: "FUGUE_DNS_ZONE", Value: "example.test"}, {Name: "FUGUE_EDGE_GROUP_ID", Value: "group-a"}, {Name: "FUGUE_DNS_EXTRA_ZONES", Value: "static.test"},
	}}}}}}}
	hosted := []model.HostedZone{{ZoneName: "new.test", Status: model.HostedZoneStatusPendingDelegation}, {ZoneName: "degraded.test", Status: model.HostedZoneStatusDegraded}, {ZoneName: "deleted.test", Status: model.HostedZoneStatusDeleted}, {ZoneName: "suspended.test", Status: model.HostedZoneStatusSuspended}}
	zones, err := dnsConsumerZonesFromWorkloads([]appsv1.DaemonSet{workload}, hosted)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"degraded.test", "example.test", "new.test", "static.test"}
	if !reflect.DeepEqual(zones["group-a"], want) {
		t.Fatalf("wrong desired zones: %v", zones)
	}
	nodes := []model.DNSNode{
		{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "example.test", PublicIPv4: "8.8.8.8", Healthy: false},
		{ID: "dns-a-old-zone", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "deleted.test", PublicIPv4: "9.9.9.9", Healthy: true},
		{ID: "dns-a-unknown-zone", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "forgotten.test", PublicIPv4: "9.9.9.9", Healthy: true},
	}
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Scope: "global"}}
	if err := projectDNSConsumerDeclarations(&result, nodes, zones, 60, now); err != nil {
		t.Fatal(err)
	}
	if len(result.Intent.DNSConsumers) != 1 || !reflect.DeepEqual(result.Intent.DNSConsumers[0].Zones, want) || result.RuntimeSnapshot.DNSConsumers[0].A[0] != "8.8.8.8" {
		t.Fatalf("history altered desired topology: %+v", result.Intent.DNSConsumers)
	}
	duplicate := workload.DeepCopy()
	if _, err := dnsConsumerZonesFromWorkloads([]appsv1.DaemonSet{workload, *duplicate}, hosted); err == nil {
		t.Fatal("ambiguous group accepted")
	}
	duplicate.DeletionTimestamp = &metav1.Time{Time: now}
	if _, err := dnsConsumerZonesFromWorkloads([]appsv1.DaemonSet{workload, *duplicate}, hosted); err != nil {
		t.Fatal("removed workload blocks capture", err)
	}
	workload.Spec.Template.Spec.Containers[0].Env[0].ValueFrom = &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{Key: "zone"}}
	if _, err := dnsConsumerZonesFromWorkloads([]appsv1.DaemonSet{workload}, hosted); err == nil {
		t.Fatal("unresolved external declaration accepted")
	}
}
