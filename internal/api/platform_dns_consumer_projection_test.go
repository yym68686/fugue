package api

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
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
	if err := projectDNSConsumerDeclarations(&first, nodes, 60, now); err != nil {
		t.Fatal(err)
	}
	if len(first.Intent.DNSConsumers) != 2 || len(first.RuntimeSnapshot.DNSConsumers) != 2 || len(first.Intent.DNSConsumers[0].Zones) != 2 {
		t.Fatalf("alias became a physical process: %+v", first.Intent.DNSConsumers)
	}
	slices.Reverse(nodes)
	second := makeDraft()
	if err := projectDNSConsumerDeclarations(&second, nodes, 60, now); err != nil {
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
	if err := projectDNSConsumerDeclarations(&third, nodes, 60, now.Add(time.Hour)); err != nil {
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
			if err := projectDNSConsumerDeclarations(&draft, nodes, 60, now); err == nil {
				t.Fatal("invalid ownership accepted")
			}
			after, _ := json.Marshal(draft)
			if string(before) != string(after) {
				t.Fatal("failed capture partially updated projection")
			}
		})
	}
}
