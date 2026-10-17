package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func placementPolicyReaderFixture(hosts, policyRules int) platformIntentProjectionResponse {
	projection, _, strategy, _ := directQueryFixture()
	templateRoute, templateDNS := projection.Intent.Routes[0], projection.Intent.DNS[0]
	projection.Intent.Routes, projection.Intent.DNS = nil, nil
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	projection.Policy.DNSQueryPolicy = &strategy
	for i := 0; i < hosts; i++ {
		host := fmt.Sprintf("app-%04d.example.test", i)
		route := templateRoute
		route.Hostname = host
		record := templateDNS
		record.Hostname = host
		intent := *templateDNS.Route
		intent.Hostnames = []string{host}
		record.Route = &intent
		projection.Intent.Routes = append(projection.Intent.Routes, route)
		projection.Intent.DNS = append(projection.Intent.DNS, record)
	}
	for i := 0; i < policyRules; i++ {
		host := fmt.Sprintf("policy-%04d.example.test", i)
		projection.Policy.DNSAnswerRules = append(projection.Policy.DNSAnswerRules, platformconfig.DNSAnswerRule{
			NodeID: "dns-a", Hostname: host, Type: "A", SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder,
			PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}, TTLSeconds: 60,
		})
	}
	return projection
}

func TestPlacementPolicyReaderMatchesEveryHostnameAndRejection(t *testing.T) {
	for _, mode := range []string{"valid", "alias", "shared", "disabled", "missing-origin", "stale-origin", "foreign-owner", "invalid-unrelated-policy", "invalid-dns-policy", "invalid-route", "missing-host", "error-order"} {
		t.Run(mode, func(t *testing.T) {
			projection := placementPolicyReaderFixture(3, 16)
			switch mode {
			case "alias":
				projection.Intent.DNS[0].Hostname = "alias.example.test"
				projection.Intent.DNS[0].RecordKind = model.EdgeDNSRecordKindCustomDomainTarget
			case "shared":
				projection.Intent.DNS[0].Route.Hostnames = []string{projection.Intent.Routes[0].Hostname, projection.Intent.Routes[1].Hostname}
			case "disabled":
				projection.Intent.Routes[0].Enabled = false
			case "missing-origin":
				projection.RuntimeSnapshot.Origins = nil
			case "stale-origin":
				projection.RuntimeSnapshot.Origins[0].ObservedAt = projection.CapturedAt.Add(-48 * time.Hour)
			case "foreign-owner":
				projection.Intent.DNS[0].TenantID = "foreign"
			case "invalid-unrelated-policy":
				projection.Policy.CanaryWeights = []int{101}
			case "invalid-dns-policy":
				projection.Policy.DNSAnswerRules[0].TTLSeconds = -1
			case "invalid-route":
				projection.Intent.Routes[0].UpstreamURL = "not-http"
			case "missing-host":
				projection.Intent.DNS[0].Route.Hostnames = []string{"absent.example.test"}
			case "error-order":
				projection.Policy.CanaryWeights = []int{101}
				projection.RuntimeSnapshot.Origins = nil
			}
			before, _ := json.Marshal(projection)
			read := newPlacementRecordRouteReader(projection)
			for _, record := range projection.Intent.DNS {
				wantRoutes, wantProjection, wantErr := placementRecordRoutes(projection, record)
				for range 2 {
					gotRoutes, gotProjection, err := read(record)
					if !reflect.DeepEqual(gotRoutes, wantRoutes) || !reflect.DeepEqual(gotProjection, wantProjection) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
						t.Fatal("phase reader changed result or error order", record.Hostname, err, wantErr)
					}
				}
			}
			after, _ := json.Marshal(projection)
			if string(before) != string(after) {
				t.Fatal("projection input mutated")
			}
		})
	}
}

func TestPlacementPolicyReaderConcurrentCallsOwnOutputs(t *testing.T) {
	projection := placementPolicyReaderFixture(8, 32)
	read := newPlacementRecordRouteReader(projection)
	var wg sync.WaitGroup
	for _, record := range projection.Intent.DNS {
		want, expected, err := placementRecordRoutes(projection, record)
		if err != nil {
			t.Fatal(err)
		}
		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, projected, err := read(record)
				if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(projected, expected) {
					t.Error("concurrent result changed", err)
					return
				}
				got[0].Hostname = "caller-edit.example.test"
				projected[0].Hostname = "caller-edit.example.test"
			}()
		}
	}
	wg.Wait()
}

func TestPlacementPolicyReaderRebuiltAtPolicyAndTimeBoundaries(t *testing.T) {
	projection := placementPolicyReaderFixture(2, 4)
	record := projection.Intent.DNS[0]
	read := newPlacementRecordRouteReader(projection)
	if _, _, err := read(record); err != nil {
		t.Fatal(err)
	}
	projection.Policy.DNSAnswerRules[0].TTLSeconds = -1
	if _, _, err := newPlacementRecordRouteReader(projection)(record); err == nil {
		t.Fatal("policy change reused old validation")
	}
	projection.Policy.DNSAnswerRules[0].TTLSeconds = 60
	// A new phase with a later capture time cannot renew old origin evidence.
	later := projection.CapturedAt.Add(48 * time.Hour)
	projection.RuntimeSnapshot.CapturedAt = &later
	if _, _, err := newPlacementRecordRouteReader(projection)(record); err == nil {
		t.Fatal("stale origin accepted at new phase time")
	}
}

func BenchmarkPlacementPolicyPhase(b *testing.B) {
	for _, count := range []int{16, 64} {
		projection := placementPolicyReaderFixture(count, count*16)
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("hosts=%d/policy_rules=%d/reuse=%t", count, count*16, reuse), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					read := func(record platformconfig.DNSIntent) ([]platformconfig.CompiledRoute, []model.EdgeRouteIntent, error) {
						return placementRecordRoutes(projection, record)
					}
					if reuse {
						read = newPlacementRecordRouteReader(projection)
					}
					for _, record := range projection.Intent.DNS {
						if _, _, err := read(record); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
