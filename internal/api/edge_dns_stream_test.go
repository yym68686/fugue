package api

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestStreamedDNSProfilesMatchMaterializedEveryField(t *testing.T) {
	for _, minute := range []int{0, 3} {
		t.Run(fmt.Sprint(minute), func(t *testing.T) {
			now := time.Date(2026, 9, 5, 12, minute, 0, 0, time.UTC)
			st := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			samples := benchmarkEdgeQualitySamples(4096, now, 1)
			// Distinct group performance makes rank, cooldown and degradation visible.
			for i := range samples {
				if samples[i].EdgeGroupID == "edge-group-0" {
					samples[i].TTFBMS = 10
					samples[i].TotalMS = 10
					samples[i].ErrorCount = 0
				}
			}
			if err := st.RecordEdgePerformanceSamples(samples, time.Time{}); err != nil {
				t.Fatal(err)
			}
			ordered, err := st.ListEdgePerformanceSamples("", now.Add(-edgeDNSLatencyWindow))
			if err != nil {
				t.Fatal(err)
			}
			_, decisions := legacyMaterializedDNSProfiles(ordered, nil, now.Add(-time.Minute))
			if len(decisions) == 0 {
				t.Fatal("fixture must exercise enabled routing profiles")
			}
			for i := range decisions {
				decisions[i].SelectedEdgeGroupID = "edge-group-2"
				decisions[i].CooldownUntil = now.Add(time.Minute)
			}
			server := &Server{store: st}
			for _, severe := range []bool{false, true} {
				builder, degradation, err := server.loadEdgeDNSLatencyProfileBuilder(context.Background(), now, severe)
				if err != nil {
					t.Fatal(err)
				}
				got, gotUpdates := builder.finish(decisions, now)
				want, wantUpdates := legacyMaterializedDNSProfiles(ordered, decisions, now)
				if severe {
					edgeDNSApplySevereDegradeGroupsToCatalog(&got, degradation)
					edgeDNSApplySevereDegradeToCatalog(&want, ordered, now)
					if !reflect.DeepEqual(degradation, edgeDNSSevereDegradeGroups(ordered, now)) {
						t.Fatal("severity changed")
					}
				}
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotUpdates, wantUpdates) {
					t.Fatal("streaming changed catalog, rank, weights or cooldown decisions")
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if builder, severity, err := server.loadEdgeDNSLatencyProfileBuilder(ctx, now, true); err == nil || builder != nil || severity != nil {
				t.Fatal("canceled scan published partial evidence")
			}
		})
	}
}

func TestStreamedDNSProfilesPostgresParity(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	samples := benchmarkEdgeQualitySamples(8192, now, 1)
	st := benchmarkEdgeQualityPostgresStore(t, samples)
	ordered, err := st.ListEdgePerformanceSamples("", now.Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	builder, degraded, err := (&Server{store: st}).loadEdgeDNSLatencyProfileBuilder(context.Background(), now, true)
	if err != nil {
		t.Fatal(err)
	}
	want, updates := legacyMaterializedDNSProfiles(ordered, nil, now)
	edgeDNSApplySevereDegradeToCatalog(&want, ordered, now)
	got, gotUpdates := builder.finish(nil, now)
	edgeDNSApplySevereDegradeGroupsToCatalog(&got, degraded)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(updates, gotUpdates) {
		t.Fatal("Postgres streaming DNS output differs")
	}
}

func BenchmarkDNSProfilesPostgres(b *testing.B) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	samples := benchmarkEdgeQualitySamples(600000, now, 1)
	st := benchmarkEdgeQualityPostgresStore(b, samples)
	server := &Server{store: st}
	samples = nil
	for _, streamed := range []bool{false, true} {
		b.Run(fmt.Sprint("streamed=", streamed), func(b *testing.B) {
			b.ReportAllocs()
			runtime.GC()
			var baseline runtime.MemStats
			runtime.ReadMemStats(&baseline)
			var peak atomic.Uint64
			peak.Store(baseline.HeapAlloc)
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						var m runtime.MemStats
						runtime.ReadMemStats(&m)
						for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
							if peak.CompareAndSwap(old, m.HeapAlloc) {
								break
							}
						}
					}
				}
			}()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if streamed {
					builder, degraded, err := server.loadEdgeDNSLatencyProfileBuilder(context.Background(), now, true)
					if err != nil {
						b.Fatal(err)
					}
					dnsBenchmarkSink, _ = builder.finish(nil, now)
					edgeDNSApplySevereDegradeGroupsToCatalog(&dnsBenchmarkSink, degraded)
				} else {
					rows, err := st.ListEdgePerformanceSamples("", now.Add(-edgeDNSLatencyWindow))
					if err != nil {
						b.Fatal(err)
					}
					dnsBenchmarkSink, _ = legacyMaterializedDNSProfiles(rows, nil, now)
					edgeDNSApplySevereDegradeToCatalog(&dnsBenchmarkSink, rows, now)
				}
			}
			b.StopTimer()
			close(stop)
			<-done
			b.ReportMetric(float64(peak.Load())/1048576, "peak-heap-MiB")
		})
	}
}

var dnsBenchmarkSink edgeDNSLatencyProfileCatalog

// Frozen pre-change aggregation for full-output parity, not a second call to
// the new implementation.
func legacyMaterializedDNSProfiles(samples []model.EdgePerformanceSample, decisions []model.EdgeDNSRoutingDecision, now time.Time) (edgeDNSLatencyProfileCatalog, []model.EdgeDNSRoutingDecision) {
	byHostnameScope := make(map[string]map[string]map[string]*edgeDNSLatencyGroupAccumulator)
	for _, sample := range samples {
		hostname := normalizeExternalAppDomain(sample.Hostname)
		edgeGroupID := strings.TrimSpace(sample.EdgeGroupID)
		if hostname == "" || edgeGroupID == "" {
			continue
		}
		for _, scope := range edgeDNSLatencyScopesForSample(sample) {
			if _, ok := byHostnameScope[hostname]; !ok {
				byHostnameScope[hostname] = make(map[string]map[string]*edgeDNSLatencyGroupAccumulator)
			}
			scopeKey := scope.key()
			if _, ok := byHostnameScope[hostname][scopeKey]; !ok {
				byHostnameScope[hostname][scopeKey] = make(map[string]*edgeDNSLatencyGroupAccumulator)
			}
			edgeDNSLatencyAccumulate(byHostnameScope[hostname][scopeKey], edgeGroupID, sample)
		}
	}

	decisionByKey := make(map[string]model.EdgeDNSRoutingDecision, len(decisions))
	for _, decision := range decisions {
		key := edgeDNSRoutingDecisionKey(normalizeExternalAppDomain(decision.Hostname), strings.TrimSpace(decision.ScopeKey))
		if key != "" {
			decisionByKey[key] = decision
		}
	}

	catalog := edgeDNSLatencyProfileCatalog{
		Global: make(map[string]*edgeDNSLatencyProfile),
		Scoped: make(map[string][]edgeDNSLatencyProfile),
	}
	updates := []model.EdgeDNSRoutingDecision{}
	hostnames := make([]string, 0, len(byHostnameScope))
	for hostname := range byHostnameScope {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	for _, hostname := range hostnames {
		scopes := byHostnameScope[hostname]
		scopeKeys := make([]string, 0, len(scopes))
		for scopeKey := range scopes {
			scopeKeys = append(scopeKeys, scopeKey)
		}
		sort.Strings(scopeKeys)
		for _, scopeKey := range scopeKeys {
			groups := scopes[scopeKey]
			scope := edgeDNSLatencyScopeFromKey(scopeKey)
			profile := buildEdgeDNSLatencyProfile(hostname, scope, groups)
			if profile == nil || !profile.Enabled {
				continue
			}
			decisionKey := edgeDNSRoutingDecisionKey(hostname, scopeKey)
			profile, decision := applyEdgeDNSRoutingDecision(profile, decisionByKey[decisionKey], now)
			updates = append(updates, decision)
			if profile.Scope.global() {
				catalog.Global[hostname] = profile
				continue
			}
			catalog.Scoped[hostname] = append(catalog.Scoped[hostname], *profile)
		}
	}
	sortEdgeDNSRoutingDecisionUpdates(updates)
	for hostname := range catalog.Scoped {
		sort.Slice(catalog.Scoped[hostname], func(i, j int) bool {
			return catalog.Scoped[hostname][i].Scope.key() < catalog.Scoped[hostname][j].Scope.key()
		})
	}
	return catalog, updates
}
