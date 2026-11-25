package api

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDNSMigrationComparisonPreservesSelectionSemantics(t *testing.T) {
	now := time.Now().UTC()
	base := model.EdgeDNSRecord{Name: "app.example.test", Type: "A", Values: []string{"192.0.2.1", "192.0.2.2"}, TTL: 60, Status: "active", RecordKind: "platform",
		AnswerPolicy:     model.DNSAnswerPolicy{PolicyKind: "geo", ECSEnabled: true, PreferredEdgeGroups: []string{"group-a", "group-b"}, TTLSeconds: 60},
		Candidates:       []model.EdgeDNSAnswerCandidate{{IP: "192.0.2.1", EdgeID: "edge-a", EdgeGroupID: "group-a", Healthy: true, RouteReady: true, TLSReady: true}},
		ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{{ScopeKey: "country:ZZ", Country: "ZZ", SelectedEdgeGroupID: "group-a", Candidates: []model.EdgeDNSAnswerCandidate{{IP: "192.0.2.1", EdgeID: "edge-a", EdgeGroupID: "group-a", Weight: 100}}}},
	}
	clone := func() model.EdgeDNSRecord {
		raw, _ := json.Marshal(base)
		var r model.EdgeDNSRecord
		json.Unmarshal(raw, &r)
		return r
	}
	for name, tc := range map[string]struct {
		field  string
		change func(*model.EdgeDNSRecord)
	}{
		"policy":              {"answer_policy", func(r *model.EdgeDNSRecord) { r.AnswerPolicy.ECSEnabled = false }},
		"preference order":    {"answer_policy", func(r *model.EdgeDNSRecord) { slices.Reverse(r.AnswerPolicy.PreferredEdgeGroups) }},
		"candidate readiness": {"candidates", func(r *model.EdgeDNSRecord) { r.Candidates[0].TLSReady = false }},
		"scope selection":     {"scoped_candidates", func(r *model.EdgeDNSRecord) { r.ScopedCandidates[0].SelectedEdgeGroupID = "group-b" }},
		"ttl":                 {"ttl", func(r *model.EdgeDNSRecord) { r.TTL = 30 }},
		"ownership":           {"tenant_id", func(r *model.EdgeDNSRecord) { r.TenantID = "other" }},
	} {
		t.Run(name, func(t *testing.T) {
			changed := clone()
			tc.change(&changed)
			result, err := comparePlatformDNSRecords([]model.EdgeDNSRecord{base}, []model.EdgeDNSRecord{changed}, now)
			if err != nil {
				t.Fatal(err)
			}
			if result.Equivalent || len(result.Differences) != 1 || !slices.Contains(result.Differences[0].Fields, tc.field) {
				t.Fatalf("lost DNS semantics: %+v", result)
			}
		})
	}
	reordered := clone()
	slices.Reverse(reordered.Values)
	reordered.RecordGeneration = "other"
	result, err := comparePlatformDNSRecords([]model.EdgeDNSRecord{base}, []model.EdgeDNSRecord{reordered}, now)
	if err != nil || !result.Equivalent || result.MatchingRecordCount != 1 {
		t.Fatalf("representation drift reported as behavior: %+v %v", result, err)
	}
}

func TestDNSMigrationComparisonLeasesNamesAndTXTBytes(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	leased := model.EdgeDNSRecord{Name: "_acme-challenge.example.test", Type: "TXT", Values: []string{" old ", "new"}, TTL: 60, ValueExpirations: map[string]time.Time{" old ": now.Add(-time.Second), "new": now.Add(10 * time.Second)}}
	raw, _ := json.Marshal(leased)
	result, err := comparePlatformDNSRecords([]model.EdgeDNSRecord{leased}, []model.EdgeDNSRecord{leased}, now)
	if err != nil || !result.Equivalent || result.ExpiredCandidateValueCount != 1 {
		t.Fatalf("lease comparison invalid: %+v %v", result, err)
	}
	after, _ := json.Marshal(leased)
	if string(raw) != string(after) {
		t.Fatal("comparison mutated original leases")
	}
	index, _, err := dnsMigrationRecordIndex([]model.EdgeDNSRecord{leased}, now)
	if err != nil {
		t.Fatal(err)
	}
	if index[leased.Name+"\x00TXT"]["ttl"] != float64(10) {
		t.Fatal("remaining TTL was renewed")
	}
	expired, _, err := dnsMigrationRecordIndex([]model.EdgeDNSRecord{leased}, now.Add(time.Minute))
	if err != nil || len(expired) != 1 {
		t.Fatal("expired authoritative name was discarded", err)
	}
	txt := model.EdgeDNSRecord{Name: "text.example.test", Type: "TXT", Values: []string{" value "}, TTL: 60}
	changed := txt
	changed.Values = []string{"value"}
	result, err = comparePlatformDNSRecords([]model.EdgeDNSRecord{txt}, []model.EdgeDNSRecord{changed}, now)
	if err != nil || result.Equivalent {
		t.Fatal("TXT bytes were trimmed", err)
	}
	if _, err = comparePlatformDNSRecords([]model.EdgeDNSRecord{txt, txt}, []model.EdgeDNSRecord{txt}, now); err == nil {
		t.Fatal("duplicate RRsets accepted")
	}
	leased.Candidates = []model.EdgeDNSAnswerCandidate{{IP: "192.0.2.1"}}
	if _, err = comparePlatformDNSRecords([]model.EdgeDNSRecord{leased}, nil, now); err == nil {
		t.Fatal("unsupported leased dynamic candidate accepted")
	}
}
