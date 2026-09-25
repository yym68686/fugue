package entryfailover

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCloudflareBatchChangesOnlyAllowlistedAddressRecords(t *testing.T) {
	records := map[string]Record{
		"example.test":       {ID: "apex", Name: "example.test", Type: "A", Content: "192.0.2.10", TTL: 1, Comment: "keep", Tags: []string{"owner:customer"}},
		"api.example.test":   {ID: "api", Name: "api.example.test", Type: "A", Content: "192.0.2.10", TTL: 1},
		"other.example.test": {ID: "other", Name: "other.example.test", Type: "A", Content: "192.0.2.99", TTL: 1},
	}
	writes := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "auth", 403)
			return
		}
		if r.Method == "GET" {
			host := r.URL.Query().Get("name.exact")
			found := []Record{records[host]}
			if host == "example.test" {
				found = append(found, Record{ID: "mx", Name: host, Type: "MX", Content: "mail.example.test"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": found})
			return
		}
		if r.Method != "POST" || r.URL.Path != "/zones/zone/dns_records/batch" {
			http.Error(w, "mutation", 405)
			return
		}
		var body struct {
			Patches []map[string]any `json:"patches"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if len(body.Patches) != 2 {
			t.Errorf("patches=%d", len(body.Patches))
		}
		for _, p := range body.Patches {
			if len(p) != 3 || p["type"] != "CNAME" || p["content"] != "d-canonical.dns.fugue.test" {
				t.Errorf("unsafe patch: %v", p)
			}
			id := p["id"].(string)
			for h, old := range records {
				if old.ID == id {
					old.Type, old.Content = p["type"].(string), p["content"].(string)
					records[h] = old
				}
			}
		}
		writes++
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": map[string]any{}})
	}))
	defer srv.Close()
	cf, err := NewCloudflare(srv.URL, "zone", "test-token", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	hosts := []string{"example.test", "api.example.test"}
	before, err := cf.Snapshot(context.Background(), hosts)
	if err != nil {
		t.Fatal(err)
	}
	if err = cf.Batch(context.Background(), before, testPolicy().Targets[1]); err != nil {
		t.Fatal(err)
	}
	after, err := cf.Snapshot(context.Background(), hosts)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range hosts {
		if !recordMatches(after[host], targetRecord(before[host], testPolicy().Targets[1])) {
			t.Errorf("record drift: %s", host)
		}
	}
	if records["other.example.test"].Content != "192.0.2.99" || writes != 1 {
		t.Fatalf("scope violated: %v writes=%d", records, writes)
	}
}

func TestRecordComparisonCatchesSettingsDrift(t *testing.T) {
	a := Record{ID: "id", Name: "example.test", Type: "CNAME", Content: "target.example.test", TTL: 1,
		Settings: json.RawMessage(`{"flatten_cname":true,"ipv4_only":false}`)}
	b := a
	b.Settings = json.RawMessage(`{"ipv4_only":false,"flatten_cname":true}`)
	if !recordMatches(a, b) {
		t.Fatal("equivalent structured settings should match")
	}
	b.Settings = json.RawMessage(`{"flatten_cname":false,"ipv4_only":false}`)
	if recordMatches(a, b) {
		t.Fatal("flattening drift accepted")
	}
}

func TestOnlyApexTypeSpecificFlatteningIsAllowed(t *testing.T) {
	baseline := Record{ID: "record", Name: "example.test", Type: "A", Content: "192.0.2.10", TTL: 1,
		Settings: json.RawMessage(`{}`)}
	current := targetRecord(baseline, Target{Kind: "fugue-domain", Address: "d-canonical.dns.fugue.test"})
	current.Settings = json.RawMessage(`{"flatten_cname":true,"ipv4_only":false}`)
	if !allowedSettings(current, baseline, "example.test") {
		t.Fatal("mandatory apex flattening rejected")
	}
	current.Name, baseline.Name = "api.example.test", "api.example.test"
	if allowedSettings(current, baseline, "example.test") {
		t.Fatal("unexpected subdomain flattening accepted")
	}
	current.Settings = json.RawMessage(`{"ipv4_only":false,"private_routing":true}`)
	if allowedSettings(current, baseline, "example.test") {
		t.Fatal("unexpected settings expansion accepted")
	}
}
