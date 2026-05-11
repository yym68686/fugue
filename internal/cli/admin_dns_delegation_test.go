package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"
)

func TestAdminDNSDelegationPlanCommandDryRun(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/dns/delegation/preflight" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("min_healthy_nodes"); got != "2" {
			t.Fatalf("unexpected min_healthy_nodes %q", got)
		}
		writeJSONResponse(t, w, model.DNSDelegationPreflightResponse{
			Pass:             true,
			Zone:             "dns.fugue.pro",
			ProbeName:        "d-test.dns.fugue.pro",
			MinHealthyNodes:  2,
			HealthyNodeCount: 2,
			DNSBundleVersion: "dnsgen_test",
			Nodes: []model.DNSDelegationNodeCheck{
				{DNSNodeID: "dns-us", Pass: true, NodeReady: true},
				{DNSNodeID: "dns-de", Pass: true, NodeReady: true},
			},
			DelegationPlan: sampleDNSDelegationPlan(),
		})
	}))
	t.Cleanup(server.Close)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "dns", "delegation", "plan",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run delegation plan: %v\nstderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"operation=plan",
		"dry_run=true",
		"preflight_pass=true",
		"ns1.dns.fugue.pro",
		"dns.fugue.pro",
		"dry-run",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
}

func TestDNSDelegationPlanSafetyRejectsNonDelegationRecords(t *testing.T) {
	t.Parallel()

	plan := model.DNSDelegationPlan{
		PlannedARecords: []model.DNSDelegationRecord{{
			Name:   "*.fugue.pro",
			Type:   "A",
			Values: []string{"203.0.113.10"},
			TTL:    300,
		}},
	}

	if err := validateDNSDelegationPlanSafety("dns.fugue.pro", plan); err == nil {
		t.Fatal("expected wildcard delegation plan to be rejected")
	}
}

func writeJSONResponse(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write json response: %v", err)
	}
}

func TestDNSDelegationPreflightForApplyRequiresPassingHealthyNodes(t *testing.T) {
	t.Parallel()

	response := model.DNSDelegationPreflightResponse{
		Pass:             true,
		MinHealthyNodes:  2,
		HealthyNodeCount: 1,
		Nodes: []model.DNSDelegationNodeCheck{{
			DNSNodeID: "dns-us-1",
			Pass:      true,
			NodeReady: true,
		}},
	}

	if err := validateDNSDelegationPreflightForApply(response); err == nil {
		t.Fatal("expected preflight with one healthy node to be rejected")
	}
}

func TestResolveCloudflareTokenFromEnvFile(t *testing.T) {
	t.Setenv("CLOUDFLARE_DNS_API_TOKEN", "")
	t.Setenv("CLOUDFLARE_API_TOKEN", "")
	envPath := t.TempDir() + "/cloudflare.env"
	if err := os.WriteFile(envPath, []byte("CLOUDFLARE_DNS_API_TOKEN='test-token'\n"), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	token, err := resolveCloudflareToken(dnsDelegationCommandOptions{CloudflareEnvFile: envPath})
	if err != nil {
		t.Fatalf("resolve token: %v", err)
	}
	if token != "test-token" {
		t.Fatalf("unexpected token %q", token)
	}
}

func TestCloudflareDelegationApplyAndRollback(t *testing.T) {
	t.Parallel()

	fake := newFakeCloudflareDNS()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	client := &cloudflareDNSClient{
		baseURL: server.URL + "/client/v4",
		token:   "test-token",
		http:    server.Client(),
	}

	plan := sampleDNSDelegationPlan()
	fake.addRecord(cloudflareDNSRecord{
		ID:      "stale-a",
		Type:    "A",
		Name:    "ns1.dns.fugue.pro",
		Content: "198.51.100.99",
		TTL:     300,
	})

	actions, err := client.applyDNSDelegationPlan(context.Background(), "fugue.pro", plan)
	if err != nil {
		t.Fatalf("apply delegation plan: %v", err)
	}
	if !hasDNSDelegationAction(actions, "delete", "ns1.dns.fugue.pro", "A", "198.51.100.99", "deleted-extra") {
		t.Fatalf("expected apply to delete stale glue record, got %#v", actions)
	}
	for _, want := range []cloudflareDNSRecord{
		{Name: "ns1.dns.fugue.pro", Type: "A", Content: "203.0.113.10"},
		{Name: "ns2.dns.fugue.pro", Type: "A", Content: "203.0.113.20"},
		{Name: "dns.fugue.pro", Type: "NS", Content: "ns1.dns.fugue.pro"},
		{Name: "dns.fugue.pro", Type: "NS", Content: "ns2.dns.fugue.pro"},
	} {
		if !fake.hasRecord(want.Name, want.Type, want.Content) {
			t.Fatalf("missing applied record %#v in %#v", want, fake.snapshot())
		}
	}
	if fake.hasRecord("ns1.dns.fugue.pro", "A", "198.51.100.99") {
		t.Fatal("stale glue record was not removed")
	}

	actions, err = client.rollbackDNSDelegationPlan(context.Background(), "fugue.pro", plan)
	if err != nil {
		t.Fatalf("rollback delegation plan: %v", err)
	}
	if !hasDNSDelegationAction(actions, "delete", "dns.fugue.pro", "NS", "ns1.dns.fugue.pro", "deleted") {
		t.Fatalf("expected rollback to delete delegated NS records, got %#v", actions)
	}
	if got := fake.recordCount(); got != 0 {
		t.Fatalf("expected rollback to leave no records, got %d: %#v", got, fake.snapshot())
	}
}

func sampleDNSDelegationPlan() model.DNSDelegationPlan {
	return model.DNSDelegationPlan{
		PlannedARecords: []model.DNSDelegationRecord{
			{Name: "ns1.dns.fugue.pro", Type: "A", Values: []string{"203.0.113.10"}, TTL: 300},
			{Name: "ns2.dns.fugue.pro", Type: "A", Values: []string{"203.0.113.20"}, TTL: 300},
		},
		PlannedNSRecords: []model.DNSDelegationRecord{{
			Name:   "dns.fugue.pro",
			Type:   "NS",
			Values: []string{"ns1.dns.fugue.pro", "ns2.dns.fugue.pro"},
			TTL:    300,
		}},
		RollbackDeleteRecords: []model.DNSDelegationRecord{
			{Name: "ns1.dns.fugue.pro", Type: "A", Values: []string{"203.0.113.10"}, TTL: 300},
			{Name: "ns2.dns.fugue.pro", Type: "A", Values: []string{"203.0.113.20"}, TTL: 300},
			{Name: "dns.fugue.pro", Type: "NS", Values: []string{"ns1.dns.fugue.pro", "ns2.dns.fugue.pro"}, TTL: 300},
		},
	}
}

func hasDNSDelegationAction(actions []dnsDelegationCloudflareAction, operation, name, recordType, value, result string) bool {
	for _, action := range actions {
		if action.Operation == operation && action.Name == name && action.Type == recordType && action.Value == value && action.Result == result {
			return true
		}
	}
	return false
}

type fakeCloudflareDNS struct {
	mu      sync.Mutex
	nextID  int
	records map[string]cloudflareDNSRecord
}

func newFakeCloudflareDNS() *fakeCloudflareDNS {
	return &fakeCloudflareDNS{
		nextID:  1,
		records: map[string]cloudflareDNSRecord{},
	}
}

func (f *fakeCloudflareDNS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer test-token" {
		f.writeError(w, http.StatusUnauthorized, "bad token")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/client/v4")
	switch {
	case r.Method == http.MethodGet && path == "/zones":
		f.writeOK(w, []cloudflareZone{{ID: "zone123", Name: r.URL.Query().Get("name")}})
	case strings.HasPrefix(path, "/zones/zone123/dns_records"):
		f.handleRecords(w, r, strings.TrimPrefix(path, "/zones/zone123/dns_records"))
	default:
		f.writeError(w, http.StatusNotFound, "not found")
	}
}

func (f *fakeCloudflareDNS) handleRecords(w http.ResponseWriter, r *http.Request, suffix string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && suffix == "":
		f.writeOK(w, f.filteredRecords(r.URL.Query()))
	case r.Method == http.MethodPost && suffix == "":
		var payload cloudflareDNSRecordPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			f.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		record := f.recordFromPayload(payload)
		f.records[record.ID] = record
		f.writeOK(w, record)
	case r.Method == http.MethodPut && strings.HasPrefix(suffix, "/"):
		id, _ := url.PathUnescape(strings.TrimPrefix(suffix, "/"))
		var payload cloudflareDNSRecordPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			f.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		record := f.recordFromPayload(payload)
		record.ID = id
		f.records[id] = record
		f.writeOK(w, record)
	case r.Method == http.MethodDelete && strings.HasPrefix(suffix, "/"):
		id, _ := url.PathUnescape(strings.TrimPrefix(suffix, "/"))
		delete(f.records, id)
		f.writeOK(w, map[string]string{"id": id})
	default:
		f.writeError(w, http.StatusNotFound, "not found")
	}
}

func (f *fakeCloudflareDNS) addRecord(record cloudflareDNSRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if record.ID == "" {
		record.ID = f.nextRecordID()
	}
	f.records[record.ID] = record
}

func (f *fakeCloudflareDNS) hasRecord(name, recordType, content string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, record := range f.records {
		if record.Name == name && record.Type == recordType && record.Content == content {
			return true
		}
	}
	return false
}

func (f *fakeCloudflareDNS) recordCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

func (f *fakeCloudflareDNS) snapshot() []cloudflareDNSRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]cloudflareDNSRecord, 0, len(f.records))
	for _, record := range f.records {
		out = append(out, record)
	}
	return out
}

func (f *fakeCloudflareDNS) filteredRecords(values url.Values) []cloudflareDNSRecord {
	recordType := values.Get("type")
	name := values.Get("name")
	out := []cloudflareDNSRecord{}
	for _, record := range f.records {
		if recordType != "" && record.Type != recordType {
			continue
		}
		if name != "" && record.Name != name {
			continue
		}
		out = append(out, record)
	}
	return out
}

func (f *fakeCloudflareDNS) recordFromPayload(payload cloudflareDNSRecordPayload) cloudflareDNSRecord {
	return cloudflareDNSRecord{
		ID:      f.nextRecordID(),
		Type:    payload.Type,
		Name:    payload.Name,
		Content: payload.Content,
		TTL:     payload.TTL,
	}
}

func (f *fakeCloudflareDNS) nextRecordID() string {
	id := f.nextID
	f.nextID++
	return "record-" + string(rune('0'+id))
}

func (f *fakeCloudflareDNS) writeOK(w http.ResponseWriter, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"errors":  []any{},
		"result":  result,
	})
}

func (f *fakeCloudflareDNS) writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": false,
		"errors":  []map[string]any{{"message": message}},
	})
}
