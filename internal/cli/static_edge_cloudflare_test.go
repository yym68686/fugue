package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type cutoverCloudflareFixture struct {
	mu           sync.Mutex
	records      map[string]staticEdgeDNSRecord
	writes       []string
	failName     string
	lostResponse bool
}

func fixtureRecord(id, name, ip string) staticEdgeDNSRecord {
	b, _ := json.Marshal(map[string]any{"id": id, "name": name, "type": "A", "content": ip, "ttl": 1, "proxied": false, "comment": "keep", "tags": []string{"owner:edge"}, "settings": map[string]bool{"ipv4_only": true}})
	var r staticEdgeDNSRecord
	_ = json.Unmarshal(b, &r)
	return r
}
func (f *cutoverCloudflareFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(w, "missing auth", 403)
		return
	}
	if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/dns_records") {
		rs := []staticEdgeDNSRecord{}
		for _, x := range f.records {
			if x.str("name") == r.URL.Query().Get("name.exact") {
				rs = append(rs, x)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": rs})
		return
	}
	if r.Method == "PATCH" {
		id := filepath.Base(r.URL.Path)
		original, ok := f.records[id]
		if !ok {
			http.Error(w, "missing", 404)
			return
		}
		if original.str("name") == f.failName {
			http.Error(w, "injected", 503)
			return
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if len(body) != 1 || body["content"] == "" {
			http.Error(w, "unsafe fields", 400)
			return
		}
		f.records[id] = original.withIP(body["content"])
		f.writes = append(f.writes, id)
		if f.lostResponse {
			f.lostResponse = false
			http.Error(w, "committed but response lost", 502)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": f.records[id]})
		return
	}
	http.Error(w, "unexpected mutation", 405)
}
func newCutoverFixture(t *testing.T) (*cutoverCloudflareFixture, staticEdgeCutoverOptions) {
	t.Helper()
	f := &cutoverCloudflareFixture{records: map[string]staticEdgeDNSRecord{"apex": fixtureRecord("apex", "example.test", "192.0.2.10"), "api": fixtureRecord("api", "api.example.test", "192.0.2.10"), "other": fixtureRecord("other", "other.example.test", "192.0.2.99")}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	t.Setenv("FUGUE_STATIC_EDGE_CLOUDFLARE_API_URL", srv.URL)
	t.Setenv("FUGUE_STATIC_EDGE_CLOUDFLARE_TOKEN", "test-token")
	t.Setenv("FUGUE_STATIC_EDGE_STATE_DIR", t.TempDir())
	return f, staticEdgeCutoverOptions{Zone: "example.test", ZoneID: "zid", Hostnames: []string{"example.test", "api.example.test"}, FromIP: "192.0.2.10", ToIP: "192.0.2.20", Candidate: "candidate", Execute: true, ProbePath: "/health", Observe: 5 * time.Second, Timeout: time.Second}
}
func goodCutoverCheck(context.Context, staticEdgeCutoverOptions) error { return nil }
func runCutoverFixture(o staticEdgeCutoverOptions) (string, error) {
	var out bytes.Buffer
	cli := newCLI(&out, &bytes.Buffer{})
	e := cli.runStaticEdgeCutoverWithChecks(context.Background(), o, goodCutoverCheck, goodCutoverCheck, goodCutoverCheck)
	return out.String(), e
}
func TestStaticEdgeCutoverPreservesAttributesAndResumes(t *testing.T) {
	f, o := newCutoverFixture(t)
	f.failName = "example.test"
	if _, e := runCutoverFixture(o); e == nil {
		t.Fatal("expected injected failure")
	}
	if len(f.writes) != 1 || f.records["api"].str("content") != o.ToIP || f.records["apex"].str("content") != o.FromIP {
		t.Fatal(f.writes)
	}
	f.failName = ""
	out, e := runCutoverFixture(o)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out, "completed_old_retained") {
		t.Fatal(out)
	}
	if len(f.writes) != 2 || f.records["other"].str("content") != "192.0.2.99" {
		t.Fatal(f.writes)
	}
	for _, id := range []string{"apex", "api"} {
		if f.records[id].str("comment") != "keep" || string(f.records[id]["tags"]) != `["owner:edge"]` {
			t.Fatal("attributes lost")
		}
	}
	if _, e = runCutoverFixture(o); e != nil {
		t.Fatal(e)
	}
	if len(f.writes) != 2 {
		t.Fatal("completed intent repeated writes")
	}
}
func TestStaticEdgeCutoverReadsBackAmbiguousWrite(t *testing.T) {
	f, o := newCutoverFixture(t)
	f.lostResponse = true
	if _, e := runCutoverFixture(o); e != nil {
		t.Fatal(e)
	}
	if len(f.writes) != 2 {
		t.Fatal("blind retry", f.writes)
	}
}
func TestStaticEdgeCutoverStopsOnDriftAndFailedGate(t *testing.T) {
	f, o := newCutoverFixture(t)
	f.failName = "example.test"
	_, _ = runCutoverFixture(o)
	f.failName = ""
	f.records["apex"] = f.records["apex"].withIP("192.0.2.88")
	if _, e := runCutoverFixture(o); e == nil {
		t.Fatal("drift accepted")
	}
	if len(f.writes) != 1 {
		t.Fatal(f.writes)
	}
}
func TestStaticEdgeCutoverProbeFailureNeverWrites(t *testing.T) {
	f, o := newCutoverFixture(t)
	cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	e := cli.runStaticEdgeCutoverWithChecks(context.Background(), o, goodCutoverCheck, func(context.Context, staticEdgeCutoverOptions) error { return errors.New("candidate down") }, goodCutoverCheck)
	if e == nil || len(f.writes) != 0 {
		t.Fatal(e, f.writes)
	}
}
func TestStaticEdgeCutoverRejectsAlternateAddresses(t *testing.T) {
	f, o := newCutoverFixture(t)
	r := fixtureRecord("v6", "example.test", "2001:db8::1")
	r["type"] = json.RawMessage(`"AAAA"`)
	f.records["v6"] = r
	if _, e := runCutoverFixture(o); e == nil {
		t.Fatal("AAAA ignored")
	}
	if len(f.writes) != 0 {
		t.Fatal(f.writes)
	}
	o.Hostnames = []string{"outside.test"}
	if _, e := normalizeStaticEdgeCutover(o); e == nil {
		t.Fatal("out of zone accepted")
	}
	if _, e := readStaticEdgeCutoverJournal("../../bad"); e == nil {
		t.Fatal("path traversal accepted")
	}
}
func TestStaticEdgeCutoverLockReleasesWithoutDeletingInode(t *testing.T) {
	t.Setenv("FUGUE_STATIC_EDGE_STATE_DIR", t.TempDir())
	release, e := acquireStaticEdgeCutoverLock("example.test")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = acquireStaticEdgeCutoverLock("example.test"); e == nil {
		t.Fatal("concurrent lock accepted")
	}
	release()
	release, e = acquireStaticEdgeCutoverLock("example.test")
	if e != nil {
		t.Fatal(e)
	}
	release()
}
func TestStaticEdgeCloudflareTokenImportIsPrivate(t *testing.T) {
	p := filepath.Join(t.TempDir(), "nested", "token")
	t.Setenv("FUGUE_STATIC_EDGE_CLOUDFLARE_TOKEN_FILE", p)
	var out, stderr bytes.Buffer
	cmd := newCLI(&out, &stderr).newRootCommand()
	cmd.SetArgs([]string{"static-edge", "cloudflare", "auth", "import", "--zone", "example.test", "--token-stdin"})
	cmd.SetIn(strings.NewReader("private-token\n"))
	if e := cmd.Execute(); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(out.String(), "private-token") {
		t.Fatal("token leaked")
	}
	st, e := os.Stat(p)
	if e != nil || st.Mode().Perm() != 0600 {
		t.Fatal(e, st)
	}
}
