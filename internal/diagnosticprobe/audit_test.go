package diagnosticprobe

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/livediagnostics"
)

func TestAuditAggregationKeepsCallerTimingAndCacheOptionsWithoutPayloads(t *testing.T) {
	var event auditEvent
	raw := `{"stage":"ResponseComplete","auditID":"id","verb":"list","user":{"username":"system:serviceaccount:ns:reader"},"userAgent":"client","objectRef":{"resource":"replicasets"},"responseStatus":{"code":200},"requestURI":"/apis/apps/v1/replicasets?resourceVersion=0&labelSelector=credential%3Dprivate-selector&token=private-token","requestReceivedTimestamp":"2026-01-01T00:00:00Z","stageTimestamp":"2026-01-01T00:00:00.250Z","requestObject":{"secret":"private-body"},"responseObject":{"data":"private-response"}}`
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatal(err)
	}
	groups := map[string]*auditGroup{}
	for i := 0; i < 2; i++ {
		if !aggregateAuditEvent(groups, event) {
			t.Fatal("aggregation rejected")
		}
	}
	if len(groups) != 1 {
		t.Fatal("duplicate group")
	}
	for _, g := range groups {
		if g.Count != 2 || g.DurationMillis != 500 || g.MaxMillis != 250 || g.Options["resourceVersion=0"] != 2 || g.Options["labelSelector=present"] != 2 {
			t.Fatalf("incorrect aggregation: %+v", g)
		}
	}
	output, _ := json.Marshal(groups)
	if strings.Contains(string(output), "private-") {
		t.Fatalf("audit payload leaked: %s", output)
	}
}

func TestAuditReaderWindowDuplicateStagesAndMissingCoverage(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "var/log/audit")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	line := func(id, stage string, at time.Time) string {
		v := map[string]any{"auditID": id, "stage": stage, "stageTimestamp": at, "requestReceivedTimestamp": at.Add(-time.Second), "verb": "list", "objectRef": map[string]string{"resource": "configmaps"}, "responseStatus": map[string]int{"code": 200}}
		raw, _ := json.Marshal(v)
		return string(raw) + "\n"
	}
	data := line("old", "ResponseComplete", now.Add(-2*time.Minute)) + line("in-window", "RequestReceived", now.Add(-10*time.Second)) + strings.Repeat(line("in-window", "ResponseComplete", now.Add(-9*time.Second)), 2) + line("future", "ResponseComplete", now.Add(time.Hour))
	path := filepath.Join(dir, "audit.log")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	req := livediagnostics.ProbeRequest{Target: livediagnostics.Target{Type: livediagnostics.TargetNodeProcess}}
	c := Collector{Path: "/var/log/audit/*.log", SinceSeconds: 60}
	v, err := hostKubernetesAuditAt(context.Background(), req, c, root)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := v.(map[string]any)
	if !ok || result["completed_requests"] != 1 {
		t.Fatalf("window or dedup incorrect: %+v", v)
	}
	if err := os.WriteFile(path, []byte(line("recent", "ResponseComplete", now.Add(-time.Second))), 0600); err != nil {
		t.Fatal(err)
	}
	v, err = hostKubernetesAuditAt(context.Background(), req, c, root)
	if _, ok := v.(partialValue); err != nil || !ok {
		t.Fatalf("partial history not marked: %v %v", v, err)
	}
}
