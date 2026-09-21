package diagnosticprobe

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/livediagnostics"
)

func TestLogWindowUsesBoundedExplicitTimeAndReportsSourceCoverage(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second).Format(time.RFC3339)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if q.Get("sinceTime") != at || q.Get("sinceSeconds") != "" || q.Get("limitBytes") != "524288" {
			t.Errorf("wrong time/byte bound: %v", q)
		}
		fmt.Fprintf(w, "%s HEAD sample\n%s GET sample\n", at, at)
	}))
	defer server.Close()
	k := &kubeReader{client: server.Client(), base: server.URL}
	c := Collector{Namespace: "system-test", ObjectName: "sample", SinceTime: at}
	v, err := k.logs(context.Background(), livediagnostics.ProbeRequest{}, c)
	if err != nil {
		t.Fatal(err)
	}
	row := v.(map[string]any)
	if row["first_timestamp"] != at || row["last_timestamp"] != at || row["lines_by_second"].(map[string]int)[at] != 2 {
		t.Fatalf("missing source coverage: %+v", row)
	}
	for _, invalid := range []string{"bad-time", time.Now().Add(-25 * time.Hour).Format(time.RFC3339), time.Now().Add(time.Hour).Format(time.RFC3339)} {
		c.SinceTime = invalid
		if _, err := k.logs(context.Background(), livediagnostics.ProbeRequest{}, c); err == nil {
			t.Fatal("accepted invalid or out-of-scope time")
		}
	}
	if calls != 1 {
		t.Fatal("invalid time reached the source")
	}
}
