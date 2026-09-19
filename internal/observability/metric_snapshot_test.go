package observability

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestMetricSnapshotScrapeDoesNotWaitForRefresh(t *testing.T) {
	var s MetricSnapshot
	if err := s.Refresh(t.Context(), func(_ context.Context, w io.Writer) error { _, e := io.WriteString(w, "sample 1\n"); return e }); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Refresh(t.Context(), func(_ context.Context, w io.Writer) error {
			close(started)
			<-release
			return errors.New("backend down")
		})
	}()
	<-started
	scraped := make(chan string, 1)
	go func() { var b strings.Builder; s.Write(&b, "test"); scraped <- b.String() }()
	select {
	case b := <-scraped:
		if !strings.Contains(b, "sample 1") {
			t.Fatal(b)
		}
	case <-time.After(time.Second):
		t.Fatal("scrape blocked on collection")
	}
	close(release)
	<-done
	var b strings.Builder
	s.Write(&b, "test")
	if !strings.Contains(b.String(), "sample 1") || !strings.Contains(b.String(), `fugue_metrics_snapshot_refresh_failed{component="test"} 1`) {
		t.Fatal(b.String())
	}
}
