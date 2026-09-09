package api

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestServerTimingAggregatesRetriesAndBoundsHeaderMetrics(t *testing.T) {
	r := newServerTimingRecorder()
	for i := 0; i < 500; i++ {
		r.Add("billing_inputs", time.Millisecond)
	}
	if value := r.headerValue(); value != "billing_inputs;dur=500.0" {
		t.Fatalf("retry timings were not aggregated: %s", value)
	}
	for i := 0; i < 100; i++ {
		r.Add(fmt.Sprintf("stage_%d", i), time.Millisecond)
	}
	if count := len(strings.Split(r.headerValue(), ",")); count != 64 {
		t.Fatalf("unbounded metric count: %d", count)
	}
}
