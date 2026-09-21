package runtimeobservation

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOperationsBoundedConcurrentCounters(t *testing.T) {
	o, err := NewOperations("local-get", "network-get")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				o.Observe("local-get", time.Millisecond)
				o.Observe("private-dynamic", time.Second)
			}
		}()
	}
	wg.Wait()
	v, err := o.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "private") || !strings.Contains(string(raw), `"count":800`) || !strings.Contains(string(raw), `"total_ns":800000000`) || !strings.Contains(string(raw), `"max_ns":1000000`) {
		t.Fatalf("incorrect bounded observation: %s", raw)
	}
	if _, err := NewOperations("same", "same"); err == nil {
		t.Fatal("duplicate stages")
	}
}
