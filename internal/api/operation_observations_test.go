package api

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

func TestOperationObservationsAreBoundedAndConcurrent(t *testing.T) {
	s := &Server{}
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.observeOperation("consumer-assignment")()
			s.observeOperation("untrusted-request-value")()
		}()
	}
	wg.Wait()
	s.observeOperation("dns-snapshot-replacement-retry")()
	value, err := s.OperationSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(value)
	var result struct {
		Stages []struct {
			Stage string `json:"stage"`
			Count int    `json:"count"`
		} `json:"stages"`
	}
	if json.Unmarshal(raw, &result) != nil || len(result.Stages) != 41 {
		t.Fatal("unexpected stage inventory")
	}
	stages := map[string]int{}
	for _, stage := range result.Stages {
		if stage.Stage == "untrusted-request-value" || stage.Stage == "consumer-assignment" && stage.Count != 24 {
			t.Fatal("unbounded or inaccurate operation observations")
		}
		stages[stage.Stage] = stage.Count
	}
	if count, ok := stages["dns-snapshot-replacement-retry"]; !ok || count != 1 {
		t.Fatal("replacement retry was not counted")
	}
	if count, ok := stages["dns-snapshot-replacement-retry-verified"]; !ok || count != 0 {
		t.Fatal("starting a replacement retry must not count as verification")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.OperationSnapshot(ctx); err == nil {
		t.Fatal("canceled snapshot accepted")
	}
}
