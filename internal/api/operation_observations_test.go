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
	if json.Unmarshal(raw, &result) != nil || len(result.Stages) != 26 {
		t.Fatal("unexpected stage inventory")
	}
	for _, stage := range result.Stages {
		if stage.Stage == "untrusted-request-value" || stage.Stage == "consumer-assignment" && stage.Count != 24 {
			t.Fatal("unbounded or inaccurate operation observations")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.OperationSnapshot(ctx); err == nil {
		t.Fatal("canceled snapshot accepted")
	}
}
