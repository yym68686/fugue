package runtimeobservation

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDatabaseObserverNeverRetainsPayloadAndKeepsLargeOperations(t *testing.T) {
	r := &DatabaseRecorder{}
	ctx := WithDatabaseOperation(context.Background(), "compiler-input-persist")
	secret := strings.Repeat("private-payload", 100000)
	id := r.Start(ctx, 31, 1, time.Now(), "INSERT INTO examples VALUES ($1)", []any{secret, 42})
	r.Finish(id, false)
	for i := 0; i < 300; i++ {
		id := r.Start(context.Background(), 31, 1, time.Now(), "SELECT 1", nil)
		r.Finish(id, false)
	}
	v, err := r.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "private-payload") || strings.Contains(string(raw), "INSERT INTO") {
		t.Fatal("SQL or value escaped observer")
	}
	m := v.(map[string]any)
	large := m["large_queries"].([]DatabaseQueryFact)
	if len(large) != 1 || large[0].InputBytes != int64(len(secret)) || large[0].UnknownInputs != 1 || large[0].Operation.ID == 0 || large[0].BackendPID != 31 || large[0].OutputBytes != nil {
		t.Fatalf("wrong measurements %+v", large)
	}
	if len(m["queries"].([]DatabaseQueryFact)) != 256 || m["overwritten"] != true {
		t.Fatal("unbounded or silent overwrites")
	}
}
func TestDatabaseObservationDropsInsteadOfBlocking(t *testing.T) {
	r := &DatabaseRecorder{}
	r.mu.Lock()
	id := r.Start(context.Background(), 1, 1, time.Now(), "SELECT 1", nil)
	r.mu.Unlock()
	if id != 0 || r.dropped.Load() != 1 {
		t.Fatal("busy observer must drop")
	}
}
func TestDatabaseOperationContextDoesNotCopyCancellation(t *testing.T) {
	src, cancel := context.WithCancel(WithDatabaseOperation(context.Background(), "parent"))
	cancel()
	copied := CopyDatabaseOperation(context.Background(), src)
	if copied.Err() != nil {
		t.Fatal("changed store cancellation semantics")
	}
	a := copied.Value(databaseOperationKey{}).(DatabaseOperation)
	child := WithDatabaseOperation(copied, "child").Value(databaseOperationKey{}).(DatabaseOperation)
	if child.Parent != a.ID || child.ID == a.ID {
		t.Fatal("operation lineage lost")
	}
}
func TestDatabaseObserverConcurrentReaders(t *testing.T) {
	r := &DatabaseRecorder{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				id := r.Start(context.Background(), 1, 1, time.Now(), "SELECT $1", []any{"value"})
				r.Finish(id, false)
				r.Snapshot(context.Background())
			}
		}()
	}
	wg.Wait()
	v, _ := r.Snapshot(context.Background())
	if len(v.(map[string]any)["active"].([]DatabaseQueryFact)) > 128 {
		t.Fatal("active budget exceeded")
	}
}
func BenchmarkDatabaseObservation(b *testing.B) {
	r := &DatabaseRecorder{}
	ctx := context.Background()
	args := []any{make([]byte, 26<<20)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := r.Start(ctx, 31, 1, time.Time{}, "INSERT INTO examples VALUES($1)", args)
		r.Finish(id, false)
	}
}

func TestDatabaseOutputBoundToExactChildOperation(t *testing.T) {
	r := &DatabaseRecorder{}
	ctx := WithDatabaseOperation(context.Background(), "payload-read")
	id := r.Start(ctx, 7, 1, time.Now(), "SELECT payload FROM examples", nil)
	r.Finish(id, false)
	r.RecordOutput(ctx, 2<<20)
	v, _ := r.Snapshot(context.Background())
	facts := v.(map[string]any)["queries"].([]DatabaseQueryFact)
	if len(facts) != 1 || facts[0].OutputBytes == nil || *facts[0].OutputBytes != 2<<20 {
		t.Fatalf("result measurement lost: %+v", facts)
	}
	if len(v.(map[string]any)["large_queries"].([]DatabaseQueryFact)) != 1 {
		t.Fatal("large output not retained")
	}
}
