package runtimeobservation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type syntheticSQLFailure string

func (e syntheticSQLFailure) Error() string    { return "private database message" }
func (e syntheticSQLFailure) SQLState() string { return string(e) }

func databaseFailures(t *testing.T, r *DatabaseRecorder) map[string]any {
	t.Helper()
	v, err := r.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return v.(map[string]any)["failures"].(map[string]any)
}

func TestDatabaseFailureSurvivesLostStartAndSuccessfulChurn(t *testing.T) {
	r := &DatabaseRecorder{}
	ctx := WithDatabaseOperation(context.Background(), "consumer-heartbeat")
	for range 128 {
		r.Start(context.Background(), 1, 1, time.Now(), "SELECT 1", nil)
	}
	first := time.Now().UTC()
	fact := r.StartQuery(ctx, 7, 19, first, "SELECT private_sql($1)", []any{"private-payload"})
	if _, ok := r.active[fact.ID]; ok {
		t.Fatal("fixture must exhaust the ordinary active budget")
	}
	r.FinishQuery(fact, fmt.Errorf("private wrapper: %w", syntheticSQLFailure("55P03")))
	for range 400 {
		r.FinishQuery(r.StartQuery(context.Background(), 1, 1, first, "SELECT 1", nil), nil)
	}
	v := databaseFailures(t, r)
	rows := v["receipts"].([]DatabaseFailureFact)
	if len(rows) != 1 || v["recorded"] != uint64(1) || v["total"] != uint64(1) || v["dropped"] != uint64(0) {
		t.Fatal("failure depended on active admission or was evicted by success", v)
	}
	f := rows[0]
	if f.ID != fact.ID || f.BackendPID != 7 || f.Connection != 19 || !f.ConnectionFirstObserved.Equal(first) || f.Operation != DatabaseOperationFromContext(ctx) || f.Fingerprint != SQLFingerprint("SELECT private_sql($1)") || f.SQLState != "55P03" || f.Class != "database" || !f.Failed || f.Finished == nil {
		t.Fatal("original query identity or typed failure lost", f)
	}
	raw, _ := json.Marshal(v)
	for _, secret := range []string{"private_sql", "private-payload", "private wrapper", "private database message"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("database receipt retained private input")
		}
	}
	for range databaseFailureLimit + 1 {
		r.FinishQuery(r.StartQuery(context.Background(), 1, 1, first, "SELECT 1", nil), context.DeadlineExceeded)
	}
	if rows[0].ID != fact.ID {
		t.Fatal("later eviction mutated a returned snapshot")
	}
	v = databaseFailures(t, r)
	if len(v["receipts"].([]DatabaseFailureFact)) != databaseFailureLimit || v["evicted"] != uint64(2) {
		t.Fatal("failure retention unbounded or eviction unaccounted", v)
	}
}

func TestDatabaseFailuresAreNonblockingAndTyped(t *testing.T) {
	r := &DatabaseRecorder{}
	fact := r.StartQuery(context.Background(), 1, 1, time.Now(), "SELECT 1", nil)
	r.mu.Lock()
	r.FinishQuery(fact, syntheticSQLFailure("55P03"))
	r.mu.Unlock()
	r.failureMu.Lock()
	r.FinishQuery(fact, context.Canceled)
	r.failureMu.Unlock()
	v := databaseFailures(t, r)
	if v["recorded"] != uint64(1) || v["dropped"] != uint64(1) || v["total"] != uint64(2) {
		t.Fatal("failure observations blocked or silently dropped", v)
	}
	for _, test := range []struct {
		err   error
		class string
		state string
	}{
		{syntheticSQLFailure("private-untrusted-state"), "database", "other"},
		{context.DeadlineExceeded, "deadline", ""},
		{context.Canceled, "canceled", ""},
		{errors.New("55P03 private message"), "unclassified", ""},
	} {
		r := &DatabaseRecorder{}
		r.FinishQuery(r.StartQuery(context.Background(), 1, 1, time.Now(), "SELECT 1", nil), test.err)
		rows := databaseFailures(t, r)["receipts"].([]DatabaseFailureFact)
		if len(rows) != 1 || rows[0].Class != test.class || rows[0].SQLState != test.state {
			t.Fatal("failure text was parsed or typed category lost", rows)
		}
	}
}

func TestDatabaseFailureConcurrentSnapshots(t *testing.T) {
	r := &DatabaseRecorder{}
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for range 30 {
				r.FinishQuery(r.StartQuery(context.Background(), 1, 1, time.Now(), "SELECT 1", nil), context.Canceled)
				_, _ = r.Snapshot(context.Background())
			}
		})
	}
	wg.Wait()
	v := databaseFailures(t, r)
	if v["total"] != uint64(180) || v["recorded"].(uint64)+v["dropped"].(uint64) != uint64(180) || len(v["receipts"].([]DatabaseFailureFact)) > databaseFailureLimit {
		t.Fatal("concurrent coverage or bounds violated", v)
	}
}
