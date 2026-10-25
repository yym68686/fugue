package dnsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/platformconsumer"
)

func TestDNSSyncObservationPreservesContextAndHidesError(t *testing.T) {
	var output bytes.Buffer
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx, finish := startDNSSyncTrace(parent, log.New(&output, "", 0), "serving")
	child, finishRead := observeDNSSyncPhase(ctx, "source_read")
	before, _ := parent.Deadline()
	after, _ := child.Deadline()
	id, path := dnsSyncTraceIdentity(child)
	if before != after || child.Done() != parent.Done() || id == "" || path != "serving/source_read" {
		t.Fatal("diagnostic trace changed the operation context or lost correlation")
	}
	err := fmt.Errorf("sensitive-diagnostic-fixture: %w", platformconsumer.ErrAssignmentChanged)
	finishRead(err)
	finishRead(nil) // A repeated finish cannot invent another successful phase.
	finish(err)
	if strings.Contains(output.String(), "sensitive-diagnostic-fixture") || child.Err() != nil {
		t.Fatal("trace leaked error text or changed cancellation")
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatal("missing or duplicate phases", len(lines))
	}
	for _, line := range lines {
		var got struct {
			TraceID string    `json:"sync_trace_id"`
			Outcome string    `json:"outcome"`
			Started time.Time `json:"started_at"`
			Ended   time.Time `json:"finished_at"`
		}
		if json.Unmarshal([]byte(line), &got) != nil || got.TraceID != id || got.Outcome != "assignment_changed" || got.Started.IsZero() || got.Ended.Before(got.Started) {
			t.Fatal("phase did not retain its exact result and clock", line)
		}
	}
	cancel()
	if child.Err() != context.Canceled {
		t.Fatal("diagnostic context hid parent cancellation")
	}
}

func TestDNSSyncObservationBoundsConcurrentCompletion(t *testing.T) {
	var output bytes.Buffer
	ctx, finish := startDNSSyncTrace(context.Background(), log.New(&output, "", 0), "serving")
	var workers sync.WaitGroup
	for range 100 {
		_, end := observeDNSSyncPhase(ctx, "source_read")
		workers.Go(func() { end(nil); end(context.Canceled) })
	}
	workers.Wait()
	finish(nil)
	if lines := bytes.Count(output.Bytes(), []byte{'\n'}); lines != 64 {
		t.Fatal("trace output is not bounded", lines)
	}
	plain := context.Background()
	withoutLogger, end := startDNSSyncTrace(plain, nil, "serving")
	end(nil)
	if withoutLogger != plain {
		t.Fatal("disabled diagnostics changed context")
	}
}
