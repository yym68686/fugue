package runtimeobservation

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DatabaseObservations is an in-process Live Diagnostics provider. It never
// stores SQL text, parameters, error strings, connection addresses or results.
var DatabaseObservations = &DatabaseRecorder{}

type databaseOperationKey struct{}
type DatabaseOperation struct {
	ID     uint64 `json:"id"`
	Parent uint64 `json:"parent,omitempty"`
	Name   string `json:"name"`
}

var operationSequence atomic.Uint64

func WithDatabaseOperation(ctx context.Context, name string) context.Context {
	if !namePattern.MatchString(name) {
		return ctx
	}
	parent, _ := ctx.Value(databaseOperationKey{}).(DatabaseOperation)
	return context.WithValue(ctx, databaseOperationKey{}, DatabaseOperation{ID: operationSequence.Add(1), Parent: parent.ID, Name: name})
}

// Copy only observation metadata; leave existing SQL timeout/cancellation
// semantics intact when a store operation deliberately owns its context.
func CopyDatabaseOperation(dst, src context.Context) context.Context {
	if operation, ok := src.Value(databaseOperationKey{}).(DatabaseOperation); ok {
		return context.WithValue(dst, databaseOperationKey{}, operation)
	}
	return dst
}

type DatabaseQueryFact struct {
	ID                      uint64            `json:"id"`
	Operation               DatabaseOperation `json:"operation"`
	BackendPID              uint32            `json:"backend_pid"`
	Connection              uint64            `json:"connection_sequence"`
	ConnectionFirstObserved time.Time         `json:"connection_first_observed"`
	Fingerprint             string            `json:"query_fingerprint"`
	PrefixFingerprint       string            `json:"query_prefix_1023_fingerprint"`
	SQLBytes                int               `json:"sql_bytes"`
	InputBytes              int64             `json:"known_input_bytes"`
	UnknownInputs           int               `json:"unknown_input_count"`
	OutputBytes             *int64            `json:"output_bytes,omitempty"`
	Caller                  string            `json:"caller"`
	Started                 time.Time         `json:"started_at"`
	Finished                *time.Time        `json:"finished_at,omitempty"`
	Failed                  bool              `json:"failed"`
}

type DatabaseRecorder struct {
	mu              sync.Mutex
	recent          []DatabaseQueryFact
	large           []DatabaseQueryFact
	active          map[uint64]DatabaseQueryFact
	next, largeNext int
	sequence        atomic.Uint64
	dropped         atomic.Uint64
	completed       uint64
	largeCompleted  uint64
}

func SQLFingerprint(sql string) string { h := md5.Sum([]byte(sql)); return hex.EncodeToString(h[:]) }
func DatabaseCaller() string {
	var pcs [24]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	caller := ""
	for {
		frame, more := frames.Next()
		if strings.HasPrefix(frame.Function, "fugue/internal/store.") && !strings.Contains(frame.Function, "postgresReadTracer") {
			caller = frame.Function
			break
		}
		if !more {
			break
		}
	}
	if len(caller) > 180 {
		return caller[:180]
	}
	return caller
}
func (r *DatabaseRecorder) Start(ctx context.Context, pid uint32, connection uint64, first time.Time, sql string, args []any) uint64 {
	id := r.sequence.Add(1)
	operation, _ := ctx.Value(databaseOperationKey{}).(DatabaseOperation)
	fact := DatabaseQueryFact{ID: id, Operation: operation, BackendPID: pid, Connection: connection, ConnectionFirstObserved: first, Fingerprint: SQLFingerprint(sql), PrefixFingerprint: SQLFingerprint(sql[:min(len(sql), 1023)]), SQLBytes: len(sql), Caller: DatabaseCaller(), Started: time.Now().UTC()}
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			fact.InputBytes += int64(len(v))
		case []byte:
			fact.InputBytes += int64(len(v))
		case nil:
		default:
			fact.UnknownInputs++
		}
	}
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		return 0
	}
	defer r.mu.Unlock()
	if r.active == nil {
		r.active = map[uint64]DatabaseQueryFact{}
	}
	if len(r.active) >= 128 {
		for oldID, old := range r.active {
			if time.Since(old.Started) > 10*time.Minute {
				delete(r.active, oldID)
				r.dropped.Add(1)
			}
		}
	}
	if len(r.active) >= 128 {
		r.dropped.Add(1)
		return 0
	}
	r.active[id] = fact
	return id
}
func (r *DatabaseRecorder) Finish(id uint64, failed bool) {
	if id == 0 {
		return
	}
	// Non-blocking producer. If another observer owns the lock, retain the
	// start fact as explicitly unfinished rather than blocking business work.
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		return
	}
	defer r.mu.Unlock()
	f, ok := r.active[id]
	if !ok {
		return
	}
	delete(r.active, id)
	now := time.Now().UTC()
	f.Finished = &now
	f.Failed = failed
	r.completed++
	if len(r.recent) < 256 {
		r.recent = append(r.recent, f)
	} else {
		r.recent[r.next] = f
		r.next = (r.next + 1) % 256
	}
	if f.InputBytes >= 1<<20 {
		r.largeCompleted++
		if len(r.large) < 128 {
			r.large = append(r.large, f)
		} else {
			r.large[r.largeNext] = f
			r.largeNext = (r.largeNext + 1) % 128
		}
	}
}
func (r *DatabaseRecorder) Snapshot(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	rows := append([]DatabaseQueryFact(nil), r.recent...)
	large := append([]DatabaseQueryFact(nil), r.large...)
	active := make([]DatabaseQueryFact, 0, len(r.active))
	now := time.Now().UTC()
	for id, f := range r.active {
		if now.Sub(f.Started) > 10*time.Minute {
			delete(r.active, id)
			r.dropped.Add(1)
		} else {
			active = append(active, f)
		}
	}
	completed, big := r.completed, r.largeCompleted
	r.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	sort.Slice(large, func(i, j int) bool { return large[i].ID < large[j].ID })
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	return map[string]any{"schema": "fugue.database.observation.v1", "observed_at": now, "queries": rows, "large_queries": large, "active": active, "completed_total": completed, "large_completed_total": big, "overwritten": completed > 256 || big > 128, "dropped_events": r.dropped.Load(), "note": "fingerprints only; known_input_bytes counts existing string/byte buffers, not protocol bytes; other parameter encodings and generic result sizes are unknown; active includes starts whose completion observation was dropped; correlate PID and connection lifetime with database backend_start"}, nil
}

// RecordDatabaseOutput measures an existing decoded buffer at a known boundary.
// Callers use a child operation containing exactly one query. No result is kept.
func (r *DatabaseRecorder) RecordOutput(ctx context.Context, bytes int64) {
	op, ok := ctx.Value(databaseOperationKey{}).(DatabaseOperation)
	if !ok || bytes < 0 {
		return
	}
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		return
	}
	defer r.mu.Unlock()
	var id uint64
	for i := range r.recent {
		if r.recent[i].Operation.ID == op.ID && r.recent[i].ID > id {
			id = r.recent[i].ID
		}
	}
	if id == 0 {
		r.dropped.Add(1)
		return
	}
	var updated DatabaseQueryFact
	for i := range r.recent {
		if r.recent[i].ID == id {
			v := bytes
			r.recent[i].OutputBytes = &v
			updated = r.recent[i]
		}
	}
	if bytes >= 1<<20 && updated.InputBytes < 1<<20 {
		r.largeCompleted++
		if len(r.large) < 128 {
			r.large = append(r.large, updated)
		} else {
			r.large[r.largeNext] = updated
			r.largeNext = (r.largeNext + 1) % 128
		}
	}
	for i := range r.large {
		if r.large[i].ID == id {
			v := bytes
			r.large[i].OutputBytes = &v
		}
	}
}

func (r *DatabaseRecorder) Drop() { r.dropped.Add(1) }
