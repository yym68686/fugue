package runtimeobservation

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"time"
)

// Operations records fixed, source-defined stages. Request values cannot grow
// its cardinality. Counters are lifetime totals; consumers compare snapshots.
type Operations struct {
	started time.Time
	stages  map[string]*operationCounts
}

type operationCounts struct {
	count   atomic.Uint64
	nanos   atomic.Uint64
	maximum atomic.Uint64
}

func NewOperations(names ...string) (*Operations, error) {
	if len(names) == 0 || len(names) > 64 {
		return nil, errors.New("operation observations require 1-64 fixed stages")
	}
	o := &Operations{started: time.Now().UTC(), stages: make(map[string]*operationCounts, len(names))}
	for _, name := range names {
		if !namePattern.MatchString(name) || o.stages[name] != nil {
			return nil, errors.New("invalid or duplicate operation stage")
		}
		o.stages[name] = &operationCounts{}
	}
	return o, nil
}

func (o *Operations) Observe(name string, duration time.Duration) {
	if o == nil || duration < 0 {
		return
	}
	c := o.stages[name]
	if c == nil {
		return
	}
	n := uint64(duration)
	c.nanos.Add(n)
	for previous := c.maximum.Load(); n > previous; previous = c.maximum.Load() {
		if c.maximum.CompareAndSwap(previous, n) {
			break
		}
	}
	c.count.Add(1)
}

func (o *Operations) Snapshot(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o == nil {
		return nil, errors.New("operation observations unavailable")
	}
	type row struct {
		Stage      string `json:"stage"`
		Count      uint64 `json:"count"`
		TotalNanos uint64 `json:"total_ns"`
		MaxNanos   uint64 `json:"max_ns"`
	}
	rows := make([]row, 0, len(o.stages))
	for name, c := range o.stages {
		rows = append(rows, row{name, c.count.Load(), c.nanos.Load(), c.maximum.Load()})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Stage < rows[j].Stage })
	return map[string]any{"schema": "fugue.operations.observation.v1", "started_at": o.started, "observed_at": time.Now().UTC(), "stages": rows, "note": "concurrent cumulative counters; use successive snapshots from the same process; durations are summed work, not wall time"}, nil
}
