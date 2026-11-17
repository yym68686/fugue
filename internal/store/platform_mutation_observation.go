package store

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/runtimeobservation"
)

// Publication SQL facts have their own bounded lifetime. Ordinary successful
// query churn must not erase the transaction which held a promotion lock.
// These are driver timings, not proof of server CPU use or lock hold time.
const platformMutationReceiptLimit = 12
const platformMutationQueryLimit = 48
const platformMutationQueryHead = 16
const platformMutationLockQueryLimit = 8
const platformMutationLockQueryHead = 4

type platformMutationQuery struct {
	BackendPID              uint32    `json:"backend_pid"`
	Connection              uint64    `json:"connection_sequence"`
	ConnectionFirstObserved time.Time `json:"connection_first_observed"`
	Fingerprint             string    `json:"query_fingerprint"`
	Caller                  string    `json:"caller"`
	Started                 time.Time `json:"started_at"`
	Finished                time.Time `json:"finished_at"`
	Failed                  bool      `json:"failed"`
}

type platformMutationDatabaseTrace struct {
	mu            sync.Mutex
	queries       []platformMutationQuery
	next          int
	completed     uint64
	locks         []platformMutationQuery
	lockNext      int
	lockCompleted uint64
	dropped       atomic.Uint64
}

func observePlatformMutationQuery(ctx context.Context, fact runtimeobservation.DatabaseQueryFact, err error) {
	trace, _ := ctx.Value(platformMutationTraceKey{}).(*platformMutationTrace)
	if trace == nil {
		return
	}
	query := platformMutationQuery{BackendPID: fact.BackendPID, Connection: fact.Connection,
		ConnectionFirstObserved: fact.ConnectionFirstObserved, Fingerprint: fact.Fingerprint,
		Caller: fact.Caller, Started: fact.Started, Finished: time.Now().UTC(), Failed: err != nil}
	db := &trace.database
	if !db.mu.TryLock() {
		db.dropped.Add(1)
		return
	}
	defer db.mu.Unlock()
	db.completed++
	// Preparation may consume the entire ordinary head before any promotion
	// lock is acquired. Keep these original driver facts independently so a
	// long transaction's ordinary tail cannot erase its acquisition evidence.
	if fact.Caller == "fugue/internal/store.pgLockPromotionScope" {
		db.lockCompleted++
		if len(db.locks) < platformMutationLockQueryLimit {
			db.locks = append(db.locks, query)
		} else {
			db.locks[platformMutationLockQueryHead+db.lockNext] = query
			db.lockNext = (db.lockNext + 1) % (platformMutationLockQueryLimit - platformMutationLockQueryHead)
		}
	}
	if len(db.queries) < platformMutationQueryLimit {
		db.queries = append(db.queries, query)
	} else {
		// Keep preparation's head and the tail before commit. Promotion
		// acquisitions have their own bounded retention above.
		db.queries[platformMutationQueryHead+db.next] = query
		db.next = (db.next + 1) % (platformMutationQueryLimit - platformMutationQueryHead)
	}
}

type platformMutationStage struct {
	Stage        string `json:"stage"`
	Milliseconds int64  `json:"elapsed_ms"`
}

type platformMutationReceipt struct {
	Operation     runtimeobservation.DatabaseOperation `json:"database_operation"`
	Started       time.Time                            `json:"started_at"`
	Finished      time.Time                            `json:"finished_at"`
	Stage         string                               `json:"final_stage"`
	Failed        bool                                 `json:"failed"`
	Failure       PlatformInputReadFailure             `json:"failure"`
	Stages        []platformMutationStage              `json:"stages"`
	Queries       []platformMutationQuery              `json:"queries"`
	Completed     uint64                               `json:"queries_completed"`
	Omitted       uint64                               `json:"queries_evicted"`
	Dropped       uint64                               `json:"query_observations_dropped"`
	Locks         []platformMutationQuery              `json:"promotion_lock_queries"`
	LockCompleted uint64                               `json:"promotion_lock_queries_completed"`
	LockOmitted   uint64                               `json:"promotion_lock_queries_evicted"`
}

type platformMutationRecorder struct {
	mu               sync.Mutex
	recent, failures []platformMutationReceipt
	next, failedNext int
	recorded         uint64
	failedRecorded   uint64
	dropped          atomic.Uint64
}

var platformMutationObservations platformMutationRecorder

func (r *platformMutationRecorder) record(trace *platformMutationTrace, err error) {
	now := trace.now()
	receipt := platformMutationReceipt{Operation: runtimeobservation.DatabaseOperationFromContext(trace.ctx),
		Started: trace.started, Finished: now, Stage: trace.stage, Failed: err != nil}
	// Reuse the fixed typed error classifier. Never retain err.Error().
	ctx := context.WithValue(context.Background(), platformInputReadObservationKey{}, &receipt.Failure)
	observePlatformInputReadFailure(ctx, "publication", trace.stage, nil, nil, err)
	for _, name := range trace.order {
		duration := trace.durations[name]
		if name == trace.stage {
			duration += now.Sub(trace.timeOfStage)
		}
		receipt.Stages = append(receipt.Stages, platformMutationStage{name, duration.Milliseconds()})
	}
	if _, exists := trace.durations[trace.stage]; !exists {
		receipt.Stages = append(receipt.Stages, platformMutationStage{trace.stage, now.Sub(trace.timeOfStage).Milliseconds()})
	}
	db := &trace.database
	if db.mu.TryLock() {
		receipt.Queries = append([]platformMutationQuery(nil), db.queries...)
		receipt.Completed = db.completed
		receipt.Omitted = db.completed - uint64(len(db.queries))
		receipt.Locks = append([]platformMutationQuery(nil), db.locks...)
		receipt.LockCompleted = db.lockCompleted
		receipt.LockOmitted = db.lockCompleted - uint64(len(db.locks))
		db.mu.Unlock()
		sort.Slice(receipt.Queries, func(i, j int) bool { return receipt.Queries[i].Started.Before(receipt.Queries[j].Started) })
		sort.Slice(receipt.Locks, func(i, j int) bool { return receipt.Locks[i].Started.Before(receipt.Locks[j].Started) })
	} else {
		db.dropped.Add(1)
	}
	receipt.Dropped = db.dropped.Load()
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		return
	}
	defer r.mu.Unlock()
	r.recorded++
	appendPlatformMutationReceipt(&r.recent, &r.next, receipt)
	if err != nil {
		r.failedRecorded++
		appendPlatformMutationReceipt(&r.failures, &r.failedNext, receipt)
	}
}

func appendPlatformMutationReceipt(rows *[]platformMutationReceipt, next *int, receipt platformMutationReceipt) {
	if len(*rows) < platformMutationReceiptLimit {
		*rows = append(*rows, receipt)
	} else {
		(*rows)[*next] = receipt
		*next = (*next + 1) % platformMutationReceiptLimit
	}
}

func (r *platformMutationRecorder) snapshot() map[string]any {
	r.mu.Lock()
	recent := append([]platformMutationReceipt(nil), r.recent...)
	failures := append([]platformMutationReceipt(nil), r.failures...)
	recorded, failed := r.recorded, r.failedRecorded
	r.mu.Unlock()
	sort.Slice(recent, func(i, j int) bool { return recent[i].Started.Before(recent[j].Started) })
	sort.Slice(failures, func(i, j int) bool { return failures[i].Started.Before(failures[j].Started) })
	return map[string]any{"receipts": recent, "failures": failures, "recorded": recorded,
		"failure_recorded": failed, "evicted": recorded - uint64(len(recent)),
		"failure_evicted": failed - uint64(len(failures)), "dropped": r.dropped.Load(),
		"limit": platformMutationReceiptLimit, "query_limit": platformMutationQueryLimit,
		"promotion_lock_query_limit": platformMutationLockQueryLimit,
		"coverage":                   "completed_publication_stage_and_driver_timings_not_lock_hold_or_cpu_measurements"}
}

func advancePlatformMutationTrace(ctx context.Context, stage string) {
	if trace, _ := ctx.Value(platformMutationTraceKey{}).(*platformMutationTrace); trace != nil {
		trace.advance(stage)
	}
}
