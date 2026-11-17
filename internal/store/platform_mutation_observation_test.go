package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtimeobservation"

	"github.com/jackc/pgx/v5"
)

func TestPlatformMutationObservationStagesAndOriginalErrors(t *testing.T) {
	ctx, trace := startPlatformMutationTrace(context.Background(), "platform-release")
	at := trace.started
	trace.now = func() time.Time { return at }
	trace.advance("read-artifact")
	at = at.Add(12 * time.Millisecond)
	trace.advance("verify-members")
	at = at.Add(30 * time.Millisecond)
	trace.advance("read-artifact")
	at = at.Add(8 * time.Millisecond)
	r := &platformMutationRecorder{}
	r.record(trace, ErrConflict)
	got := r.snapshot()["receipts"].([]platformMutationReceipt)
	if len(got) != 1 || !got[0].Failed || got[0].Failure.ErrorClass != "conflict" || got[0].Operation != runtimeobservation.DatabaseOperationFromContext(ctx) {
		t.Fatal("missing original operation or classified failure", got)
	}
	want := []platformMutationStage{{"setup", 0}, {"read-artifact", 20}, {"verify-members", 30}}
	if !reflect.DeepEqual(got[0].Stages, want) || got[0].Stage != "read-artifact" {
		t.Fatal("repeated and final stages lost their duration", got[0].Stages)
	}
	for _, original := range []error{nil, ErrConflict, ErrNotFound, errors.New("private-error-content")} {
		_, tr := startPlatformMutationTrace(context.Background(), "platform-release")
		if tr.finish(original) != original {
			t.Fatal("diagnostic changed original ordinary error identity")
		}
	}
}

func TestPlatformMutationObservationDriverHookRetainsBoundedHeadAndTail(t *testing.T) {
	ctx, trace := startPlatformMutationTrace(context.Background(), "platform-release")
	first := time.Now().UTC()
	for i := 0; i < 100; i++ {
		fact := runtimeobservation.DatabaseQueryFact{ID: uint64(i + 1), BackendPID: 71,
			Connection: 9, ConnectionFirstObserved: first, Fingerprint: runtimeobservation.SQLFingerprint("private-query"),
			Caller: "fugue/internal/store.neutral", Started: first.Add(time.Duration(i) * time.Nanosecond)}
		queryCtx := context.WithValue(ctx, databaseQueryKey{}, fact)
		(&postgresReadTracer{}).TraceQueryEnd(queryCtx, nil, pgx.TraceQueryEndData{})
	}
	r := &platformMutationRecorder{}
	r.record(trace, nil)
	receipt := r.snapshot()["receipts"].([]platformMutationReceipt)[0]
	if receipt.Completed != 100 || receipt.Omitted != 52 || receipt.Dropped != 0 || len(receipt.Queries) != platformMutationQueryLimit {
		t.Fatal("query limit or coverage counters incorrect", receipt.Completed, receipt.Omitted, receipt.Dropped, len(receipt.Queries))
	}
	for i, q := range receipt.Queries {
		index := i
		if i >= platformMutationQueryHead {
			index += 52
		}
		if q.Started != first.Add(time.Duration(index)*time.Nanosecond) || q.BackendPID != 71 || q.Connection != 9 || q.ConnectionFirstObserved != first || q.Failed || q.Fingerprint == "" {
			t.Fatal("driver identity or bounded head/tail lost", i, q)
		}
	}
	encoded, err := json.Marshal(receipt)
	if err != nil || strings.Contains(string(encoded), "private-query") {
		t.Fatal("query content leaked or encoding failed", err)
	}
}

func TestPlatformMutationObservationFailureRetentionAndResponseBudget(t *testing.T) {
	r := &platformMutationRecorder{}
	add := func(failed bool) {
		ctx, trace := startPlatformMutationTrace(context.Background(), "platform-release")
		for i := 0; i < platformMutationQueryLimit; i++ {
			observePlatformMutationQuery(ctx, runtimeobservation.DatabaseQueryFact{BackendPID: ^uint32(0),
				Connection: ^uint64(0), ConnectionFirstObserved: trace.started,
				Fingerprint: strings.Repeat("f", 32), Caller: strings.Repeat("c", 180), Started: trace.started}, nil)
		}
		for i := 0; i < platformMutationLockQueryLimit; i++ {
			observePlatformMutationQuery(ctx, runtimeobservation.DatabaseQueryFact{BackendPID: ^uint32(0),
				Connection: ^uint64(0), ConnectionFirstObserved: trace.started,
				Fingerprint: strings.Repeat("f", 32), Caller: "fugue/internal/store.pgLockPromotionScope", Started: trace.started}, nil)
		}
		for i := 0; i < 40; i++ {
			trace.advance("neutral-fixed-stage-" + strings.Repeat("x", i))
		}
		for i := 0; i < platformAdmissionObservationLimit; i++ {
			admission := beginPlatformAdmissionTrace(ctx, nil)
			dns := beginPlatformDNSReferenceTrace(admission)
			for phase := dnsReferenceAuthentication; phase < dnsReferencePhaseCount; phase++ {
				dns.advance(phase)
			}
			dns.finish()
			for phase := admissionSetup; phase < admissionPhaseCount; phase++ {
				admission.advance(phase)
			}
			admission.finish(nil)
		}
		var err error
		if failed {
			err = errors.New("never-retain-this-error-or-credential")
		}
		r.record(trace, err)
	}
	for i := 0; i < platformMutationReceiptLimit+2; i++ {
		add(true)
	}
	for i := 0; i < platformMutationReceiptLimit+3; i++ {
		add(false)
	}
	snapshot := r.snapshot()
	recent := snapshot["receipts"].([]platformMutationReceipt)
	failures := snapshot["failures"].([]platformMutationReceipt)
	if len(recent) != platformMutationReceiptLimit || len(failures) != platformMutationReceiptLimit || snapshot["failure_evicted"] != uint64(2) || snapshot["evicted"] != uint64(platformMutationReceiptLimit+5) {
		t.Fatal("bounded rings or explicit evictions incorrect", snapshot["evicted"], snapshot["failure_evicted"])
	}
	for _, f := range failures {
		if !f.Failed || f.Failure.ErrorClass != "unclassified" {
			t.Fatal("success erased or relabeled failures")
		}
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) > 800<<10 || strings.Contains(string(encoded), "never-retain") {
		t.Fatal("owner-only observation exceeded budget or retained private errors", len(encoded), err)
	}
}

func TestPlatformAdmissionObservationIsBoundedAndRedacted(t *testing.T) {
	ctx, trace := startPlatformMutationTrace(context.Background(), "platform-release")
	at := trace.started
	trace.now = func() time.Time { return at }
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{{ID: "private-artifact-marker"}},
		ExpectedConsumerSets:      []model.PlatformExpectedConsumerSet{{ID: "private-expectation-marker"}},
		PlatformConsumerInstances: []model.PlatformConsumerInstance{{ConsumerID: "private-consumer-marker"}}}
	for i := 0; i < platformAdmissionObservationLimit+1; i++ {
		observation := beginPlatformAdmissionTrace(ctx, state)
		if observation == nil {
			continue
		}
		at = at.Add(5 * time.Millisecond)
		observation.advance(admissionTopology)
		at = at.Add(7 * time.Millisecond)
		observation.value.RequiredConsumers = 2
		observation.value.CapabilityProjections = 2
		observation.value.CapabilityFacts = 5
		observation.advance(admissionCapabilityProjection)
		at = at.Add(11 * time.Millisecond)
		observation.finish(ErrConflict)
	}
	recorder := &platformMutationRecorder{}
	recorder.record(trace, nil)
	receipts := recorder.snapshot()["receipts"].([]platformMutationReceipt)
	if len(receipts) != 1 || len(receipts[0].Admissions) != platformAdmissionObservationLimit || receipts[0].AdmissionsOmitted != 1 {
		t.Fatal("admission observation limit or omission count incorrect", receipts)
	}
	got := receipts[0].Admissions[0]
	want := []platformMutationStage{{"setup", 5}, {"topology", 7}, {"capability_projection", 11}}
	if !got.Failed || got.FinalPhase != "capability_projection" || !reflect.DeepEqual(got.Stages, want) ||
		got.Artifacts != 1 || got.ExpectedSets != 1 || got.ServingFacts != 1 || got.RequiredConsumers != 2 ||
		got.CapabilityProjections != 2 || got.CapabilityFacts != 5 {
		t.Fatal("fixed admission phase measurements or bounded counts incorrect", got)
	}
	encoded, err := json.Marshal(receipts[0])
	if err != nil || strings.Contains(string(encoded), "private-") || len(encoded) > 800<<10 {
		t.Fatal("admission receipt retained private state or exceeded budget", len(encoded), err)
	}
}

func TestPlatformMutationObservationRetainsPromotionLocksAfterPreparationAndQueryChurn(t *testing.T) {
	ctx, trace := startPlatformMutationTrace(context.Background(), "platform-release")
	first := time.Now().UTC()
	sequence := 0
	query := func(caller string, failed bool) {
		sequence++
		fact := runtimeobservation.DatabaseQueryFact{ID: uint64(sequence), BackendPID: 71,
			Connection: 9, ConnectionFirstObserved: first, Fingerprint: runtimeobservation.SQLFingerprint("private-query"),
			Caller: caller, Started: first.Add(time.Duration(sequence) * time.Nanosecond)}
		var err error
		if failed {
			err = errors.New("private-error")
		}
		(&postgresReadTracer{}).TraceQueryEnd(context.WithValue(ctx, databaseQueryKey{}, fact), nil, pgx.TraceQueryEndData{Err: err})
	}
	// Production preparation already exceeded the ordinary first 16 facts;
	// all promotion locks can then fall between that head and the final tail.
	for i := 0; i < 40; i++ {
		query("fugue/internal/store.prepare", false)
	}
	for i := 0; i < 20; i++ {
		query("fugue/internal/store.pgLockPromotionScope", i == 19)
	}
	for i := 0; i < 60; i++ {
		query("fugue/internal/store.persist", false)
	}
	r := &platformMutationRecorder{}
	r.record(trace, ErrConflict)
	receipt := r.snapshot()["receipts"].([]platformMutationReceipt)[0]
	if receipt.LockCompleted != 20 || receipt.LockOmitted != 12 || len(receipt.Locks) != platformMutationLockQueryLimit || receipt.Dropped != 0 {
		t.Fatal("promotion lock retention or coverage incorrect", receipt.LockCompleted, receipt.LockOmitted, len(receipt.Locks), receipt.Dropped)
	}
	for i, lock := range receipt.Locks {
		index := 41 + i
		if i >= platformMutationLockQueryHead {
			index += 12
		}
		if lock.Started != first.Add(time.Duration(index)*time.Nanosecond) || lock.BackendPID != 71 || lock.Connection != 9 || lock.Failed != (index == 60) {
			t.Fatal("original lock driver identity, outcome or head/tail timing lost", i, lock)
		}
	}
	for _, ordinary := range receipt.Queries {
		if ordinary.Caller == "fugue/internal/store.pgLockPromotionScope" {
			t.Fatal("fixture did not reproduce loss from ordinary head/tail")
		}
	}
	encoded, err := json.Marshal(receipt)
	if err != nil || strings.Contains(string(encoded), "private-query") || strings.Contains(string(encoded), "private-error") {
		t.Fatal("lock observation retained private data", err)
	}
}

func TestPlatformMutationObservationNonblockingAndConcurrent(t *testing.T) {
	r := &platformMutationRecorder{}
	ctx, trace := startPlatformMutationTrace(context.Background(), "platform-release")
	trace.database.mu.Lock()
	observePlatformMutationQuery(ctx, runtimeobservation.DatabaseQueryFact{}, nil)
	trace.database.mu.Unlock()
	if trace.database.dropped.Load() != 1 {
		t.Fatal("contended observer blocked instead of explicitly dropping")
	}
	r.mu.Lock()
	r.record(trace, nil)
	r.mu.Unlock()
	if r.dropped.Load() != 1 {
		t.Fatal("contended recorder did not explicitly drop")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ctx, tr := startPlatformMutationTrace(context.Background(), "platform-release")
				observePlatformMutationQuery(ctx, runtimeobservation.DatabaseQueryFact{}, ErrConflict)
				r.record(tr, ErrConflict)
				_ = r.snapshot()
			}
		}()
	}
	wg.Wait()
	if r.recorded+r.dropped.Load() != 801 {
		t.Fatal("concurrent completion coverage lost", r.recorded, r.dropped.Load())
	}
}

func TestPlatformMutationObservationPostgresPublicationDriverAndStages(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 0)
	publicationInputValidate(t, s, "activation-root")
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	ctx, trace := startPlatformMutationTrace(t.Context(), "publication-observation-test")
	trace.advance("prepare-input-admission")
	request := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}
	proof, pins, err := fresh.prepareCompilerInputPublication(ctx, "activation-root", "", nil, request)
	if err != nil {
		t.Fatal(err)
	}
	_, release, _, _, err := fresh.pgReleasePreparedPlatformArtifact(ctx, "activation-root", request, testPlatformPrincipal(), nil, proof, pins, trace)
	if err = trace.finish(err); err != nil || release.ID == "" {
		t.Fatal("observed publication failed", err)
	}
	operation := runtimeobservation.DatabaseOperationFromContext(ctx)
	var receipt *platformMutationReceipt
	for _, row := range platformMutationObservations.snapshot()["receipts"].([]platformMutationReceipt) {
		if row.Operation == operation {
			receipt = &row
			break
		}
	}
	if receipt == nil || receipt.Failed || receipt.Stage != "admission-commit" || len(receipt.Queries) == 0 || receipt.Dropped != 0 {
		t.Fatal("real driver completion or admission context was lost", receipt)
	}
	stages := map[string]bool{}
	for _, stage := range receipt.Stages {
		stages[stage.Stage] = true
	}
	for _, stage := range []string{"prepare-input-admission", "admission-publication-locks", "admission-barrier", "admission-verified-pins", "admission-dependencies", "admission-commit-keyring", "admission-commit"} {
		if !stages[stage] {
			t.Fatal("publication did not retain the original stage", stage)
		}
	}
	for _, query := range receipt.Queries {
		if query.BackendPID == 0 || query.Connection == 0 || query.ConnectionFirstObserved.IsZero() || query.Fingerprint == "" || query.Finished.Before(query.Started) {
			t.Fatal("real driver identity or timing missing", query)
		}
	}
	if receipt.LockCompleted == 0 || len(receipt.Locks) == 0 || receipt.LockOmitted != 0 {
		t.Fatal("real publication promotion lock driver facts missing", receipt.LockCompleted, receipt.LockOmitted)
	}
	for _, lock := range receipt.Locks {
		if lock.Caller != "fugue/internal/store.pgLockPromotionScope" || lock.BackendPID == 0 || lock.Connection == 0 || lock.Failed || lock.Finished.Before(lock.Started) {
			t.Fatal("real promotion lock identity or outcome missing", lock)
		}
	}
}
