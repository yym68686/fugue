package observability

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
)

// Enforce the API's actual resourceVersion precondition. The client-go object
// tracker does not enforce it and would hide a stale-writer regression.
type checkpointLeaseAPI struct {
	coordinationclient.LeaseInterface
	mu       sync.Mutex
	lease    *coordinationv1.Lease
	revision int
	err      error
}

func (a *checkpointLeaseAPI) Get(context.Context, string, metav1.GetOptions) (*coordinationv1.Lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	if a.lease == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "cursor")
	}
	return a.lease.DeepCopy(), nil
}

func (a *checkpointLeaseAPI) Create(_ context.Context, lease *coordinationv1.Lease, _ metav1.CreateOptions) (*coordinationv1.Lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	if a.lease != nil {
		return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "leases"}, lease.Name)
	}
	a.revision++
	a.lease = lease.DeepCopy()
	a.lease.ResourceVersion = strconv.Itoa(a.revision)
	return a.lease.DeepCopy(), nil
}

func (a *checkpointLeaseAPI) Update(_ context.Context, lease *coordinationv1.Lease, _ metav1.UpdateOptions) (*coordinationv1.Lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.err
	}
	if a.lease == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "leases"}, lease.Name)
	}
	if a.lease.ResourceVersion != lease.ResourceVersion {
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "leases"}, lease.Name, errors.New("stale resourceVersion"))
	}
	a.revision++
	a.lease = lease.DeepCopy()
	a.lease.ResourceVersion = strconv.Itoa(a.revision)
	return a.lease.DeepCopy(), nil
}

func checkpointStore(t *testing.T, api *checkpointLeaseAPI) *logCheckpoint {
	t.Helper()
	store, err := newLogCheckpoint(api, "system", "log-cursors")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestLogCheckpointFencesPausedPredecessorAndPreservesStateOnTakeover(t *testing.T) {
	api := &checkpointLeaseAPI{}
	a, b := checkpointStore(t, api), checkpointStore(t, api)
	now := time.Now().UTC()
	state, acquired, err := a.acquire(t.Context(), now)
	if err != nil || !acquired {
		t.Fatal(acquired, err)
	}
	key := logCheckpointKey("synthetic-source")
	state.Sources[key] = logCheckpointPosition{Time: now.Add(-20 * time.Minute).UnixNano(), Visited: now.Unix()}
	if err := a.save(t.Context(), state, now, false); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := b.acquire(t.Context(), now.Add(30*time.Second)); err != nil || acquired {
		t.Fatal("live owner was replaced", acquired, err)
	}
	recovered, acquired, err := b.acquire(t.Context(), now.Add(61*time.Second))
	if err != nil || !acquired || !reflect.DeepEqual(recovered, state) {
		t.Fatal("expired owner lost recovery state", recovered, err)
	}
	state.Sources[key] = logCheckpointPosition{Time: now.UnixNano(), Visited: now.Unix()}
	if err := a.save(t.Context(), state, now.Add(62*time.Second), false); !errors.Is(err, errLogCheckpointOwnership) {
		t.Fatal("paused predecessor overwrote successor", err)
	}
	if _, _, err := a.acquire(t.Context(), now.Add(123*time.Second)); !errors.Is(err, errLogCheckpointOwnership) {
		t.Fatal("fenced predecessor reacquired old in-memory cursors", err)
	}
	if err := b.save(t.Context(), recovered, now.Add(62*time.Second), true); err != nil {
		t.Fatal(err)
	}
	c := checkpointStore(t, api)
	if actual, acquired, err := c.acquire(t.Context(), now.Add(63*time.Second)); err != nil || !acquired || !reflect.DeepEqual(actual, recovered) {
		t.Fatal("graceful release did not hand over immediately", actual, err)
	}
}

func TestLogCheckpointRejectsCorruptionStorageFailureAndOverflowWithoutWriting(t *testing.T) {
	api := &checkpointLeaseAPI{}
	s := checkpointStore(t, api)
	now := time.Now().UTC()
	state, ok, err := s.acquire(t.Context(), now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	before := api.lease.DeepCopy()
	for i := 0; i <= logCheckpointSources; i++ {
		state.Sources[logCheckpointKey(fmt.Sprint(i))] = logCheckpointPosition{Time: now.UnixNano(), Visited: now.Unix()}
	}
	if err := s.save(t.Context(), state, now, false); err == nil || !reflect.DeepEqual(api.lease, before) {
		t.Fatal("oversized checkpoint was written", err)
	}
	api.err = errors.New("storage offline")
	if _, ok, err := s.acquire(t.Context(), now); err == nil || ok {
		t.Fatal("storage error reset the frontier")
	}
	api.err = nil
	api.lease.Annotations[logCheckpointAnnotation] = `{"v":999,"s":{}}`
	before = api.lease.DeepCopy()
	if _, ok, err := checkpointStore(t, api).acquire(t.Context(), now.Add(time.Hour)); err == nil || ok || !reflect.DeepEqual(api.lease, before) {
		t.Fatal("unknown schema reset the frontier")
	}
	for _, raw := range []string{`{}`, `{"v":1,"s":null}`, `{"v":1,"s":{"invalid":[1,0,1]}}`, strings.Repeat(" ", logCheckpointLimit+1)} {
		if _, err := decodeLogCheckpoint(raw); err == nil {
			t.Fatal("invalid checkpoint accepted")
		}
	}
}

func TestLogCheckpointRestoresBacklogBeyondFiveMinutesFromRetainedFiles(t *testing.T) {
	c, target, rotated, _ := rotationFixture(t, 10, "A stdout F before\nB stdout F retained\n", "C stdout F after\n")
	c.deliveries = &logDeliveryTracker{}
	rotationPoll(t, c, target)
	state, err := c.checkpointState()
	if err != nil {
		t.Fatal(err)
	}
	persisted, _ := json.Marshal(state)
	// The admitted boundary is still unexported, so the durable cursor must
	// replay it rather than treating queue admission as an acknowledgement.
	rotated.Store(true)
	replacement := newKubernetesLogCollectorWithClient(NewPipeline(c.pipeline.cfg, nil), c.client)
	replacement.restored = state.Sources
	replacement.clock = func() time.Time { return time.Now().UTC().Add(20 * time.Minute) }
	rotationPoll(t, replacement, target)
	if got := rotationMessages(t, replacement.pipeline); !reflect.DeepEqual(got, []string{"before", "retained", "after"}) {
		t.Fatal("restart silently reset to the newest five minutes", got)
	}
	if replacement.pipeline.kubernetesLogCursorGaps.Load() != 0 {
		t.Fatal("retained boundary was incorrectly reported missing")
	}
	// A missing persisted witness remains a real gap after process replacement.
	other, otherTarget, otherRotated, _ := rotationFixture(t, 10, "B stdout F retained\n", "C stdout F after\n")
	reloaded, err := decodeLogCheckpoint(string(persisted))
	if err != nil {
		t.Fatal(err)
	}
	other.restored = reloaded.Sources
	otherRotated.Store(true)
	rotationPoll(t, other, otherTarget)
	if other.pipeline.kubernetesLogCursorGaps.Load() != 1 {
		t.Fatal("restart erased the missing-boundary evidence")
	}
}

func TestLogCheckpointReplayKeepsEqualTimestampOccurrencesAcrossBudgets(t *testing.T) {
	c, target, rotated, _ := rotationFixture(t, 1, "A stdout F before\nA stdout F before\nB stdout F retained\n", "C stdout F after\n")
	rotationPoll(t, c, target)
	key := logTargetKey(target)
	cur := c.cursors[key]
	cur.Boundary[sha256.Sum256([]byte("before"))] = 2
	c.cursors[key] = cur
	state, err := c.checkpointState()
	if err != nil {
		t.Fatal(err)
	}
	replacement := newKubernetesLogCollectorWithClient(NewPipeline(c.pipeline.cfg, nil), c.client)
	replacement.restored = state.Sources
	rotated.Store(true)
	for range 6 {
		rotationPoll(t, replacement, target)
	}
	if got := rotationMessages(t, replacement.pipeline); !reflect.DeepEqual(got, []string{"before", "before", "retained", "after"}) {
		t.Fatal("restart witness was used as a delivery acknowledgement or multiplicity changed", got)
	}
	if replacement.pipeline.kubernetesLogCursorGaps.Load() != 0 || replacement.cursors[key].RecoveringRotation {
		t.Fatal("budgeted recovery fabricated a gap or never completed")
	}
}

func TestLogCheckpointKeepsUndiscoveredUnreadFrontierBeyondInactiveCacheTTL(t *testing.T) {
	now := time.Now().UTC()
	key := logCheckpointKey("undiscovered-source")
	position := logCheckpointPosition{Time: now.Add(-3 * time.Hour).UnixNano(), Visited: now.Add(-2 * time.Hour).Unix()}
	c := &kubernetesLogCollector{pipeline: NewPipeline(Config{}, nil), clock: func() time.Time { return now }, restored: map[string]logCheckpointPosition{key: position}}
	state, err := c.checkpointState()
	if err != nil || state.Sources[key].Time != position.Time {
		t.Fatal("inactive cache TTL silently discarded a durable unread frontier", state, err)
	}
}

func TestLogCheckpointCancelledExporterLeavesQueuedFrontierPinned(t *testing.T) {
	p := NewPipeline(Config{Enabled: true, BatchSize: 1, RetryMaxAttempts: 1}, nil)
	entered := make(chan struct{})
	var once sync.Once
	p.exporter = exportObservationTestExporter{name: "blocked", run: func(ctx context.Context, _ []Event) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	c := &kubernetesLogCollector{pipeline: p, deliveries: &logDeliveryTracker{}, cursors: map[string]kubernetesLogCursor{"source": {Time: now, Visited: now}}}
	at := now.Add(-20 * time.Minute)
	if !p.ingestLogLineWithDelivery(ctx, "source", "first", nil, at, true, nil, c.deliveries.begin("source", at)) {
		t.Fatal("admission failed")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not start")
	}
	if !p.ingestLogLineWithDelivery(ctx, "source", "queued", nil, at.Add(time.Second), true, nil, c.deliveries.begin("source", at.Add(time.Second))) {
		t.Fatal("queue admission failed")
	}
	cancel()
	if err := p.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, err := c.checkpointState()
	if err != nil || state.Sources[logCheckpointKey("source")].Time != at.UnixNano() {
		t.Fatal("cancellation persisted an unacknowledged record as delivered", state, err)
	}
}

func TestDeliveryFrontierWaitsForEveryExporterAndRetriesFailedCompletedSources(t *testing.T) {
	now := time.Now().UTC()
	tracker := &logDeliveryTracker{}
	first := tracker.begin("source", now)
	second := tracker.begin("source", now.Add(time.Second))
	first.finish(false)
	if !tracker.takeFailure("source").IsZero() {
		t.Fatal("replayed before other batches finished")
	}
	second.finish(true)
	if !tracker.floors()["source"].Equal(now) {
		t.Fatal("failed export advanced the durable frontier")
	}
	cur := kubernetesLogCursor{Time: now.Add(time.Minute), Complete: true}
	c := &kubernetesLogCollector{deliveries: tracker}
	c.retryFailedDelivery("source", &cur)
	if cur.Complete || !cur.RecoveringRotation || !cur.Time.Equal(now) || len(tracker.floors()) != 0 {
		t.Fatal("completed source was not reopened at its failed export", cur)
	}
	first.finish(true) // Duplicate completion must not clear another receipt.
	if len(tracker.inflight) != 0 || len(tracker.pending) != 0 {
		t.Fatal("delivery receipts leaked")
	}
}

func TestCheckpointNeverAcknowledgesQueuedOrFailedPayloads(t *testing.T) {
	now := time.Now().UTC()
	key := "source"
	c := &kubernetesLogCollector{deliveries: &logDeliveryTracker{}, cursors: map[string]kubernetesLogCursor{key: {Time: now, DrainedThrough: now, Visited: now, Boundary: map[[32]byte]int{sha256.Sum256([]byte("last")): 1}}}}
	pending := c.deliveries.begin(key, now.Add(-time.Minute))
	state, _ := c.checkpointState()
	position := state.Sources[logCheckpointKey(key)]
	if position.Time != pending.at.UnixNano() || position.Drained != 0 || len(position.Boundary) != 0 {
		t.Fatal("admission was persisted as delivery", position)
	}
	pending.finish(false)
	state, _ = c.checkpointState()
	if state.Sources[logCheckpointKey(key)].Time != pending.at.UnixNano() {
		t.Fatal("exhausted export retries lost the frontier")
	}
	c.deliveries.takeFailure(key)
	state, _ = c.checkpointState()
	position = state.Sources[logCheckpointKey(key)]
	encoded, _ := json.Marshal(state)
	if position.Time != now.UnixNano() || len(position.Boundary) != 1 || strings.Contains(string(encoded), key) || strings.Contains(string(encoded), "last") {
		t.Fatal("acknowledged position or metadata isolation is wrong", position)
	}
	for hash := range position.Boundary {
		if raw, err := base64.RawURLEncoding.DecodeString(hash); err != nil || len(raw) != 32 {
			t.Fatal("invalid witness")
		}
	}
}

func TestLogCheckpointPersistsDiscoveryBeforeOpeningAnySource(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint(unavailable), func(t *testing.T) {
			api := &checkpointLeaseAPI{}
			store := checkpointStore(t, api)
			if _, ok, err := store.acquire(t.Context(), time.Now()); err != nil || !ok {
				t.Fatal(err)
			}
			if unavailable {
				api.err = errors.New("checkpoint storage unavailable")
			}
			var reads atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/pods" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{runningLogPod("source")}})
					return
				}
				reads.Add(1)
				api.mu.Lock()
				state, err := decodeLogCheckpoint(api.lease.Annotations[logCheckpointAnnotation])
				api.mu.Unlock()
				if err != nil || len(state.Sources) != 1 {
					t.Error("source was opened before its initial frontier was durable")
				}
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			c := newKubernetesLogCollectorWithClient(NewPipeline(Config{Enabled: true, QueueSize: 32}, nil), client)
			c.checkpoint, c.deliveries = store, &logDeliveryTracker{}
			c.collectOnce(t.Context())
			want := int64(1)
			if unavailable {
				want = 0
			}
			if reads.Load() != want {
				t.Fatal("storage failure did not stop source collection", reads.Load())
			}
		})
	}
}

func TestLogDeliveryAcknowledgesOnlyAfterAllExportersComplete(t *testing.T) {
	for _, fails := range []bool{false, true} {
		t.Run(fmt.Sprint(fails), func(t *testing.T) {
			p := NewPipeline(Config{Enabled: true, BatchSize: 1, RetryMaxAttempts: 1}, nil)
			entered, release := make(chan struct{}), make(chan struct{})
			p.exporter = MultiExporter{exporters: []Exporter{
				exportObservationTestExporter{name: "first", run: func(context.Context, []Event) error { return nil }},
				exportObservationTestExporter{name: "second", run: func(context.Context, []Event) error {
					close(entered)
					<-release
					if fails {
						return errors.New("export unavailable")
					}
					return nil
				}},
			}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := p.Start(ctx); err != nil {
				t.Fatal(err)
			}
			tracker := &logDeliveryTracker{}
			at := time.Now().UTC()
			receipt := tracker.begin("source", at)
			if !p.ingestLogLineWithDelivery(ctx, "source", "synthetic", nil, at, true, nil, receipt) {
				t.Fatal("admission failed")
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("export did not start")
			}
			if !tracker.floors()["source"].Equal(at) {
				t.Error("partial exporter success acknowledged the record")
			}
			close(release)
			deadline := time.Now().Add(5 * time.Second)
			for {
				tracker.mu.Lock()
				pending := len(tracker.pending)
				tracker.mu.Unlock()
				if pending == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("export did not finish its delivery receipt")
				}
				time.Sleep(time.Millisecond)
			}
			_, pinned := tracker.floors()["source"]
			if pinned != fails {
				t.Error("export result did not preserve the correct replay frontier")
			}
			if err := p.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLogCheckpointEnforcesByteBoundAndRetainsUndiscoveredSources(t *testing.T) {
	now := time.Now().UTC()
	state := logCheckpointState{Version: 1, Sources: map[string]logCheckpointPosition{}}
	boundary := map[string]int{}
	for i := 0; i < 6000; i++ {
		hash := sha256.Sum256([]byte(fmt.Sprint(i)))
		boundary[base64.RawURLEncoding.EncodeToString(hash[:])] = 1
	}
	state.Sources[logCheckpointKey("source")] = logCheckpointPosition{Time: now.UnixNano(), Visited: now.Unix(), Boundary: boundary}
	raw, _ := json.Marshal(state)
	if len(raw) <= logCheckpointLimit {
		t.Fatal("fixture did not exceed byte bound")
	}
	if _, err := decodeLogCheckpoint(string(raw)); err == nil {
		t.Fatal("unbounded boundary accepted")
	}
	fresh, expired := logCheckpointKey("fresh"), logCheckpointKey("expired")
	c := &kubernetesLogCollector{clock: func() time.Time { return now }, restored: map[string]logCheckpointPosition{
		fresh:   {Time: now.Add(-20 * time.Minute).UnixNano(), Visited: now.Unix()},
		expired: {Time: now.Add(-2 * time.Hour).UnixNano(), Visited: now.Add(-2 * time.Hour).Unix()},
	}}
	saved, err := c.checkpointState()
	if err != nil || len(saved.Sources) != 2 || saved.Sources[fresh].Time != c.restored[fresh].Time || saved.Sources[expired].Time != c.restored[expired].Time {
		t.Fatal("discovery failure discarded unread positions", saved, err)
	}
}

func TestLogCheckpointCompactionNeverAdvancesUnreadFrontier(t *testing.T) {
	now := time.Now().UTC()
	for _, witnesses := range []int{0, 6000} {
		t.Run(fmt.Sprint(witnesses), func(t *testing.T) {
			state := logCheckpointState{Version: 1, Sources: map[string]logCheckpointPosition{}}
			original := map[string]logCheckpointPosition{}
			for i := range logCheckpointSources + 100 {
				position := logCheckpointPosition{Time: now.Add(-time.Duration(i+10) * time.Minute).UnixNano(), Visited: now.Add(-time.Duration(i) * time.Second).Unix()}
				if i == 0 && witnesses > 0 {
					position.Boundary = map[string]int{}
					for j := range witnesses {
						position.Boundary[logCheckpointKey(fmt.Sprint(j))] = 1
					}
				}
				key := logCheckpointKey(fmt.Sprint(i))
				state.Sources[key], original[key] = position, position
			}
			bounded, err := compactLogCheckpoint(state)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(bounded)
			if len(raw) > logCheckpointLimit || len(bounded.Sources) > logCheckpointSources || bounded.ReplayFloor == 0 {
				t.Fatal("checkpoint not bounded", len(raw), len(bounded.Sources))
			}
			for key, before := range original {
				if after, ok := bounded.Sources[key]; ok {
					if !reflect.DeepEqual(after, before) {
						t.Fatal("retained frontier changed")
					}
				} else if bounded.ReplayFloor > before.Time {
					t.Fatal("compaction advanced unread frontier")
				}
			}
			decoded, err := decodeLogCheckpoint(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			c := &kubernetesLogCollector{restored: decoded.Sources, replayFloor: decoded.ReplayFloor, replayFilter: decoded.ReplayFilter}
			for i := range logCheckpointSources + 100 {
				identity := fmt.Sprint(i)
				cur := c.restoreCursor(identity, now)
				if cur.Time.UnixNano() > original[logCheckpointKey(identity)].Time || !cur.RecoveringRotation {
					t.Fatal("restart reset compacted source", i, cur)
				}
			}
		})
	}
}

func TestLogCheckpointCompleteRequiresAllExportAcknowledgements(t *testing.T) {
	now := time.Now().UTC()
	c := &kubernetesLogCollector{deliveries: &logDeliveryTracker{}, cursors: map[string]kubernetesLogCursor{"source": {Time: now, Visited: now, Complete: true}}}
	receipt := c.deliveries.begin("source", now)
	state, err := c.checkpointState()
	if err != nil || state.Sources[logCheckpointKey("source")].Complete {
		t.Fatal("queue admission marked source complete", state, err)
	}
	receipt.finish(true)
	state, err = c.checkpointState()
	if err != nil || !state.Sources[logCheckpointKey("source")].Complete {
		t.Fatal("acknowledged EOF was not saved", state, err)
	}
	c.restored = state.Sources
	if cur := c.restoreCursor("source", now); !cur.Complete {
		t.Fatal("completed source was reopened")
	}
}

func TestLogCheckpointPrunesCompleteSourcesBeforeAdmissionSave(t *testing.T) {
	now := time.Now().UTC()
	p := NewPipeline(Config{}, nil)
	api := &checkpointLeaseAPI{}
	store := checkpointStore(t, api)
	if _, ok, err := store.acquire(t.Context(), now); err != nil || !ok {
		t.Fatal(err)
	}
	c := &kubernetesLogCollector{pipeline: p, checkpoint: store, deliveries: &logDeliveryTracker{}, clock: func() time.Time { return now }, cursors: map[string]kubernetesLogCursor{
		"complete": {Time: now, Visited: now.Add(-2 * time.Hour), Complete: true},
		"pending":  {Time: now, Visited: now.Add(-2 * time.Hour), Complete: true},
		"unread":   {Time: now.Add(-3 * time.Hour), Visited: now.Add(-2 * time.Hour)},
	}, restored: map[string]logCheckpointPosition{logCheckpointKey("restored-complete"): {Time: now.UnixNano(), Visited: now.Add(-2 * time.Hour).Unix(), Complete: true}}}
	c.deliveries.begin("pending", now)
	c.pruneInactiveLogCursors()
	if _, ok := c.cursors["complete"]; ok || len(c.restored) != 0 || len(c.cursors) != 2 {
		t.Fatal("unsafe completed-source pruning", c.cursors, c.restored)
	}
	if err := c.saveCheckpoint(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	state, err := decodeLogCheckpoint(api.lease.Annotations[logCheckpointAnnotation])
	if err != nil || len(state.Sources) != 2 || c.pipeline.sourceCheckpoint.Sources != 2 {
		t.Fatal("admission save lost pending/unread frontier", state, err)
	}
	if c.pipeline.sourceCheckpoint.SerializedBytes != len(api.lease.Annotations[logCheckpointAnnotation]) {
		t.Fatal("checkpoint bytes describe the cache rather than durable state")
	}
	if c.cursors["pending"].Time.IsZero() || c.restored[logCheckpointKey("unread")].Time == 0 {
		t.Fatal("pending identity lost or inactive frontier not retained")
	}
}

func BenchmarkLogCheckpointCompaction(b *testing.B) {
	now := time.Now().UTC()
	for b.Loop() {
		state := logCheckpointState{Version: 1, Sources: map[string]logCheckpointPosition{}}
		for i := range 3000 {
			state.Sources[logCheckpointKey(fmt.Sprint(i))] = logCheckpointPosition{Time: now.Add(-time.Duration(i) * time.Second).UnixNano(), Visited: now.Unix()}
		}
		if _, err := compactLogCheckpoint(state); err != nil {
			b.Fatal(err)
		}
	}
}

func TestLogCheckpointUnprovenReplaySurvivesBudgetsAndAnotherRestart(t *testing.T) {
	c, target, rotated, _ := rotationFixture(t, 1, "A stdout F before\nB stdout F retained\n", "C stdout F after\n")
	c.replayFloor = time.Now().UTC().Add(-20 * time.Minute).UnixNano()
	c.replayFilter = make([]byte, logCheckpointFilterBytes)
	logCheckpointMembership(c.replayFilter, logCheckpointKey(logTargetKey(target)), true)
	rotated.Store(true)
	for range 2 {
		rotationPoll(t, c, target)
	}
	key := logTargetKey(target)
	if cur := c.cursors[key]; !cur.RecoveringRotation || !cur.RecoveryUnproven {
		t.Fatal("budget stop erased uncertainty", cur)
	}
	state, err := c.checkpointState()
	if err != nil || !state.Sources[logCheckpointKey(key)].Unproven {
		t.Fatal("checkpoint erased replay uncertainty", state, err)
	}
	replacement := newKubernetesLogCollectorWithClient(NewPipeline(c.pipeline.cfg, nil), c.client)
	replacement.restored, replacement.replayFloor, replacement.replayFilter = state.Sources, state.ReplayFloor, state.ReplayFilter
	for range 5 {
		rotationPoll(t, replacement, target)
	}
	if replacement.pipeline.sourceCheckpoint.UnprovenReplays != 1 {
		t.Fatal("budgeted restart claimed proven recovery", replacement.pipeline.sourceCheckpoint)
	}
	if got := rotationMessages(t, replacement.pipeline); !reflect.DeepEqual(got, []string{"retained", "after"}) {
		t.Fatal("inclusive restart frontier changed", got)
	}
}

func TestLogCheckpointRetentionResetDoesNotKeepObsoleteWitness(t *testing.T) {
	c, target, rotated, _ := rotationFixture(t, 10, "A stdout F before\nB stdout F retained\n", "C stdout F after\n")
	rotationPoll(t, c, target)
	state, err := c.checkpointState()
	if err != nil {
		t.Fatal(err)
	}
	replacement := newKubernetesLogCollectorWithClient(NewPipeline(c.pipeline.cfg, nil), c.client)
	replacement.pipeline.cfg.Retention = 5 * time.Minute
	replacement.restored = state.Sources
	replacement.clock = func() time.Time { return time.Now().UTC().Add(20 * time.Minute) }
	rotated.Store(true)
	rotationPoll(t, replacement, target)
	if replacement.pipeline.kubernetesLogCursorGaps.Load() != 1 || replacement.pipeline.sourceGapReceipts[0].Reason != "retention_window" {
		t.Fatal("obsolete witness fabricated a second rotation gap", replacement.pipeline.sourceGapReceipts)
	}
	if replacement.pipeline.sourceCheckpoint.UnprovenReplays != 1 {
		t.Fatal("retention reset claimed proven checkpoint replay")
	}
}

func TestLogCheckpointCompactedMembershipHasNoFalseNegatives(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	c := &kubernetesLogCollector{replayFloor: now.Add(-48 * time.Hour).UnixNano(), replayFilter: make([]byte, logCheckpointFilterBytes)}
	for i := range 10000 {
		logCheckpointMembership(c.replayFilter, logCheckpointKey(fmt.Sprint(i)), true)
	}
	for i := range 10000 {
		cur := c.restoreCursor(fmt.Sprint(i), now)
		if cur.Time.UnixNano() != c.replayFloor || !cur.RecoveringRotation {
			t.Fatal("compacted identity lost its recovery floor", i)
		}
	}
	// Negative membership permits the ordinary first-discovery lookback. A
	// false positive only causes conservative replay, never a skipped frontier.
	fresh := &kubernetesLogCollector{replayFloor: c.replayFloor, replayFilter: make([]byte, logCheckpointFilterBytes)}
	if cur := fresh.restoreCursor("new-source", now); !cur.Time.Equal(now.Add(-5*time.Minute)) || cur.RecoveringRotation {
		t.Fatal("negative identity inherited compacted history", cur)
	}
	key := logCheckpointKey("0")
	c.restored = map[string]logCheckpointPosition{key: {Time: now.Add(-20 * time.Minute).UnixNano(), Visited: now.Unix()}}
	if cur := c.restoreCursor("0", now); !cur.Time.Equal(now.Add(-20*time.Minute)) || !cur.RecoveringRotation {
		t.Fatal("exact source position lost precedence", cur)
	}
}

func TestLogCheckpointCompactionPreservesMembershipAcrossChurnAndSaturation(t *testing.T) {
	now := time.Now().UTC()
	state := logCheckpointState{Version: 1, Sources: map[string]logCheckpointPosition{}}
	original := map[string]int64{}
	for round := range 4 {
		for i := range 3000 {
			identity := fmt.Sprintf("source-%d-%d", round, i)
			key := logCheckpointKey(identity)
			at := now.Add(-time.Duration(round*3000+i+1) * time.Second).UnixNano()
			state.Sources[key] = logCheckpointPosition{Time: at, Visited: now.Unix()}
			original[identity] = at
		}
		var err error
		state, err = compactLogCheckpoint(state)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(state)
		state, err = decodeLogCheckpoint(string(raw))
		if err != nil || len(raw) > logCheckpointLimit {
			t.Fatal("churn exceeded durable bound", len(raw), err)
		}
		// Each restart gets an independent exact-position map.
		restored := make(map[string]logCheckpointPosition, len(state.Sources))
		for key, position := range state.Sources {
			restored[key] = position
		}
		c := &kubernetesLogCollector{restored: restored, replayFloor: state.ReplayFloor, replayFilter: state.ReplayFilter}
		for identity, at := range original {
			cur := c.restoreCursor(identity, now)
			if cur.Time.UnixNano() > at || !cur.RecoveringRotation {
				t.Fatal("churn silently advanced a frontier", identity)
			}
		}
	}
	for i := range state.ReplayFilter {
		state.ReplayFilter[i] = 255
	}
	c := &kubernetesLogCollector{replayFloor: state.ReplayFloor, replayFilter: state.ReplayFilter}
	cur := c.restoreCursor("previously-unseen-source", now)
	if cur.Time.UnixNano() != state.ReplayFloor || !cur.RecoveryUnproven {
		t.Fatal("saturated membership claimed proven recovery", cur)
	}
	for _, filter := range [][]byte{nil, make([]byte, 1)} {
		state.ReplayFilter = filter
		raw, _ := json.Marshal(state)
		if _, err := decodeLogCheckpoint(string(raw)); err == nil {
			t.Fatal("compacted floor accepted without fixed membership")
		}
	}
}

func TestLogCheckpointCompactsAcknowledgedCompleteSourcesWithoutReplayFloor(t *testing.T) {
	now := time.Now().UTC()
	state := logCheckpointState{Version: 1, Sources: map[string]logCheckpointPosition{}}
	for i := range 3000 {
		state.Sources[logCheckpointKey(fmt.Sprint(i))] = logCheckpointPosition{Time: now.UnixNano(), Visited: now.Unix(), Complete: true}
	}
	state, err := compactLogCheckpoint(state)
	if err != nil || len(state.Sources) > logCheckpointSources || state.ReplayFloor != 0 || len(state.ReplayFilter) != 0 {
		t.Fatal("completed-only churn created unread replay state", err)
	}
}

func TestLogCheckpointConfigurationRequiresPairAndDelivery(t *testing.T) {
	for _, cfg := range []Config{
		{LogCheckpointName: "positions"}, {LogCheckpointNamespace: "system"},
		{KubernetesLogsEnabled: true, LogCheckpointNamespace: "system", LogCheckpointName: "positions"},
	} {
		if cfg.Validate() == nil {
			t.Fatal("invalid durable collection config accepted")
		}
	}
	if err := (Config{KubernetesLogsEnabled: true, LogCheckpointNamespace: "system", LogCheckpointName: "positions", LokiURL: "http://logs.example.test"}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLogCheckpointRollingSuccessorWaitsAndReleasesPinnedFrontier(t *testing.T) {
	api := &checkpointLeaseAPI{}
	owner := checkpointStore(t, api)
	now := time.Now().UTC()
	state, ok, err := owner.acquire(t.Context(), now)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	var discoveries atomic.Int64
	c, p := coverageCollector(t, Config{Enabled: true, KubernetesLogPollInterval: 200 * time.Millisecond, QueueSize: 32}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			discoveries.Add(1)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{runningLogPod("source")}})
			return
		}
		fmt.Fprintf(w, "%s synthetic\n", now.Format(time.RFC3339Nano))
	})
	c.checkpoint, c.deliveries = checkpointStore(t, api), &logDeliveryTracker{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p.ctx = ctx
	done := make(chan struct{})
	go func() { c.run(); close(done) }()
	wait := func(match func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !match() {
			if time.Now().After(deadline) {
				t.Fatal("collector condition did not converge")
			}
			time.Sleep(time.Millisecond)
		}
	}
	wait(func() bool {
		p.sourceObservationMu.Lock()
		defer p.sourceObservationMu.Unlock()
		return p.sourceCheckpoint.State == "standby"
	})
	if discoveries.Load() != 0 {
		t.Fatal("standby collector performed source I/O")
	}
	if err := owner.save(t.Context(), state, now, true); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return p.received.Load() > 0 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("collector did not release on shutdown")
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.lease.Spec.HolderIdentity == nil || *api.lease.Spec.HolderIdentity != "" {
		t.Fatal("graceful shutdown did not release ownership")
	}
	saved, err := decodeLogCheckpoint(api.lease.Annotations[logCheckpointAnnotation])
	key := logCheckpointKey(logTargetKey(kubernetesLogTarget{pod: runningLogPod("source"), container: "app"}))
	if err != nil || saved.Sources[key].Time > now.UnixNano() || saved.Sources[key].Complete {
		t.Fatal("queued payload was lost on handover", saved, err)
	}
}
