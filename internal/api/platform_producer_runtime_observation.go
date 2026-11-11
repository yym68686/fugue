package api

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

const producerRuntimeObservationSchema = "fugue.producer.runtime-observation/v1"

// These are process-local function lifetimes, not business success, database
// commits, durable ticket counts or an assertion that every replica enrolled.
type producerRuntimeCalls struct {
	Started  uint64 `json:"started"`
	Returned uint64 `json:"returned"`
	Active   uint64 `json:"active"`
}

type producerRuntimeScope struct {
	Scope    string               `json:"scope"`
	Workers  producerRuntimeCalls `json:"workers"`
	NewInput producerRuntimeCalls `json:"new_input_boundary_calls"`
}

type producerRuntimeWorker struct {
	ID        string
	Scope     string
	StartedAt time.Time
	Context   context.Context
}

type producerCaptureRuntime struct {
	mu         sync.Mutex
	bootID     string
	startedAt  time.Time
	incomplete bool
	managers   producerRuntimeCalls
	workers    producerRuntimeCalls
	newInput   producerRuntimeCalls
	scopes     map[string]*producerRuntimeScope
	active     map[string]producerRuntimeWorker
}

func newProducerCaptureRuntime() *producerCaptureRuntime {
	return &producerCaptureRuntime{bootID: model.NewID("producer-boot"), startedAt: time.Now().UTC(), scopes: map[string]*producerRuntimeScope{}, active: map[string]producerRuntimeWorker{}}
}

func (s *Server) platformProducerRuntime() *producerCaptureRuntime {
	s.producerCaptureMu.Lock()
	defer s.producerCaptureMu.Unlock()
	if s.producerCaptureRuntime == nil {
		s.producerCaptureRuntime = newProducerCaptureRuntime()
	}
	return s.producerCaptureRuntime
}

func (r *producerCaptureRuntime) start(c *producerRuntimeCalls) {
	if c.Started == math.MaxUint64 || c.Active == math.MaxUint64 {
		r.incomplete = true
		return
	}
	c.Started++
	c.Active++
}

func (r *producerCaptureRuntime) returned(c *producerRuntimeCalls) {
	if c.Returned == math.MaxUint64 || c.Active == 0 {
		r.incomplete = true
		return
	}
	c.Returned++
	c.Active--
}

func (r *producerCaptureRuntime) scope(key string) *producerRuntimeScope {
	if value := r.scopes[key]; value != nil {
		return value
	}
	valid := key == platformproducer.Scope
	if !valid && strings.HasPrefix(key, platformproducer.Scope+":") {
		canonical, err := platformproducer.PolicyScopeForTarget(platformconfig.AuthorityCellScope(strings.TrimPrefix(key, platformproducer.Scope+":")))
		valid = err == nil && canonical == key
	}
	// Overflow never stops business work or evicts an earlier scope to suggest
	// a complete roster. A subsequent zero remains explicitly incomplete.
	if !valid || len(key) > 256 || len(r.scopes) >= 32 {
		r.incomplete = true
		return nil
	}
	value := &producerRuntimeScope{Scope: key}
	r.scopes[key] = value
	return value
}

func (r *producerCaptureRuntime) manager() func() {
	r.mu.Lock()
	r.start(&r.managers)
	r.mu.Unlock()
	return sync.OnceFunc(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.returned(&r.managers)
	})
}

func (r *producerCaptureRuntime) worker(ctx context.Context, scope string) func() {
	r.mu.Lock()
	r.start(&r.workers)
	scoped := r.scope(scope)
	if scoped != nil {
		r.start(&scoped.Workers)
	}
	id := model.NewID("producer-worker")
	if len(r.active) < 128 && ctx != nil && scoped != nil {
		r.active[id] = producerRuntimeWorker{ID: id, Scope: scope, StartedAt: time.Now().UTC(), Context: ctx}
	} else {
		r.incomplete = true
	}
	r.mu.Unlock()
	return sync.OnceFunc(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.active, id)
		r.returned(&r.workers)
		if scoped != nil {
			r.returned(&scoped.Workers)
		}
	})
}

func (r *producerCaptureRuntime) input(scope string) func() {
	r.mu.Lock()
	r.start(&r.newInput)
	scoped := r.scope(scope)
	if scoped != nil {
		r.start(&scoped.NewInput)
	}
	r.mu.Unlock()
	return sync.OnceFunc(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.returned(&r.newInput)
		if scoped != nil {
			r.returned(&scoped.NewInput)
		}
	})
}

type producerRuntimeWorkerObservation struct {
	ID               string    `json:"id"`
	Scope            string    `json:"scope"`
	StartedAt        time.Time `json:"started_at"`
	CancellationSeen bool      `json:"context_canceled"`
}

type producerRuntimeParticipantObservation struct {
	ID            string   `json:"id"`
	Scopes        []string `json:"scopes"`
	LocalAttempts int      `json:"local_attempts"`
	Closing       bool     `json:"closing"`
	Closed        bool     `json:"closed"`
}

type producerRuntimeObservation struct {
	SchemaVersion         string                                 `json:"schema_version"`
	ObservedAt            time.Time                              `json:"observed_at"`
	BootID                string                                 `json:"boot_id"`
	StartedAt             time.Time                              `json:"started_at"`
	PID                   int                                    `json:"pid"`
	TrackingComplete      bool                                   `json:"tracking_complete"`
	ProducerStarted       bool                                   `json:"producer_started"`
	Managers              producerRuntimeCalls                   `json:"managers"`
	Workers               producerRuntimeCalls                   `json:"workers"`
	NewInput              producerRuntimeCalls                   `json:"new_input_boundary_calls"`
	Scopes                []producerRuntimeScope                 `json:"scopes"`
	ActiveWorkers         []producerRuntimeWorkerObservation     `json:"active_workers"`
	Participant           *producerRuntimeParticipantObservation `json:"participant,omitempty"`
	AdmissionInstalled    bool                                   `json:"admission_installed"`
	InputDeferralProtocol string                                 `json:"input_deferral_protocol"`
	FleetCoverageProven   bool                                   `json:"fleet_coverage_proven"`
}

// ProducerRuntimeObservation is exposed only through the existing private Unix
// provider. It performs no DB/network I/O or enrollment. External evidence must
// bind the socket process to exact Pod/container/owner identities and bracket
// fleet/old-process coverage; a local snapshot cannot certify its own fleet.
func (s *Server) ProducerRuntimeObservation(ctx context.Context) (any, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("producer runtime observation context unavailable")
	}
	// Private enrollment can perform I/O while holding this setup mutex. A
	// demand-driven observation refuses contention rather than awaiting it.
	if !s.producerCaptureMu.TryLock() {
		return nil, fmt.Errorf("producer runtime setup is busy")
	}
	defer s.producerCaptureMu.Unlock()
	r := s.producerCaptureRuntime
	if r == nil {
		return nil, fmt.Errorf("producer runtime tracking unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := producerRuntimeObservation{SchemaVersion: producerRuntimeObservationSchema, ObservedAt: time.Now().UTC(), BootID: r.bootID, StartedAt: r.startedAt, PID: os.Getpid(), TrackingComplete: !r.incomplete, ProducerStarted: s.producerCaptureStarted, Managers: r.managers, Workers: r.workers, NewInput: r.newInput, Scopes: []producerRuntimeScope{}, ActiveWorkers: []producerRuntimeWorkerObservation{}, AdmissionInstalled: s.producerCaptureAdmission != nil, InputDeferralProtocol: platformproducer.InputDeferralSchema}
	for _, scoped := range r.scopes {
		out.Scopes = append(out.Scopes, *scoped)
	}
	slices.SortFunc(out.Scopes, func(a, b producerRuntimeScope) int { return strings.Compare(a.Scope, b.Scope) })
	for _, worker := range r.active {
		out.ActiveWorkers = append(out.ActiveWorkers, producerRuntimeWorkerObservation{ID: worker.ID, Scope: worker.Scope, StartedAt: worker.StartedAt, CancellationSeen: worker.Context.Err() != nil})
	}
	slices.SortFunc(out.ActiveWorkers, func(a, b producerRuntimeWorkerObservation) int { return strings.Compare(a.ID, b.ID) })
	if p := s.producerCaptureParticipant; p != nil {
		p.mu.Lock()
		out.Participant = &producerRuntimeParticipantObservation{ID: p.identity.ID, Scopes: slices.Clone(p.identity.Scopes), LocalAttempts: p.active, Closing: p.closing, Closed: p.closed}
		p.mu.Unlock()
		if p.identity.ID != r.bootID {
			out.TrackingComplete = false
		}
	}
	return out, nil
}
