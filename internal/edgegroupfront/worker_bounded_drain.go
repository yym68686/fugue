package edgegroupfront

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	WorkerDrainSchema        = "edge-worker-bounded-drain/v1"
	WorkerDrainLimit         = 10 * time.Minute
	workerDrainQuiet         = 2 * time.Second
	workerDrainMaxWitnessAge = 30 * time.Second
)

// WorkerDrainGrant is written by a release authority only after observing
// every relevant ingress and a successful real connection to Serving.
// It is read from a dedicated read-only mount, never from an HTTP request.
// Reconcile time, connection age and Pod readiness are not cutover evidence.
type WorkerDrainGrant struct {
	Schema   string          `json:"schema"`
	FrontUID string          `json:"front_uid"`
	Node     string          `json:"node"`
	Previous ActivationState `json:"previous"`
	Serving  ActivationState `json:"serving"`
	Start    time.Time       `json:"start"`
	Deadline time.Time       `json:"deadline"`
	// HealthyAt is refreshed only after verifying the same serving identity
	// and ingress after cutover. A stale projection cannot force-close.
	HealthyAt time.Time `json:"healthy_at"`
}

func (g WorkerDrainGrant) validate(cfg Config) error {
	return g.ValidateFor(cfg.WorkerDrainFrontUID, cfg.EdgeID, cfg.EdgeGroupID)
}

// ValidateFor is shared by the authority writer and the Front projection
// consumer so they cannot disagree about the activation or deadline.
func (g WorkerDrainGrant) ValidateFor(frontUID, node, group string) error {
	if g.Schema != WorkerDrainSchema || !nodeWorkerUID.MatchString(g.FrontUID) || g.FrontUID != frontUID ||
		g.Node == "" || g.Node != node ||
		validateActivationState(g.Previous) != nil || validateActivationState(g.Serving) != nil ||
		g.Previous.GroupID != group || g.Serving.GroupID != group ||
		g.Previous.Generation == ^uint64(0) || g.Previous.Generation+1 != g.Serving.Generation ||
		g.Serving.Operation != ActivationOperationPromote || g.Serving.PreviousSlot != g.Previous.ActiveSlot ||
		g.Serving.ActiveSlot == g.Previous.ActiveSlot || g.Serving.WorkerImageDigest == g.Previous.WorkerImageDigest ||
		g.Serving.UpdatedAt.Before(g.Previous.UpdatedAt) || g.Start.Before(g.Serving.UpdatedAt) ||
		!g.Start.Add(WorkerDrainLimit).Equal(g.Deadline) || g.HealthyAt.Before(g.Start) {
		return errors.New("invalid or unrelated Edge Worker drain grant")
	}
	return nil
}

func (cfg Config) validateWorkerDrain() error {
	if cfg.WorkerDrainGrantPath == "" && cfg.WorkerDrainFrontUID == "" {
		return nil
	}
	if !cfg.RequireActivationState || cfg.WorkerTransport == "" || cfg.EdgeID == "" ||
		!nodeWorkerUID.MatchString(cfg.WorkerDrainFrontUID) || !filepath.IsAbs(cfg.WorkerDrainGrantPath) ||
		filepath.Clean(cfg.WorkerDrainGrantPath) != cfg.WorkerDrainGrantPath ||
		cfg.WorkerDrainGrantPath == cfg.ActiveSlotFile {
		return errors.New("bounded Worker drain requires a pinned Front, exact Worker transport and a dedicated grant file")
	}
	return nil
}

func readWorkerDrainGrant(cfg Config) (WorkerDrainGrant, error) {
	f, err := os.Open(cfg.WorkerDrainGrantPath)
	if err != nil {
		return WorkerDrainGrant{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 16<<10))
	if err != nil || len(raw) == 16<<10 {
		return WorkerDrainGrant{}, errors.New("unreadable or oversized Edge Worker drain grant")
	}
	var g WorkerDrainGrant
	if err := json.Unmarshal(raw, &g); err != nil {
		return g, err
	}
	canonical, err := json.Marshal(g)
	if err != nil || !bytes.Equal(canonical, bytes.TrimSpace(raw)) {
		return g, errors.New("non-canonical Edge Worker drain grant")
	}
	return g, g.validate(cfg)
}

func workerPin(a ActivationState) workerImagePin {
	return workerImagePin{a.ActiveSlot, a.WorkerSourceCommit, a.WorkerImageDigest}
}

type workerDrainConnection struct {
	connectionID string
	worker       workerImagePin
	generation   uint64
	downstream   net.Conn
	upstream     net.Conn
	cancel       context.CancelFunc
	stopping     bool
}

type workerDrainObservation struct {
	Grant      WorkerDrainGrant
	QuietSince time.Time
	Remaining  int
	Forced     int
	Outcome    string
}

// The same mutex covers selection + registration and deadline enforcement.
// A connection gets an entry before Dial, including during a concurrent CAS.
func (s *Service) selectDrainTracked(cfg Config, downstream net.Conn) (SlotTargets, *workerDrainConnection, context.Context, bool) {
	s.workerDrainMu.Lock()
	defer s.workerDrainMu.Unlock()
	_, a, targets, ok := s.selectedWorker(cfg)
	if !ok || a == nil {
		return SlotTargets{}, nil, nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &workerDrainConnection{worker: workerPin(*a), generation: a.Generation, downstream: downstream, cancel: cancel}
	if s.workerDrainConnections == nil {
		s.workerDrainConnections = make(map[*workerDrainConnection]struct{})
	}
	s.workerDrainConnections[c] = struct{}{}
	return targets, c, ctx, true
}

func (s *Service) attachDrainUpstream(c *workerDrainConnection, upstream net.Conn) bool {
	s.workerDrainMu.Lock()
	defer s.workerDrainMu.Unlock()
	if c.stopping {
		return false
	}
	c.upstream = upstream
	return true
}

func (s *Service) forgetDrainConnection(c *workerDrainConnection) {
	s.workerDrainMu.Lock()
	defer s.workerDrainMu.Unlock()
	delete(s.workerDrainConnections, c)
	c.cancel()
}

func (s *Service) runWorkerDrain(ctx context.Context, cfg Config) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastError := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := s.reconcileWorkerDrain(cfg, time.Now().UTC())
			if err == nil {
				lastError = ""
			} else if err.Error() != lastError {
				lastError = err.Error()
				s.Logger.Printf("worker drain withheld: %v", err)
			}
		}
	}
}

func (s *Service) reconcileWorkerDrain(cfg Config, now time.Time) error {
	var forcedIDs []string
	var forcedGeneration uint64
	defer func() {
		// Emit opaque connection IDs only, after releasing the activation and
		// connection locks. Do not expose client addresses via this receipt.
		for _, id := range forcedIDs {
			s.Logger.Printf("worker drain forced: generation=%d connection=%s", forcedGeneration, id)
		}
	}()
	// This also fences rollback performed by the external activation helper.
	// Do not wait on an occupied lock, especially during service shutdown.
	lock, err := acquireActivationLockMode(cfg.ActiveSlotFile, syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return err
	}
	defer lock.Close()
	g, err := readWorkerDrainGrant(cfg)
	if errors.Is(err, os.ErrNotExist) {
		return nil // absent grant is opt-out, including legacy nodes
	}
	if err != nil {
		return err
	}
	current, exists, err := ReadActivationState(cfg.ActiveSlotFile)
	if err != nil || !exists || current != g.Serving || now.Before(g.HealthyAt) {
		return errors.New("bounded drain no longer matches the live activation")
	}
	next, binding, nextOK := cfg.activationWorkerTargets(current)
	prior, priorOK := cfg.workerTargets[workerPin(g.Previous)]
	if cfg.fixedWorkerRotation {
		// The two endpoints must come from the same journal as the actual
		// serving pointer, not from an old pin for a recycled fixed port.
		prior, priorOK = cfg.Slots[g.Previous.ActiveSlot]
		priorOK = priorOK && binding.Digest != "" && binding.Expected == g.Previous
	}
	if !priorOK || !nextOK || strings.TrimSpace(prior.HTTPSAddress) == "" || prior == next {
		return errors.New("bounded drain targets are not distinct pinned transports")
	}

	s.workerDrainMu.Lock()
	defer s.workerDrainMu.Unlock()
	o := &s.workerDrainObservation
	old := o.Grant
	old.HealthyAt = g.HealthyAt
	if old != g {
		if o.Grant.Serving == g.Serving && !o.Grant.Start.IsZero() {
			return errors.New("bounded drain grant changed for the same activation")
		}
		*o = workerDrainObservation{Grant: g}
	}
	if g.HealthyAt.Before(o.Grant.HealthyAt) {
		return errors.New("bounded drain health witness regressed")
	}
	o.Grant.HealthyAt = g.HealthyAt
	if o.Outcome != "" {
		return nil
	}

	o.Remaining = 0
	for c := range s.workerDrainConnections {
		if c.worker != workerPin(g.Previous) || c.generation > g.Previous.Generation {
			continue
		}
		o.Remaining++
		if !now.Before(g.Deadline) && now.Sub(g.HealthyAt) <= workerDrainMaxWitnessAge && !c.stopping {
			c.stopping = true
			c.cancel()
			_ = c.downstream.Close()
			if c.upstream != nil {
				_ = c.upstream.Close()
			}
			o.Forced++
			if c.connectionID != "" {
				forcedGeneration = g.Serving.Generation
				forcedIDs = append(forcedIDs, c.connectionID)
			}
		}
	}
	if o.Remaining != 0 {
		o.QuietSince = time.Time{}
		return nil
	}
	if o.QuietSince.IsZero() {
		o.QuietSince = now
		return nil
	}
	if now.Sub(o.QuietSince) >= workerDrainQuiet {
		o.Outcome = "drained_naturally"
		if o.Forced != 0 {
			o.Outcome = "closed_at_deadline"
		}
		s.Logger.Printf("worker drain finished: generation=%d outcome=%s forced=%d", g.Serving.Generation, o.Outcome, o.Forced)
	}
	return nil
}

// These are local observations only, not authority to retire a Worker.
// In particular, zero from this Front says nothing about other Fronts.
func (s *Service) writeWorkerDrainMetrics(w io.Writer) {
	s.workerDrainMu.Lock()
	o := s.workerDrainObservation
	s.workerDrainMu.Unlock()
	fmt.Fprintln(w, "# TYPE fugue_edge_front_worker_drain_capability gauge")
	fmt.Fprintln(w, "fugue_edge_front_worker_drain_capability 1")
	if o.Grant.Start.IsZero() {
		return
	}
	fmt.Fprintln(w, "# TYPE fugue_edge_front_worker_drain_last_generation gauge")
	fmt.Fprintf(w, "fugue_edge_front_worker_drain_last_generation %d\n", o.Grant.Serving.Generation)
	fmt.Fprintln(w, "# TYPE fugue_edge_front_worker_drain_last_deadline_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_front_worker_drain_last_deadline_seconds %d\n", o.Grant.Deadline.Unix())
	fmt.Fprintln(w, "# TYPE fugue_edge_front_worker_drain_last_remaining gauge")
	fmt.Fprintf(w, "fugue_edge_front_worker_drain_last_remaining %d\n", o.Remaining)
	fmt.Fprintln(w, "# TYPE fugue_edge_front_worker_drain_last_forced gauge")
	fmt.Fprintf(w, "fugue_edge_front_worker_drain_last_forced %d\n", o.Forced)
	fmt.Fprintln(w, "# TYPE fugue_edge_front_worker_drain_last_completed gauge")
	complete := 0
	if o.Outcome != "" {
		complete = 1
	}
	fmt.Fprintf(w, "fugue_edge_front_worker_drain_last_completed %d\n", complete)
}
