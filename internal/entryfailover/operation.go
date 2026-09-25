package entryfailover

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	c "fugue/internal/staticedgecontract"
)

var operationID = regexp.MustCompile(`^efe_[0-9a-f]{32}$`)

type DNSWriter interface {
	Snapshot(context.Context, []string) (map[string]Record, error)
	Batch(context.Context, map[string]Record, Target) error
}

type Operation struct {
	Schema       string            `json:"schema"`
	ID           string            `json:"id"`
	PolicyDigest string            `json:"policy_digest"`
	From         string            `json:"from"`
	To           string            `json:"to"`
	Before       map[string]Record `json:"before"`
	Phase        string            `json:"phase"`
	StartedAt    time.Time         `json:"started_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	Error        string            `json:"error,omitempty"`
}

type Executor struct {
	Policy   Policy
	DNS      DNSWriter
	StateDir string
	// VerifyTarget must prove the target's complete business checks. It is
	// called before every write, including explicit manual switches.
	VerifyTarget func(context.Context, Target) error
}

func (e *Executor) validate() error {
	if err := e.Policy.Validate(); err != nil {
		return err
	}
	if e.DNS == nil || e.VerifyTarget == nil || !filepath.IsAbs(e.StateDir) {
		return errors.New("DNS writer, target verifier and absolute state directory required")
	}
	return nil
}

func (e *Executor) target(id string) (Target, error) {
	for _, t := range e.Policy.Targets {
		if t.ID == id {
			return t, nil
		}
	}
	return Target{}, fmt.Errorf("target %q is outside signed policy", id)
}

func (e *Executor) current(records map[string]Record) (Target, error) {
	var selected *Target
	for _, h := range e.Policy.Hostnames {
		r, ok := records[h]
		if !ok || r.Name != h || r.Proxied {
			return Target{}, fmt.Errorf("missing or proxied DNS record for %s", h)
		}
		var found *Target
		for _, t := range e.Policy.Targets {
			if recordMatches(r, targetRecord(r, t)) {
				copy := t
				found = &copy
				break
			}
		}
		if found == nil {
			return Target{}, fmt.Errorf("%s points outside the signed target list", h)
		}
		if selected != nil && selected.ID != found.ID {
			return Target{}, errors.New("business hostnames point to different targets")
		}
		selected = found
	}
	if selected == nil {
		return Target{}, errors.New("no current target")
	}
	return *selected, nil
}

func (e *Executor) snapshot(ctx context.Context) (map[string]Record, Target, error) {
	records, err := e.DNS.Snapshot(ctx, e.Policy.Hostnames)
	if err != nil {
		return nil, Target{}, err
	}
	current, err := e.current(records)
	if err == nil {
		baseline := make(map[string]Record, len(e.Policy.DNSBaseline))
		for _, r := range e.Policy.DNSBaseline {
			baseline[r.Name] = r
		}
		for _, h := range e.Policy.Hostnames {
			if !recordBindingMatches(records[h], baseline[h]) || !allowedSettings(records[h], baseline[h], e.Policy.Zone) {
				return nil, Target{}, fmt.Errorf("%s DNS record ID or protected attributes differ from signed baseline", h)
			}
		}
	}
	return records, current, err
}

func (e *Executor) Plan(ctx context.Context, to string) (Target, Target, error) {
	if err := e.validate(); err != nil {
		return Target{}, Target{}, err
	}
	if !e.Policy.ExpiresAt.After(time.Now().UTC()) {
		return Target{}, Target{}, errors.New("signed policy expired; serving DNS remains unchanged")
	}
	goal, err := e.target(to)
	if err != nil {
		return Target{}, Target{}, err
	}
	_, current, err := e.snapshot(ctx)
	if err != nil {
		return Target{}, Target{}, err
	}
	if err = e.VerifyTarget(ctx, goal); err != nil {
		return current, goal, err
	}
	return current, goal, nil
}

func (e *Executor) opPath(id string) string {
	return filepath.Join(e.StateDir, "operations", id+".json")
}
func (e *Executor) activePath() string { return filepath.Join(e.StateDir, "active.json") }

func (e *Executor) save(op *Operation) error {
	op.UpdatedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(op, "", "  ")
	if err != nil {
		return err
	}
	return writeDurable(e.opPath(op.ID), append(raw, '\n'))
}

func (e *Executor) loadActive() (*Operation, error) {
	raw, err := os.ReadFile(e.activePath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var pointer struct {
		ID string `json:"id"`
	}
	if err = c.StrictJSON(raw, &pointer); err != nil {
		return nil, err
	}
	if !operationID.MatchString(pointer.ID) {
		return nil, errors.New("invalid active operation ID")
	}
	raw, err = os.ReadFile(e.opPath(pointer.ID))
	if err != nil {
		return nil, err
	}
	var op Operation
	if err = c.StrictJSON(raw, &op); err != nil {
		return nil, err
	}
	if op.Schema != PolicySchema || op.ID != pointer.ID {
		return nil, errors.New("active operation identity or policy differs")
	}
	if op.PolicyDigest != e.Policy.Digest() && op.Phase == "completed" {
		return nil, nil
	}
	if op.PolicyDigest != e.Policy.Digest() || len(op.Before) != len(e.Policy.Hostnames) {
		return nil, errors.New("active operation policy differs and is unresolved")
	}
	for _, h := range e.Policy.Hostnames {
		if op.Before[h].Name != h || op.Before[h].ID == "" {
			return nil, errors.New("active operation snapshot invalid")
		}
	}
	if _, err = e.target(op.From); err != nil {
		return nil, err
	}
	if _, err = e.target(op.To); err != nil {
		return nil, err
	}
	return &op, nil
}

func (e *Executor) ActiveOperation(ctx context.Context) (*Operation, string, error) {
	if err := e.validate(); err != nil {
		return nil, "", err
	}
	op, err := e.loadActive()
	if err != nil || op == nil {
		return op, "", err
	}
	phase, probeErr := e.observedPhase(ctx, op)
	if probeErr != nil {
		return op, "unknown", probeErr
	}
	return op, phase, nil
}

func (e *Executor) observedPhase(ctx context.Context, op *Operation) (string, error) {
	actual, err := e.DNS.Snapshot(ctx, e.Policy.Hostnames)
	if err != nil {
		return "unknown", err
	}
	to, err := e.target(op.To)
	if err != nil {
		return "unknown", err
	}
	before, after := true, true
	for _, h := range e.Policy.Hostnames {
		baseline := Record{}
		for _, r := range e.Policy.DNSBaseline {
			if r.Name == h {
				baseline = r
				break
			}
		}
		if !recordBindingMatches(actual[h], baseline) || !allowedSettings(actual[h], baseline, e.Policy.Zone) {
			return "unknown", fmt.Errorf("%s DNS record ID or attributes differ from signed baseline", h)
		}
		if !recordMatches(actual[h], op.Before[h]) {
			before = false
		}
		if !recordBindingMatches(actual[h], op.Before[h]) || actual[h].Type != targetRecord(op.Before[h], to).Type || actual[h].Content != to.Address || !allowedSettings(actual[h], op.Before[h], e.Policy.Zone) {
			after = false
		}
	}
	if after {
		return "completed", nil
	}
	if before {
		return "before", nil
	}
	return "unknown", errors.New("DNS result is mixed or differs from the saved operation")
}

func (e *Executor) reconcile(ctx context.Context, op *Operation) error {
	if op.Phase == "completed" {
		return nil
	}
	phase, err := e.observedPhase(ctx, op)
	if err != nil {
		op.Phase, op.Error = "indeterminate", err.Error()
		_ = e.save(op)
		return err
	}
	if phase == "completed" {
		op.Phase, op.Error = "completed", ""
		return e.save(op)
	}
	// A timed-out HTTP request might still commit after the readback. Never
	// infer a settled failure from a single observation of the old values.
	op.Phase, op.Error = "indeterminate", "DNS remained at the saved source; earlier write outcome cannot be fenced"
	if err := e.save(op); err != nil {
		return err
	}
	return errors.New(op.Error)
}

// Switch is the only DNS mutation entrypoint. A previous unknown outcome
// quarantines the writer until it can be proved completed or manually resolved.
func (e *Executor) Switch(ctx context.Context, to string, automatic bool) (Operation, error) {
	if err := e.validate(); err != nil {
		return Operation{}, err
	}
	if !e.Policy.ExpiresAt.After(time.Now().UTC()) {
		return Operation{}, errors.New("signed policy expired; serving DNS remains unchanged")
	}
	if automatic && e.Policy.Mode != "automatic" {
		return Operation{}, errors.New("automatic DNS writes are disabled by signed policy")
	}
	goal, err := e.target(to)
	if err != nil {
		return Operation{}, err
	}
	unlock, err := lockState(e.StateDir)
	if err != nil {
		return Operation{}, err
	}
	defer unlock()
	previous, err := e.loadActive()
	if err != nil {
		return Operation{}, err
	}
	if previous != nil && previous.Phase != "completed" {
		if err = e.reconcile(ctx, previous); err != nil {
			return *previous, err
		}
	}
	before, from, err := e.snapshot(ctx)
	if err != nil {
		return Operation{}, err
	}
	if from.ID == goal.ID {
		return Operation{Schema: PolicySchema, PolicyDigest: e.Policy.Digest(), From: from.ID, To: goal.ID, Phase: "already_selected"}, nil
	}
	if err = e.VerifyTarget(ctx, goal); err != nil {
		return Operation{}, err
	}
	idRaw := make([]byte, 16)
	if _, err = rand.Read(idRaw); err != nil {
		return Operation{}, err
	}
	op := Operation{Schema: PolicySchema, ID: "efe_" + hex.EncodeToString(idRaw), PolicyDigest: e.Policy.Digest(), From: from.ID, To: goal.ID, Before: before, Phase: "prepared", StartedAt: time.Now().UTC()}
	if err = e.save(&op); err != nil {
		return op, err
	}
	pointer, _ := json.Marshal(map[string]string{"id": op.ID})
	if err = writeDurable(e.activePath(), append(pointer, '\n')); err != nil {
		return op, err
	}
	op.Phase = "write_pending"
	if err = e.save(&op); err != nil {
		return op, err
	}
	writeErr := e.DNS.Batch(ctx, before, goal)
	reconcileErr := e.reconcile(ctx, &op)
	if reconcileErr != nil && writeErr != nil {
		return op, fmt.Errorf("DNS write: %v; readback: %w", writeErr, reconcileErr)
	}
	if reconcileErr != nil {
		return op, reconcileErr
	}
	return op, nil
}
