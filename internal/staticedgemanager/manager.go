package staticedgemanager

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	c "fugue/internal/staticedgecontract"
)

type Config struct {
	Role             string
	EdgeID           string
	StateDir         string
	VerificationKeys map[string]ed25519.PublicKey
	Runtime          Runtime
	Credentials      Credentials
}
type Credentials interface {
	Validate(string) error
	Select(string) error
	Status() (string, string)
}
type recorded struct {
	Hash     string     `json:"hash"`
	Response c.Response `json:"response"`
}
type pending struct {
	Request      c.Request       `json:"request"`
	Actor        string          `json:"actor"`
	Hash         string          `json:"hash"`
	Before       json.RawMessage `json:"before"`
	PreviousSlot string          `json:"previous_slot,omitempty"`
}
type state struct {
	Revision       uint64     `json:"revision"`
	MaxGeneration  uint64     `json:"max_generation"`
	Active         *c.Bundle  `json:"active,omitempty"`
	Candidate      *c.Bundle  `json:"candidate,omitempty"`
	LKG            *c.Bundle  `json:"lkg,omitempty"`
	Pending        *pending   `json:"pending,omitempty"`
	CredentialSlot string     `json:"credential_slot,omitempty"`
	Records        []recorded `json:"records,omitempty"`
}
type Manager struct {
	cfg      Config
	mu       sync.Mutex
	st       state
	lock     *os.File
	poisoned bool
}

func New(cfg Config) (*Manager, error) {
	if !c.ValidID(cfg.EdgeID) || (cfg.Role != "edge" && cfg.Role != "origin") || !filepath.IsAbs(cfg.StateDir) || cfg.Runtime == nil || len(cfg.VerificationKeys) == 0 {
		return nil, errors.New("manager requires identity, role, absolute state_dir, runtime and verification keys")
	}
	for id, k := range cfg.VerificationKeys {
		if !c.ValidID(id) || len(k) != ed25519.PublicKeySize {
			return nil, errors.New("invalid verification key")
		}
	}
	if e := os.MkdirAll(cfg.StateDir, 0700); e != nil {
		return nil, e
	}
	lock, e := lockState(filepath.Join(cfg.StateDir, "manager.lock"))
	if e != nil {
		return nil, e
	}
	m := &Manager{cfg: cfg, lock: lock}
	raw, e := os.ReadFile(m.path())
	if e != nil && !os.IsNotExist(e) {
		m.Close()
		return nil, e
	}
	if e == nil {
		if e = c.StrictJSON(raw, &m.st); e != nil {
			m.Close()
			return nil, fmt.Errorf("state corrupt; refusing reset: %w", e)
		}
	}
	for _, b := range []*c.Bundle{m.st.Active, m.st.Candidate, m.st.LKG} {
		if b != nil {
			if e = m.verify(*b); e != nil {
				m.Close()
				return nil, fmt.Errorf("persisted bundle invalid: %w", e)
			}
		}
	}
	if cfg.Credentials != nil && m.st.CredentialSlot != "" {
		if e = cfg.Credentials.Select(m.st.CredentialSlot); e != nil {
			m.Close()
			return nil, e
		}
	}
	return m, nil
}
func (m *Manager) Close() error {
	if m.lock != nil {
		return m.lock.Close()
	}
	return nil
}
func (m *Manager) path() string { return filepath.Join(m.cfg.StateDir, "state.json") }
func (m *Manager) save() error {
	raw, e := json.Marshal(m.st)
	if e != nil {
		return e
	}
	if len(raw) > c.MaxBytes {
		return errors.New("state size limit; refusing unsafe growth")
	}
	return atomicBytes(m.path(), raw, 0600)
}
func (m *Manager) verify(b c.Bundle) error {
	if b.EdgeID != m.cfg.EdgeID || b.Role != m.cfg.Role {
		return errors.New("bundle target identity mismatch")
	}
	return c.VerifyBundle(b, m.cfg.VerificationKeys[b.SigningKeyID])
}
func (m *Manager) base(req c.Request) c.Response {
	return c.Response{Schema: c.RPCSchema, RequestID: req.RequestID, EdgeID: m.cfg.EdgeID, OK: true, Status: 200}
}
func (m *Manager) fail(req c.Request, status int, e error) c.Response {
	r := m.base(req)
	r.OK = false
	r.Status = status
	r.Error = e.Error()
	return r
}
func (m *Manager) observed(ctx context.Context) c.Observed {
	s := m.st
	o := c.Observed{EdgeID: m.cfg.EdgeID, Role: m.cfg.Role, Revision: s.Revision}
	if s.Candidate != nil {
		o.CandidateDigest = s.Candidate.BundleDigest
		o.CandidateGeneration = s.Candidate.Generation
	}
	if s.LKG != nil {
		o.LKGDigest = s.LKG.BundleDigest
		o.LKGGeneration = s.LKG.Generation
	}
	if s.Pending != nil {
		o.PendingRequestID = s.Pending.Request.RequestID
	}
	if m.cfg.Credentials != nil {
		o.CredentialSlot, o.CertificateNotAfter = m.cfg.Credentials.Status()
	}
	if s.Active != nil {
		o.ActiveGeneration = s.Active.Generation
		o.ActiveDigest = s.Active.BundleDigest
		o.Draining = s.Active.Mode == "draining"
	}
	raw, e := m.cfg.Runtime.Snapshot(ctx)
	if e != nil {
		o.LastError = e.Error()
		return o
	}
	o.RuntimeDigest, e = c.ConfigDigest(raw)
	if e != nil {
		o.LastError = e.Error()
		return o
	}
	if s.Active != nil {
		b := s.Active
		o.ActiveGeneration = b.Generation
		o.ActiveDigest = b.BundleDigest
		o.Draining = b.Mode == "draining"
		d, _ := c.ConfigDigest(b.CaddyConfig)
		o.RuntimeMatches = d == o.RuntimeDigest
		if o.RuntimeMatches {
			e = m.verifyStartup(ctx, *b)
			if e == nil {
				e = m.cfg.Runtime.Probe(ctx, b.HealthChecks)
			}
			o.Ready = e == nil && s.Pending == nil
			if e != nil {
				o.LastError = e.Error()
			}
		} else {
			o.LastError = "runtime config differs from managed active"
		}
	}
	return o
}

// Execute uses its own bounded transaction context after acceptance. Dropping an
// SSH/mTLS client does not cancel a half-applied configuration transaction.
func (m *Manager) Execute(req c.Request, actor, grant string) c.Response {
	if !m.mu.TryLock() {
		return m.fail(req, 409, errors.New("manager transaction in progress; query operation after it completes"))
	}
	defer m.mu.Unlock()
	if req.Schema != c.RPCSchema || req.EdgeID != m.cfg.EdgeID || !c.ValidID(req.RequestID) || !c.OperationAllowed(req.Operation) {
		return m.fail(req, 400, errors.New("invalid protocol, identity, request id or operation"))
	}
	if grant != "read" && grant != "operator" && grant != "admin" {
		return m.fail(req, 403, errors.New("unrecognized local grant"))
	}
	if !c.ReadOnly(req.Operation) && grant == "read" {
		return m.fail(req, 403, errors.New("write grant required"))
	}
	if (req.Operation == "adopt" || req.Operation == "cert-rotate" || req.Operation == "recover") && grant != "admin" {
		return m.fail(req, 403, errors.New("admin grant required"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	hashraw, _ := json.Marshal(req)
	hash := c.Hash(hashraw)
	if !c.ReadOnly(req.Operation) {
		if m.poisoned {
			return m.fail(req, 409, errors.New("state persistence failed; restart manager to inspect durable journal before any write"))
		}
		for _, record := range m.st.Records {
			if record.Response.RequestID == req.RequestID {
				if record.Hash != hash || record.Response.Receipt == nil || record.Response.Receipt.Actor != actor {
					return m.fail(req, 409, errors.New("request id already used by different request or actor"))
				}
				return record.Response
			}
		}
		if req.ExpectedRevision == nil || *req.ExpectedRevision != m.st.Revision {
			return m.fail(req, 409, errors.New("expected_revision does not match; inspect state and re-plan"))
		}
		if m.st.Pending != nil && req.Operation != "recover" {
			return m.fail(req, 409, errors.New("interrupted operation requires explicit recover"))
		}
	}
	switch req.Operation {
	case "status", "health", "cert-status":
		out := m.base(req)
		obs := m.observed(ctx)
		out.Result = &obs
		if req.Operation == "health" && !obs.Ready {
			out.OK = false
			out.Status = 503
			out.Error = "runtime health unverified"
		}
		return out
	case "operation", "evidence":
		out := m.base(req)
		if req.Operation == "operation" {
			for _, x := range m.st.Records {
				if x.Response.RequestID == req.LookupRequestID {
					out.Receipt = x.Response.Receipt
					return out
				}
			}
			return m.fail(req, 404, errors.New("receipt unavailable; check pending_request_id and current revision"))
		}
		for _, x := range m.st.Records {
			if x.Response.Receipt != nil {
				out.Receipts = append(out.Receipts, *x.Response.Receipt)
			}
		}
		obs := m.observed(ctx)
		out.Result = &obs
		return out
	case "plan", "stage", "adopt":
		if req.Bundle == nil {
			return m.fail(req, 400, errors.New("bundle required"))
		}
		b := *req.Bundle
		if e := m.verify(b); e != nil {
			return m.fail(req, 403, e)
		}
		if e := m.cfg.Runtime.Validate(ctx, b); e != nil {
			return m.fail(req, 400, e)
		}
		if req.Operation == "plan" {
			out := m.base(req)
			obs := m.observed(ctx)
			out.Result = &obs
			return out
		}
		if b.Generation <= m.st.MaxGeneration {
			return m.fail(req, 409, errors.New("generation must exceed every previously staged generation"))
		}
		if req.Operation == "adopt" {
			if m.st.Active != nil {
				return m.fail(req, 409, errors.New("adoption requires unmanaged state"))
			}
			if e := m.verifyRuntime(ctx, b); e != nil {
				return m.fail(req, 409, e)
			}
			if e := m.verifyStartup(ctx, b); e != nil {
				return m.fail(req, 409, e)
			}
			m.st.Active = &b
			m.st.LKG = &b
		} else {
			m.st.Candidate = &b
		}
		m.st.MaxGeneration = b.Generation
		outcome := "staged"
		if req.Operation == "adopt" {
			outcome = "adopted"
		}
		return m.complete(req, actor, hash, outcome, b.BundleDigest, req.Operation == "adopt", nil)
	case "activate", "drain", "undrain", "rollback":
		if m.st.Active == nil {
			return m.fail(req, 409, errors.New("adopt existing healthy runtime before first activation"))
		}
		b := m.st.Candidate
		if req.Operation == "rollback" {
			b = m.st.LKG
		}
		if b == nil || req.TargetDigest == "" || req.TargetDigest != b.BundleDigest {
			return m.fail(req, 409, errors.New("exact target_digest required for staged candidate or LKG"))
		}
		if (req.Operation == "drain" && b.Mode != "draining") || (req.Operation == "undrain" && b.Mode != "serving") {
			return m.fail(req, 400, errors.New("operation requires candidate with matching mode"))
		}
		if e := m.verify(*b); e != nil {
			return m.fail(req, 403, e)
		}
		if e := m.cfg.Runtime.Validate(ctx, *b); e != nil {
			return m.fail(req, 400, e)
		}
		raw, e := m.cfg.Runtime.Snapshot(ctx)
		if e != nil {
			return m.fail(req, 503, e)
		}
		d, _ := c.ConfigDigest(raw)
		want, _ := c.ConfigDigest(m.st.Active.CaddyConfig)
		if d != want {
			return m.fail(req, 409, errors.New("runtime drift; refusing to overwrite unowned changes"))
		}
		if e = m.verifyStartup(ctx, *m.st.Active); e != nil {
			return m.fail(req, 409, e)
		}
		if e = m.cfg.Runtime.Probe(ctx, b.HealthChecks); e != nil {
			return m.fail(req, 503, fmt.Errorf("preflight: %w", e))
		}
		m.st.Pending = &pending{Request: req, Actor: actor, Hash: hash, Before: raw}
		if e = m.save(); e != nil {
			m.st.Pending = nil
			return m.fail(req, 500, e)
		}
		if e = m.cfg.Runtime.Apply(ctx, b.CaddyConfig); e == nil {
			e = m.verifyRuntime(ctx, *b)
		}
		if e == nil {
			e = m.cfg.Runtime.Persist(ctx, b.CaddyConfig)
		}
		if e != nil {
			return m.restore(ctx, req, actor, hash, e)
		}
		old := m.st.Active
		m.st.Active = b
		m.st.LKG = old
		m.st.Candidate = nil
		m.st.Pending = nil
		outcome := map[string]string{"activate": "activated", "drain": "drained", "undrain": "undrained", "rollback": "rolled_back"}[req.Operation]
		return m.complete(req, actor, hash, outcome, b.BundleDigest, true, nil)
	case "recover":
		if m.st.Pending == nil {
			return m.fail(req, 409, errors.New("no interrupted operation"))
		}
		p := m.st.Pending
		out := m.restore(ctx, p.Request, p.Actor, p.Hash, errors.New("explicit recovery of interrupted operation"))
		if out.Receipt == nil || out.Receipt.Outcome != "failed_restored" {
			return out
		}
		return m.complete(req, actor, hash, "recovered", digest(m.st.Active), true, nil)
	case "cert-rotate":
		if m.cfg.Credentials == nil {
			return m.fail(req, 409, errors.New("management credential rotation is not configured"))
		}
		if e := m.cfg.Credentials.Validate(req.CredentialSlot); e != nil {
			return m.fail(req, 400, e)
		}
		prev, _ := m.cfg.Credentials.Status()
		m.st.Pending = &pending{Request: req, Actor: actor, Hash: hash, PreviousSlot: prev}
		if e := m.save(); e != nil {
			m.st.Pending = nil
			return m.fail(req, 500, e)
		}
		if e := m.cfg.Credentials.Select(req.CredentialSlot); e != nil {
			return m.restore(ctx, req, actor, hash, e)
		}
		m.st.CredentialSlot = req.CredentialSlot
		m.st.Pending = nil
		return m.complete(req, actor, hash, "credentials_rotated", digest(m.st.Active), false, nil)
	}
	return m.fail(req, 400, errors.New("unsupported operation"))
}
func digest(b *c.Bundle) string {
	if b != nil {
		return b.BundleDigest
	}
	return ""
}
func (m *Manager) verifyStartup(ctx context.Context, b c.Bundle) error {
	raw, e := m.cfg.Runtime.Startup(ctx)
	if e != nil {
		return e
	}
	d, e := c.ConfigDigest(raw)
	if e != nil {
		return e
	}
	want, _ := c.ConfigDigest(b.CaddyConfig)
	if d != want {
		return errors.New("startup configuration differs from managed active")
	}
	return nil
}

func (m *Manager) verifyRuntime(ctx context.Context, b c.Bundle) error {
	raw, e := m.cfg.Runtime.Snapshot(ctx)
	if e != nil {
		return e
	}
	d, e := c.ConfigDigest(raw)
	if e != nil {
		return e
	}
	want, _ := c.ConfigDigest(b.CaddyConfig)
	if d != want {
		return errors.New("Caddy loaded config does not match candidate")
	}
	return m.cfg.Runtime.Probe(ctx, b.HealthChecks)
}
func (m *Manager) restore(ctx context.Context, req c.Request, actor, hash string, cause error) c.Response {
	// Recovery gets its own budget even if candidate verification exhausted its
	// deadline. The request may return an uncertain result; the journal survives.
	recovery, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = recovery
	p := m.st.Pending
	if p == nil {
		return m.fail(req, 409, errors.New("pending recovery state missing"))
	}
	var e error
	if p.PreviousSlot != "" {
		e = m.cfg.Credentials.Select(p.PreviousSlot)
	} else {
		e = m.cfg.Runtime.Apply(ctx, p.Before)
		if e == nil && m.st.Active != nil {
			e = m.verifyRuntime(ctx, *m.st.Active)
		}
		if e == nil {
			e = m.cfg.Runtime.Persist(ctx, p.Before)
		}
	}
	if e != nil {
		out := m.fail(req, 503, fmt.Errorf("operation failed; recovery required: %v; %w", cause, e))
		out.Receipt = &c.Receipt{RequestID: req.RequestID, Operation: req.Operation, Actor: actor, Outcome: "recovery_required", Revision: m.st.Revision, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		return out
	}
	m.st.Pending = nil
	return m.complete(req, actor, hash, "failed_restored", digest(m.st.Active), true, cause)
}
func (m *Manager) complete(req c.Request, actor, hash, outcome, bundle string, verified bool, cause error) c.Response {
	m.st.Revision++
	out := m.base(req)
	out.Receipt = &c.Receipt{RequestID: req.RequestID, Operation: req.Operation, Actor: actor, Outcome: outcome, Revision: m.st.Revision, BundleDigest: bundle, RuntimeVerified: verified, LKGPreserved: m.st.LKG != nil, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if cause != nil {
		out.OK = false
		out.Status = 503
		out.Error = cause.Error()
		out.Receipt.Error = out.Error
	}
	m.st.Records = append(m.st.Records, recorded{Hash: hash, Response: out})
	if len(m.st.Records) > 256 {
		m.st.Records = m.st.Records[len(m.st.Records)-256:]
	}
	if e := m.save(); e != nil {
		m.poisoned = true
		// Reload the durable journal so any future mutation sees an interrupted
		// transition rather than inventing a committed result.
		raw, re := os.ReadFile(m.path())
		if re == nil {
			var durable state
			if c.StrictJSON(raw, &durable) == nil {
				m.st = durable
			}
		}
		return m.fail(req, 500, fmt.Errorf("commit uncertain; query state and recover before retry: %w", e))
	}
	return out
}
