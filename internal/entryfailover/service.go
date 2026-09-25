package entryfailover

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	sc "fugue/internal/staticedgecontract"
)

type Service struct {
	mu           sync.Mutex
	BuildVersion string
	policyPath   string
	keys         map[string]ed25519.PublicKey
	signed       SignedPolicy
	vault        CredentialVault
	executor     *Executor
	engine       *Engine
	last         Decision
	lastError    string
	cloudflare   string
}

type credentialDNS struct{ service *Service }

func (d credentialDNS) client() (*Cloudflare, error) {
	s := d.service
	token, err := s.vault.Token(s.signed.Policy)
	if err != nil {
		return nil, errors.New("Cloudflare credential unavailable")
	}
	return NewCloudflare(s.cloudflare, s.signed.Policy.ZoneID, token, 15*time.Second)
}
func (d credentialDNS) Snapshot(ctx context.Context, hosts []string) (map[string]Record, error) {
	client, err := d.client()
	if err != nil {
		return nil, err
	}
	return client.Snapshot(ctx, hosts)
}
func (d credentialDNS) Batch(ctx context.Context, before map[string]Record, to Target) error {
	client, err := d.client()
	if err != nil {
		return err
	}
	return client.Batch(ctx, before, to)
}

func NewService(policyPath string, keys map[string]ed25519.PublicKey, vault CredentialVault, cloudflareBase string) (*Service, error) {
	if !filepath.IsAbs(policyPath) || !filepath.IsAbs(vault.StateDir) {
		return nil, errors.New("absolute policy and state paths required")
	}
	lkgPath := filepath.Join(vault.StateDir, "policy-lkg.json")
	raw, err := os.ReadFile(policyPath)
	var signed SignedPolicy
	if err == nil {
		signed, err = ParseSignedPolicy(raw, keys)
	}
	if err != nil {
		lkgRaw, lkgErr := os.ReadFile(lkgPath)
		if lkgErr != nil {
			return nil, fmt.Errorf("current policy invalid and no LKG: %w", err)
		}
		signed, lkgErr = ParseSignedPolicy(lkgRaw, keys)
		if lkgErr != nil {
			return nil, fmt.Errorf("current policy invalid and LKG invalid: %w", lkgErr)
		}
	} else {
		lkgRaw, lkgErr := os.ReadFile(lkgPath)
		var prior SignedPolicy
		if lkgErr == nil {
			prior, lkgErr = ParseSignedPolicy(lkgRaw, keys)
		}
		if lkgErr != nil || prior.Digest != signed.Digest {
			if lkgErr = writeDurable(lkgPath, append(raw, '\n')); lkgErr != nil {
				return nil, lkgErr
			}
		}
	}
	s := &Service{policyPath: policyPath, keys: keys, signed: signed, vault: vault, cloudflare: cloudflareBase}
	s.executor = &Executor{Policy: signed.Policy, DNS: credentialDNS{service: s}, StateDir: vault.StateDir,
		VerifyTarget: func(ctx context.Context, target Target) error {
			result := (Prober{}).Probe(ctx, s.signed.Policy, target)
			if !result.Healthy {
				return fmt.Errorf("target %s lacks complete local business-route proof", target.ID)
			}
			return nil
		}}
	s.engine = &Engine{Executor: s.executor, Collector: SSHCollector{SignedPolicy: signed}}
	return s, nil
}

func (s *Service) Cycle(ctx context.Context) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.signed.Policy.ExpiresAt.After(time.Now().UTC()) {
		return Decision{}, errors.New("signed policy expired; no new DNS writes")
	}
	decision, err := s.engine.Cycle(ctx)
	s.last = decision
	if err != nil {
		s.lastError = err.Error()
	} else {
		s.lastError = ""
	}
	if raw, marshalErr := json.Marshal(decision); marshalErr == nil {
		_ = writeDurable(filepath.Join(s.vault.StateDir, "latest-decision.json"), append(raw, '\n'))
	}
	return decision, err
}

func (s *Service) Run(ctx context.Context, onError func(error)) {
	for {
		started := time.Now()
		if _, err := s.Cycle(ctx); err != nil && onError != nil {
			onError(err)
		}
		s.mu.Lock()
		interval := s.signed.Policy.Interval()
		s.mu.Unlock()
		wait := interval - time.Since(started)
		if wait < time.Second {
			wait = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

type Status struct {
	Schema            string    `json:"schema"`
	RuntimeVersion    string    `json:"runtime_version"`
	PoolID            string    `json:"pool_id"`
	TenantID          string    `json:"tenant_id"`
	ProjectID         string    `json:"project_id"`
	PolicyDigest      string    `json:"policy_digest"`
	PolicyGeneration  uint64    `json:"policy_generation"`
	PolicyExpiresAt   time.Time `json:"policy_expires_at"`
	Mode              string    `json:"mode"`
	CredentialPresent bool      `json:"credential_present"`
	CredentialVersion uint64    `json:"credential_version,omitempty"`
	Current           string    `json:"current,omitempty"`
	ActiveOperationID string    `json:"active_operation_id,omitempty"`
	ActivePhase       string    `json:"active_phase,omitempty"`
	LastDecision      Decision  `json:"last_decision"`
	LastError         string    `json:"last_error,omitempty"`
}

type Preflight struct {
	Schema          string                            `json:"schema"`
	PolicyDigest    string                            `json:"policy_digest"`
	Current         string                            `json:"current,omitempty"`
	Records         map[string]Record                 `json:"records,omitempty"`
	CredentialReady bool                              `json:"credential_ready"`
	PolicyValid     bool                              `json:"policy_valid"`
	Targets         map[string]map[string]ProbeResult `json:"targets"`
	Errors          []string                          `json:"errors,omitempty"`
}

func (s *Service) preflight(ctx context.Context, signed SignedPolicy) Preflight {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	p := signed.Policy
	result := Preflight{Schema: PolicySchema, PolicyDigest: signed.Digest, PolicyValid: p.ExpiresAt.After(time.Now().UTC()),
		Targets: make(map[string]map[string]ProbeResult, len(p.Targets))}
	token, err := s.vault.Token(p)
	if err == nil {
		result.CredentialReady = true
	} else {
		result.Errors = append(result.Errors, "DNS credential unavailable")
	}
	if result.CredentialReady {
		client, clientErr := NewCloudflare(s.cloudflare, p.ZoneID, token, 15*time.Second)
		if clientErr != nil {
			result.Errors = append(result.Errors, "DNS client unavailable")
		} else if records, current, snapshotErr := (&Executor{Policy: p, DNS: client}).snapshot(ctx); snapshotErr == nil {
			result.Records, result.Current = records, current.ID
		} else {
			result.Errors = append(result.Errors, "current DNS baseline unavailable or differs")
		}
	}
	collector := SSHCollector{SignedPolicy: signed}
	var wg sync.WaitGroup
	var mu sync.Mutex
	sem := make(chan struct{}, 4)
	for _, target := range p.Targets {
		result.Targets[target.ID] = make(map[string]ProbeResult, len(p.Vantages))
	}
	for _, target := range p.Targets {
		for _, vantage := range p.Vantages {
			target, vantage := target, vantage
			wg.Add(1)
			go func() {
				defer wg.Done()
				probe := ProbeResult{TargetID: target.ID, At: time.Now().UTC(), Error: "vantage unavailable"}
				select {
				case sem <- struct{}{}:
					observed, collectErr := collector.Collect(ctx, p, target, vantage)
					if collectErr == nil && observed.TargetID == target.ID {
						probe = observed
					}
					<-sem
				case <-ctx.Done():
				}
				mu.Lock()
				result.Targets[target.ID][vantage.ID] = probe
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	if ctx.Err() != nil {
		result.Errors = append(result.Errors, "preflight deadline exceeded")
	}
	return result
}

func (s *Service) readExecutor(signed SignedPolicy) (*Executor, error) {
	token, err := s.vault.Token(signed.Policy)
	if err != nil {
		return nil, errors.New("DNS credential unavailable")
	}
	client, err := NewCloudflare(s.cloudflare, signed.Policy.ZoneID, token, 15*time.Second)
	if err != nil {
		return nil, err
	}
	return &Executor{Policy: signed.Policy, DNS: client, StateDir: s.vault.StateDir,
		VerifyTarget: func(context.Context, Target) error { return errors.New("read-only executor cannot switch DNS") }}, nil
}

func (s *Service) status(ctx context.Context, signed SignedPolicy, last Decision, lastError string) (Status, error) {
	p := signed.Policy
	present, version, err := s.vault.Present(p)
	if err != nil {
		return Status{}, err
	}
	status := Status{Schema: PolicySchema, RuntimeVersion: s.BuildVersion, PoolID: p.PoolID, TenantID: p.TenantID, ProjectID: p.ProjectID,
		PolicyDigest: signed.Digest, PolicyGeneration: p.Generation, PolicyExpiresAt: p.ExpiresAt,
		Mode: p.Mode, CredentialPresent: present, CredentialVersion: version, LastDecision: last, LastError: lastError}
	if present {
		observer, observerErr := s.readExecutor(signed)
		if observerErr == nil {
			_, current, observeErr := observer.snapshot(ctx)
			if observeErr == nil {
				status.Current = current.ID
			} else {
				status.LastError = observeErr.Error()
			}
			if op, phase, observeErr := observer.ActiveOperation(ctx); observeErr == nil && op != nil {
				status.ActiveOperationID, status.ActivePhase = op.ID, phase
			} else if observeErr != nil {
				status.LastError = observeErr.Error()
			}
		} else {
			status.LastError = observerErr.Error()
		}
	} else if op, err := (&Executor{Policy: p, StateDir: s.vault.StateDir}).loadActive(); err == nil && op != nil {
		status.ActiveOperationID, status.ActivePhase = op.ID, "unobserved"
	} else if err != nil {
		status.LastError = err.Error()
	}
	return status, nil
}

func (s *Service) applyPolicy(raw []byte) error {
	next, err := ParseSignedPolicy(raw, s.keys)
	if err != nil {
		return err
	}
	old := s.signed.Policy
	if next.Policy.TenantID != old.TenantID || next.Policy.ProjectID != old.ProjectID || next.Policy.PoolID != old.PoolID ||
		next.Policy.ZoneID != old.ZoneID || next.Policy.Zone != old.Zone || next.Policy.Generation <= old.Generation {
		return errors.New("new policy must preserve pool ownership and advance generation")
	}
	active, err := s.executor.loadActive()
	if err != nil {
		return err
	}
	if active != nil && active.Phase != "completed" {
		return errors.New("unresolved DNS operation blocks policy replacement")
	}
	if err := writeDurable(filepath.Join(s.vault.StateDir, "policy-lkg.json"), append(raw, '\n')); err != nil {
		return err
	}
	if err := writeDurable(s.policyPath, append(raw, '\n')); err != nil {
		return err
	}
	s.signed = next
	s.executor.Policy = next.Policy
	s.engine.Collector = SSHCollector{SignedPolicy: next}
	return nil
}

func (s *Service) Handler(authorize func(*http.Request) (string, string, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, grant, err := authorize(r)
		if err != nil {
			http.Error(w, "management identity rejected", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/entry-failover/status" {
			s.mu.Lock()
			signed, last, lastError := s.signed, s.last, s.lastError
			s.mu.Unlock()
			status, err := s.status(r.Context(), signed, last, lastError)
			if err != nil {
				http.Error(w, "status unavailable", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(status)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/entry-failover/evidence" {
			s.mu.Lock()
			latest := s.last
			s.mu.Unlock()
			_ = json.NewEncoder(w).Encode(latest)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/entry-failover/operation" {
			s.mu.Lock()
			signed := s.signed
			s.mu.Unlock()
			observer, err := s.readExecutor(signed)
			if err != nil {
				http.Error(w, "operation observation unavailable", http.StatusServiceUnavailable)
				return
			}
			op, observed, err := observer.ActiveOperation(r.Context())
			if err != nil {
				http.Error(w, "operation observation unavailable", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"operation": op, "observed_phase": observed})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/entry-failover/preflight" {
			s.mu.Lock()
			signed := s.signed
			s.mu.Unlock()
			result := s.preflight(r.Context(), signed)
			_ = json.NewEncoder(w).Encode(result)
			return
		}
		if r.Method != http.MethodPost || (grant != "operator" && grant != "admin") {
			http.Error(w, "operation forbidden", http.StatusForbidden)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
		if err != nil {
			http.Error(w, "invalid request size", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/v1/entry-failover/credential":
			if grant != "admin" {
				http.Error(w, "admin grant required", http.StatusForbidden)
				return
			}
			var body struct {
				Token string `json:"token"`
			}
			if err = sc.StrictJSON(raw, &body); err != nil {
				http.Error(w, "invalid credential envelope", http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			version, putErr := s.vault.Put(s.signed.Policy, body.Token)
			s.mu.Unlock()
			if putErr != nil {
				http.Error(w, "credential import failed", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"stored": true, "version": version})
		case "/v1/entry-failover/policy":
			if grant != "admin" {
				http.Error(w, "admin grant required", http.StatusForbidden)
				return
			}
			s.mu.Lock()
			applyErr := s.applyPolicy(raw)
			digest := s.signed.Digest
			s.mu.Unlock()
			if applyErr != nil {
				http.Error(w, "policy update refused", http.StatusConflict)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"policy_digest": digest})
		case "/v1/entry-failover/switch":
			var body struct {
				TargetID             string `json:"target_id"`
				ExpectedPolicyDigest string `json:"expected_policy_digest"`
			}
			if err = sc.StrictJSON(raw, &body); err != nil {
				http.Error(w, "invalid switch request", http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			if body.ExpectedPolicyDigest != s.signed.Digest {
				s.mu.Unlock()
				http.Error(w, "policy generation conflict", http.StatusConflict)
				return
			}
			op, switchErr := s.executor.Switch(r.Context(), body.TargetID, false)
			s.mu.Unlock()
			if switchErr != nil {
				http.Error(w, "DNS switch failed or is indeterminate", http.StatusConflict)
				return
			}
			_ = json.NewEncoder(w).Encode(op)
		default:
			http.NotFound(w, r)
		}
	})
}
