package staticedgemanager

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"

	c "fugue/internal/staticedgecontract"
)

type fakeRuntime struct {
	mu         sync.Mutex
	raw        json.RawMessage
	startup    json.RawMessage
	bad        string
	badRestore bool
	writes     int
	healthFail bool
}

func (r *fakeRuntime) Snapshot(context.Context) (json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(json.RawMessage(nil), r.raw...), nil
}
func (r *fakeRuntime) Validate(context.Context, c.Bundle) error { return nil }
func (r *fakeRuntime) Apply(_ context.Context, b json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes++
	r.raw = append(json.RawMessage(nil), b...)
	if string(b) == r.bad || r.badRestore {
		return errors.New("injected failure after write")
	}
	return nil
}
func (r *fakeRuntime) Startup(context.Context) (json.RawMessage, error) { return r.startup, nil }
func (r *fakeRuntime) Persist(_ context.Context, raw json.RawMessage) error {
	r.startup = append(json.RawMessage(nil), raw...)
	return nil
}
func (r *fakeRuntime) Probe(context.Context, []string) error {
	if r.healthFail {
		return errors.New("unhealthy")
	}
	return nil
}

type fixture struct {
	m   *Manager
	r   *fakeRuntime
	key ed25519.PrivateKey
	cfg Config
}

func setup(t *testing.T) *fixture {
	t.Helper()
	pub, key, _ := ed25519.GenerateKey(nil)
	r := &fakeRuntime{raw: json.RawMessage(`{"id":1}`), startup: json.RawMessage(`{"id":1}`)}
	cfg := Config{Role: "edge", EdgeID: "test-edge", StateDir: t.TempDir(), VerificationKeys: map[string]ed25519.PublicKey{"test": pub}, Runtime: r}
	m, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	return &fixture{m, r, key, cfg}
}
func (f *fixture) bundle(n uint64, mode string) c.Bundle {
	raw, _ := json.Marshal(map[string]uint64{"id": n})
	b := c.Bundle{Schema: c.SchemaV1, EdgeID: "test-edge", Role: "edge", Generation: n, Mode: mode, CaddyConfig: raw, HealthChecks: []string{"health"}, SigningKeyID: "test"}
	_ = c.SignBundle(&b, f.key, "test")
	return b
}
func (f *fixture) request(op, id string, b *c.Bundle, digest string) c.Request {
	r := f.m.st.Revision
	return c.Request{Schema: c.RPCSchema, EdgeID: "test-edge", RequestID: id, Operation: op, ExpectedRevision: &r, Bundle: b, TargetDigest: digest}
}
func (f *fixture) call(op, id string, b *c.Bundle, digest string) c.Response {
	return f.m.Execute(f.request(op, id, b, digest), "tester", "admin")
}
func adopt(t *testing.T, f *fixture) {
	t.Helper()
	b := f.bundle(1, "serving")
	r := f.call("adopt", "adopt", &b, "")
	if !r.OK {
		t.Fatal(r.Error)
	}
	if f.r.writes != 0 {
		t.Fatal("adopt changed runtime")
	}
}
func TestVerifiedActivationRollbackAndRestart(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	b := f.bundle(2, "serving")
	if r := f.call("stage", "s2", &b, ""); !r.OK {
		t.Fatal(r.Error)
	}
	req := f.request("activate", "a2", nil, b.BundleDigest)
	r := f.m.Execute(req, "tester", "admin")
	if !r.OK || !r.Receipt.RuntimeVerified {
		t.Fatal(r)
	}
	if f.r.writes != 1 {
		t.Fatal("runtime wasn't applied")
	}
	if r = f.m.Execute(req, "tester", "admin"); !r.OK || f.r.writes != 1 {
		t.Fatal("duplicate was applied twice")
	}
	req.TargetDigest = "changed"
	if f.m.Execute(req, "tester", "admin").Status != 409 {
		t.Fatal("id reuse accepted")
	}
	old := f.m.st.LKG.BundleDigest
	if r = f.call("rollback", "rb", nil, old); !r.OK {
		t.Fatal(r.Error)
	}
	f.m.Close()
	m, e := New(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	o := m.observed(context.Background())
	if !o.Ready || o.ActiveGeneration != 1 {
		t.Fatal(o)
	}
}
func TestFailedApplyRestoresAndPreservesLKG(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	b := f.bundle(2, "serving")
	f.call("stage", "s", &b, "")
	f.r.bad = string(b.CaddyConfig)
	r := f.call("activate", "a", nil, b.BundleDigest)
	if r.OK || r.Receipt.Outcome != "failed_restored" || f.m.st.Active.Generation != 1 || f.m.st.LKG.Generation != 1 || string(f.r.raw) != `{"id":1}` {
		t.Fatal(r)
	}
}
func TestInterruptedWriteRequiresRecovery(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	b := f.bundle(2, "serving")
	f.call("stage", "s", &b, "")
	req := f.request("activate", "a", nil, b.BundleDigest)
	f.m.st.Pending = &pending{Request: req, Actor: "tester", Before: json.RawMessage(`{"id":1}`)}
	if e := f.m.save(); e != nil {
		t.Fatal(e)
	}
	f.r.raw = b.CaddyConfig
	f.m.Close()
	m, e := New(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	f.m = m
	if r := f.call("activate", "another", nil, b.BundleDigest); r.Status != 409 {
		t.Fatal(r)
	}
	if r := f.call("recover", "recover", nil, ""); !r.OK || r.Receipt.Outcome != "recovered" {
		t.Fatal(r)
	}
	if string(f.r.raw) != `{"id":1}` {
		t.Fatal("original runtime not restored")
	}
}
func TestManagerRefusesIdentitySignatureDriftAndStaleRevision(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	b := f.bundle(2, "serving")
	b.EdgeID = "other"
	if f.call("stage", "wrong", &b, "").Status != 403 {
		t.Fatal("identity accepted")
	}
	b = f.bundle(2, "serving")
	b.Signature = ""
	if f.call("stage", "unsigned", &b, "").Status != 403 {
		t.Fatal("unsigned accepted")
	}
	b = f.bundle(2, "serving")
	f.call("stage", "s", &b, "")
	req := f.request("activate", "a", nil, b.BundleDigest)
	zero := uint64(0)
	req.ExpectedRevision = &zero
	if f.m.Execute(req, "tester", "admin").Status != 409 {
		t.Fatal("stale accepted")
	}
	f.r.raw = json.RawMessage(`{"foreign":true}`)
	if f.call("activate", "drift", nil, b.BundleDigest).Status != 409 {
		t.Fatal("drift accepted")
	}
	if f.r.writes != 0 {
		t.Fatal("unexpected runtime mutation")
	}
}
func TestDrainActuallyAppliesCandidate(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	b := f.bundle(2, "draining")
	f.call("stage", "s", &b, "")
	r := f.call("drain", "d", nil, b.BundleDigest)
	if !r.OK || f.r.writes != 1 || !f.m.observed(context.Background()).Draining {
		t.Fatal(r)
	}
}
func TestCorruptStateAndConcurrentManagerFailClosed(t *testing.T) {
	f := setup(t)
	if m, e := New(f.cfg); e == nil {
		m.Close()
		t.Fatal("second writer accepted")
	}
	f.m.Close()
	if e := os.WriteFile(f.m.path(), []byte(`not JSON`), 0600); e != nil {
		t.Fatal(e)
	}
	if m, e := New(f.cfg); e == nil {
		m.Close()
		t.Fatal("corrupt state reset")
	}
}
func TestReadGrantCannotWrite(t *testing.T) {
	f := setup(t)
	b := f.bundle(1, "serving")
	if r := f.m.Execute(f.request("stage", "s", &b, ""), "reader", "read"); r.Status != 403 {
		t.Fatal(r)
	}
}

func TestManagerStartupDriftAndHealthRemainUnknown(t *testing.T) {
	f := setup(t)
	f.r.startup = json.RawMessage(`{"id":99}`)
	b := f.bundle(1, "serving")
	if r := f.call("adopt", "adopt-drift", &b, ""); r.Status != 409 {
		t.Fatal(r)
	}
	if r := f.call("health", "health", nil, ""); r.OK {
		t.Fatal("unadopted runtime reported ready")
	}
}
func TestManagerFailedRestoreRemainsBlocked(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	b := f.bundle(2, "serving")
	f.call("stage", "s", &b, "")
	f.r.badRestore = true
	r := f.call("activate", "a", nil, b.BundleDigest)
	if r.OK || r.Receipt.Outcome != "recovery_required" || f.m.st.Pending == nil {
		t.Fatal(r)
	}
	if r = f.call("stage", "different", &b, ""); r.Status != 409 {
		t.Fatal(r)
	}
	f.r.badRestore = false
	if r = f.call("recover", "recover", nil, ""); !r.OK {
		t.Fatal(r)
	}
}

type fakeCredentials struct{ slot string }

func (p *fakeCredentials) Validate(s string) error {
	if s != "a" && s != "b" {
		return errors.New("invalid slot")
	}
	return nil
}
func (p *fakeCredentials) Select(s string) error {
	if e := p.Validate(s); e != nil {
		return e
	}
	p.slot = s
	return nil
}
func (p *fakeCredentials) Status() (string, string) { return p.slot, "" }
func TestManagerCredentialSlotPersistsIndependently(t *testing.T) {
	f := setup(t)
	p := &fakeCredentials{slot: "a"}
	f.m.cfg.Credentials = p
	req := f.request("cert-rotate", "rotate", nil, "")
	req.CredentialSlot = "b"
	r := f.m.Execute(req, "admin", "admin")
	if !r.OK || p.slot != "b" {
		t.Fatal(r)
	}
	f.m.Close()
	f.cfg.Credentials = p
	p.slot = "a"
	m, e := New(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	if p.slot != "b" {
		t.Fatal("credential slot lost on restart")
	}
	if f.r.writes != 0 {
		t.Fatal("management rotation modified traffic")
	}
}
