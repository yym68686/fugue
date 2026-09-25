package entryfailover

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type fakeDNS struct {
	records  map[string]Record
	writes   int
	commit   bool
	writeErr error
}

func (d *fakeDNS) Snapshot(_ context.Context, hosts []string) (map[string]Record, error) {
	out := make(map[string]Record, len(hosts))
	for _, h := range hosts {
		out[h] = d.records[h]
	}
	return out, nil
}
func (d *fakeDNS) Batch(_ context.Context, before map[string]Record, to Target) error {
	d.writes++
	if d.commit {
		for h, r := range before {
			d.records[h] = targetRecord(r, to)
		}
	}
	return d.writeErr
}

func newExecutorTest(t *testing.T) (*Executor, *fakeDNS) {
	t.Helper()
	p := testPolicy()
	d := &fakeDNS{records: map[string]Record{
		"example.test":     {ID: "a", Name: "example.test", Type: "A", Content: "192.0.2.10", TTL: 1},
		"api.example.test": {ID: "b", Name: "api.example.test", Type: "A", Content: "192.0.2.10", TTL: 1},
	}, commit: true}
	e := &Executor{Policy: p, DNS: d, StateDir: filepath.Join(t.TempDir(), "state"), VerifyTarget: func(context.Context, Target) error { return nil }}
	return e, d
}

func TestExecutorRequiresSignedAutomaticModeAndProvedCandidate(t *testing.T) {
	e, d := newExecutorTest(t)
	if _, err := e.Switch(context.Background(), "managed", true); err == nil {
		t.Fatal("shadow policy permitted automatic mutation")
	}
	if d.writes != 0 {
		t.Fatal("automatic shadow wrote DNS")
	}
	e.VerifyTarget = func(context.Context, Target) error { return errors.New("business route not ready") }
	if _, err := e.Switch(context.Background(), "managed", false); err == nil {
		t.Fatal("unproved target selected")
	}
	if d.writes != 0 {
		t.Fatal("failed target proof wrote DNS")
	}
	e.VerifyTarget = func(context.Context, Target) error { return nil }
	op, err := e.Switch(context.Background(), "managed", false)
	if err != nil || op.Phase != "completed" || d.writes != 1 {
		t.Fatalf("switch=%+v err=%v writes=%d", op, err, d.writes)
	}
	for _, r := range d.records {
		if r.Type != "CNAME" || r.Content != e.Policy.Targets[1].Address {
			t.Fatalf("record=%+v", r)
		}
	}
	op, err = e.Switch(context.Background(), "managed", false)
	if err != nil || op.Phase != "already_selected" || d.writes != 1 {
		t.Fatalf("repeat=%+v err=%v writes=%d", op, err, d.writes)
	}
}

func TestExecutorRecoversCommittedLostResponse(t *testing.T) {
	e, d := newExecutorTest(t)
	d.writeErr = errors.New("lost response")
	op, err := e.Switch(context.Background(), "managed", false)
	if err != nil || op.Phase != "completed" || d.writes != 1 {
		t.Fatalf("committed write=%+v err=%v", op, err)
	}
	if _, err = e.Switch(context.Background(), "west", false); err != nil || d.writes != 2 {
		t.Fatalf("subsequent switch err=%v writes=%d", err, d.writes)
	}
}

func TestExecutorQuarantinesUncertainAndMixedDNS(t *testing.T) {
	e, d := newExecutorTest(t)
	d.commit = false
	d.writeErr = errors.New("request timed out")
	op, err := e.Switch(context.Background(), "managed", false)
	if err == nil || op.Phase != "indeterminate" || d.writes != 1 {
		t.Fatalf("timeout=%+v err=%v", op, err)
	}
	if _, err = e.Switch(context.Background(), "managed", false); err == nil || d.writes != 1 {
		t.Fatalf("uncertain write retried: err=%v writes=%d", err, d.writes)
	}
	d.records["example.test"] = targetRecord(d.records["example.test"], e.Policy.Targets[1])
	if _, err = e.Switch(context.Background(), "managed", false); err == nil || d.writes != 1 {
		t.Fatalf("mixed DNS overwritten: err=%v writes=%d", err, d.writes)
	}
}

func TestExecutorRejectsUnownedDNSValue(t *testing.T) {
	e, d := newExecutorTest(t)
	d.records["api.example.test"] = Record{ID: "b", Name: "api.example.test", Type: "A", Content: "192.0.2.91", TTL: 1}
	if _, _, err := e.Plan(context.Background(), "managed"); err == nil {
		t.Fatal("drift accepted")
	}
	if _, err := e.Switch(context.Background(), "managed", false); err == nil || d.writes != 0 {
		t.Fatalf("drift wrote DNS: %v", err)
	}
}
