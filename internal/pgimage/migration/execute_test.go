package migration

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"
)

var errLostResponse = errors.New("patch response lost")
var errInactive = errors.New("no longer held by this executor")

type testLedger struct {
	events    *[]string
	intent    *model.OperationEvidence
	readErr   error
	inactive  bool
	completed bool
}

func (l *testLedger) ReadIntent() (*model.OperationEvidence, bool, error) {
	*l.events = append(*l.events, "read")
	return l.intent, false, l.readErr
}

func (l *testLedger) WriteWitness(kind string, w pgimage.Witness) error {
	*l.events = append(*l.events, kind)
	if kind == pgimage.IntentEvidence {
		raw, err := json.Marshal(w)
		if err != nil {
			return err
		}
		l.intent = &model.OperationEvidence{CollectedAt: time.Now()}
		return json.Unmarshal(raw, &l.intent.Payload)
	}
	if kind == pgimage.VerifiedEvidence && w.VerifiedPodUID != "replacement" {
		return errors.New("replacement identity was not persisted")
	}
	return nil
}

func (l *testLedger) CheckOwnership() error {
	*l.events = append(*l.events, "check")
	if l.inactive {
		return errInactive
	}
	return nil
}

func (*testLedger) IsInactive(err error) bool { return errors.Is(err, errInactive) }
func (*testLedger) Progress(string)           {}

func (l *testLedger) Complete() error {
	*l.events = append(*l.events, "complete")
	l.completed = true
	return nil
}

type testPlatform struct {
	Platform
	events   *[]string
	witness  pgimage.Witness
	replaced bool
}

func (p *testPlatform) Capture(context.Context) (pgimage.Witness, error) {
	*p.events = append(*p.events, "capture")
	return p.witness, nil
}

func (p *testPlatform) Observe(context.Context) (Observation, error) {
	o := Observation{PodUID: p.witness.PodUID, CurrentImage: p.witness.SourceImage, PodImage: p.witness.SourceImage}
	if p.replaced {
		o.PodUID, o.CurrentImage, o.PodImage = "replacement", p.witness.TargetImage, p.witness.TargetImage
	}
	o.Verify = func(pgimage.Witness) error { return nil }
	o.VerifyVolumes = func(context.Context, pgimage.Witness) error {
		*p.events = append(*p.events, "verify-volumes")
		return nil
	}
	o.VerifyReplacement = func(string) error {
		*p.events = append(*p.events, "verify-replacement")
		return nil
	}
	o.Probe = func(context.Context) error {
		*p.events = append(*p.events, "probe")
		return nil
	}
	return o, nil
}

func (p *testPlatform) PatchImage(context.Context, pgimage.Witness, map[string]any) error {
	*p.events = append(*p.events, "patch")
	p.replaced = true
	return errLostResponse
}

func TestLostPatchRecoveryVerifiesBeforeCompletionWithoutReapplying(t *testing.T) {
	events := []string{}
	ledger := &testLedger{events: &events}
	checkedPatch := ""
	platform := &testPlatform{events: &events, witness: pgimage.Witness{
		Namespace: "tenant", Cluster: "db", ClusterUID: "cluster-id", PodUID: "original", SystemID: "system",
		SpecHash: "spec", SourceImage: "old", TargetImage: "new", Volumes: map[string]string{"data": "volume"}, BootstrapPodPatch: &checkedPatch,
	}}
	target := Target{Namespace: "tenant", Cluster: "db", Image: "new"}
	prepare := func(context.Context) (Target, Platform, error) { return target, platform, nil }
	if err := Execute(context.Background(), ledger, prepare); !errors.Is(err, ErrUnsettled) || ledger.completed || ledger.intent == nil {
		t.Fatalf("lost response did not leave a resumable operation: %v, complete=%v, intent=%v", err, ledger.completed, ledger.intent != nil)
	}
	want := []string{"read", "capture", "probe", pgimage.BaselineEvidence, "check", pgimage.IntentEvidence, "check", "verify-volumes", "check", "patch"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("before recovery: got %v, want %v", events, want)
	}
	events = nil
	ledger.inactive = true
	if err := Execute(context.Background(), ledger, prepare); !errors.Is(err, errInactive) || !reflect.DeepEqual(events, []string{"read", "check"}) {
		t.Fatalf("ownership loss did not halt recovery: %v, events %v", err, events)
	}
	events = nil
	ledger.inactive = false
	if err := Execute(context.Background(), ledger, prepare); err != nil || !ledger.completed {
		t.Fatalf("recovery failed: %v, complete=%v", err, ledger.completed)
	}
	want = []string{"read", "check", "verify-volumes", "verify-replacement", "probe", pgimage.VerifiedEvidence, "check", "complete"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("recovery: got %v, want %v", events, want)
	}
}

func TestUnavailableLedgerDoesNotOpenPlatform(t *testing.T) {
	events := []string{}
	ledger := &testLedger{events: &events, readErr: errors.New("unavailable ledger")}
	err := Execute(context.Background(), ledger, func(context.Context) (Target, Platform, error) {
		t.Fatal("opened a mutation path without knowing the durable intent")
		return Target{}, nil, nil
	})
	if !errors.Is(err, ErrUnsettled) || !reflect.DeepEqual(events, []string{"read"}) {
		t.Fatalf("got %v, events %v", err, events)
	}
}
