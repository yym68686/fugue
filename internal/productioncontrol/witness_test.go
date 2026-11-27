package productioncontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"testing"
	"time"
)

type memoryWitnesses map[string][]byte

func (m memoryWitnesses) ReadExecutionWitness(_ context.Context, digest string) ([]byte, error) {
	if raw, ok := m[digest]; ok {
		return raw, nil
	}
	return nil, errors.New("witness unavailable")
}

func fixtureWitness(t *testing.T, key ed25519.PrivateKey, w ExecutionWitness, store memoryWitnesses) string {
	t.Helper()
	message, err := w.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	w.Signature = ed25519.Sign(key, message)
	raw, id, err := EncodeExecutionWitness(w)
	if err != nil {
		t.Fatal(err)
	}
	store[id] = raw
	return id
}

func TestExecutionWitnessAuthenticatesObservationAndWriteClosure(t *testing.T) {
	ctx := context.Background()
	intent, _, owner, approvers, now := approvalFixture(t)
	intentDigest, err := intent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	j, err := NewJournal(new(sql.DB), approvers)
	if err != nil {
		t.Fatal(err)
	}
	currentTime := now
	j.now = func() time.Time { return currentTime }
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	store := memoryWitnesses{}
	scope := intent.scope()
	if _, err := NewWitnessedExecutionJournal(j, store, map[string]TrustedWitness{"observer": {scope, owner.Public().(ed25519.PublicKey)}}); err == nil {
		t.Fatal("accepted an approval key as a provider witness key")
	}
	journal, err := NewWitnessedExecutionJournal(j, store, map[string]TrustedWitness{"observer": {scope, pub}})
	if err != nil {
		t.Fatal(err)
	}
	clear(pub) // Trust must not change when the caller mutates the configuration.
	seen := ExecutionWitness{Kind: BaselineWitness, KeyID: "observer", ClusterID: scope.ClusterID, ResourceID: scope.ResourceID,
		Generation: intent.Generation, VerifiedDigest: intent.CurrentDigest, ObservationDigest: digest([]byte("live predecessor")),
		AuthorityDigest: digest([]byte("provider old-writer drainage")), ObservedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix()}
	enrollment := Baseline{scope, seen.version(), fixtureWitness(t, key, seen, store)}
	if err := journal.evidence.VerifyBaseline(ctx, enrollment); err != nil {
		t.Fatal(err)
	}
	if err := journal.evidence.VerifyBaseline(ctx, Baseline{scope, enrollment.Verified, fixtureWitness(t, owner, seen, store)}); err == nil {
		t.Fatal("accepted an observer envelope signed by the release owner")
	}
	misbound := enrollment
	misbound.Verified.Generation++
	if err := journal.evidence.VerifyBaseline(ctx, misbound); err == nil {
		t.Fatal("wrong baseline version was accepted")
	}
	misbound = enrollment
	misbound.ResourceID = "namespace/other"
	if err := journal.evidence.VerifyBaseline(ctx, misbound); err == nil {
		t.Fatal("baseline was replayed for a different resource")
	}
	otherScope := seen
	otherScope.ResourceID = misbound.ResourceID
	misbound.EvidenceDigest = fixtureWitness(t, key, otherScope, store)
	if err := journal.evidence.VerifyBaseline(ctx, misbound); err == nil {
		t.Fatal("observer key was trusted for an unenrolled resource")
	}
	whitespace := append(bytes.Clone(store[enrollment.EvidenceDigest]), '\n')
	id := digest(whitespace)
	store[id] = whitespace
	misbound = enrollment
	misbound.EvidenceDigest = id
	if err := journal.evidence.VerifyBaseline(ctx, misbound); err == nil {
		t.Fatal("noncanonical baseline witness was accepted")
	}
	store[id] = store[enrollment.EvidenceDigest]
	if err := journal.evidence.VerifyBaseline(ctx, misbound); err == nil {
		t.Fatal("witness fetch returned different bytes under the requested digest")
	}
	for _, at := range []time.Time{now.Add(-time.Second), now.Add(time.Minute)} {
		currentTime = at
		if err := journal.evidence.VerifyBaseline(ctx, enrollment); err == nil {
			t.Fatal("future or stale baseline evidence was accepted")
		}
	}
	currentTime = now

	claim := Claim{scope, 3, intentDigest, digest([]byte("exact approval"))}
	terminal := seen
	terminal.Kind, terminal.Fence, terminal.Outcome = TerminalWitness, claim.Fence, Applied
	terminal.IntentDigest, terminal.ApprovalDigest = claim.IntentDigest, claim.ApprovalDigest
	terminal.Generation++
	terminal.VerifiedDigest = intent.TargetDigest
	terminal.ObservationDigest = digest([]byte("healthy exact image and workload"))
	terminal.AuthorityDigest = digest([]byte("provider enforcement closed execution fence"))
	result := ExecutionResult{Applied, terminal.version(), fixtureWitness(t, key, terminal, store)}
	if err := journal.evidence.VerifyTerminal(ctx, claim, intent, result); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ExecutionWitness){
		"fence":    func(w *ExecutionWitness) { w.Fence++ },
		"intent":   func(w *ExecutionWitness) { w.IntentDigest = w.ApprovalDigest },
		"approval": func(w *ExecutionWitness) { w.ApprovalDigest = w.IntentDigest },
		"outcome":  func(w *ExecutionWitness) { w.Outcome = NotApplied },
		"version":  func(w *ExecutionWitness) { w.Generation++ },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := terminal
			mutate(&wrong)
			changed := result
			changed.EvidenceDigest = fixtureWitness(t, key, wrong, store)
			if err := journal.evidence.VerifyTerminal(ctx, claim, intent, changed); err == nil {
				t.Fatal("accepted a signed witness for a different execution")
			}
		})
	}
	result.EvidenceDigest = enrollment.EvidenceDigest
	if err := journal.evidence.VerifyTerminal(ctx, claim, intent, result); err == nil {
		t.Fatal("baseline evidence was replayed as a terminal witness")
	}
	result.EvidenceDigest = digest([]byte("missing"))
	if err := journal.evidence.VerifyTerminal(ctx, claim, intent, result); err == nil {
		t.Fatal("accepted a result whose authenticated witness was unavailable")
	}
	for _, omit := range []string{"", terminal.ObservationDigest} {
		terminal.AuthorityDigest = omit
		if _, err := terminal.SigningInput(); err == nil {
			t.Fatal("health evidence alone was treated as proof of write closure")
		}
	}
}

func testPostgresWitnessGate(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	intent, payload, owner, approvers, now := approvalFixture(t)
	intent.ResourceID, intent.RollbackDigest = "namespace/attested-resource", intent.CurrentDigest
	j, err := NewJournal(db, approvers)
	if err != nil {
		t.Fatal(err)
	}
	j.now = func() time.Time { return now }
	pub, observer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	source := memoryWitnesses{}
	scope := intent.scope()
	execution, err := NewWitnessedExecutionJournal(j, source, map[string]TrustedWitness{"observer": {scope, pub}})
	if err != nil {
		t.Fatal(err)
	}
	seen := ExecutionWitness{Kind: BaselineWitness, KeyID: "observer", ClusterID: scope.ClusterID, ResourceID: scope.ResourceID,
		Generation: intent.Generation, VerifiedDigest: intent.CurrentDigest, ObservationDigest: digest([]byte("observed predecessor")),
		AuthorityDigest: digest([]byte("observed drain")), ObservedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix()}
	baseline := Baseline{scope, seen.version(), fixtureWitness(t, owner, seen, source)}
	if created, err := execution.AnchorResource(ctx, "observer", baseline); err == nil || created {
		t.Fatal("approval key was able to anchor the resource")
	}
	if _, err := execution.Resource(ctx, scope); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected baseline left a resource: %v", err)
	}
	baseline.EvidenceDigest = fixtureWitness(t, observer, seen, source)
	if created, err := execution.AnchorResource(ctx, "observer", baseline); err != nil || !created {
		t.Fatalf("valid witness failed to anchor: created=%v err=%v", created, err)
	}
	record, err := j.RecordIntent(ctx, "operator", intent, payload)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := SignApproval(intent, "owner-a", owner, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := j.RecordApproval(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	claim, created, err := execution.ReserveExecution(ctx, "guardian", record.Digest, approved.Digest)
	if err != nil || !created {
		t.Fatalf("claim: created=%v err=%v", created, err)
	}
	seen.Kind, seen.Outcome, seen.Fence = TerminalWitness, Applied, claim.Fence
	seen.IntentDigest, seen.ApprovalDigest = claim.IntentDigest, claim.ApprovalDigest
	seen.Generation++
	seen.VerifiedDigest = intent.TargetDigest
	seen.ObservationDigest = digest([]byte("observed candidate"))
	seen.AuthorityDigest = digest([]byte("observed dispatch closure"))
	result := ExecutionResult{Applied, seen.version(), fixtureWitness(t, observer, seen, source)}
	body := source[result.EvidenceDigest]
	delete(source, result.EvidenceDigest)
	if created, err := execution.CompleteExecution(ctx, "observer", claim, result); err == nil || created {
		t.Fatal("unavailable witness was allowed to settle the claim")
	}
	state, err := execution.Resource(ctx, scope)
	if err != nil || state.Pending != claim.IntentDigest || state.LastVerified != baseline.Verified {
		t.Fatalf("unavailable witness changed the durable head: %+v, %v", state, err)
	}
	source[result.EvidenceDigest] = body
	if created, err := execution.CompleteExecution(ctx, "observer", claim, result); err != nil || !created {
		t.Fatalf("valid witness failed to settle: created=%v err=%v", created, err)
	}
	state, err = execution.Resource(ctx, scope)
	if err != nil || state.Pending != "" || state.LastVerified != result.Verified {
		t.Fatalf("valid witness did not advance the durable head: %+v, %v", state, err)
	}
}
