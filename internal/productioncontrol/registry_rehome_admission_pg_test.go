package productioncontrol

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type registryRehomeFixture struct {
	intent           Intent
	payload          []byte
	record, approval string
	observer         ed25519.PrivateKey
	now              time.Time
}

func anchorRegistryRehomeFixture(t *testing.T, ctx context.Context, j *Journal, owner ed25519.PrivateKey, suffix string) registryRehomeFixture {
	t.Helper()
	i, p, _, _, now := approvalFixture(t)
	i.ResourceID = "kubernetes/isolated-fixture/deployment/" + suffix
	i.Operation = ComponentRegistryMove
	i.RollbackDigest = i.CurrentDigest
	b := Baseline{i.scope(), Version{i.Generation, i.CurrentDigest}, digest([]byte("fixture enrollment " + suffix))}
	w, err := NewExecutionJournal(j, &testExecutionEvidence{enrollment: b.EvidenceDigest})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := w.AnchorResource(ctx, "fixture-observer", b); !ok || err != nil {
		t.Fatalf("could not anchor rehome fixture: %v %v", ok, err)
	}
	id, approval := recordRehomeFixtureApproval(t, ctx, j, i, p, owner, now)
	_, observer, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return registryRehomeFixture{i, p, id, approval, observer, now}
}

func recordRehomeFixtureApproval(t *testing.T, ctx context.Context, j *Journal, i Intent, payload []byte, owner ed25519.PrivateKey, now time.Time) (string, string) {
	t.Helper()
	rec, err := j.RecordIntent(ctx, "fixture-operator", i, payload)
	if err != nil || !rec.Created {
		t.Fatalf("record rehome fixture intent: %+v %v", rec, err)
	}
	approval, err := SignApproval(i, "owner-a", owner, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := j.RecordApproval(ctx, approval)
	if err != nil || !approved.Created {
		t.Fatalf("record rehome fixture approval: %+v %v", approved, err)
	}
	return rec.Digest, approved.Digest
}

func (f registryRehomeFixture) report(fence int64) RegistryRehomeAdmission {
	hash := func(name string) string { return digest([]byte("fixture rehome evidence " + name)) }
	return RegistryRehomeAdmission{ClusterID: f.intent.ClusterID, ResourceID: f.intent.ResourceID, IntentDigest: f.record,
		ApprovalDigest: f.approval, Fence: fence, PolicyScope: hash("policy"), ProviderWrite: hash("exact provider write"),
		EndpointAdmission: hash("endpoints"), LegacyRevocation: hash("revocation"), LegacyRequestDrain: hash("drain"),
		ForwardNodePull: hash("forward pull"), RollbackNodePull: hash("rollback pull"), RegistryRetention: hash("retention"),
		ObservedAt: f.now.Unix(), ExpiresAt: f.now.Add(time.Minute).Unix()}
}

func signRegistryRehomeFixture(t *testing.T, a RegistryRehomeAdmission, signer ed25519.PrivateKey) (RegistryRehomeAdmission, []byte, string) {
	t.Helper()
	msg, err := a.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	a.Signature = ed25519.Sign(signer, msg)
	raw, d, err := EncodeRegistryRehomeAdmission(a)
	if err != nil {
		t.Fatal(err)
	}
	return a, raw, d
}

func testPostgresRegistryRehome(t *testing.T, ctx context.Context, ownerDB, ordinaryDB, dedicatedDB *sql.DB) {
	t.Helper()
	_, _, owner, trusted, now := approvalFixture(t)
	journal := func(db *sql.DB) *Journal {
		t.Helper()
		j, err := NewJournal(db, trusted)
		if err != nil {
			t.Fatal(err)
		}
		j.now = func() time.Time { return now }
		return j
	}
	ordinary, specialized := journal(ordinaryDB), journal(dedicatedDB)
	f := anchorRegistryRehomeFixture(t, ctx, ordinary, owner, "rehome-single")
	signed, raw, pin := signRegistryRehomeFixture(t, f.report(1), f.observer)
	observer := f.observer.Public().(ed25519.PublicKey)
	check := func(_ context.Context, a RegistryRehomeAdmission, s Submission) error {
		if a.ProviderWrite != signed.ProviderWrite || s.Intent != f.intent || string(s.Payload) != string(f.payload) {
			return errors.New("provider boundary drift")
		}
		return nil
	}
	if _, err := NewRegistryRehomeJournal(specialized, f.intent.scope(), owner.Public().(ed25519.PublicKey), check); err == nil {
		t.Fatal("owner approval key was accepted as technical observer")
	}
	if _, err := NewRegistryRehomeJournal(specialized, f.intent.scope(), observer, nil); err == nil {
		t.Fatal("missing live boundary was accepted")
	}
	dedicated, err := NewRegistryRehomeJournal(specialized, f.intent.scope(), observer, check)
	if err != nil {
		t.Fatal(err)
	}
	unprivileged, err := NewRegistryRehomeJournal(ordinary, f.intent.scope(), observer, check)
	if err != nil {
		t.Fatal(err)
	}
	_, ownerRaw, _ := signRegistryRehomeFixture(t, f.report(1), owner)
	expired := f.report(1)
	expired.ObservedAt--
	expired.ExpiresAt = now.Unix()
	_, staleRaw, _ := signRegistryRehomeFixture(t, expired, f.observer)
	_, nextFenceRaw, _ := signRegistryRehomeFixture(t, f.report(2), f.observer)
	drift := f.report(1)
	drift.ProviderWrite = digest([]byte("moved provider boundary"))
	_, driftRaw, _ := signRegistryRehomeFixture(t, drift, f.observer)
	for mode, candidate := range map[string][]byte{
		"no evidence": nil, "owner signature": ownerRaw, "expired": staleRaw,
		"wrong fence": nextFenceRaw, "noncanonical": append(append([]byte{}, raw...), ' '),
		"live drift": driftRaw,
	} {
		t.Run(mode, func(t *testing.T) {
			if got, created, err := dedicated.Reserve(ctx, "fixture-rehome", f.record, f.approval, candidate); err == nil || created || got != (Claim{}) {
				t.Fatalf("unsafe evidence granted a claim: %+v %v %v", got, created, err)
			}
		})
	}
	moving := journal(dedicatedDB)
	observerNow := now
	moving.now = func() time.Time { return observerNow }
	movingGate, err := NewRegistryRehomeJournal(moving, f.intent.scope(), observer, func(context.Context, RegistryRehomeAdmission, Submission) error {
		observerNow = now.Add(30 * time.Second)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expiring := f.report(1)
	expiring.ExpiresAt = now.Add(30 * time.Second).Unix()
	_, expiringRaw, _ := signRegistryRehomeFixture(t, expiring, f.observer)
	if claim, created, err := movingGate.Reserve(ctx, "fixture-rehome", f.record, f.approval, expiringRaw); !errors.Is(err, ErrStaleFence) ||
		created || claim != (Claim{}) {
		t.Fatalf("proof that expired after provider checks was accepted: %+v %v %v", claim, created, err)
	}
	if _, created, err := unprivileged.Reserve(ctx, "fixture-rehome", f.record, f.approval, raw); err == nil || created {
		t.Fatalf("ordinary SQL role could write specialized admission: %v %v", created, err)
	}
	_, err = ordinaryDB.ExecContext(ctx, `INSERT INTO fugue_control.execution_claims
		(cluster_id,resource_id,fence,intent_digest,approval_digest,actor) VALUES ($1,$2,1,$3,$4,'fixture-direct')`,
		f.intent.ClusterID, f.intent.ResourceID, f.record, f.approval)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("direct ordinary SQL claim bypassed required atomic receipt: %v", err)
	}
	if _, created, err := dedicated.Reserve(ctx, "audit-error", f.record, f.approval, raw); err == nil || created {
		t.Fatalf("audit failure did not cancel rehome claim: %v %v", created, err)
	}
	var n int
	if err := ownerDB.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM fugue_control.execution_claims WHERE intent_digest=$1) +
		(SELECT count(*) FROM fugue_control.registry_rehome_admissions WHERE intent_digest=$1)`, f.record).Scan(&n); err != nil || n != 0 {
		t.Fatalf("failed admission left durable claim/evidence: count=%d err=%v", n, err)
	}
	claim, created, err := dedicated.Reserve(ctx, "fixture-rehome", f.record, f.approval, raw)
	if err != nil || !created || claim.Fence != 1 {
		t.Fatalf("reserve exact live move: %+v %v %v", claim, created, err)
	}
	if actual, d, err := dedicated.ReadAdmission(ctx, claim); err != nil || d != pin || !reflect.DeepEqual(actual, signed) {
		t.Fatalf("technical approval was not retained exactly: %+v %s %v", actual, d, err)
	}
	restart, err := NewRegistryRehomeJournal(journal(dedicatedDB), f.intent.scope(), observer, check)
	if err != nil {
		t.Fatal(err)
	}
	if replay, created, err := restart.Reserve(ctx, "fixture-restart", f.record, f.approval, raw); err != nil || created || replay != claim {
		t.Fatalf("restart did not preserve single dispatch: %+v %v %v", replay, created, err)
	}
	newRetention := f.report(1)
	newRetention.RegistryRetention = digest([]byte("new retention observation"))
	_, newRetentionRaw, _ := signRegistryRehomeFixture(t, newRetention, f.observer)
	if replay, created, err := restart.Reserve(ctx, "fixture-restart", f.record, f.approval, newRetentionRaw); !errors.Is(err, ErrStaleFence) ||
		created || replay != (Claim{}) {
		t.Fatalf("a second technical envelope could replace the claim evidence: %+v %v %v", replay, created, err)
	}
	if consumed, err := ordinary.ReserveReleaseLaunch(ctx, "fixture-guardian", f.intent, f.payload, claim.Fence); err == nil || consumed {
		t.Fatalf("registry rehome could be launched as ordinary release: %v %v", consumed, err)
	}
	exec, err := NewExecutionJournal(ordinary, &testExecutionEvidence{terminal: pin, fenced: true})
	if err != nil {
		t.Fatal(err)
	}
	if done, err := exec.CompleteExecution(ctx, "fixture-observer", claim, ExecutionResult{Applied, Version{f.intent.Generation + 1, f.intent.TargetDigest}, pin}); err == nil || done {
		t.Fatalf("a draft digest could become a verified release head: %v %v", done, err)
	}
	_, err = ordinaryDB.ExecContext(ctx, `INSERT INTO fugue_control.execution_results
		(intent_digest,outcome,generation,verified_digest,evidence_digest,actor)
		VALUES ($1,'applied',$2,$3,$4,'fixture-direct')`, f.record, f.intent.Generation+1, f.intent.TargetDigest, pin)
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("direct ordinary result could change the verified head: %v", err)
	}
	state, err := ordinary.ReadResource(ctx, claim.Scope)
	if err != nil || state.Pending != claim.IntentDigest || state.LastVerified.Digest != f.intent.CurrentDigest {
		t.Fatalf("unresolved write dropped the previous verified version: %+v %v", state, err)
	}

	race := anchorRegistryRehomeFixture(t, ctx, ordinary, owner, "rehome-race")
	_, raceRaw, _ := signRegistryRehomeFixture(t, race.report(1), race.observer)
	launch, err := NewRegistryRehomeJournal(specialized, race.intent.scope(), race.observer.Public().(ed25519.PublicKey),
		func(context.Context, RegistryRehomeAdmission, Submission) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	release := race.intent
	release.Operation, release.RequestID = ComponentRelease, "concurrent-component-release"
	id, approve := recordRehomeFixtureApproval(t, ctx, ordinary, release, race.payload, owner, now)
	start := make(chan struct{})
	var wg sync.WaitGroup
	type attempt struct {
		created bool
		err     error
	}
	done := make(chan attempt, 2)
	wg.Go(func() {
		<-start
		_, created, err := launch.Reserve(ctx, "fixture-rehome", race.record, race.approval, raceRaw)
		done <- attempt{created, err}
	})
	wg.Go(func() {
		<-start
		_, created, err := ordinary.ReserveExecution(ctx, "fixture-guardian", id, approve)
		done <- attempt{created, err}
	})
	close(start)
	wg.Wait()
	first, second := <-done, <-done
	if !first.created {
		first, second = second, first
	}
	if !first.created || first.err != nil || second.created || !errors.Is(second.err, ErrResourceBusy) {
		t.Fatalf("same Deployment fence allowed zero or two dispatches: %+v %+v", first, second)
	}
}
