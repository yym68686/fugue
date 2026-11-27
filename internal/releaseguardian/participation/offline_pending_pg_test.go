package participation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

type fixtureOfflineWitnesses map[string][]byte

func (s fixtureOfflineWitnesses) ReadExecutionWitness(_ context.Context, digest string) ([]byte, error) {
	return s[digest], nil
}

func storeFixtureOfflineWitness(t *testing.T, source fixtureOfflineWitnesses, signer ed25519.PrivateKey, w productioncontrol.ExecutionWitness) string {
	t.Helper()
	message, err := w.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	w.Signature = ed25519.Sign(signer, message)
	raw, digest, err := productioncontrol.EncodeExecutionWitness(w)
	if err != nil {
		t.Fatal(err)
	}
	source[digest] = raw
	return digest
}

func TestPendingOfflineHoldReadsReopenedPostgresJournal(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_PARTICIPATION_CONTROL_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_TEST_PARTICIPATION_CONTROL_DATABASE_URL to an empty disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_control') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatalf("expected an empty isolated journal schema: exists=%v err=%v", exists, err)
	}
	if err := productioncontrol.ApplySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	pub, owner, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := productioncontrol.Approvers{"owner": {ClusterID: "fixture", PublicKey: pub}}
	j, err := productioncontrol.NewJournal(db, keys)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	topology := offlineFixtureTopology(now)
	topology.Policy.Readers.CanaryProber = "canary-prober"
	const node, ns = "node-a", "fixture"
	const writer = "system:serviceaccount:fixture:hold-writer"
	var live atomic.Pointer[corev1.Node]
	var target atomic.Pointer[corev1.Node]
	var nodeReads, writes atomic.Int32
	var readGuard atomic.Pointer[installedOfflineGuard]
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/apis/authentication.k8s.io/v1/selfsubjectreviews" && r.Header.Get("Authorization") == "Bearer fixture-writer":
			_ = json.NewEncoder(w).Encode(authv1.SelfSubjectReview{Status: authv1.SelfSubjectReviewStatus{
				UserInfo: authv1.UserInfo{Username: writer},
			}})
			return
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/nodes/"+node && r.Header.Get("Authorization") == "Bearer fixture-writer":
			var ops []nodePatchOperation
			var body json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || json.Unmarshal(body, &ops) != nil || len(ops) != 4 ||
				ops[0] != (nodePatchOperation{Op: "test", Path: "/metadata/uid", Value: string(live.Load().UID)}) ||
				ops[1] != (nodePatchOperation{Op: "test", Path: "/metadata/resourceVersion", Value: live.Load().ResourceVersion}) {
				t.Error("dispatch did not condition the write on the stored predecessor")
				http.Error(w, "unexpected patch", http.StatusBadRequest)
				return
			}
			patch, err := jsonpatch.DecodePatch(body)
			if err != nil {
				t.Errorf("invalid RFC 6902 patch: %v", err)
				http.Error(w, "invalid patch", http.StatusBadRequest)
				return
			}
			current, _ := json.Marshal(live.Load())
			patched, err := patch.Apply(current)
			var updated corev1.Node
			if err != nil || json.Unmarshal(patched, &updated) != nil || !maps.Equal(updated.Annotations, target.Load().Annotations) {
				t.Errorf("patch did not apply the exact target annotations: %v", err)
				http.Error(w, "wrong target annotations", http.StatusBadRequest)
				return
			}
			writes.Add(1)
			live.Store(target.Load())
			// Simulate a committed write whose HTTP response is lost.
			panic(http.ErrAbortHandler)
		case r.Method == http.MethodGet && r.Header.Get("Authorization") == "Bearer fixture-reader":
		default:
			t.Errorf("unexpected provider request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/api/v1/namespaces/" + ns:
			_ = json.NewEncoder(w).Encode(corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, UID: "ns-a", ResourceVersion: "1"}})
		case "/api/v1/nodes/" + node:
			nodeReads.Add(1)
			_ = json.NewEncoder(w).Encode(live.Load())
		default:
			fact := offlineDispatchProviderFixture(r, live.Load(), readGuard.Load())
			if fact == nil {
				t.Errorf("unexpected path %s", r.URL.Path)
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(fact)
		}
	}))
	defer server.Close()
	topology.Policy.Kubernetes.Endpoint = server.URL
	topology.Policy.Kubernetes.CADigest = digest(server.Certificate().Raw)
	restConfig := &rest.Config{Host: server.URL, BearerToken: "fixture-reader", Timeout: 2 * time.Second, TLSClientConfig: rest.TLSClientConfig{
		CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	}}
	writeConfig := rest.CopyConfig(restConfig)
	writeConfig.BearerToken = "fixture-writer"
	record := func(s productioncontrol.Submission) (string, string) {
		t.Helper()
		r, err := j.RecordIntent(ctx, "fixture-owner", s.Intent, s.Payload)
		if err != nil || !r.Created {
			t.Fatalf("record fixture intent: %+v %v", r, err)
		}
		a, err := productioncontrol.SignApproval(s.Intent, "owner", owner, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		ra, err := j.RecordApproval(ctx, a)
		if err != nil || !ra.Created {
			t.Fatalf("record fixture approval: %+v %v", ra, err)
		}
		return r.Digest, ra.Digest
	}
	policy, err := json.Marshal(topology.Policy)
	if err != nil {
		t.Fatal(err)
	}
	draft, err := productioncontrol.DraftParticipationTopology("fixture", "topology-a", productioncontrol.Version{}, policy)
	if err != nil {
		t.Fatal(err)
	}
	intake, approval := record(draft)
	if _, err := j.ActivateParticipationTopology(ctx, "fixture", "fixture-owner", intake, approval); err != nil {
		t.Fatal(err)
	}
	topology, err = j.ReadParticipationTopology(ctx, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	s, before, after := draftFixtureOfflineHold(t, topology, time.Now().UTC())
	hold, approval := record(s)
	scope := HoldResource("fixture", node)
	predecessor := productioncontrol.Version{Generation: s.Intent.Generation, Digest: s.Intent.CurrentDigest}
	settlementKey, settlementSigner, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	witnesses := fixtureOfflineWitnesses{}
	runtime, err := NewOfflineHoldSettlement(j, restConfig, "fixture", node, witnesses, settlementKey)
	if err != nil {
		t.Fatal(err)
	}
	live.Store(before)
	closed := fixtureClosedOfflineGuard(t, node)
	readGuard.Store(&closed)
	baseline, err := ObserveOfflineHoldCheckpoint(ctx, j, restConfig, "fixture", node, "")
	if err != nil {
		t.Fatal(err)
	}
	baselineDigest, err := baseline.Digest()
	if err != nil {
		t.Fatal(err)
	}
	makeWitness := func() productioncontrol.ExecutionWitness {
		at := time.Now().UTC()
		return productioncontrol.ExecutionWitness{
			KeyID: "provider-observer", ClusterID: scope.ClusterID, ResourceID: scope.ResourceID,
			ObservedAt: at.Unix(), ExpiresAt: at.Add(time.Minute).Unix(),
			AuthorityDigest: digest([]byte("synthetic closed fixture")),
		}
	}
	anchor := makeWitness()
	anchor.Kind, anchor.Generation, anchor.VerifiedDigest, anchor.ObservationDigest = productioncontrol.BaselineWitness, predecessor.Generation, predecessor.Digest, baselineDigest
	anchorID := storeFixtureOfflineWitness(t, witnesses, settlementSigner, anchor)
	if created, err := runtime.AnchorResource(ctx, "fixture-observer", productioncontrol.Baseline{
		Scope: scope, Verified: predecessor, EvidenceDigest: anchorID,
	}); err != nil || !created {
		t.Fatal(err)
	}
	beforeUnclaimed := nodeReads.Load()
	readerCfg := cfg.Copy()
	readerCfg.RuntimeParams["default_transaction_read_only"] = "on"
	readerDB := stdlib.OpenDB(*readerCfg)
	defer readerDB.Close()
	var readonly string
	if err := readerDB.QueryRowContext(ctx, `SHOW transaction_read_only`).Scan(&readonly); err != nil || readonly != "on" {
		t.Fatalf("observer connection is not read-only: %q, %v", readonly, err)
	}
	reopened, err := productioncontrol.NewJournal(readerDB, keys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ObservePendingOfflineHold(ctx, reopened, restConfig, "fixture", hold); err == nil || nodeReads.Load() != beforeUnclaimed {
		t.Fatal("unclaimed intent read provider state")
	}
	guard := fixtureInstalledOfflineGuard(t, s, writer)
	readGuard.Store(&guard)
	boundary, err := OfflineHoldAuthorityBoundary(topology, s, writer, guard.Observation)
	if err != nil {
		t.Fatal(err)
	}
	observer, attestation := signFixtureOfflineAuthority(t, boundary, time.Now().UTC())
	visible := before.DeepCopy()
	visible.Annotations, visible.ResourceVersion = after, "11"
	live.Store(before)
	target.Store(visible)
	if _, err := DispatchOfflineHold(ctx, j, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, hold, approval, pub, attestation); err == nil || writes.Load() != 0 {
		t.Fatal("owner signing key could also act as the independent provider observer")
	}
	dispatched, err := DispatchOfflineHold(ctx, j, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, hold, approval, observer, attestation)
	if err == nil || !dispatched.Reserved || writes.Load() != 1 || dispatched.ProviderResponseVersion != "" {
		t.Fatalf("committed request with lost response was not left pending: %+v %v writes=%d", dispatched, err, writes.Load())
	}
	claim := dispatched.Claim
	beforeReadback := nodeReads.Load()
	currentVersion := topology.Version
	successorPolicy := topology.Policy
	successorPolicy.Generation++
	raw, err := json.Marshal(successorPolicy)
	if err != nil {
		t.Fatal(err)
	}
	successor, err := productioncontrol.DraftParticipationTopology("fixture", "topology-b", currentVersion, raw)
	if err != nil {
		t.Fatal(err)
	}
	successorIntent, successorApproval := record(successor)
	if _, err := j.ActivateParticipationTopology(ctx, "fixture", "fixture-owner", successorIntent, successorApproval); !errors.Is(err, productioncontrol.ErrResourceBusy) {
		t.Fatalf("topology moved while a Node claim was pending: %v", err)
	}
	report, err := ObservePendingOfflineHold(ctx, reopened, restConfig, "fixture", hold)
	if err != nil || report.Provider.State != OfflineHoldTargetVisible || report.Fence != claim.Fence ||
		report.ApprovalDigest != approval || report.LastVerified != predecessor || nodeReads.Load() != beforeReadback+2 {
		t.Fatalf("observer did not bind live state to persisted claim without advancing LKG: %+v %v", report, err)
	}
	stored, priorResult, err := reopened.ReadExecution(ctx, scope, hold)
	if err != nil || stored != claim || priorResult != nil {
		t.Fatalf("observer changed the durable claim: %+v %+v %v", stored, priorResult, err)
	}
	if replay, err := DispatchOfflineHold(ctx, reopened, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, hold, approval, observer, attestation); err != nil ||
		replay.Reserved || replay.Claim != claim || writes.Load() != 1 {
		t.Fatalf("reopened worker attempted a second dispatch: %+v %v writes=%d", replay, err, writes.Load())
	}
	if _, err := ObserveOfflineHoldCheckpoint(ctx, j, restConfig, "fixture", node, hold); err == nil {
		t.Fatal("open guard qualified as a closed terminal checkpoint")
	}
	terminal := makeWitness()
	terminal.Kind, terminal.IntentDigest, terminal.ApprovalDigest, terminal.Fence = productioncontrol.TerminalWitness, hold, approval, claim.Fence
	terminal.Outcome, terminal.Generation, terminal.VerifiedDigest = productioncontrol.Applied, predecessor.Generation+1, s.Intent.TargetDigest
	terminal.ObservationDigest = digest([]byte("wrong observation"))
	badID := storeFixtureOfflineWitness(t, witnesses, settlementSigner, terminal)
	result := productioncontrol.ExecutionResult{Outcome: terminal.Outcome,
		Verified: productioncontrol.Version{Generation: terminal.Generation, Digest: terminal.VerifiedDigest}, EvidenceDigest: badID}
	if created, err := runtime.CompleteExecution(ctx, "fixture-observer", claim, result); err == nil || created {
		t.Fatal("open guard allowed settlement despite a trusted signature")
	}
	closed.Policy.ResourceVersion, closed.Binding.ResourceVersion = "31", "32"
	readGuard.Store(&closed)
	checkpoint, err := ObserveOfflineHoldCheckpoint(ctx, j, restConfig, "fixture", node, hold)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := runtime.CompleteExecution(ctx, "fixture-observer", claim, result); err == nil || created {
		t.Fatal("closed guard allowed settlement for a different signed observation")
	}
	terminal.ObservationDigest, err = checkpoint.Digest()
	if err != nil {
		t.Fatal(err)
	}
	delayed := terminal
	delayed.Outcome, delayed.Generation, delayed.VerifiedDigest = productioncontrol.NotApplied, predecessor.Generation, predecessor.Digest
	wrongOutcome := productioncontrol.ExecutionResult{Outcome: delayed.Outcome, Verified: predecessor,
		EvidenceDigest: storeFixtureOfflineWitness(t, witnesses, settlementSigner, delayed)}
	if created, err := runtime.CompleteExecution(ctx, "fixture-observer", claim, wrongOutcome); err == nil || created {
		t.Fatal("signed NotApplied displaced the actual target")
	}
	invalidGeneration := terminal
	invalidGeneration.Generation++
	wrongGeneration := productioncontrol.ExecutionResult{Outcome: terminal.Outcome,
		Verified:       productioncontrol.Version{Generation: invalidGeneration.Generation, Digest: terminal.VerifiedDigest},
		EvidenceDigest: storeFixtureOfflineWitness(t, witnesses, settlementSigner, invalidGeneration)}
	if created, err := runtime.CompleteExecution(ctx, "fixture-observer", claim, wrongGeneration); err == nil || created {
		t.Fatal("signed witness skipped the next generation")
	}
	result.EvidenceDigest = storeFixtureOfflineWitness(t, witnesses, settlementSigner, terminal)
	if created, err := runtime.CompleteExecution(ctx, "fixture-observer", claim, result); err != nil || !created {
		t.Fatalf("fixture settlement: %v, %v", created, err)
	}
	head, err := j.ReadResource(ctx, scope)
	if err != nil || head.Pending != "" || head.LastVerified != result.Verified {
		t.Fatalf("settlement failed to advance the verified head: %+v %v", head, err)
	}
	if created, err := runtime.CompleteExecution(ctx, "fixture-observer", claim, result); err != nil || created {
		t.Fatalf("settlement did not replay the durable receipt: %v, %v", created, err)
	}
	// The readmission authorizes a new operation, not a replay/reversal
	// of the original execution receipt. Draft/review only from the closed fence.
	const rollbackRequest = "offline-rollback-a"
	rollback, err := DraftOfflineHoldRollback(ctx, reopened, restConfig, "fixture", hold, rollbackRequest)
	if err != nil {
		t.Fatal(err)
	}
	p, err := decodeHold(rollback)
	if err != nil || p.Reverses != hold || rollback.Intent.TargetDigest != predecessor.Digest || rollback.Intent.Generation != result.Verified.Generation {
		t.Fatalf("wrong rollback binding: %+v %v", p, err)
	}
	guardRollback := fixtureInstalledOfflineGuard(t, rollback, writer)
	guardRollback.Policy.ResourceVersion, guardRollback.Binding.ResourceVersion = "33", "34"
	readGuard.Store(&guardRollback)
	observerReader, err := NewBoundKubernetesObserver(restConfig, topology.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observerReader.ObserveOfflineHoldRollbackBoundary(ctx, topology, rollback, writer, time.Now()); err == nil {
		t.Fatal("NotReady Node passed rollback readmission")
	}
	// A changed resourceVersion forces a new draft even if the owned fields
	// happen to be identical. Readiness data is controlled by the provider.
	ready := visible.DeepCopy()
	ready.ResourceVersion = "12"
	ready.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-20 * time.Minute))}}
	live.Store(ready)
	if _, err := observerReader.ObserveOfflineHoldRollbackBoundary(ctx, topology, rollback, writer, time.Now()); err == nil {
		t.Fatal("stale resourceVersion passed rollback readmission")
	}
	closed.Policy.ResourceVersion, closed.Binding.ResourceVersion = "35", "36"
	readGuard.Store(&closed)
	rollback, err = DraftOfflineHoldRollback(ctx, reopened, restConfig, "fixture", hold, "offline-rollback-reviewed")
	if err != nil {
		t.Fatal(err)
	}
	p, err = decodeHold(rollback)
	if err != nil {
		t.Fatal(err)
	}
	forgedOrigin, err := decodeHold(s)
	if err != nil {
		t.Fatal(err)
	}
	changed := "not the predecessor"
	forgedOrigin.From.Annotations[participationAnnotation] = &changed
	forged, err := encodeHoldRollback("fixture", "offline-rollback-forged", forgedOrigin, hold, p.ResourceVersion, result.Verified, p.ShadowReportDigest)
	if err != nil {
		t.Fatal(err)
	}
	forgedID, forgedApproval := record(forged)
	if _, created, err := j.ReserveExecution(ctx, "fixture-writer", forgedID, forgedApproval); !errors.Is(err, productioncontrol.ErrResourceDrift) || created {
		t.Fatalf("journal reserved a different rollback target: %v, %v", created, err)
	}
	rollbackID, rollbackApproval := record(rollback)
	guardRollback = fixtureInstalledOfflineGuard(t, rollback, writer)
	guardRollback.Policy.ResourceVersion, guardRollback.Binding.ResourceVersion = "37", "38"
	readGuard.Store(&guardRollback)
	at := time.Now().UTC()
	positiveBoundary, err := observerReader.ObserveOfflineHoldRollbackBoundary(ctx, topology, rollback, writer, at)
	if err != nil {
		t.Fatal(err)
	}
	rollbackObserver, withoutReadmission := signFixtureOfflineAuthority(t, positiveBoundary.Digest, at)
	if dispatch, err := DispatchOfflineHold(ctx, j, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, rollbackID, rollbackApproval, rollbackObserver, withoutReadmission); err == nil || dispatch.Reserved || writes.Load() != 1 {
		t.Fatalf("dispatch gained write authority without positive recovery evidence: %+v %v", dispatch, err)
	}
	recovered := &OfflineHoldReadmission{
		NetworkRecovery: digest([]byte("fixture approved recovery")), ServingAuthority: digest([]byte("fixture current Front/Worker")),
		ServingFence: digest([]byte("fixture serving authority fence")),
		StableSince:  at.Add(-10 * time.Minute).Unix(), ActivatedAt: at.Add(-11 * time.Minute).Unix(),
		NetworkSuccessAt: at.Add(-time.Minute).Unix(),
	}
	rollbackObserver, rollbackAttestation := signFixtureOfflineAuthority(t, positiveBoundary.Digest, at, recovered)
	restored := ready.DeepCopy()
	restored.Annotations, restored.ResourceVersion = maps.Clone(before.Annotations), "13"
	target.Store(restored)
	if dispatch, err := DispatchOfflineHold(ctx, j, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, rollbackID, approval, rollbackObserver, rollbackAttestation); err == nil || dispatch.Reserved || writes.Load() != 1 {
		t.Fatalf("original approval authorized a rollback: %+v %v", dispatch, err)
	}
	dispatched, err = DispatchOfflineHold(ctx, j, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, rollbackID, rollbackApproval, rollbackObserver, rollbackAttestation)
	if err == nil || !dispatched.Reserved || writes.Load() != 2 {
		t.Fatalf("rollback lost response did not leave one pending write: %+v %v", dispatched, err)
	}
	rollbackClaim := dispatched.Claim
	pending, err := ObservePendingOfflineHold(ctx, reopened, restConfig, "fixture", rollbackID)
	if err != nil || pending.Fence != claim.Fence+1 || pending.LastVerified != result.Verified ||
		pending.Provider.State != OfflineHoldTargetVisible || pending.Provider.ObservedDigest != predecessor.Digest {
		t.Fatalf("rollback readback didn't retain prior LKG while pending: %+v %v", pending, err)
	}
	if replay, err := DispatchOfflineHold(ctx, reopened, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, rollbackID, rollbackApproval, rollbackObserver, nil); err != nil ||
		replay.Reserved || replay.Claim != rollbackClaim || writes.Load() != 2 {
		t.Fatalf("rollback replay requested another write: %+v %v", replay, err)
	}
	closed.Policy.ResourceVersion, closed.Binding.ResourceVersion = "43", "44"
	readGuard.Store(&closed)
	checkpoint, err = ObserveOfflineHoldCheckpoint(ctx, reopened, restConfig, "fixture", node, rollbackID)
	if err != nil {
		t.Fatal(err)
	}
	rollbackTerminal := makeWitness()
	rollbackTerminal.Kind, rollbackTerminal.IntentDigest, rollbackTerminal.ApprovalDigest, rollbackTerminal.Fence = productioncontrol.TerminalWitness, rollbackID, rollbackApproval, rollbackClaim.Fence
	rollbackTerminal.Outcome, rollbackTerminal.Generation, rollbackTerminal.VerifiedDigest = productioncontrol.Applied, result.Verified.Generation+1, predecessor.Digest
	rollbackTerminal.ObservationDigest, err = checkpoint.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rolledBack := productioncontrol.ExecutionResult{Outcome: productioncontrol.Applied,
		Verified:       productioncontrol.Version{Generation: rollbackTerminal.Generation, Digest: predecessor.Digest},
		EvidenceDigest: storeFixtureOfflineWitness(t, witnesses, settlementSigner, rollbackTerminal)}
	if created, err := runtime.CompleteExecution(ctx, "fixture-observer", rollbackClaim, rolledBack); err != nil || !created {
		t.Fatalf("independent rollback settlement failed: %v, %v", created, err)
	}
	if head, err := reopened.ReadResource(ctx, scope); err != nil || head.Pending != "" || head.LastVerified != rolledBack.Verified {
		t.Fatalf("rollback didn't restore the exact predecessor as a new generation: %+v %v", head, err)
	}
	if _, err := DraftOfflineHoldRollback(ctx, reopened, restConfig, "fixture", hold, "offline-rollback-superseded"); err == nil {
		t.Fatal("superseded original hold was eligible to revert again")
	}
	if next, err := j.ActivateParticipationTopology(ctx, "fixture", "fixture-owner", successorIntent, successorApproval); err != nil || !next.Created {
		t.Fatalf("settled operation prevented topology advancement: %+v, %v", next, err)
	}
	newRequest := s
	newRequest.Intent.RequestID = "stale-topology"
	newRequest.Intent.Generation = rolledBack.Verified.Generation
	stale, staleApproval := record(newRequest)
	if _, created, err := j.ReserveExecution(ctx, "fixture-writer", stale, staleApproval); !errors.Is(err, productioncontrol.ErrResourceDrift) || created {
		t.Fatalf("new claim on previous topology was accepted: %v, %v", created, err)
	}

}

func fixtureClosedOfflineGuard(t *testing.T, node string) installedOfflineGuard {
	t.Helper()
	p, b, err := FreezeNodePolicy(node)
	if err != nil {
		t.Fatal(err)
	}
	p.UID, p.ResourceVersion, p.Generation = "policy", "19", 1
	p.Status.ObservedGeneration, p.Status.TypeChecking = 1, &admissionv1.TypeChecking{}
	b.UID, b.ResourceVersion = "binding", "20"
	return installedOfflineGuard{Policy: p, Binding: b}
}

type installedOfflineGuard struct {
	Policy      *admissionv1.ValidatingAdmissionPolicy
	Binding     *admissionv1.ValidatingAdmissionPolicyBinding
	Observation FreezeObservation
}

func fixtureInstalledOfflineGuard(t *testing.T, s productioncontrol.Submission, writer string) installedOfflineGuard {
	t.Helper()
	policy, binding, err := RenderOfflineHoldGuard(s, writer)
	if err != nil {
		t.Fatal(err)
	}
	policy.UID, policy.ResourceVersion, policy.Generation = "policy", "21", 1
	policy.Status.ObservedGeneration, policy.Status.TypeChecking = 1, &admissionv1.TypeChecking{}
	binding.UID, binding.ResourceVersion = "binding", "22"
	return installedOfflineGuard{Policy: policy, Binding: binding, Observation: FreezeObservation{
		Node: "node-a", Policy: describeObject(policy), Binding: describeObject(binding), CurrentTypeChecked: true,
	}}
}

func offlineDispatchProviderFixture(r *http.Request, live *corev1.Node, guard *installedOfflineGuard) any {
	switch path := r.URL.Path; {
	case path == "/api/v1/nodes":
		peer := live.DeepCopy()
		peer.Name, peer.UID = "node-b", "uid-b"
		peer.Annotations = nil
		return corev1.NodeList{ListMeta: metav1.ListMeta{ResourceVersion: "40"}, Items: []corev1.Node{*live, *peer}}
	case path == "/api/v1/namespaces/fixture/services":
		return corev1.ServiceList{ListMeta: metav1.ListMeta{ResourceVersion: "40"}}
	case strings.HasPrefix(path, "/apis/apps/v1/namespaces/fixture/daemonsets/"):
		name := strings.TrimPrefix(path, "/apis/apps/v1/namespaces/fixture/daemonsets/")
		return appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fixture", UID: types.UID(name), ResourceVersion: "41"},
			Spec: appsv1.DaemonSetSpec{UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{participationGroup: "cell-a"}}}}}
	case guard != nil && path == "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"+guard.Policy.Name:
		return guard.Policy
	case guard != nil && path == "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/"+guard.Binding.Name:
		return guard.Binding
	}
	for _, role := range []string{"guardian", "canary-prober"} {
		d, rs, p := readerObjects(role)
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/fixture/deployments/" + d.Name:
			return d
		case "/apis/apps/v1/namespaces/fixture/replicasets/" + rs.Name:
			return rs
		case "/api/v1/namespaces/fixture/pods":
			if r.URL.Query().Get("labelSelector") == "reader="+role {
				return corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "42"}, Items: []corev1.Pod{p}}
			}
		}
	}
	return nil
}
