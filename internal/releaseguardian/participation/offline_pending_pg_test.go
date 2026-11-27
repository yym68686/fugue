package participation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
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
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

type pendingFixtureEvidence struct{}

func (pendingFixtureEvidence) VerifyBaseline(context.Context, productioncontrol.Baseline) error {
	return nil // Disposable database only.
}

type pendingFixtureTerminalEvidence struct{ pendingFixtureEvidence }

func (pendingFixtureTerminalEvidence) VerifyTerminal(context.Context, productioncontrol.Claim, productioncontrol.Intent, productioncontrol.ExecutionResult) error {
	return nil // Used only for the synthetic transition at the end of this test.
}

func (pendingFixtureEvidence) VerifyTerminal(context.Context, productioncontrol.Claim, productioncontrol.Intent, productioncontrol.ExecutionResult) error {
	panic("this path must not try to settle a claim")
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
			if err := json.NewDecoder(r.Body).Decode(&ops); err != nil || len(ops) != 4 ||
				ops[0] != (nodePatchOperation{Op: "test", Path: "/metadata/uid", Value: string(live.Load().UID)}) ||
				ops[1] != (nodePatchOperation{Op: "test", Path: "/metadata/resourceVersion", Value: live.Load().ResourceVersion}) {
				t.Error("dispatch did not condition the write on the stored predecessor")
				http.Error(w, "unexpected patch", http.StatusBadRequest)
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
	runtime, err := productioncontrol.NewExecutionJournal(j, pendingFixtureEvidence{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AnchorResource(ctx, "fixture-observer", productioncontrol.Baseline{
		Scope: scope, Verified: predecessor, EvidenceDigest: digest([]byte("fixture baseline")),
	}); err != nil {
		t.Fatal(err)
	}
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
	if _, err := ObservePendingOfflineHold(ctx, reopened, restConfig, "fixture", hold); err == nil || nodeReads.Load() != 0 {
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
	stored, terminal, err := reopened.ReadExecution(ctx, scope, hold)
	if err != nil || stored != claim || terminal != nil {
		t.Fatalf("observer changed the durable claim: %+v %+v %v", stored, terminal, err)
	}
	if replay, err := DispatchOfflineHold(ctx, reopened, restConfig, writeConfig, nil, "fixture", "fixture-writer", writer, hold, approval, observer, attestation); err != nil ||
		replay.Reserved || replay.Claim != claim || writes.Load() != 1 {
		t.Fatalf("reopened worker attempted a second dispatch: %+v %v writes=%d", replay, err, writes.Load())
	}
	terminalWriter, err := productioncontrol.NewExecutionJournal(j, pendingFixtureTerminalEvidence{})
	if err != nil {
		t.Fatal(err)
	}
	result := productioncontrol.ExecutionResult{Outcome: productioncontrol.NotApplied, Verified: predecessor,
		EvidenceDigest: digest([]byte("synthetic terminal fence"))}
	if created, err := terminalWriter.CompleteExecution(ctx, "fixture-observer", claim, result); err != nil || !created {
		t.Fatalf("fixture settlement: %v, %v", created, err)
	}
	if next, err := j.ActivateParticipationTopology(ctx, "fixture", "fixture-owner", successorIntent, successorApproval); err != nil || !next.Created {
		t.Fatalf("settled operation prevented topology advancement: %+v, %v", next, err)
	}
	newRequest := s
	newRequest.Intent.RequestID = "stale-topology"
	stale, staleApproval := record(newRequest)
	if _, created, err := runtime.ReserveExecution(ctx, "fixture-writer", stale, staleApproval); !errors.Is(err, productioncontrol.ErrResourceDrift) || created {
		t.Fatalf("new claim on previous topology was accepted: %v, %v", created, err)
	}

	current, err := j.ReadParticipationTopology(ctx, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	racingHold, _, _ := draftFixtureOfflineHold(t, current, time.Now().UTC())
	racingHold.Intent.RequestID = "racing-hold"
	raceIntent, raceApproval := record(racingHold)
	successorPolicy.Generation++
	raw, err = json.Marshal(successorPolicy)
	if err != nil {
		t.Fatal(err)
	}
	successor, err = productioncontrol.DraftParticipationTopology("fixture", "topology-c", current.Version, raw)
	if err != nil {
		t.Fatal(err)
	}
	successorIntent, successorApproval = record(successor)
	start := make(chan struct{})
	type transition struct {
		created bool
		err     error
	}
	reserved, activated := make(chan transition, 1), make(chan transition, 1)
	go func() {
		<-start
		_, created, err := runtime.ReserveExecution(ctx, "fixture-writer", raceIntent, raceApproval)
		reserved <- transition{created, err}
	}()
	go func() {
		<-start
		r, err := j.ActivateParticipationTopology(ctx, "fixture", "fixture-owner", successorIntent, successorApproval)
		activated <- transition{r.Created, err}
	}()
	close(start)
	h, a := <-reserved, <-activated
	if h.created && h.err == nil && !a.created && errors.Is(a.err, productioncontrol.ErrResourceBusy) {
		return
	}
	if a.created && a.err == nil && !h.created && errors.Is(h.err, productioncontrol.ErrResourceDrift) {
		return
	}
	t.Fatalf("concurrent topology activation and hold must have one winner: reserve=%+v activate=%+v", h, a)
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
