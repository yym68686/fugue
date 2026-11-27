package participation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

type pendingFixtureEvidence struct{}

func (pendingFixtureEvidence) VerifyBaseline(context.Context, productioncontrol.Baseline) error {
	return nil // Disposable database only.
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
	const node, ns = "node-a", "fixture"
	var live atomic.Pointer[corev1.Node]
	var nodeReads atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("observer sent a write request")
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/" + ns:
			_ = json.NewEncoder(w).Encode(corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, UID: "ns-a", ResourceVersion: "1"}})
		case "/api/v1/nodes/" + node:
			nodeReads.Add(1)
			_ = json.NewEncoder(w).Encode(live.Load())
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	topology.Policy.Kubernetes.Endpoint = server.URL
	topology.Policy.Kubernetes.CADigest = digest(server.Certificate().Raw)
	restConfig := &rest.Config{Host: server.URL, Timeout: 2 * time.Second, TLSClientConfig: rest.TLSClientConfig{
		CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	}}
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
	claim, created, err := runtime.ReserveExecution(ctx, "fixture-writer", hold, approval)
	if err != nil || !created {
		t.Fatalf("reserve fixture claim: %+v %v", claim, err)
	}
	visible := before.DeepCopy()
	visible.Annotations, visible.ResourceVersion = after, "11"
	live.Store(visible)
	report, err := ObservePendingOfflineHold(ctx, reopened, restConfig, "fixture", hold)
	if err != nil || report.Provider.State != OfflineHoldTargetVisible || report.Fence != claim.Fence ||
		report.ApprovalDigest != approval || report.LastVerified != predecessor || nodeReads.Load() != 2 {
		t.Fatalf("observer did not bind live state to persisted claim without advancing LKG: %+v %v", report, err)
	}
	stored, terminal, err := reopened.ReadExecution(ctx, scope, hold)
	if err != nil || stored != claim || terminal != nil {
		t.Fatalf("observer changed the durable claim: %+v %+v %v", stored, terminal, err)
	}
	if replay, created, err := runtime.ReserveExecution(ctx, "fixture-writer", hold, approval); err != nil || created || replay != claim {
		t.Fatalf("observation refreshed dispatch permission: %+v %v %v", replay, created, err)
	}
}
