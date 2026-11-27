package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"fugue/internal/productioncontrol"

	_ "github.com/jackc/pgx/v5/stdlib"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

type publicationTestEvidence struct {
	baseline productioncontrol.Baseline
}

func (v publicationTestEvidence) VerifyBaseline(_ context.Context, b productioncontrol.Baseline) error {
	if v.baseline != b {
		return errors.New("test fixture is not enrolled")
	}
	return nil
}

func (publicationTestEvidence) VerifyTerminal(context.Context, productioncontrol.Claim, productioncontrol.Intent, productioncontrol.ExecutionResult) error {
	return errors.New("the publication test never verifies a rollout")
}

func TestNativePublicationPersistsClaimBeforeProviderWrite(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_NATIVE_PUBLICATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_TEST_NATIVE_PUBLICATION_DATABASE_URL to an empty disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var existingSchema bool
	if err := db.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_control') IS NOT NULL`).Scan(&existingSchema); err != nil {
		t.Fatal(err)
	}
	if existingSchema {
		t.Fatal("refusing to run the publication test with a pre-existing Fugue control schema")
	}
	if err := productioncontrol.ApplySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	f := healthyPublicationFixture(t)
	draft, err := f.store.DraftNativeRelease(ctx, f.key, "isolated-cluster", "request-a", f.candidate)
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trusted := productioncontrol.Approvers{"fixture-key": {ClusterID: draft.Intent.ClusterID, PublicKey: pub}}
	journal, err := productioncontrol.NewJournal(db, trusted)
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := journal.RecordIntent(ctx, "fixture-owner", draft.Intent, draft.Payload)
	if err != nil {
		t.Fatal(err)
	}
	baseline := productioncontrol.Baseline{
		Scope:          productioncontrol.Scope{ClusterID: draft.Intent.ClusterID, ResourceID: draft.Intent.ResourceID},
		Verified:       productioncontrol.Version{Generation: draft.Intent.Generation, Digest: draft.Intent.CurrentDigest},
		EvidenceDigest: digest([]byte("isolated provider baseline")),
	}
	evidence := publicationTestEvidence{baseline: baseline}
	execution, err := productioncontrol.NewExecutionJournal(journal, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := execution.AnchorResource(ctx, "fixture-observer", baseline); err != nil || !created {
		t.Fatalf("anchor: created=%t error=%v", created, err)
	}
	// A separate connection pool models a restarted process independently
	// observing the claim from the provider's request boundary.
	readerDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer readerDB.Close()
	reader, err := productioncontrol.NewJournal(readerDB, trusted)
	if err != nil {
		t.Fatal(err)
	}
	readerExecution, err := productioncontrol.NewExecutionJournal(reader, evidence)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewNativePublisher(f.store, execution, publicationAdmissionFunc(func(context.Context, NativePublicationBoundary) error { return nil }), draft.Intent.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, "fixture-executor", recorded.Digest, testDigest); err == nil {
		t.Fatal("publication accepted an artifact without a recorded owner approval")
	}
	if _, _, err := readerExecution.Execution(ctx, baseline.Scope, recorded.Digest); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unapproved operation acquired a durable claim: %v", err)
	}
	approval, err := productioncontrol.SignApproval(draft.Intent, "fixture-key", priv, time.Now(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := journal.RecordApproval(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	f.client.PrependReactor("create", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		writes++
		persisted, terminal, err := readerExecution.Execution(ctx, baseline.Scope, recorded.Digest)
		if err != nil || terminal != nil || persisted.Fence != 1 || persisted.ApprovalDigest != approved.Digest {
			t.Fatalf("provider write preceded durable authorization: claim=%+v terminal=%+v error=%v", persisted, terminal, err)
		}
		return false, nil, nil
	})
	publication, err := publisher.Publish(ctx, "fixture-executor", recorded.Digest, approved.Digest)
	if err != nil || !publication.Submitted || writes != 3 {
		t.Fatalf("publication=%+v writes=%d error=%v", publication, writes, err)
	}
	state, err := readerExecution.Resource(ctx, baseline.Scope)
	if err != nil || state.LastVerified != baseline.Verified || state.Pending != recorded.Digest {
		t.Fatalf("publication advanced verified serving state: state=%+v error=%v", state, err)
	}
	restarted, err := NewNativePublisher(f.store, readerExecution, publisher.admission, draft.Intent.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.Publish(ctx, "fixture-executor", recorded.Digest, approved.Digest)
	if err != nil || replayed.Submitted || replayed.Claim != publication.Claim || writes != 3 {
		t.Fatalf("replayed publication=%+v writes=%d error=%v", replayed, writes, err)
	}
}
