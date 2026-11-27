package productioncontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

const participationFixture = `{
	"schema":"fugue.automatic-node-participation/v1","generation":2,"namespace":"fixture",
	"origin":"https://api.fixture.invalid","readers":{"guardian":"guardian","canary-prober":"canary"},
	"members":[{"node":"member-one","node_uid":"uid-a","group":"cell-a","component":"control",
	"front":"front","workers":{"a":"worker-a","b":"worker-b"},"reason":"network_unavailable",
	"adopt_hold":true,"stable_seconds":300,"probes":[{"host":"a.fixture.invalid","path":"/"},
	{"host":"b.fixture.invalid","path":"/health"}]}]}`

func TestParticipationTopologyBindsApprovedPhysicalIdentity(t *testing.T) {
	initial, err := DraftParticipationTopology("cluster-a", "import-a", Version{}, []byte(participationFixture))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := validateParticipationIntent(initial.Intent, initial.Payload)
	if err != nil || policy.Generation != 2 || policy.Members[0].NodeUID != "uid-a" {
		t.Fatalf("lost legacy generation or physical node: %+v %v", policy, err)
	}
	next := policy
	next.Generation++
	next.Members[0].NodeUID = "uid-b"
	payload, err := json.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	version := Version{policy.Generation, initial.Intent.TargetDigest}
	successor, err := DraftParticipationTopology("cluster-a", "replace-a", version, payload)
	if err != nil || successor.Intent.RollbackDigest != version.Digest || successor.Intent.TargetDigest == version.Digest {
		t.Fatalf("successor not bound to previous version: %+v %v", successor, err)
	}
	if _, err := DraftParticipationTopology("cluster-a", "replace-a", version, []byte(participationFixture)); err == nil {
		t.Fatal("accepted a nonconsecutive revision")
	}
	next.Members[0].AdoptHold = nil
	raw, _ := json.Marshal(next)
	if _, err := DecodeParticipationPolicy(raw); err == nil {
		t.Fatal("accepted a missing explicit hold policy")
	}
}

func TestParticipationTopologyBindsKubernetesInVersionTwo(t *testing.T) {
	legacy, err := DraftParticipationTopology("cluster-a", "import", Version{}, []byte(participationFixture))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := validateParticipationIntent(legacy.Intent, legacy.Payload)
	if err != nil || policy.Kubernetes != nil || strings.Contains(string(legacy.Payload), `"kubernetes"`) {
		t.Fatalf("legacy payload changed: %v", err)
	}
	policy.Generation++
	policy.Schema = BoundParticipationPolicySchema
	policy.Kubernetes = &ParticipationKubernetesBinding{
		Endpoint: "https://kube.fixture.invalid:6443", CADigest: digest([]byte("CA")), NamespaceUID: "namespace-a",
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	next, err := DraftParticipationTopology("cluster-a", "bind-kubernetes", Version{policy.Generation - 1, legacy.Intent.TargetDigest}, raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := validateParticipationIntent(next.Intent, next.Payload)
	if err != nil || got.Kubernetes == nil || *got.Kubernetes != *policy.Kubernetes {
		t.Fatalf("approved policy did not bind Kubernetes: %+v %v", got.Kubernetes, err)
	}
	altered := strings.Replace(string(next.Payload), "namespace-a", "namespace-b", 1)
	if _, err := validateParticipationIntent(next.Intent, []byte(altered)); err == nil {
		t.Fatal("an unsigned namespace replacement survived topology validation")
	}
	duplicate := strings.Replace(string(raw), `"namespace_uid":"namespace-a"`, `"namespace_uid":"namespace-b","namespace_uid":"namespace-a"`, 1)
	if _, err := DecodeParticipationPolicy([]byte(duplicate)); err == nil {
		t.Fatal("duplicate namespace UID accepted")
	}
}

func testPostgresParticipationTopology(t *testing.T, ctx context.Context, db, runtime *sql.DB) {
	t.Helper()
	fixture, _, private, trusted, now := approvalFixture(t)
	j, err := NewJournal(runtime, trusted)
	if err != nil {
		t.Fatal(err)
	}
	j.now = func() time.Time { return now }
	cluster := fixture.ClusterID
	register := func(request string, previous Version, raw []byte) (string, string) {
		t.Helper()
		submission, err := DraftParticipationTopology(cluster, request, previous, raw)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := j.RecordIntent(ctx, "topology-operator", submission.Intent, submission.Payload)
		if err != nil {
			t.Fatal(err)
		}
		approval, err := SignApproval(submission.Intent, "owner-a", private, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		approved, err := j.RecordApproval(ctx, approval)
		if err != nil {
			t.Fatal(err)
		}
		return receipt.Digest, approved.Digest
	}
	first, firstApproval := register("topology-import", Version{}, []byte(participationFixture))
	if _, err := j.ActivateParticipationTopology(ctx, cluster, "topology-operator", first, digest([]byte("unknown"))); err == nil {
		t.Fatal("topology activated without the persisted owner approval")
	}
	if _, err := j.ActivateParticipationTopology(ctx, cluster, "audit-error", first, firstApproval); err == nil {
		t.Fatal("activation survived failed audit")
	}
	if _, err := j.ReadParticipationTopology(ctx, cluster); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected activation left a topology: %v", err)
	}
	if r, err := j.ActivateParticipationTopology(ctx, cluster, "topology-operator", first, firstApproval); err != nil || !r.Created {
		t.Fatalf("activate: %+v %v", r, err)
	}
	if r, err := j.ActivateParticipationTopology(ctx, cluster, "topology-operator", first, firstApproval); err != nil || r.Created {
		t.Fatalf("not replay safe: %+v %v", r, err)
	}
	base, err := j.ReadParticipationTopology(ctx, cluster)
	if err != nil || base.Version.Generation != 2 {
		t.Fatalf("read topology: %+v %v", base, err)
	}
	e, err := NewExecutionJournal(j, &testExecutionEvidence{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ReserveExecution(ctx, "guardian", first, firstApproval); err == nil || errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("topology approval entered the provider reservation path: %v", err)
	}
	next := base.Policy
	next.Generation++
	next.Members[0].StableSeconds++
	rawA, _ := json.Marshal(next)
	next.Members[0].StableSeconds++
	rawB, _ := json.Marshal(next)
	a, aApproval := register("topology-a", base.Version, rawA)
	b, bApproval := register("topology-b", base.Version, rawB)
	var wg sync.WaitGroup
	winner := make(chan string, 2)
	for _, candidate := range [][2]string{{a, aApproval}, {b, bApproval}} {
		wg.Go(func() {
			r, err := j.ActivateParticipationTopology(ctx, cluster, "topology-operator", candidate[0], candidate[1])
			if errors.Is(err, ErrResourceDrift) {
				return
			}
			if err != nil || !r.Created {
				t.Errorf("concurrent topology: %+v %v", r, err)
				return
			}
			winner <- candidate[0]
		})
	}
	wg.Wait()
	if len(winner) != 1 {
		t.Fatalf("%d revisions won the same version CAS", len(winner))
	}
	j.now = func() time.Time { return now.Add(time.Hour) }
	installed, err := j.ReadParticipationTopology(ctx, cluster)
	if err != nil || installed.IntentDigest != <-winner || installed.Version.Generation != 3 {
		t.Fatalf("installed topology did not survive approval expiry: %+v %v", installed, err)
	}
	historical, err := j.ReadParticipationTopologyVersion(ctx, cluster, base.Version.Generation)
	if err != nil || historical.Version != base.Version || historical.IntentDigest != first || historical.Policy.Members[0].StableSeconds != 300 {
		t.Fatalf("rollback source was not retained: %+v %v", historical, err)
	}
	if _, err := j.ReadParticipationTopologyVersion(ctx, "other-cluster", base.Version.Generation); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("historic topology crossed cluster scope: %v", err)
	}
	var audits int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM fugue_control.audit WHERE kind = 'topology.participation_activated'`).Scan(&audits); err != nil || audits != 2 {
		t.Fatalf("expected exactly two durable topology audit events, got %d: %v", audits, err)
	}
}
