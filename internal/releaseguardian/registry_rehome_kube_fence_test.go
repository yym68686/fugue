package releaseguardian

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/testfixture/kubeadmission"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func registryRehomeTestProvider(t *testing.T, ctx context.Context, client kubernetes.Interface, config *rest.Config, namespace string) productioncontrol.ParticipationKubernetesBinding {
	t.Helper()
	ns, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	ca, _ := pem.Decode(config.CAData)
	if err != nil || ca == nil {
		t.Fatalf("fixture provider enrollment: %v", err)
	}
	return productioncontrol.ParticipationKubernetesBinding{Endpoint: config.Host, CADigest: digest(ca.Bytes), NamespaceUID: string(ns.UID)}
}

func TestRegistryRehomeKubernetesFenceAtProvider(t *testing.T) {
	base := registryRehomeTestDeployment()
	base.UID, base.ResourceVersion, base.Generation = "", "", 0
	frozen := cutoverFixture()
	frozen.Namespace, frozen.Deployment = base.Namespace, base.Name
	const writer = "system:serviceaccount:fixture-identities:temporary-rehome"
	const legacy = "system:serviceaccount:fixture-identities:old-publisher"
	cfg, principals := kubeadmission.Start(t, writer, legacy, frozen.Guardian, frozen.DeploymentStatusWriter)
	admin := admissionClient(t, cfg, "")
	mover := admissionClient(t, principals[writer], "")
	old := admissionClient(t, principals[legacy], "")
	ordinary := admissionClient(t, principals[frozen.Guardian], "")
	controller := admissionClient(t, principals[frozen.DeploymentStatusWriter], "")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	createAdmissionTestNamespace(t, ctx, admin, base.Namespace)
	workloads := admin.AppsV1().Deployments(base.Namespace)
	source, err := workloads.Create(ctx, base.DeepCopy(), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	review := registryRehomeTestReview(t, source)
	cas, err := prepareRegistryRehomeDeploymentCAS(review, source)
	if err != nil {
		t.Fatal(err)
	}
	policies := admin.AdmissionregistrationV1()
	fenced := registryRehomeKubeScope{writer, string(source.UID), source.ResourceVersion, source.Generation}
	var handler atomic.Pointer[registryRehomeAdmissionHandler]
	var unavailable atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h := handler.Load(); h != nil && !unavailable.Load() {
			h.ServeHTTP(w, r)
		} else {
			http.Error(w, "fixture admission unavailable", http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	provider := registryRehomeTestProvider(t, ctx, admin, cfg, base.Namespace)
	gate, err := renderRegistryRehomeKubeFence(frozen, provider, fenced, server.URL+registryRehomeAdmissionPath, ca)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest, providerDigest, err := gate.digests(cas)
	if err != nil {
		t.Fatal(err)
	}
	var pending atomic.Bool
	var liveChecks atomic.Int32
	validator, err := registryRehomeAdmissionServer(gate, review, func(_ context.Context, policy, provider string) error {
		liveChecks.Add(1)
		if !pending.Load() || policy != policyDigest || provider != providerDigest {
			return errors.New("fixture has no exact pending signed claim")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(validator)
	patch := func(client kubernetes.Interface, payload []byte, dry bool) error {
		opts := metav1.PatchOptions{}
		if dry {
			opts.DryRun = []string{metav1.DryRunAll}
		}
		_, err := client.AppsV1().Deployments(base.Namespace).Patch(ctx, base.Name, types.JSONPatchType, payload, opts)
		return err
	}
	waitForCutover(t, ctx, admin, frozen, func(context.Context) (bool, error) {
		return isCutoverDenial(patch(mover, cas.patch, true)), nil
	})
	if err := gate.verify(ctx, mover); err == nil {
		t.Fatal("plain freeze was accepted as enrolled exact admission")
	}
	if _, err := policies.ValidatingWebhookConfigurations().Create(ctx, gate.hook, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, pair := range gate.pairs {
		oldPolicy, err := policies.ValidatingAdmissionPolicies().Get(ctx, pair.Policy.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		next := pair.Policy.DeepCopy()
		next.ResourceVersion = oldPolicy.ResourceVersion
		if _, err := policies.ValidatingAdmissionPolicies().Update(ctx, next, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var probeErr error
	if err := wait.PollUntilContextTimeout(ctx, 150*time.Millisecond, 12*time.Second, true, func(context.Context) (bool, error) {
		probeErr = patch(mover, cas.patch, true)
		return probeErr == nil, nil
	}); err != nil {
		t.Fatalf("exact dry-run did not reach the provider admission gate: %v (last response %v)", err, probeErr)
	}
	if err := gate.verify(ctx, mover); err != nil {
		t.Fatalf("exact enforced policy failed readback: %v", err)
	}
	denied := func(err error, fragment string) {
		t.Helper()
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), fragment) {
			t.Fatalf("expected a forbidden request from %q, got %v", fragment, err)
		}
	}
	denied(patch(mover, cas.patch, false), "no matching live journal claim")
	for _, client := range []kubernetes.Interface{old, ordinary, admin} {
		denied(patch(client, cas.patch, true), "Fugue")
	}
	pending.Store(true)
	for _, op := range []rehomePatchOperation{
		{"replace", "/spec/replicas", 3},
		{"replace", "/spec/template/spec/imagePullSecrets/0/name", "unreviewed-credentials"},
		{"replace", "/spec/template/spec/containers/0/args/0", "unexpected-argument"},
		{"replace", "/metadata/labels/app", "unexpected-label"},
	} {
		var changes []rehomePatchOperation
		if err := json.Unmarshal(cas.patch, &changes); err != nil {
			t.Fatal(err)
		}
		p, err := json.Marshal(append(changes, op))
		if err != nil {
			t.Fatal(err)
		}
		denied(patch(mover, p, false), "object differs from the reviewed image-only change")
	}
	if liveChecks.Load() != 1 {
		t.Fatalf("authorized wrong proposals or ran authorization on dry-run: %d checks", liveChecks.Load())
	}
	unavailable.Store(true)
	if err := patch(mover, cas.patch, false); err == nil || !strings.Contains(err.Error(), "failed calling webhook") {
		t.Fatalf("missing provider webhook did not fail closed: %v", err)
	}
	if _, err := controller.AppsV1().Deployments(base.Namespace).UpdateStatus(ctx, source.DeepCopy(),
		metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		t.Fatalf("normal status reconciliation must remain independent of the rehome webhook: %v", err)
	}
	if rv, err := workloads.Get(ctx, base.Name, metav1.GetOptions{}); err != nil || rv.ResourceVersion != source.ResourceVersion {
		t.Fatalf("a rejected request changed the Deployment: %v", err)
	}
	unavailable.Store(false)
	applied, err := writeRegistryRehomeDeploymentOnce(ctx, mover.AppsV1().RESTClient(), cas)
	if err != nil || applied.Generation != source.Generation+1 || liveChecks.Load() != 2 {
		t.Fatalf("the exact journalled image-only CAS did not commit: %v", err)
	}
	if err := patch(mover, cas.patch, false); err == nil {
		t.Fatal("consumed provider CAS was replayed")
	}
	// The bundle is restricted to the exact Deployment, not the namespace's
	// unrelated workloads or controllers.
	unrelated := base.DeepCopy()
	unrelated.Name += "-unrelated"
	if _, err := workloads.Create(ctx, unrelated, metav1.CreateOptions{}); err != nil {
		t.Fatalf("unrelated workload was included in rehome fence: %v", err)
	}
}

func TestRegistryRehomeKubernetesDispatchWithJournal(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_REHOME_DISPATCH_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_TEST_REHOME_DISPATCH_DATABASE_URL to a disposable empty PostgreSQL database")
	}
	const writer = "system:serviceaccount:fixture-identities:rehome-dispatch"
	cfg, clients := kubeadmission.Start(t, writer)
	admin, mover := admissionClient(t, cfg, ""), admissionClient(t, clients[writer], "")
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var existing bool
	if err := db.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_control') IS NOT NULL`).Scan(&existing); err != nil || existing {
		t.Fatalf("fixture requires an empty database: %v", err)
	}
	if err := productioncontrol.ApplySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	images := map[string]registryTestContent{}
	oldImage, _ := registryImageFixture(t, images, "fixture/rehome", strings.Repeat("2", 40), false)
	servingImage, _ := registryImageFixture(t, images, "fixture/rehome", strings.Repeat("3", 40), true)
	const repoPath = "/v2/fixture/rehome/manifests/"
	for _, d := range []string{oldImage.TopDigest, servingImage.TopDigest} {
		images[repoPath+"fugue-recovery-sha256-"+strings.TrimPrefix(d, "sha256:")] = images[repoPath+d]
	}
	synthetic, _, _ := rehomeArchiveFixture(t, oldImage, servingImage, "")
	initial, err := readReleaseRecoveryArchive(synthetic, digest(synthetic))
	if err != nil {
		t.Fatal(err)
	}
	seed := fakeRehomeProvider(t, synthetic, initial)
	pair := initial.report.Pair
	id := recoveryWorkloadKey(initial.forward.workload).resource
	createAdmissionTestNamespace(t, ctx, admin, id.Namespace)
	maps, err := seed.CoreV1().ConfigMaps(id.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range maps.Items {
		m.UID, m.ResourceVersion = "", ""
		if _, err := admin.CoreV1().ConfigMaps(id.Namespace).Create(ctx, &m, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	companions, err := seed.CoreV1().Services(id.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, svc := range companions.Items {
		svc.UID, svc.ResourceVersion = "", ""
		if _, err := admin.CoreV1().Services(id.Namespace).Create(ctx, &svc, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	x, err := seed.AppsV1().Deployments(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	x.UID, x.ResourceVersion, x.Generation = "", "", 0
	x.Spec.Template.Labels = map[string]string{"app": "rehome-fixture"}
	x.Spec.Selector = &metav1.LabelSelector{MatchLabels: x.Spec.Template.Labels}
	apps := admin.AppsV1().Deployments(id.Namespace)
	x, err = apps.Create(ctx, x, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	x.Status.ObservedGeneration, x.Status.Replicas, x.Status.UpdatedReplicas, x.Status.ReadyReplicas, x.Status.AvailableReplicas = x.Generation, 1, 1, 1, 1
	x, err = apps.UpdateStatus(ctx, x, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	raw, archive, err := CaptureReleaseRecoveryArchive(ctx, admin.CoreV1(), id.Namespace, pair.Component)
	if err != nil {
		t.Fatal(err)
	}
	group := "global"
	source, err := ObserveRegistryRehomeSource(ctx, admin, raw, archive.ArchiveDigest, group)
	if err != nil {
		t.Fatal(err)
	}
	owner, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	observer, witness, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	scope := productioncontrol.Scope{ClusterID: "isolated-rehome", ResourceID: nativeKubernetesResourceID(id)}
	ledger, err := productioncontrol.NewJournal(db, productioncontrol.Approvers{"fixture-owner": {ClusterID: scope.ClusterID, PublicKey: owner}})
	if err != nil {
		t.Fatal(err)
	}
	baseline := productioncontrol.Baseline{Scope: scope, Verified: source.Version, EvidenceDigest: digest([]byte("fixture baseline"))}
	execution, err := productioncontrol.NewExecutionJournal(ledger, publicationTestEvidence{baseline})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := execution.AnchorResource(ctx, "fixture-observer", baseline); !ok || err != nil {
		t.Fatalf("baseline: %v %v", ok, err)
	}
	registry, err := NewIndependentRegistry(testNativeRegistry, nil, registryRoundTrip(func(r *http.Request) (*http.Response, error) {
		o, ok := images[r.URL.Path]
		if !ok && r.URL.Path != "/v2/" {
			return nil, errors.New("missing fixture registry object")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {string(o.mediaType)}},
			ContentLength: int64(len(o.body)), Body: io.NopCloser(bytes.NewReader(o.body)), Request: r}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	s, err := DraftRegistryRehomeSubmission(ctx, admin, ledger, registry, raw, archive.ArchiveDigest, group, scope.ClusterID, "fixture-request", servingImage.Repository)
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := ledger.RecordIntent(ctx, "fixture-owner", s.Intent, s.Payload)
	if err != nil {
		t.Fatal(err)
	}
	ct := cutoverFixture()
	ct.Key, ct.MonitorComponent, ct.Namespace, ct.Deployment = source.Key, pair.Component, id.Namespace, id.Name
	var handler atomic.Pointer[registryRehomeAdmissionHandler]
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h := handler.Load(); h != nil {
			h.ServeHTTP(w, r)
			return
		}
		http.Error(w, "fixture not configured", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	provider := registryRehomeTestProvider(t, ctx, admin, cfg, x.Namespace)
	gate, err := renderRegistryRehomeKubeFence(ct, provider, registryRehomeKubeScope{writer, string(x.UID), x.ResourceVersion, x.Generation}, server.URL+registryRehomeAdmissionPath, ca)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := newRegistryRehomeDispatcher(clients[writer], gate, scope.ClusterID, ledger, observer, raw, archive.ArchiveDigest)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := dispatcher.dispatch(ctx, "fixture-mover", recorded.Digest, testDigest, nil); err == nil || got.Submitted {
		t.Fatalf("unapproved submission attempted a write: %+v %v", got, err)
	}
	a, err := productioncontrol.SignApproval(s.Intent, "fixture-owner", key, time.Now(), 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := ledger.RecordApproval(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	claim := productioncontrol.Claim{Scope: scope, Fence: 1, IntentDigest: recorded.Digest, ApprovalDigest: approval.Digest}
	if _, err := db.ExecContext(ctx, `
		CREATE ROLE fugue_fixture_rehome_reader NOLOGIN;
		GRANT USAGE ON SCHEMA fugue_control TO fugue_fixture_rehome_reader;
		GRANT SELECT ON fugue_control.intents, fugue_control.approvals, fugue_control.execution_claims,
			fugue_control.execution_results, fugue_control.resource_baselines, fugue_control.registry_rehome_admissions
		TO fugue_fixture_rehome_reader;
	`); err != nil {
		t.Fatal(err)
	}
	dbConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	readerDB := stdlib.OpenDB(*dbConfig, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET ROLE fugue_fixture_rehome_reader`)
		return err
	}))
	defer readerDB.Close()
	readerLedger, err := productioncontrol.NewJournal(readerDB, productioncontrol.Approvers{"fixture-owner": {ClusterID: scope.ClusterID, PublicKey: owner}})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := productioncontrol.NewRegistryRehomeAdmissionReader(readerLedger, scope, observer)
	if err != nil {
		t.Fatal(err)
	}
	h, err := newJournaledRegistryRehomeAdmission(gate, s, claim, reader)
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(h)
	shadow, err := InspectRegistryRehomeSubmission(s)
	if err != nil {
		t.Fatal(err)
	}
	cas, err := prepareRegistryRehomeDeploymentCAS(shadow, x)
	if err != nil {
		t.Fatal(err)
	}
	patch := func(dry bool) error {
		opts := metav1.PatchOptions{}
		if dry {
			opts.DryRun = []string{metav1.DryRunAll}
		}
		_, err := mover.AppsV1().Deployments(id.Namespace).Patch(ctx, id.Name, types.JSONPatchType, cas.patch, opts)
		return err
	}
	waitForCutover(t, ctx, admin, ct, func(context.Context) (bool, error) { return isCutoverDenial(patch(true)), nil })
	admission := admin.AdmissionregistrationV1()
	if _, err := admission.ValidatingWebhookConfigurations().Create(ctx, gate.hook, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	next := gate.pairs[1].Policy.DeepCopy()
	before, err := admission.ValidatingAdmissionPolicies().Get(ctx, next.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next.ResourceVersion = before.ResourceVersion
	if _, err := admission.ValidatingAdmissionPolicies().Update(ctx, next, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 150*time.Millisecond, 12*time.Second, true, func(context.Context) (bool, error) {
		return patch(true) == nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := patch(false); err == nil || !strings.Contains(err.Error(), "no matching live journal claim") {
		t.Fatalf("provider accepted a write before durable claim: %v", err)
	}
	_, policy, write, err := dispatcher.preflight(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	proof := productioncontrol.RegistryRehomeAdmission{ClusterID: scope.ClusterID, ResourceID: scope.ResourceID,
		IntentDigest: claim.IntentDigest, ApprovalDigest: claim.ApprovalDigest, Fence: claim.Fence, PolicyScope: policy, ProviderWrite: write,
		EndpointAdmission: digest([]byte("fixture endpoint")), LegacyRevocation: digest([]byte("fixture revocation")),
		LegacyRequestDrain: digest([]byte("fixture drain")), ForwardNodePull: digest([]byte("fixture forward pull")),
		RollbackNodePull: digest([]byte("fixture rollback pull")), RegistryRetention: digest([]byte("fixture retention")),
		ObservedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix()}
	msg, err := proof.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = ed25519.Sign(witness, msg)
	wire, _, err := productioncontrol.EncodeRegistryRehomeAdmission(proof)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dispatcher.dispatch(ctx, "fixture-mover", recorded.Digest, approval.Digest, wire)
	if err != nil || !got.Submitted || got.Claim != claim || got.Workload.UID != string(x.UID) {
		t.Fatalf("provider dispatch: %+v %v", got, err)
	}
	restart, err := newRegistryRehomeDispatcher(clients[writer], gate, scope.ClusterID, ledger, observer, raw, archive.ArchiveDigest)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restart.dispatch(ctx, "fixture-restart", recorded.Digest, approval.Digest, nil)
	if err != nil || replay.Submitted || replay.Claim != claim {
		t.Fatalf("restart did not stay inspection-only: %+v %v", replay, err)
	}
	state, err := readerLedger.ReadResource(ctx, scope)
	if err != nil || state.Pending != claim.IntentDigest || state.LastVerified != source.Version {
		t.Fatalf("provider response settled an unverified release: %+v %v", state, err)
	}
}
