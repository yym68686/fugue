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

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	"fugue/internal/testfixture/kubeadmission"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
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
	testRegistryRehomeKubernetesWithJournal(t, "FUGUE_TEST_REHOME_DISPATCH_DATABASE_URL", true)
}

func TestRegistryRehomeKubernetesNoWriteSettlement(t *testing.T) {
	testRegistryRehomeKubernetesWithJournal(t, "FUGUE_TEST_REHOME_NO_WRITE_DATABASE_URL", false)
}

func TestRegistryRehomeKubernetesRBACShadow(t *testing.T) {
	cfg, _ := kubeadmission.Start(t)
	client := admissionClient(t, cfg, "")
	ctx := t.Context()
	const roleName = "fixture-read"
	const bindingName = "fixture-bind"
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: roleName},
		Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get"}}}}
	binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: bindingName},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "fixture", Namespace: "default"}}}
	roles, bindings := client.RbacV1().ClusterRoles(), client.RbacV1().ClusterRoleBindings()
	liveRole, err := roles.Create(ctx, role, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	liveBinding, err := bindings.Create(ctx, binding, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	makeRead := func(kind string, original, live metav1.Object) func() (RegistryRehomeObservedCompanion, error) {
		t.Helper()
		id := declarativerelease.ResourceIdentity{APIVersion: registryRehomeRBACVersion, Kind: kind, Name: original.GetName()}
		item, err := rehomeJSONMap(original)
		if err != nil {
			t.Fatal(err)
		}
		// Use a sealed declaration without system-owned metadata/defaults.
		item["metadata"] = map[string]any{"name": id.Name}
		item["apiVersion"], item["kind"] = id.APIVersion, id.Kind
		decl, err := registryRehomeCompanionShape(item, id, "default")
		if err != nil {
			t.Fatal(err)
		}
		read := func() (RegistryRehomeObservedCompanion, error) {
			return readRegistryRehomeRBAC(ctx, client, id, item, decl)
		}
		if observed, err := read(); err != nil || observed.Live.UID != string(live.GetUID()) || observed.ResourceVersion != live.GetResourceVersion() {
			t.Fatalf("read-only RBAC observation did not bind the live object: %+v %v", observed, err)
		}
		return read
	}
	readRole := makeRead("ClusterRole", role, liveRole)
	readBinding := makeRead("ClusterRoleBinding", binding, liveBinding)
	liveRole.Rules[0].Verbs = append(liveRole.Rules[0].Verbs, "list")
	if _, err := roles.Update(ctx, liveRole, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	liveBinding.Subjects = append(liveBinding.Subjects, rbacv1.Subject{Kind: "ServiceAccount", Name: "unexpected", Namespace: "default"})
	if _, err := bindings.Update(ctx, liveBinding, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, read := range []func() (RegistryRehomeObservedCompanion, error){readRole, readBinding} {
		if _, err := read(); err == nil || !strings.Contains(err.Error(), "RBAC access differs") {
			t.Fatalf("accepted drifted RBAC: %v", err)
		}
	}
}

func testRegistryRehomeKubernetesWithJournal(t *testing.T, dbEnv string, writeWorkload bool) {
	t.Helper()
	dsn := os.Getenv(dbEnv)
	if dsn == "" {
		t.Skipf("set %s to a disposable empty PostgreSQL database", dbEnv)
	}
	const writer = "system:serviceaccount:fixture-identities:rehome-dispatch"
	const statusWriter = "system:serviceaccount:fixture-identities:deployment-status"
	cfg, clients := kubeadmission.Start(t, writer, statusWriter)
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
	oldImage, oldLayer := registryImageFixture(t, images, "fixture/rehome", strings.Repeat("2", 40), false)
	servingImage, _ := registryImageFixture(t, images, "fixture/rehome", strings.Repeat("3", 40), true)
	const repoPath = "/v2/fixture/rehome/manifests/"
	for _, d := range []string{oldImage.TopDigest, servingImage.TopDigest} {
		images[repoPath+"fugue-recovery-sha256-"+strings.TrimPrefix(d, "sha256:")] = images[repoPath+d]
	}
	synthetic, _, _ := rehomeArchiveFixture(t, oldImage, servingImage, "serving")
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
	var duringRegistryRead atomic.Pointer[func()]
	registry, err := NewIndependentRegistry(testNativeRegistry, nil, registryRoundTrip(func(r *http.Request) (*http.Response, error) {
		if f := duringRegistryRead.Swap(nil); f != nil {
			(*f)()
		}
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
	ct.DeploymentStatusWriter = statusWriter
	var handler atomic.Pointer[http.Handler]
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h := handler.Load(); h != nil {
			(*h).ServeHTTP(w, r)
			return
		}
		http.Error(w, "fixture not configured", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	provider := registryRehomeTestProvider(t, ctx, admin, cfg, x.Namespace)
	enrollment := RegistryRehomeKubeEnrollment{Cutover: ct, Provider: provider, Writer: writer, DeploymentUID: string(x.UID),
		DeploymentResourceVersion: x.ResourceVersion, DeploymentGeneration: x.Generation,
		AdmissionEndpoint: server.URL + registryRehomeAdmissionPath, AdmissionCABundle: ca}
	gate, err := enrollment.render()
	if err != nil {
		t.Fatal(err)
	}
	preview, err := RenderRegistryRehomeKubeFence(enrollment)
	if err != nil || preview.Webhook.Name != gate.hook.Name || len(preview.Policies) != len(gate.pairs) {
		t.Fatalf("unexpected staged fence preview: %+v %v", preview, err)
	}
	dispatcher, err := newRegistryRehomeDispatcher(clients[writer], gate, scope.ClusterID, ledger, observer, raw, archive.ArchiveDigest)
	if err != nil {
		t.Fatal(err)
	}
	dispatch := func(approval string, evidence []byte) (RegistryRehomeKubernetesDispatchResult, error) {
		return DispatchRegistryRehomeKubernetes(ctx, clients[writer], enrollment, scope.ClusterID, ledger,
			observer, raw, archive.ArchiveDigest, "fixture-mover", recorded.Digest, approval, evidence)
	}
	if got, err := dispatch(testDigest, nil); err == nil || got.Submitted {
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
		DO $$ BEGIN CREATE ROLE fugue_fixture_rehome_reader NOLOGIN;
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;
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
	h, err := NewRegistryRehomeKubernetesAdmissionHandler(enrollment, scope.ClusterID, s, recorded.Digest, approval.Digest, readerLedger, observer)
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(&h)
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
	if !writeWorkload {
		proof.ExpiresAt = now.Add(7 * time.Second).Unix()
	}
	msg, err := proof.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = ed25519.Sign(witness, msg)
	wire, admissionDigest, err := productioncontrol.EncodeRegistryRehomeAdmission(proof)
	if err != nil {
		t.Fatal(err)
	}
	if writeWorkload {
		got, err := dispatch(approval.Digest, wire)
		if err != nil || !got.Submitted || got.Claim != claim || got.Workload.UID != string(x.UID) {
			t.Fatalf("provider dispatch: %+v %v", got, err)
		}
	} else {
		// Simulate a process stopping after durable reservation and before the
		// provider call. Runtime replay must not issue the withheld request.
		got, created, err := dispatcher.journal.Reserve(ctx, "fixture-mover", recorded.Digest, approval.Digest, wire)
		if err != nil || !created || got != claim {
			t.Fatalf("reserve exact no-write fixture: %+v %v", got, err)
		}
	}
	replay, err := dispatch(approval.Digest, nil)
	if err != nil || replay.Submitted || replay.Claim != claim {
		t.Fatalf("restart did not stay inspection-only: %+v %v", replay, err)
	}
	state, err := readerLedger.ReadResource(ctx, scope)
	if err != nil || state.Pending != claim.IntentDigest || state.LastVerified != source.Version {
		t.Fatalf("provider response settled an unverified release: %+v %v", state, err)
	}
	observe := func(enrolled productioncontrol.ParticipationKubernetesBinding) (RegistryRehomePendingObservation, error) {
		return ObserveFrozenRegistryRehomePending(ctx, admin, readerLedger, scope.ClusterID, claim.IntentDigest, ct, enrolled, observer, raw, archive.ArchiveDigest)
	}
	if _, err := observe(provider); err == nil || !strings.Contains(err.Error(), "plain Deployment freeze readback") {
		t.Fatalf("placement readback accepted an open temporary write exception: %v", err)
	}
	pairs, err := DeploymentCutoverPolicies(ct)
	if err != nil {
		t.Fatal(err)
	}
	closed := pairs[1].Policy.DeepCopy()
	open, err := admission.ValidatingAdmissionPolicies().Get(ctx, closed.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	closed.ResourceVersion = open.ResourceVersion
	if _, err := admission.ValidatingAdmissionPolicies().Update(ctx, closed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := admission.ValidatingWebhookConfigurations().Delete(ctx, gate.hook.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admission.ValidatingWebhookConfigurations().Get(ctx, gate.hook.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("temporary registry rehome exception was not removed: %v", err)
	}
	if writeWorkload {
		if _, err := observe(provider); err == nil || !strings.Contains(err.Error(), "placement has not converged") {
			t.Fatalf("nonconverged rollout was accepted after the freeze: %v", err)
		}
	}
	controller := admissionClient(t, clients[ct.DeploymentStatusWriter], "")
	pod := readyProviderRehomeFixture(t, ctx, admin, controller, id.Namespace, id.Name)
	badProvider := provider
	badProvider.NamespaceUID = "other-instance"
	if _, err := observe(badProvider); err == nil || !strings.Contains(err.Error(), "enrolled provider") {
		t.Fatalf("wrong namespace instance was accepted: %v", err)
	}
	observed, err := observe(provider)
	wantPlacement := registryRehomeIndependentPlacement
	if !writeWorkload {
		wantPlacement = registryRehomeOriginalPlacement
	}
	if err != nil || observed.Claim != claim || observed.AdmissionDigest != admissionDigest || observed.LastVerified != source.Version ||
		observed.Placement != wantPlacement || observed.ProviderReadback.Deployment.UID != string(x.UID) ||
		len(observed.ProviderReadback.Pods) != 1 || len(observed.ProviderReadback.Services) != 1 || !digestPattern.MatchString(observed.ReadbackDigest) {
		t.Fatalf("pending placement readback did not bind the existing claim: %+v %v", observed, err)
	}
	if after, err := readerLedger.ReadResource(ctx, scope); err != nil || after != state {
		t.Fatalf("observation mutated the journal: %+v %v", after, err)
	}
	bundle, bundleReport, err := BuildRegistryRehomeRecoveryBundle(raw, archive.ArchiveDigest, s)
	if err != nil {
		t.Fatal(err)
	}
	joint := func(pin string) (RegistryRehomeCutoverReadback, error) {
		return ObserveRegistryRehomeCutover(ctx, admin, readerLedger, scope.ClusterID, claim.IntentDigest, ct, provider, observer, bundle, pin, registry)
	}
	if _, err := joint(otherDigest); err == nil {
		t.Fatal("joint readback accepted an unpinned bundle")
	}
	if writeWorkload {
		combined, err := joint(bundleReport.BundleDigest)
		if err != nil || combined.Recovery != bundleReport || combined.Placement.ReadbackDigest != observed.ReadbackDigest {
			t.Fatalf("joint verification did not bind both mirrors to the pending placement: %+v %v", combined, err)
		}
		_, proposed, err := BuildRegistryRehomeBaseline(bundle, bundleReport.BundleDigest)
		if err != nil || combined.Baseline != proposed || combined.Baseline.Predecessor != state.LastVerified ||
			combined.Baseline.Scope != claim.Scope || combined.Baseline.Version().Digest == s.Intent.TargetDigest {
			t.Fatalf("joint readback did not identify the exact next baseline without settling it: %+v %v", combined.Baseline, err)
		}
		retained := images[oldLayer]
		delete(images, oldLayer)
		if _, err := joint(bundleReport.BundleDigest); err == nil || !strings.Contains(err.Error(), "verify rollback recovery mirror") {
			t.Fatalf("joint readback did not check the full rollback image: %v", err)
		}
		images[oldLayer] = retained
		svc, err := admin.CoreV1().Services(id.Namespace).Create(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "fixture-serving"},
			Spec: corev1.ServiceSpec{Selector: pod.Labels,
				Ports: []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("http")}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		slicesClient := admin.DiscoveryV1().EndpointSlices(id.Namespace)
		endpoints, err := slicesClient.Create(ctx, &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{Name: "fixture-endpoints", Labels: map[string]string{discoveryv1.LabelServiceName: svc.Name},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(svc, corev1.SchemeGroupVersion.WithKind("Service"))}},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To[int32](8080)}},
			Endpoints: []discoveryv1.Endpoint{{Addresses: []string{pod.Status.PodIP},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		route := RegistryRehomeRoute{Service: svc.Name, ServicePort: "http", Address: "192.0.2.10:443", Host: "route.example.test", Path: "/healthz", Expected: "ready"}
		runServing := func(probe registryRehomeHTTPReader) (RegistryRehomeServingObservation, error) {
			return observeRegistryRehomeServing(ctx, admin, readerLedger, scope.ClusterID, claim.IntentDigest, ct, provider, observer,
				bundle, bundleReport.BundleDigest, registry, route, probe)
		}
		probe := func(_ context.Context, address, host, path, marker string) (string, error) {
			if path != "/healthz" || marker != "ready" ||
				(host == "" && address != pod.Status.PodIP+":8080") || (host != "" && (host != route.Host || address != route.Address)) {
				t.Errorf("unexpected probe %q %q %q %q", address, host, path, marker)
			}
			return digest([]byte("ready")), nil
		}
		health, err := runServing(probe)
		if err != nil || health.Cutover.Baseline != proposed || health.Cutover.Placement.Claim != claim ||
			len(health.PodHTTP) != 1 || health.PodHTTP[0].PodUID != string(pod.UID) || len(health.PublicHTTP) != 1 ||
			health.Service.UID != string(svc.UID) || len(health.EndpointSlices) != 1 || !digestPattern.MatchString(health.EvidenceDigest) {
			t.Fatalf("missing exact serving evidence: %+v %v", health, err)
		}
		if _, err := runServing(func(ctx context.Context, address, host, path, expected string) (string, error) {
			if host != "" {
				return "", errors.New("fixture route offline")
			}
			return probe(ctx, address, host, path, expected)
		}); err == nil || !strings.Contains(err.Error(), "public HTTPS route probe failed") {
			t.Fatalf("accepted failed external health: %v", err)
		}
		if _, err := runServing(func(ctx context.Context, address, host, path, expected string) (string, error) {
			if host != "" {
				copy := endpoints.DeepCopy()
				copy.Ports[0].Port = ptr.To[int32](8081)
				if _, err := slicesClient.Update(ctx, copy, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			return probe(ctx, address, host, path, expected)
		}); err == nil || !strings.Contains(err.Error(), "endpoint inventory changed") {
			t.Fatalf("accepted endpoint drift during HTTPS probe: %v", err)
		}
		changeCompanion := func() {
			services := admin.CoreV1().Services(id.Namespace)
			name := observed.ProviderReadback.Services[0].Name
			service, err := services.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			if service.Annotations == nil {
				service.Annotations = map[string]string{}
			}
			service.Annotations["fixture.invalid/changed"] = "true"
			if _, err := services.Update(ctx, service, metav1.UpdateOptions{}); err != nil {
				t.Error(err)
			}
		}
		duringRegistryRead.Store(&changeCompanion)
		if _, err := joint(bundleReport.BundleDigest); err == nil || !strings.Contains(err.Error(), "changed during independent mirror verification") {
			t.Fatalf("joint readback accepted provider drift during registry verification: %v", err)
		}
		if after, err := readerLedger.ReadResource(ctx, scope); err != nil || after != state {
			t.Fatalf("joint verification mutated the journal: %+v %v", after, err)
		}
		return
	}
	if _, err := joint(bundleReport.BundleDigest); err == nil || !strings.Contains(err.Error(), "has not reached the independent placement") {
		t.Fatalf("joint readback accepted the original placement: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 8*time.Second, true, func(context.Context) (bool, error) {
		return time.Now().Unix() >= proof.ExpiresAt, nil
	}); err != nil {
		t.Fatal(err)
	}
	// These are signed fixture facts, not assertions about real outage health.
	proofTime := time.Now()
	closure := productioncontrol.RegistryRehomeNoWriteEvidence{ClusterID: claim.ClusterID, ResourceID: claim.ResourceID,
		IntentDigest: claim.IntentDigest, ApprovalDigest: claim.ApprovalDigest, Fence: claim.Fence,
		AdmissionDigest: admissionDigest, ArchiveDigest: archive.ArchiveDigest, ReadbackDigest: observed.ReadbackDigest,
		EndpointFreeze: digest([]byte("fixture closed endpoints")), WriterRevocation: digest([]byte("fixture mover revoked")),
		RequestDrain: digest([]byte("fixture mover drained")), ServingHealth: digest([]byte("fixture original healthy")),
		ObservedAt: proofTime.Unix(), ExpiresAt: proofTime.Add(time.Minute).Unix()}
	settle := func(c productioncontrol.RegistryRehomeNoWriteEvidence) (productioncontrol.Recorded, error) {
		msg, err := c.SigningInput()
		if err != nil {
			t.Fatal(err)
		}
		c.Signature = ed25519.Sign(witness, msg)
		encoded, _, err := productioncontrol.EncodeRegistryRehomeNoWriteEvidence(c)
		if err != nil {
			t.Fatal(err)
		}
		return SettleRegistryRehomeWithoutWrite(ctx, admin, ledger, scope.ClusterID, "fixture-settler", claim.IntentDigest, ct, provider,
			observer, raw, archive.ArchiveDigest, encoded)
	}
	drift := closure
	drift.ReadbackDigest = digest([]byte("fixture other readback"))
	if got, err := settle(drift); err == nil || got.Created {
		t.Fatalf("settlement did not repeat exact enrolled provider readback: %+v %v", got, err)
	}
	if got, err := settle(closure); err != nil || !got.Created {
		t.Fatalf("exact frozen no-write claim did not settle: %+v %v", got, err)
	}
	if after, err := readerLedger.ReadResource(ctx, scope); err != nil || after.Pending != "" || after.LastVerified != state.LastVerified || after.LastFence != state.LastFence {
		t.Fatalf("no-write settlement advanced the LKG or fence: %+v %v", after, err)
	}
}

// Envtest has API admission and storage, but no Deployment controller or
// kubelet. Simulate only their observations; this is not a node-pull drill.
func readyProviderRehomeFixture(t *testing.T, ctx context.Context, admin, controller kubernetes.Interface, namespace, deployment string) *corev1.Pod {
	t.Helper()
	apps := admin.AppsV1().Deployments(namespace)
	d, err := apps.Get(ctx, deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	set, err := admin.AppsV1().ReplicaSets(namespace).Create(ctx, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: deployment + "-moved", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, appsv1.SchemeGroupVersion.WithKind("Deployment"))}},
		Spec:       appsv1.ReplicaSetSpec{Replicas: d.Spec.Replicas, Selector: d.Spec.Selector, Template: d.Spec.Template},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pods := admin.CoreV1().Pods(namespace)
	pod, err := pods.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: set.Name + "-0", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(set, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}, Labels: d.Spec.Template.Labels},
		Spec:       d.Spec.Template.Spec,
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	statuses := func(containers []corev1.Container) []corev1.ContainerStatus {
		var out []corev1.ContainerStatus
		for _, c := range containers {
			out = append(out, corev1.ContainerStatus{Name: c.Name, Image: c.Image, ImageID: c.Image, Ready: true})
		}
		return out
	}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.12.34.5", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		ContainerStatuses: statuses(pod.Spec.Containers), InitContainerStatuses: statuses(pod.Spec.InitContainers)}
	pod, err = pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d.Status.ObservedGeneration, d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.ReadyReplicas, d.Status.AvailableReplicas = d.Generation, 1, 1, 1, 1
	if _, err := controller.AppsV1().Deployments(namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	return pod
}
