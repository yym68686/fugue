package releaseguardian

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

type publicationJournalFixture struct {
	draft        NativeReleaseDraft
	claim        productioncontrol.Claim
	reservations int
	checks       int
	checkErrorAt int
}

func (j *publicationJournalFixture) ReadIntent(context.Context, string) (productioncontrol.Intent, []byte, error) {
	return j.draft.Intent, j.draft.Payload, nil
}

func (j *publicationJournalFixture) Execution(context.Context, productioncontrol.Scope, string) (productioncontrol.Claim, *productioncontrol.ExecutionResult, error) {
	if j.reservations == 0 {
		return productioncontrol.Claim{}, nil, sql.ErrNoRows
	}
	return j.claim, nil, nil
}

func (j *publicationJournalFixture) ReserveExecution(_ context.Context, _ string, intent, approval string) (productioncontrol.Claim, bool, error) {
	if intent != j.claim.IntentDigest || approval != j.claim.ApprovalDigest || j.reservations != 0 {
		return productioncontrol.Claim{}, false, errors.New("test execution cannot be claimed")
	}
	j.reservations++
	return j.claim, true, nil
}

func (j *publicationJournalFixture) VerifyOpenClaim(_ context.Context, claim productioncontrol.Claim) error {
	j.checks++
	if j.reservations != 1 || claim != j.claim || j.checks == j.checkErrorAt {
		return productioncontrol.ErrStaleFence
	}
	return nil
}

type publicationAdmissionFunc func(context.Context, NativePublicationBoundary) error

func (check publicationAdmissionFunc) VerifyNativePublication(ctx context.Context, b NativePublicationBoundary) error {
	return check(ctx, b)
}

func TestNativePublicationIsBoundAndNeverRedispatched(t *testing.T) {
	for _, mode := range []string{"submitted", "legacy writer active", "workload drift", "artifact not in independent registry", "claim expires before pointer", "lost successful CAS response"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := healthyPublicationFixture(t)
			draft, err := f.store.DraftNativeRelease(ctx, f.key, "isolated-cluster", "request-a", f.candidate)
			if err != nil {
				t.Fatal(err)
			}
			intentDigest, err := draft.Intent.Digest()
			if err != nil {
				t.Fatal(err)
			}
			journal := &publicationJournalFixture{draft: draft, claim: productioncontrol.Claim{
				Scope: productioncontrol.Scope{ClusterID: draft.Intent.ClusterID, ResourceID: draft.Intent.ResourceID},
				Fence: 1, IntentDigest: intentDigest, ApprovalDigest: testDigest,
			}}
			checks := 0
			admission := publicationAdmissionFunc(func(_ context.Context, b NativePublicationBoundary) error {
				checks++
				if b.Scope != journal.claim.Scope || b.Candidate != draft.Preview.ProposedDesired || b.Predecessor != draft.Preview.CurrentDesired ||
					b.Namespace != b.Workload.Namespace || b.DesiredObject != desiredName(f.key) ||
					b.ExecutionObject != executionSnapshotName(f.key, b.Candidate.Generation) ||
					b.RecordObject != releaseRecordName(f.key, b.Candidate.RecordDigest) ||
					b.WorkloadUID != "uid" || b.WorkloadVersion != "10" || b.Workload != draft.Preview.PrewriteWorkload ||
					b.ForwardOCI == "" || b.RollbackOCI == "" || b.ForwardOCI == b.RollbackOCI ||
					b.ArtifactReceiptDigest != draft.Preview.ArtifactReceiptDigest ||
					b.ForwardReceipt.ImmutableRef != b.ForwardOCI || b.RollbackReceipt.ImmutableRef != b.RollbackOCI ||
					b.RollbackReceipt.TopDigest != f.stableGuardian.ImageDigest {
					t.Fatalf("provider is asked to prove the wrong boundary: %+v", b)
				}
				if mode == "legacy writer active" {
					return errors.New("legacy identity still has write permission")
				}
				return nil
			})
			p := &NativePublisher{store: f.store, journal: journal, clusterID: draft.Intent.ClusterID, admission: admission,
				writer: f.client.CoreV1().ConfigMaps(draft.Preview.Namespace)}
			switch mode {
			case "artifact not in independent registry":
				registry, err := NewIndependentRegistry("registry.fixture.invalid", nil, registryRoundTrip(func(*http.Request) (*http.Response, error) {
					t.Error("must not contact any registry for an out-of-origin artifact")
					return nil, errors.New("unexpected registry request")
				}))
				if err != nil {
					t.Fatal(err)
				}
				p.artifacts = registry
			case "workload drift":
				deploys := f.client.AppsV1().Deployments(draft.Preview.Namespace)
				live, err := deploys.Get(ctx, draft.Preview.PrewriteWorkload.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				live.ResourceVersion = "11"
				if _, err := deploys.Update(ctx, live, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "claim expires before pointer":
				journal.checkErrorAt = 3
			case "lost successful CAS response":
				f.client.PrependReactor("create", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					created := action.(clienttesting.CreateAction).GetObject().(*corev1.ConfigMap)
					if created.Name != desiredName(f.key) {
						return false, nil, nil
					}
					if err := f.client.Tracker().Create(action.GetResource(), created, action.GetNamespace()); err != nil {
						t.Fatal(err)
					}
					return true, nil, errors.New("connection lost after the provider committed CAS")
				})
			}
			f.client.ClearActions()
			result, err := p.Publish(ctx, "executor-a", intentDigest, testDigest)
			if (err != nil) != (mode != "submitted") || result.Submitted != (mode == "submitted") {
				t.Fatalf("publication=%+v error=%v", result, err)
			}
			writes := 0
			for _, action := range f.client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					if action.GetResource().Resource != "configmaps" || action.GetVerb() != "create" {
						t.Fatalf("unexpected provider write: %+v", action)
					}
					writes++
				}
			}
			switch mode {
			case "legacy writer active", "workload drift", "artifact not in independent registry":
				if checks != 1 || writes != 0 || journal.reservations != 0 || result.Claim != (productioncontrol.Claim{}) {
					t.Fatalf("unverified authority or predecessor reserved a claim or wrote: %+v", result)
				}
				return
			case "claim expires before pointer":
				if checks != 4 || writes != 2 || !errors.Is(err, productioncontrol.ErrStaleFence) {
					t.Fatalf("stale claim reached pointer: writes=%d checks=%d error=%v", writes, checks, err)
				}
			default:
				object, err := f.client.CoreV1().ConfigMaps(draft.Preview.Namespace).Get(ctx, desiredName(f.key), metav1.GetOptions{})
				if err != nil || checks != 4 || writes != 3 || object.Annotations[nativeFenceAnnotation] != "1" ||
					object.Annotations[nativeIntentAnnotation] != intentDigest {
					t.Fatalf("pointer missing exact claim binding: writes=%d checks=%d error=%v", writes, checks, err)
				}
			}
			f.client.ClearActions()
			replayed, err := p.Publish(ctx, "executor-b", intentDigest, testDigest)
			if err != nil || replayed.Submitted || replayed.Claim != journal.claim || result.Claim != journal.claim ||
				len(f.client.Actions()) != 0 || journal.reservations != 1 {
				t.Fatalf("claim reuse dispatched again: result=%+v err=%v actions=%v", replayed, err, f.client.Actions())
			}
			expectedPointer := NativePointerCandidate
			if mode == "claim expires before pointer" {
				expectedPointer = NativePointerUnconfirmed
			}
			observation, err := p.ObservePublication(ctx, intentDigest)
			if err != nil || observation.Pointer != expectedPointer || observation.Result != nil || observation.Claim != journal.claim {
				t.Fatalf("uncertain publication could not be read back: %+v error=%v", observation, err)
			}
			for _, action := range f.client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("observation used provider mutation: %+v", action)
				}
			}
			if mode != "submitted" {
				return
			}
			maps := f.client.CoreV1().ConfigMaps(draft.Preview.Namespace)
			pointer, err := maps.Get(ctx, desiredName(f.key), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pointer.ResourceVersion, pointer.Annotations[nativeFenceAnnotation] = "21", "10"
			if _, err := maps.Update(ctx, pointer, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if observed, err := p.ObservePublication(ctx, intentDigest); err != nil || observed.Pointer != NativePointerDiverged || observed.Result != nil {
				t.Fatalf("unrelated fenced pointer was accepted: %+v error=%v", observed, err)
			}
			pointer.ResourceVersion, pointer.Annotations[nativeFenceAnnotation] = "22", "1"
			if _, err := maps.Update(ctx, pointer, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			snapshot, err := f.store.Load(ctx, f.key)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.SetDesiredToLKG(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			if observed, err := p.ObservePublication(ctx, intentDigest); err != nil || observed.Pointer != NativePointerRollback || observed.Result != nil {
				t.Fatalf("LKG pointer was not identified, or mistaken for a verified result: %+v error=%v", observed, err)
			}
		})
	}
}

func TestLegacyPublicationRefusesMarkedResourceBeforeStaging(t *testing.T) {
	ctx := context.Background()
	f := healthyPublicationFixture(t)
	preview, err := f.store.PreviewDesired(ctx, f.key, f.candidate)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := declarativerelease.CanonicalJSON(preview.CurrentDesired)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.CoreV1().ConfigMaps(preview.Namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: desiredName(f.key), ResourceVersion: "20", Annotations: map[string]string{
			nativeIntentAnnotation: testDigest, nativeFenceAnnotation: "1",
		}},
		Data: map[string]string{"desired.json": string(raw)},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.client.ClearActions()
	if _, _, err := f.store.PublishDesired(ctx, f.key, f.candidate); err == nil || !strings.Contains(err.Error(), "marked native") {
		t.Fatalf("legacy writer was not refused: %v", err)
	}
	for _, action := range f.client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("legacy writer staged objects: %+v", action)
		}
	}
}
