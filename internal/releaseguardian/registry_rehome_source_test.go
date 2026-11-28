package releaseguardian

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type rehomeResourceStub func(context.Context, productioncontrol.Scope) (productioncontrol.ResourceState, error)

func (f rehomeResourceStub) ReadResource(ctx context.Context, scope productioncontrol.Scope) (productioncontrol.ResourceState, error) {
	return f(ctx, scope)
}

func TestRegistryRehomeApprovalReadyDraft(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"verified", "busy journal", "wrong head", "replacement Deployment", "Service drift", "journal changed", "pending legacy pointer"} {
		t.Run(mode, func(t *testing.T) {
			objects := map[string]registryTestContent{}
			rollback, _ := registryImageFixture(t, objects, "fixture/rehome", strings.Repeat("2", 40), false)
			forward, _ := registryImageFixture(t, objects, "fixture/rehome", strings.Repeat("3", 40), true)
			for _, image := range []declarativerelease.ArtifactReceipt{forward, rollback} {
				root := "/v2/fixture/rehome/manifests/"
				objects[root+"fugue-recovery-sha256-"+strings.TrimPrefix(image.TopDigest, "sha256:")] = objects[root+image.TopDigest]
			}
			raw, _, _ := rehomeArchiveFixture(t, rollback, forward, "")
			pin := digest(raw)
			archive, err := readReleaseRecoveryArchive(raw, pin)
			if err != nil {
				t.Fatal(err)
			}
			client := fakeRehomeProvider(t, raw, archive)
			source, err := ObserveRegistryRehomeSource(ctx, client, raw, pin, "global")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "Service drift" {
				s, err := client.CoreV1().Services("fugue-system").Get(ctx, "test-tls", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				s.Spec.Ports[0].Port++
				if _, err := client.CoreV1().Services(s.Namespace).Update(ctx, s, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "pending legacy pointer" {
				desired := DesiredRelease{APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: "api", Group: "global", Generation: 2, RecordDigest: otherDigest}
				value, _ := json.Marshal(desired)
				key := desired.Key()
				_, err := client.CoreV1().ConfigMaps("fugue-system").Create(ctx, &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: desiredName(key), Namespace: "fugue-system", UID: "pending-uid", ResourceVersion: "200", Labels: guardianLabels(key)},
					Data:       map[string]string{"desired.json": string(value)},
				}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			client.ClearActions()
			count, reads := 0, 0
			journal := rehomeResourceStub(func(_ context.Context, scope productioncontrol.Scope) (productioncontrol.ResourceState, error) {
				count++
				out := productioncontrol.ResourceState{Baseline: productioncontrol.Baseline{Scope: scope}, LastVerified: source.Version}
				switch {
				case mode == "busy journal":
					out.Pending = testDigest
				case mode == "wrong head" || (mode == "journal changed" && count == 2):
					out.LastVerified.Digest = otherDigest
				}
				return out, nil
			})
			registry, err := NewIndependentRegistry(testNativeRegistry, nil, registryRoundTrip(func(req *http.Request) (*http.Response, error) {
				reads++
				if mode == "replacement Deployment" && reads == 1 {
					d := source.Deployment.Identity
					obj, err := client.Tracker().Get(appsv1.SchemeGroupVersion.WithResource("deployments"), d.Namespace, d.Name)
					if err != nil {
						t.Fatal(err)
					}
					deployment := obj.(*appsv1.Deployment).DeepCopy()
					deployment.UID = "replacement-uid"
					if err := client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("deployments"), deployment, d.Namespace); err != nil {
						t.Fatal(err)
					}
				}
				content, found := objects[req.URL.Path]
				if !found && req.URL.Path != "/v2/" {
					return nil, errors.New("unexpected mock registry path")
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {string(content.mediaType)}},
					ContentLength: int64(len(content.body)), Body: io.NopCloser(strings.NewReader(string(content.body))), Request: req}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			s, err := DraftRegistryRehomeSubmission(ctx, client, journal, registry, raw, pin, "global", "fixture", "request-1", forward.Repository)
			if mode == "verified" {
				if err != nil || s.Intent.Operation != productioncontrol.ComponentRegistryMove || s.Intent.ResourceID != "kubernetes/fugue-system/deployment/edge-control-de" {
					t.Fatalf("draft=%+v: %v", s.Intent, err)
				}
				review, err := InspectRegistryRehomeSubmission(s)
				if err != nil || review.Source.Deployment.UID != "deployment-uid" || review.Source.Services[0].Live.UID != "service-uid" ||
					review.Draft.DraftDigest != s.Intent.TargetDigest || s.Intent.RollbackDigest != source.Version.Digest {
					t.Fatalf("review=%+v: %v", review, err)
				}
				if _, err := productioncontrol.NewIntent("fixture", "registry/edge-control-de", "request-1", s.Intent.Operation,
					s.Intent.Generation, s.Intent.CurrentDigest, s.Intent.TargetDigest, s.Intent.RollbackDigest, s.Payload); err == nil {
					t.Fatal("accepted a separate registry lane for the Deployment")
				}
				review.Source.MonitorDigest = otherDigest
				altered, _ := declarativerelease.CanonicalJSON(review)
				alteredIntent, err := productioncontrol.NewIntent("fixture", s.Intent.ResourceID, "request-2", s.Intent.Operation,
					s.Intent.Generation, s.Intent.CurrentDigest, s.Intent.TargetDigest, s.Intent.RollbackDigest, altered)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := InspectRegistryRehomeSubmission(productioncontrol.Submission{Intent: alteredIntent, Payload: altered}); err == nil {
					t.Fatal("accepted a differently bound predecessor in a re-sealed intent")
				}
			} else {
				want := map[string]string{"busy journal": "settled journal", "wrong head": "settled journal", "replacement Deployment": "provider changed",
					"Service drift": "differs from its sealed declaration", "journal changed": "journal head changed", "pending legacy pointer": "read retained Guardian release alias"}[mode]
				if err == nil || !strings.Contains(err.Error(), want) || len(s.Payload) != 0 {
					t.Fatalf("accepted drifted draft=%+v err=%v want=%q", s.Intent, err, want)
				}
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("draft attempted a provider write: %v", action)
				}
			}
		})
	}
}

func fakeRehomeProvider(t *testing.T, raw []byte, archive verifiedReleaseRecoveryArchive) *fake.Clientset {
	t.Helper()
	var decoded sealedRecoveryArchive
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	pair := archive.report.Pair
	state, _ := json.Marshal(decoded.State)
	pointer := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: pair.Pointer.Name, UID: types.UID(pair.Pointer.UID),
		Namespace: pair.Namespace, ResourceVersion: pair.Pointer.ResourceVersion, Labels: recoveryMonitorLabels(pair.Forward.Monitor)},
		Data: map[string]string{"recordName": pair.Forward.Record.Name, "state.json": string(state)}}
	immutable := true
	objects := []runtime.Object{pointer}
	for _, record := range []sealedRecoveryRecord{archive.forward, archive.rollback} {
		ref := record.version.Record
		objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, UID: types.UID(ref.UID),
			Namespace: pair.Namespace, ResourceVersion: ref.ResourceVersion, Labels: recoveryMonitorLabels(record.version.Monitor)}, Immutable: &immutable, Data: record.files})
	}
	workload := recoveryWorkloadKey(archive.forward.workload).resource
	deploy := &appsv1.Deployment{}
	service := &corev1.Service{}
	for id, dst := range map[declarativerelease.ResourceIdentity]any{
		workload: deploy, {APIVersion: "v1", Kind: "Service", Namespace: pair.Namespace, Name: "test-tls"}: service,
	} {
		m, err := declarativerelease.ResourceSetItem(archive.forward.forward, id)
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(m)
		if err := json.Unmarshal(encoded, dst); err != nil {
			t.Fatal(err)
		}
	}
	deploy.UID, deploy.ResourceVersion, deploy.Generation = "deployment-uid", "100", 1
	deploy.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	service.UID, service.ResourceVersion = "service-uid", "101"
	return fake.NewSimpleClientset(append(objects, deploy, service)...)
}
