package sshcontrol

import (
	"context"
	"encoding/json"
	"testing"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func TestResourcePublicationSkipsUnchangedWritesButRepairsDriftAndRemovedIntent(test *testing.T) {
	ctx := context.Background()
	slot := managedssh.ServingSlot{ID: "ssh-slot", Namespace: "platform", Artifact: managedssh.Artifact{Configuration: managedssh.Configuration{Identity: model.AppSSHIdentity{ID: "identity", TenantID: "tenant", AppID: "workspace"}}}}
	desired := &corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: slot.ID, Namespace: slot.Namespace}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: map[string]string{"component": "ssh", "obsolete": "value"}, Ports: []corev1.ServicePort{{Port: 2222}}}}
	encoded, _ := json.Marshal(desired)
	current := &unstructured.Unstructured{}
	if err := current.UnmarshalJSON(encoded); err != nil {
		test.Fatal(err)
	}
	current.SetLabels(slotLabels(slot))
	current.SetUID("service-identity")
	current.SetResourceVersion("1")
	resource := schema.GroupVersionResource{Version: "v1", Resource: "services"}
	client := dynamicfake.NewSimpleDynamicClient(kuberuntime.NewScheme(), current)
	patches := 0
	client.PrependReactor("patch", "services", func(action kubetesting.Action) (bool, kuberuntime.Object, error) {
		patches++
		patch := action.(kubetesting.PatchAction)
		updated := &unstructured.Unstructured{}
		if err := updated.UnmarshalJSON(patch.GetPatch()); err != nil {
			test.Fatal(err)
		}
		updated.SetUID("service-identity")
		if err := unstructured.SetNestedField(updated.Object, "10.43.0.9", "spec", "clusterIP"); err != nil {
			test.Fatal(err)
		}
		if err := client.Tracker().Update(resource, updated, slot.Namespace); err != nil {
			test.Fatal(err)
		}
		return true, updated, nil
	})
	runtime := &KubernetesRuntime{dynamic: client}
	for index := 0; index < 3; index++ {
		if err := runtime.applyObject(ctx, slot, desired); err != nil {
			test.Fatal(err)
		}
	}
	if patches != 1 {
		test.Fatal("unchanged resources repeatedly consume write budget", patches)
	}
	current, _ = client.Resource(resource).Namespace(slot.Namespace).Get(ctx, slot.ID, metav1.GetOptions{})
	_ = unstructured.SetNestedField(current.Object, "foreign", "spec", "selector", "component")
	if err := client.Tracker().Update(resource, current, slot.Namespace); err != nil {
		test.Fatal(err)
	}
	if err := runtime.applyObject(ctx, slot, desired); err != nil || patches != 2 {
		test.Fatal("unchanged declaration failed to repair actual resource drift", patches, err)
	}
	delete(desired.Spec.Selector, "obsolete")
	if err := runtime.applyObject(ctx, slot, desired); err != nil || patches != 3 {
		test.Fatal("removed owned configuration was mistaken for a server default", patches, err)
	}
}

func TestAuthorizationPublicationNeverCreatesMissingRuntimeResources(test *testing.T) {
	client := fake.NewClientset()
	runtime := &KubernetesRuntime{client: client}
	slot := managedssh.ServingSlot{ID: "ssh-slot", Namespace: "platform"}
	desired := corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ssh-slot-artifact", Namespace: slot.Namespace}, Data: map[string][]byte{"artifact.json": []byte("fixture")}}
	if err := runtime.updateSecret(context.Background(), slot, desired, false); err != nil {
		test.Fatal(err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			test.Fatal("authorization renewal created or changed an unprepared resource", action)
		}
	}
}
