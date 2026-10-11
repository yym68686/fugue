package sshcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestKubernetesSecretPublicationFencesIdentityAndGeneration(test *testing.T) {
	ctx := context.Background()
	immutable := true
	slot := managedssh.ServingSlot{ID: "ssh-slot", Namespace: "platform", Artifact: managedssh.Artifact{Configuration: managedssh.Configuration{Identity: model.AppSSHIdentity{ID: "identity", TenantID: "tenant", AppID: "workspace"}, Generation: 4}, Digest: "candidate"}}
	previous := slot.Artifact
	previous.Configuration.Generation = 3
	previous.Digest = "previous"
	encoded, _ := json.Marshal(previous)
	identity := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "identity", Namespace: slot.Namespace, Labels: slotLabels(slot)}, Immutable: &immutable, Data: map[string][]byte{"host-key": []byte("existing-test-private-material")}}
	artifact := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "artifact", Namespace: slot.Namespace, Labels: slotLabels(slot)}, Data: map[string][]byte{"artifact.json": encoded}}
	client := fake.NewClientset(identity, artifact)
	runtime := &KubernetesRuntime{client: client}
	changedIdentity := identity.DeepCopy()
	changedIdentity.Data = map[string][]byte{"host-key": []byte("replacement")}
	if err := runtime.applySecret(ctx, slot, *changedIdentity); !errors.Is(err, managedssh.ErrIdentityChange) {
		test.Fatal("identity replacement was admitted", err)
	}
	current, err := client.CoreV1().Secrets(slot.Namespace).Get(ctx, identity.Name, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(current.Data, identity.Data) {
		test.Fatal("rejected identity update changed serving data")
	}
	stale := slot
	stale.Artifact.Configuration.Generation = 2
	staleArtifact := artifact.DeepCopy()
	staleArtifact.Data["artifact.json"], _ = json.Marshal(stale.Artifact)
	if err := runtime.applySecret(ctx, stale, *staleArtifact); !errors.Is(err, managedssh.ErrGeneration) {
		test.Fatal("stale publisher replaced newer artifact", err)
	}
	candidate := artifact.DeepCopy()
	candidate.Data["artifact.json"], _ = json.Marshal(slot.Artifact)
	if err := runtime.applySecret(ctx, slot, *candidate); err != nil {
		test.Fatal(err)
	}
	if err := runtime.applySecret(ctx, slot, *candidate); err != nil {
		test.Fatal("idempotent publication failed", err)
	}
	current, err = client.CoreV1().Secrets(slot.Namespace).Get(ctx, candidate.Name, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(current.Data, candidate.Data) {
		test.Fatal("new artifact did not publish", err)
	}
	current.Labels["fugue.pro/ssh-tenant-id"] = "other-tenant"
	if _, err := client.CoreV1().Secrets(slot.Namespace).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		test.Fatal(err)
	}
	if err := runtime.applySecret(ctx, slot, *candidate); !errors.Is(err, managedssh.ErrUnauthorized) {
		test.Fatal("controller adopted another tenant's resource", err)
	}
}
