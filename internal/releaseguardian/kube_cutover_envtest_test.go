package releaseguardian

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/testfixture/kubeadmission"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func startAdmissionTestServer(t *testing.T) *rest.Config {
	t.Helper()
	cfg, _ := kubeadmission.Start(t)
	return cfg
}

func admissionClient(t *testing.T, cfg *rest.Config, identity string) kubernetes.Interface {
	t.Helper()
	copy := rest.CopyConfig(cfg)
	if identity != "" {
		copy.Impersonate.UserName = identity
	}
	client, err := kubernetes.NewForConfig(copy)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func applyCutoverPhase(ctx context.Context, client kubernetes.Interface, config KubeDeploymentCutover) error {
	pairs, err := DeploymentCutoverPolicies(config)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		policies := client.AdmissionregistrationV1().ValidatingAdmissionPolicies()
		old, err := policies.Get(ctx, pair.Policy.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = policies.Create(ctx, pair.Policy, metav1.CreateOptions{})
		} else if err == nil {
			pair.Policy.ResourceVersion = old.ResourceVersion
			_, err = policies.Update(ctx, pair.Policy, metav1.UpdateOptions{})
		}
		if err != nil {
			return err
		}
		bindings := client.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings()
		if _, err := bindings.Create(ctx, pair.Binding, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func requireCutoverDenial(t *testing.T, err error) {
	t.Helper()
	if !isCutoverDenial(err) {
		t.Fatalf("expected the Fugue admission policy to deny mutation, got: %v", err)
	}
}

func isCutoverDenial(err error) bool {
	return apierrors.IsForbidden(err) && strings.Contains(err.Error(), "Fugue cutover denies mutation")
}

func waitForCutover(t *testing.T, ctx context.Context, client kubernetes.Interface, config KubeDeploymentCutover, ready func(context.Context) (bool, error)) {
	t.Helper()
	if err := applyCutoverPhase(ctx, client, config); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 12*time.Second, true, ready); err != nil {
		t.Fatalf("provider never enforced phase %s: %v", config.Phase, err)
	}
}

func createAdmissionTestNamespace(t *testing.T, ctx context.Context, admin kubernetes.Interface, namespace string) {
	t.Helper()
	if _, err := admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}
