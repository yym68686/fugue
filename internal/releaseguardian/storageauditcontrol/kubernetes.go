package storageauditcontrol

import (
	"context"
	"errors"
	"net/http"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/participation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

const ConfigKey = "storage-lifecycle.json"

func NewReader(config *rest.Config, spec productioncontrol.StorageAuditShadowBinding) (*corev1.CoreV1Client, error) {
	if config == nil || config.Insecure || spec.Validate() != nil {
		return nil, errors.New("storage audit requires a verified Kubernetes endpoint and exact ConfigMap identity")
	}
	config, err := participation.BindKubernetesConfig(config, spec.Kubernetes)
	if err != nil {
		return nil, err
	}
	config.Wrap(func(next http.RoundTripper) http.RoundTripper { return getOnly{next} })
	client, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, err
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return corev1.NewForConfigAndClient(config, client)
}

type getOnly struct{ next http.RoundTripper }

func (r getOnly) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.Header.Get("Upgrade") != "" {
		return nil, errors.New("storage audit observation permits only plain Kubernetes GET")
	}
	return r.next.RoundTrip(req)
}

// ReadProjection pins the namespace and ConfigMap instances. The lifecycle
// key can be removed while unrelated storage settings remain in the ConfigMap.
func ReadProjection(ctx context.Context, client corev1.CoreV1Interface,
	spec productioncontrol.StorageAuditShadowBinding) (raw string, present bool, version string, err error) {
	ns, err := client.Namespaces().Get(ctx, spec.Namespace, metav1.GetOptions{})
	if err != nil || ns.Name != spec.Namespace || string(ns.UID) != spec.Kubernetes.NamespaceUID ||
		ns.ResourceVersion == "" || ns.DeletionTimestamp != nil {
		return "", false, "", errors.New("live namespace does not match the storage audit binding")
	}
	cm, err := client.ConfigMaps(spec.Namespace).Get(ctx, spec.ConfigMap, metav1.GetOptions{})
	if err != nil || cm.Name != spec.ConfigMap || cm.Namespace != spec.Namespace || string(cm.UID) != spec.ConfigMapUID ||
		cm.ResourceVersion == "" || cm.DeletionTimestamp != nil {
		return "", false, "", errors.New("live ConfigMap does not match the storage audit binding")
	}
	if _, binary := cm.BinaryData[ConfigKey]; binary {
		return "", false, "", errors.New("storage audit policy uses an unexpected binary projection")
	}
	raw, present = cm.Data[ConfigKey]
	return raw, present, cm.ResourceVersion, nil
}

func VerifyRetiredProjection(ctx context.Context, client corev1.CoreV1Interface, shadow productioncontrol.StorageAuditShadow) error {
	_, present, version, err := ReadProjection(ctx, client, shadow.Binding)
	if err != nil {
		return err
	}
	if !shadow.PublishedPolicyRetired || present || version != shadow.ConfigMapResourceVersion {
		return errors.New("storage audit Kubernetes projection differs from the approved retired state")
	}
	return nil
}
