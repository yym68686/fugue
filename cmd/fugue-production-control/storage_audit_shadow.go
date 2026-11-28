package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/participation"
	"fugue/internal/storageaudit"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

const storageAuditConfigKey = "storage-lifecycle.json"

func shadowStorageAudit(ctx context.Context, paths []string, out io.Writer) error {
	raw, err := readFile(paths[3], 16<<10, false)
	if err != nil {
		return err
	}
	var spec productioncontrol.StorageAuditShadowBinding
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&spec) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid storage audit shadow specification")
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	raw, err = readFile(paths[4], 4096, false)
	if err != nil {
		return err
	}
	target, err := parseShadowPolicy(raw)
	if err != nil {
		return err
	}
	cfg, err := readKubeObservationConfig(paths[1], paths[2])
	if err != nil {
		return err
	}
	if cfg.Insecure {
		return errors.New("storage audit shadow requires verified Kubernetes TLS")
	}
	cfg, err = participation.BindKubernetesConfig(cfg, spec.Kubernetes)
	if err != nil {
		return err
	}
	client, err := newStorageAuditShadowClient(cfg)
	if err != nil {
		return err
	}
	provider, db, err := openStorageAuditObserver(paths[0])
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	report, err := readStorageAuditShadow(ctx, client, spec, target, func(ctx context.Context) (storageaudit.OwnerObservation, error) {
		return provider.Observe(ctx, db)
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func newStorageAuditShadowClient(cfg *rest.Config) (*corev1.CoreV1Client, error) {
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return storageAuditGETOnly{next}
	})
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return corev1.NewForConfigAndClient(cfg, httpClient)
}

type storageAuditGETOnly struct{ next http.RoundTripper }

func (r storageAuditGETOnly) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.Header.Get("Upgrade") != "" {
		return nil, errors.New("storage audit shadow permits only plain Kubernetes GET")
	}
	return r.next.RoundTrip(req)
}

func parseShadowPolicy(raw []byte) (storageaudit.Policy, error) {
	policy, err := storageaudit.ParsePolicy(raw)
	// The legacy publisher and the PostgreSQL policy contract agree at whole seconds.
	if err != nil || policy.EffectiveFrom.Nanosecond() != 0 {
		return storageaudit.Policy{}, errors.New("invalid storage audit shadow policy or effective time precision")
	}
	return policy, nil
}

func readStorageAuditShadow(ctx context.Context, client corev1.CoreV1Interface, spec productioncontrol.StorageAuditShadowBinding,
	target storageaudit.Policy, observe func(context.Context) (storageaudit.OwnerObservation, error)) (productioncontrol.StorageAuditShadow, error) {
	readPublished := func() (storageaudit.Policy, string, error) {
		ns, err := client.Namespaces().Get(ctx, spec.Namespace, metav1.GetOptions{})
		if err != nil || ns.Name != spec.Namespace || string(ns.UID) != spec.Kubernetes.NamespaceUID ||
			ns.ResourceVersion == "" || ns.DeletionTimestamp != nil {
			return storageaudit.Policy{}, "", errors.New("live namespace does not match the storage audit shadow binding")
		}
		cm, err := client.ConfigMaps(spec.Namespace).Get(ctx, spec.ConfigMap, metav1.GetOptions{})
		if err != nil || cm.Name != spec.ConfigMap || cm.Namespace != spec.Namespace || string(cm.UID) != spec.ConfigMapUID ||
			cm.ResourceVersion == "" || cm.DeletionTimestamp != nil {
			return storageaudit.Policy{}, "", errors.New("live ConfigMap does not match the storage audit shadow binding")
		}
		policy, err := parseShadowPolicy([]byte(cm.Data[storageAuditConfigKey]))
		return policy, cm.ResourceVersion, err
	}
	before, version, err := readPublished()
	if err != nil {
		return productioncontrol.StorageAuditShadow{}, err
	}
	database, err := observe(ctx)
	if err != nil {
		return productioncontrol.StorageAuditShadow{}, err
	}
	if database.Owner != "worker" || database.Activation == nil || database.PolicyDigest != before.Digest() ||
		database.Policy != before {
		return productioncontrol.StorageAuditShadow{}, errors.New("PostgreSQL and the published storage audit policy disagree")
	}
	if target == before {
		return productioncontrol.StorageAuditShadow{}, errors.New("storage audit candidate matches the live policy")
	}
	after, afterVersion, err := readPublished()
	if err != nil {
		return productioncontrol.StorageAuditShadow{}, err
	}
	if version != afterVersion || before != after {
		return productioncontrol.StorageAuditShadow{}, errors.New("published storage audit policy changed during PostgreSQL readback")
	}
	current := productioncontrol.StorageAuditPolicyVersion{Document: before, Digest: "sha256:" + database.PolicyDigest}
	return productioncontrol.StorageAuditShadow{
		Schema: productioncontrol.StorageAuditShadowSchema, Binding: spec, ConfigMapResourceVersion: version,
		Provider: database.Provider, Owner: database.Owner, Activation: *database.Activation, Current: current,
		Target: productioncontrol.StorageAuditPolicyVersion{Document: target, Digest: "sha256:" + target.Digest()}, Rollback: current,
	}, nil
}
