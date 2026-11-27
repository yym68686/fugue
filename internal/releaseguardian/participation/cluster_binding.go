package participation

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"syscall"

	"fugue/internal/productioncontrol"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const maxKubernetesCABytes = 16 << 10

// NewBoundKubernetesObserver checks the exact approved endpoint and CA before
// constructing a transport carrying Kubernetes credentials. VerifyNamespace
// must also be called before observing objects.
func NewBoundKubernetesObserver(config *rest.Config, policy productioncontrol.ParticipationPolicy) (*KubernetesObserver, error) {
	cfg, err := bindKubernetesConfig(config, policy)
	if err != nil {
		return nil, err
	}
	o, err := NewKubernetesObserver(cfg)
	if err != nil {
		return nil, err
	}
	o.namespace = policy.Namespace
	o.namespaceUID = policy.Kubernetes.NamespaceUID
	o.binding = *policy.Kubernetes
	return o, nil
}

func bindKubernetesConfig(config *rest.Config, policy productioncontrol.ParticipationPolicy) (*rest.Config, error) {
	if config == nil || policy.Schema != productioncontrol.BoundParticipationPolicySchema ||
		policy.Kubernetes == nil || policy.Kubernetes.Validate() != nil ||
		config.Host != policy.Kubernetes.Endpoint || config.ServerName != "" {
		return nil, errors.New("Kubernetes endpoint does not match the approved participation topology")
	}
	raw := config.CAData
	if len(raw) == 0 && config.CAFile != "" {
		file, err := os.OpenFile(config.CAFile, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, errors.New("Kubernetes CA file is unavailable")
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maxKubernetesCABytes {
			return nil, errors.New("Kubernetes CA file is unavailable or exceeds its size bound")
		}
		raw, err = io.ReadAll(io.LimitReader(file, maxKubernetesCABytes+1))
		if err != nil {
			return nil, errors.New("Kubernetes CA file could not be read")
		}
	}
	if len(raw) == 0 || len(raw) > maxKubernetesCABytes {
		return nil, errors.New("Kubernetes CA certificate is unavailable or exceeds its size bound")
	}
	raw = bytes.TrimSpace(raw)
	if !bytes.HasPrefix(raw, []byte("-----BEGIN CERTIFICATE-----")) {
		return nil, errors.New("Kubernetes config does not contain one approved CA certificate")
	}
	block, restPEM := pem.Decode(raw)
	if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || len(bytes.TrimSpace(restPEM)) != 0 {
		return nil, errors.New("Kubernetes config does not contain one approved CA certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !cert.IsCA || digest(cert.Raw) != policy.Kubernetes.CADigest {
		return nil, errors.New("Kubernetes CA does not match the approved participation topology")
	}
	cfg := rest.CopyConfig(config)
	cfg.CAFile = ""
	// Preserve exactly the validated certificate, never reopen a CA path.
	cfg.CAData = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	return cfg, nil
}

// VerifyNamespace verifies the live approved namespace instance on the
// already pinned TLS endpoint; a recreated namespace fails the binding.
func (o *KubernetesObserver) VerifyNamespace(ctx context.Context) error {
	if o == nil || o.core == nil || o.namespace == "" || o.namespaceUID == "" {
		return errors.New("participation Kubernetes namespace is not bound")
	}
	ns, err := o.core.Namespaces().Get(ctx, o.namespace, metav1.GetOptions{})
	if err != nil || !exactLiveObject(ns, "", o.namespace) || string(ns.UID) != o.namespaceUID {
		return errors.New("live Kubernetes namespace does not match the approved participation topology")
	}
	return nil
}
