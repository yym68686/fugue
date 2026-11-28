package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"github.com/google/go-containerregistry/pkg/authn"
	"k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func verifyReleaseRecoveryMirror(ctx context.Context, path string, out io.Writer) error {
	raw, err := readFile(path, 16<<10, false)
	if err != nil {
		return err
	}
	type artifact struct {
		ReceiptFile   string `json:"receiptFile"`
		ReceiptDigest string `json:"receiptDigest"`
		Repository    string `json:"destinationRepository"`
	}
	var cfg struct {
		Registry string   `json:"registry"`
		AuthFile string   `json:"registryAuthFile,omitempty"`
		Forward  artifact `json:"forward"`
		Rollback artifact `json:"rollback"`
	}
	if decodeRecoveryMirrorJSON(raw, &cfg) != nil {
		return errors.New("invalid release recovery mirror configuration")
	}
	registry, err := newRecoveryRegistry(cfg.Registry, cfg.AuthFile)
	if err != nil {
		return err
	}
	readArtifact := func(input artifact) (releaseguardian.RegistryRecoveryArtifact, error) {
		if !filepath.IsAbs(input.ReceiptFile) {
			return releaseguardian.RegistryRecoveryArtifact{}, errors.New("recovery artifact receipt file must have an absolute path")
		}
		raw, err := readFile(input.ReceiptFile, 32<<10, false)
		if err != nil {
			return releaseguardian.RegistryRecoveryArtifact{}, err
		}
		receipt, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(raw))
		return releaseguardian.RegistryRecoveryArtifact{Source: receipt, SourceReceiptDigest: input.ReceiptDigest,
			DestinationRepository: input.Repository}, err
	}
	forward, err := readArtifact(cfg.Forward)
	if err != nil {
		return err
	}
	rollback, err := readArtifact(cfg.Rollback)
	if err != nil {
		return err
	}
	report, err := registry.VerifyRecoveryMirror(ctx, forward, rollback)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func verifySealedReleaseRecoveryMirror(ctx context.Context, path, kubeconfig, contextName string, out io.Writer) error {
	raw, err := readFile(path, 16<<10, false)
	if err != nil {
		return err
	}
	var cfg struct {
		Namespace          string `json:"namespace"`
		Component          string `json:"component"`
		Registry           string `json:"registry"`
		AuthFile           string `json:"registryAuthFile,omitempty"`
		ForwardRepository  string `json:"forwardRepository"`
		RollbackRepository string `json:"rollbackRepository"`
	}
	if decodeRecoveryMirrorJSON(raw, &cfg) != nil || cfg.ForwardRepository == "" || cfg.RollbackRepository == "" {
		return errors.New("invalid sealed release recovery mirror configuration")
	}
	client, err := sealedRecoveryClient(kubeconfig, contextName)
	if err != nil {
		return err
	}
	registry, err := newRecoveryRegistry(cfg.Registry, cfg.AuthFile)
	if err != nil {
		return err
	}
	pair, err := observeSealedRecoveryPair(ctx, client, cfg.Namespace, cfg.Component)
	if err != nil {
		return err
	}
	input := func(version releaseguardian.SealedRecoveryVersion, repository string) releaseguardian.RegistryRecoveryArtifact {
		return releaseguardian.RegistryRecoveryArtifact{Source: version.Receipt, SourceReceiptDigest: version.Monitor.ArtifactDigest,
			DestinationRepository: repository}
	}
	report, err := registry.VerifyRecoveryMirror(ctx, input(pair.Forward, cfg.ForwardRepository), input(pair.Rollback, cfg.RollbackRepository))
	if err != nil {
		return err
	}
	latest, err := observeSealedRecoveryPair(ctx, client, cfg.Namespace, cfg.Component)
	if err != nil {
		return err
	}
	if latest != pair {
		return errors.New("sealed recovery records changed during mirror verification; retry against a stable pointer")
	}
	return json.NewEncoder(out).Encode(struct {
		Source releaseguardian.SealedRecoveryPair     `json:"sealedSource"`
		Mirror releaseguardian.RegistryRecoveryReport `json:"mirror"`
	}{pair, report})
}

func observeSealedRecoveryPair(ctx context.Context, client coreclient.CoreV1Interface, namespace, component string) (releaseguardian.SealedRecoveryPair, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return releaseguardian.ObserveSealedRecoveryPair(ctx, client, namespace, component)
}

func sealedRecoveryClient(path, contextName string) (coreclient.CoreV1Interface, error) {
	client, err := sealedRecoveryProvider(path, contextName)
	if err != nil {
		return nil, err
	}
	return client.CoreV1(), nil
}

func sealedRecoveryProvider(path, contextName string) (kubernetes.Interface, error) {
	cfg, err := readKubeObservationConfig(path, contextName)
	if err != nil {
		return nil, err
	}
	server, err := url.Parse(cfg.Host)
	if err != nil || server.Scheme != "https" || server.Host == "" || server.User != nil || server.RawQuery != "" ||
		server.Fragment != "" || (server.Path != "" && server.Path != "/") || cfg.Insecure ||
		cfg.ExecProvider != nil || cfg.AuthProvider != nil || cfg.Proxy != nil {
		return nil, errors.New("sealed recovery observation requires a static, TLS-verified Kubernetes API context")
	}
	cfg.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper { return recoveryObservationTransport{server.Host, next} })
	client, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return kubernetes.NewForConfigAndClient(cfg, client)
}

type recoveryObservationTransport struct {
	host string
	next http.RoundTripper
}

func (t recoveryObservationTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet || r.Header.Get("Upgrade") != "" || r.URL.Scheme != "https" ||
		r.URL.Host != t.host || r.URL.User != nil {
		return nil, errors.New("sealed recovery observation only accepts plain GET to the configured Kubernetes HTTPS API")
	}
	return t.next.RoundTrip(r)
}

func newRecoveryRegistry(host, authFile string) (*releaseguardian.IndependentRegistry, error) {
	if authFile == "" {
		return releaseguardian.NewIndependentRegistry(host, nil, nil)
	}
	if !filepath.IsAbs(authFile) {
		return nil, errors.New("recovery registry credential file must have an absolute path")
	}
	raw, err := readFile(authFile, 8<<10, true)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	var secret struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if decodeRecoveryMirrorJSON(raw, &secret) != nil || secret.Username == "" || secret.Password == "" {
		return nil, errors.New("invalid independent recovery registry credential")
	}
	return releaseguardian.NewIndependentRegistry(host, authn.FromConfig(authn.AuthConfig{Username: secret.Username, Password: secret.Password}), nil)
}

func decodeRecoveryMirrorJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("expected one recovery mirror configuration object")
	}
	return nil
}
