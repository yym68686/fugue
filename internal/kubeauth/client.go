package kubeauth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const ServiceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

type Config struct{ BaseURL, Token, Namespace string }

func Load(namespace string, timeout time.Duration, maxIdle, maxIdleHost int, forceHTTP2 bool) (Config, *http.Client, error) {
	host, port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")), strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return Config{}, nil, fmt.Errorf("kubernetes service host/port is not available in the environment")
	}
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return Config{}, nil, fmt.Errorf("read service account token: %w", err)
	}
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return Config{}, nil, fmt.Errorf("read service account CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return Config{}, nil, fmt.Errorf("load service account CA")
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		if data, e := os.ReadFile(ServiceAccountNamespacePath); e == nil {
			namespace = strings.TrimSpace(string(data))
		}
	}
	if namespace == "" {
		return Config{}, nil, fmt.Errorf("resolve kubernetes namespace")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: forceHTTP2}
	if maxIdle > 0 {
		transport.MaxIdleConns = maxIdle
	}
	if maxIdleHost > 0 {
		transport.MaxIdleConnsPerHost = maxIdleHost
	}
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSHandshakeTimeout = 5 * time.Second
	client := &http.Client{Transport: transport, Timeout: timeout}
	return Config{BaseURL: "https://" + host + ":" + port, Token: strings.TrimSpace(string(token)), Namespace: namespace}, client, nil
}
