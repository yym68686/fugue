package main

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"fugue/internal/edgecontrolsupport"
)

const (
	edgeControlRouteIntentPathV1       = "/v1/edge/route-intents"
	edgeControlRouteIntentTLSBindAddr  = ":8443"
	edgeControlRouteIntentTLSDirectory = "/var/run/secrets/fugue-api-tls"
)

type edgeControlRouteIntentTLSConfig struct {
	BindAddr      string
	ProjectionDir string
	ServerName    string
}

func edgeControlRouteIntentTLSConfigFromEnv(getenv func(string) string) (edgeControlRouteIntentTLSConfig, error) {
	if getenv == nil {
		return edgeControlRouteIntentTLSConfig{}, errors.New("environment reader is nil")
	}
	config := edgeControlRouteIntentTLSConfig{
		BindAddr:      strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_BIND_ADDR")),
		ProjectionDir: strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_PROJECTION_DIR")),
		ServerName:    strings.TrimSuffix(strings.ToLower(strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_SERVER_NAME"))), "."),
	}
	if config.BindAddr == "" && config.ProjectionDir == "" && config.ServerName == "" {
		return edgeControlRouteIntentTLSConfig{}, nil
	}
	if config.BindAddr != edgeControlRouteIntentTLSBindAddr || config.ProjectionDir != edgeControlRouteIntentTLSDirectory ||
		!validEdgeControlRouteIntentTLSServerName(config.ServerName) {
		return edgeControlRouteIntentTLSConfig{}, errors.New("RouteIntent TLS environment must match the exact Service contract")
	}
	return config, nil
}

func validEdgeControlRouteIntentTLSServerName(value string) bool {
	const prefix, suffix = "fugue-api-tls.", ".svc"
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, suffix) {
		return false
	}
	namespace := strings.TrimSuffix(strings.TrimPrefix(value, prefix), suffix)
	if namespace == "" || namespace != strings.ToLower(namespace) || strings.HasPrefix(namespace, "-") || strings.HasSuffix(namespace, "-") {
		return false
	}
	for _, char := range namespace {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	return true
}

func edgeControlRouteIntentTLSHandler(next http.Handler, serverName string) (http.Handler, error) {
	serverName = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(serverName)), ".")
	if next == nil || !validEdgeControlRouteIntentTLSServerName(serverName) {
		return nil, errors.New("edge-control RouteIntent TLS handler config is invalid")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(r.Host)), ".")
		if parsedHost, _, err := net.SplitHostPort(host); err == nil {
			host = strings.TrimSuffix(strings.ToLower(parsedHost), ".")
		}
		sni := ""
		if r.TLS != nil {
			sni = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(r.TLS.ServerName)), ".")
		}
		if r.TLS == nil || sni != serverName || r.Method != http.MethodGet || r.URL.Path != edgeControlRouteIntentPathV1 ||
			r.URL.RawQuery != "" || host != serverName {
			http.NotFound(w, r)
			return
		}
		clone := r.Clone(r.Context())
		clone.Header = r.Header.Clone()
		for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP"} {
			clone.Header.Del(name)
		}
		next.ServeHTTP(w, clone)
	}), nil
}

func newEdgeControlRouteIntentTLSServer(config edgeControlRouteIntentTLSConfig, next http.Handler) (*http.Server, net.Listener, error) {
	if config.BindAddr == "" {
		return nil, nil, nil
	}
	handler, err := edgeControlRouteIntentTLSHandler(next, config.ServerName)
	if err != nil {
		return nil, nil, err
	}
	source := edgecontrolsupport.ProjectedServerCertificate{
		Directory: config.ProjectionDir, CertificateFile: "tls.crt",
		PrivateKeyFile: "tls.key", CAFile: "ca.crt", ServerName: config.ServerName,
	}
	tlsConfig, err := source.TLSConfig()
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return nil, nil, errors.New("listen on edge-control RouteIntent TLS address")
	}
	server := &http.Server{
		Addr: config.BindAddr, Handler: handler, TLSConfig: tlsConfig,
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 20 * time.Second,
		IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	return server, tls.NewListener(listener, tlsConfig), nil
}
