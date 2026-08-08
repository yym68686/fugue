package edgecontrol

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

const (
	RouteIntentPathV1           = "/v1/edge/route-intents"
	RouteIntentGenerationHeader = "X-Fugue-Route-Intent-Generation"
	routeIntentIssuerSchemaV1   = "edge-route-intent-issuer/v1"
	maxRouteIntentIssuerBytes   = 64 << 10
	maxRouteIntentCABytes       = 1 << 20
	maxRouteIntentResponseBytes = 16 << 20
	defaultRouteIntentTimeout   = 10 * time.Second
	defaultRouteIntentDialTime  = 5 * time.Second
)

var routeIntentServerNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$`)

var (
	ErrRouteIntentCredential     = errors.New("edge-control RouteIntent credential rejected")
	ErrRouteIntentUnauthorized   = errors.New("edge-control RouteIntent request unauthorized")
	ErrRouteIntentVersionBinding = errors.New("edge-control RouteIntent version binding rejected")
	ErrRouteIntentFetch          = errors.New("edge-control RouteIntent fetch failed")
)

type RouteIntentSource interface {
	FetchRouteIntents(context.Context) (model.EdgeRouteIntentSnapshot, error)
}

type RouteIntentClientConfig struct {
	Endpoint       string
	IssuerFile     string
	IdentityNodeID string
	CAFile         string
	ServerName     string
	Now            func() time.Time
}

// RouteIntentClient uses only its explicitly projected CA and fixed DNS name
// over TLS 1.3/HTTP 1.1, without environment proxies or redirects. It reads a
// short-lived component identity from a private file for every request, then
// binds the response body to the exact v1 path, schema, generation header,
// and strong ETag.
type RouteIntentClient struct {
	endpoint   *url.URL
	issuerFile string
	nodeID     string
	client     *http.Client
	now        func() time.Time
}

func NewRouteIntentClient(config RouteIntentClientConfig) (*RouteIntentClient, error) {
	if err := ValidateRouteIntentClientConfig(config); err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(strings.TrimSpace(config.Endpoint))
	if err != nil {
		return nil, errors.New("edge-control RouteIntent endpoint could not be parsed")
	}
	issuerFile := strings.TrimSpace(config.IssuerFile)
	roots, err := loadRouteIntentCAPool(strings.TrimSpace(config.CAFile))
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    roots,
		ServerName: strings.TrimSpace(config.ServerName),
		NextProtos: []string{"http/1.1"},
	}
	dialer := &net.Dialer{Timeout: defaultRouteIntentDialTime, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     false,
		TLSClientConfig:       tlsConfig,
		TLSHandshakeTimeout:   defaultRouteIntentDialTime,
		ResponseHeaderTimeout: defaultRouteIntentTimeout,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   2,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	client := &http.Client{Timeout: defaultRouteIntentTimeout, Transport: transport}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("edge-control RouteIntent redirect rejected")
	}
	now := func() time.Time { return time.Now().UTC() }
	if config.Now != nil {
		now = func() time.Time { return config.Now().UTC() }
	}
	return &RouteIntentClient{endpoint: endpoint, issuerFile: issuerFile, nodeID: strings.TrimSpace(config.IdentityNodeID), client: client, now: now}, nil
}

// ValidateRouteIntentClientConfig validates immutable names and paths without
// reading credentials. NewRouteIntentClient additionally loads and validates
// the explicit private CA at process startup.
func ValidateRouteIntentClientConfig(config RouteIntentClientConfig) error {
	endpoint, err := url.Parse(strings.TrimSpace(config.Endpoint))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Opaque != "" || endpoint.EscapedPath() != RouteIntentPathV1 {
		return errors.New("edge-control RouteIntent endpoint must be exact HTTPS /v1/edge/route-intents without userinfo, query, or fragment")
	}
	if endpoint.Hostname() == "" || endpoint.Hostname() != strings.ToLower(endpoint.Hostname()) {
		return errors.New("edge-control RouteIntent endpoint host must be canonical lowercase")
	}
	if port := endpoint.Port(); port != "" {
		parsed, err := strconv.Atoi(port)
		if err != nil || parsed < 1 || parsed > 65535 {
			return errors.New("edge-control RouteIntent endpoint port is invalid")
		}
	}
	issuerFile := strings.TrimSpace(config.IssuerFile)
	if issuerFile == "" || !filepath.IsAbs(issuerFile) || filepath.Clean(issuerFile) != issuerFile {
		return errors.New("edge-control RouteIntent issuer file must be an absolute normalized path")
	}
	caFile := strings.TrimSpace(config.CAFile)
	if caFile == "" || !filepath.IsAbs(caFile) || filepath.Clean(caFile) != caFile || caFile == issuerFile {
		return errors.New("edge-control RouteIntent CA file must be a distinct absolute normalized path")
	}
	nodeID := strings.TrimSpace(config.IdentityNodeID)
	if nodeID == "" || nodeID != strings.ToLower(nodeID) || !regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`).MatchString(nodeID) {
		return errors.New("edge-control RouteIntent identity node id is invalid")
	}
	serverName := strings.TrimSpace(config.ServerName)
	if len(serverName) > 253 || serverName != strings.ToLower(serverName) || !routeIntentServerNamePattern.MatchString(serverName) || net.ParseIP(serverName) != nil {
		return errors.New("edge-control RouteIntent server name must be an exact canonical DNS name")
	}
	if endpoint.Hostname() != serverName {
		return errors.New("edge-control RouteIntent endpoint host must exactly match the TLS server name")
	}
	return nil
}

func loadRouteIntentCAPool(path string) (*x509.CertPool, error) {
	data, err := readPrivateProjectedFile(path, maxRouteIntentCABytes)
	if err != nil {
		return nil, errors.New("edge-control RouteIntent CA bundle rejected")
	}
	pool := x509.NewCertPool()
	remaining := bytes.TrimSpace(data)
	certificates := 0
	for len(remaining) > 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errors.New("edge-control RouteIntent CA bundle rejected")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errors.New("edge-control RouteIntent CA bundle rejected")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.BasicConstraintsValid || !certificate.IsCA || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
			return nil, errors.New("edge-control RouteIntent CA bundle rejected")
		}
		pool.AddCert(certificate)
		certificates++
		remaining = bytes.TrimSpace(rest)
	}
	if certificates == 0 {
		return nil, errors.New("edge-control RouteIntent CA bundle rejected")
	}
	return pool, nil
}

func (client *RouteIntentClient) FetchRouteIntents(ctx context.Context) (model.EdgeRouteIntentSnapshot, error) {
	if client == nil || client.endpoint == nil || client.client == nil || client.now == nil {
		return model.EdgeRouteIntentSnapshot{}, ErrRouteIntentFetch
	}
	if ctx == nil {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: context is nil", ErrRouteIntentFetch)
	}
	token, err := client.readBoundCredential(client.now())
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.endpoint.String(), nil)
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: construct request", ErrRouteIntentFetch)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("User-Agent", "fugue-edge-control/route-intent-v1")
	response, err := client.client.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: transport", ErrRouteIntentFetch)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return model.EdgeRouteIntentSnapshot{}, ErrRouteIntentUnauthorized
	}
	if response.StatusCode != http.StatusOK {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: status %d", ErrRouteIntentFetch, response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: content type", ErrRouteIntentVersionBinding)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRouteIntentResponseBytes+1))
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: read body", ErrRouteIntentFetch)
	}
	if len(body) == 0 || len(body) > maxRouteIntentResponseBytes {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: response size", ErrRouteIntentVersionBinding)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var snapshot model.EdgeRouteIntentSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: decode v1 body", ErrRouteIntentVersionBinding)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: trailing body", ErrRouteIntentVersionBinding)
	}
	if err := validateRouteIntentSnapshot(snapshot); err != nil {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: schema", ErrRouteIntentVersionBinding)
	}
	generation := strings.TrimSpace(snapshot.Generation)
	if strings.TrimSpace(response.Header.Get(RouteIntentGenerationHeader)) != generation || response.Header.Get("ETag") != strconv.Quote(generation) {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("%w: generation or ETag", ErrRouteIntentVersionBinding)
	}
	return snapshot, nil
}

func (client *RouteIntentClient) readBoundCredential(now time.Time) (string, error) {
	data, err := readPrivateProjectedFile(client.issuerFile, maxRouteIntentIssuerBytes)
	if err != nil {
		return "", ErrRouteIntentCredential
	}
	defer zeroBytes(data)
	var issuer struct {
		Schema     string `json:"schema"`
		Generation uint64 `json:"generation"`
		KeyID      string `json:"key_id"`
		Key        string `json:"key"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&issuer); err != nil {
		return "", ErrRouteIntentCredential
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || issuer.Schema != routeIntentIssuerSchemaV1 || issuer.Generation == 0 ||
		strings.TrimSpace(issuer.KeyID) == "" || strings.ContainsAny(issuer.KeyID, "\r\n\t ") || len(strings.TrimSpace(issuer.Key)) < 32 {
		return "", ErrRouteIntentCredential
	}
	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring(issuer.Key, issuer.KeyID, "", "", nil)
	issuer.Key = ""
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID: "edge-control-route-intent-reader", Component: model.PlatformConsumerComponentEdgeControl,
		NodeID: client.nodeID, ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteIntent},
	}, now.UTC(), 2*time.Minute)
	if err != nil {
		return "", ErrRouteIntentCredential
	}
	return token, nil
}

func RouteIntentFailureCode(err error) string {
	switch {
	case errors.Is(err, ErrRouteIntentCredential):
		return "credential_rejected"
	case errors.Is(err, ErrRouteIntentUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrRouteIntentVersionBinding):
		return "version_binding_rejected"
	case errors.Is(err, ErrRouteIntentFetch):
		return "fetch_failed"
	case err != nil:
		return "runtime_failed"
	default:
		return ""
	}
}
