// Package projected binds the server-side-dry-run Secret writer to one
// dedicated Kubernetes atomic-writer projection. It reloads the API JWT and
// CA roots for every validation request, independently revalidates the exact
// one-cell request body, and never reuses a TLS connection.
package projected

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/secretwriter"
)

const (
	APIVersion = "backup-materializer-secret-dry-run-projection.fugue.dev/v1"

	TokenFileName = "token"
	CAFileName    = "ca.crt"

	maxTokenBytes            = int64(16 << 10)
	maxCABundleBytes         = int64(256 << 10)
	maxCACertificates        = 32
	maxResponseHeaderBytes   = int64(64 << 10)
	minimumResponseBytes     = int64(4 << 10)
	defaultHandshakeTimeout  = 5 * time.Second
	maximumHandshakeTimeout  = 15 * time.Second
	minimumOperationDuration = time.Second
)

var (
	ErrConfig     = errors.New("backup materializer projected Secret dry-run configuration invalid")
	ErrCredential = errors.New("backup materializer projected Secret dry-run credential unavailable")
	ErrTrust      = errors.New("backup materializer projected Secret dry-run trust unavailable")
)

// Config describes one exact cell dry-run writer and the dedicated projection
// that contains only its Kubernetes API JWT and CA roots. Disabled
// construction ignores and retains none of these values.
type Config struct {
	Enabled          bool
	APIServerURL     string
	ProjectionRoot   string
	ExpectedCellKey  string
	RequestTimeout   time.Duration
	HandshakeTimeout time.Duration
	MaxResponseBytes int64
	Now              func() time.Time
}

func (config Config) String() string {
	return "backup materializer projected Secret dry-run configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

// New performs no network request. Enabled construction validates the current
// projection once and returns a dry-run-only writer with a per-request token
// source and direct rotating-CA transport. Disabled construction performs no
// filesystem access and returns the core writer's inert state.
func New(config Config) (*secretwriter.Writer, error) {
	if !config.Enabled {
		writer, err := secretwriter.New(secretwriter.Config{Enabled: false})
		if err != nil {
			return nil, ErrConfig
		}
		return writer, nil
	}
	authority, err := canonicalHTTPSAuthority(config.APIServerURL)
	if err != nil {
		return nil, ErrConfig
	}
	identity, err := materialization.SecretIdentityForCell(config.ExpectedCellKey)
	if err != nil {
		return nil, ErrConfig
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = secretwriter.DefaultRequestTimeout
	}
	if !boundedDuration(requestTimeout, minimumOperationDuration, secretwriter.MaximumRequestTimeout) {
		return nil, ErrConfig
	}
	handshakeTimeout := config.HandshakeTimeout
	if handshakeTimeout == 0 {
		handshakeTimeout = defaultHandshakeTimeout
		if handshakeTimeout > requestTimeout {
			handshakeTimeout = requestTimeout
		}
	}
	if !boundedDuration(handshakeTimeout, minimumOperationDuration, maximumHandshakeTimeout) ||
		handshakeTimeout > requestTimeout {
		return nil, ErrConfig
	}
	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = secretwriter.DefaultMaxResponse
	}
	if maxResponseBytes < minimumResponseBytes || maxResponseBytes > secretwriter.MaximumResponse {
		return nil, ErrConfig
	}
	projection, err := openProjection(config.ProjectionRoot)
	if err != nil {
		return nil, ErrConfig
	}
	source := &credentialSource{projection: projection}
	if _, err := source.Credential(context.Background()); err != nil {
		return nil, ErrCredential
	}
	if _, err := projection.loadCAPool(context.Background()); err != nil {
		return nil, ErrTrust
	}
	collectionPath := "/api/v1/namespaces/" + identity.Namespace + "/secrets"
	itemPath := collectionPath + "/" + identity.SecretName
	httpClient := &http.Client{Transport: &rotatingCATransport{
		projection:        projection,
		expectedAuthority: authority,
		expectedCellKey:   identity.CellKey,
		collectionPath:    collectionPath,
		itemPath:          itemPath,
		requestTimeout:    requestTimeout,
		handshakeTimeout:  handshakeTimeout,
	}}
	writer, err := secretwriter.New(secretwriter.Config{
		Enabled:          true,
		APIServerURL:     config.APIServerURL,
		ExpectedCellKey:  identity.CellKey,
		CredentialSource: source,
		HTTPClient:       httpClient,
		RequestTimeout:   requestTimeout,
		MaxResponseBytes: maxResponseBytes,
		Now:              config.Now,
	})
	if err != nil {
		return nil, ErrConfig
	}
	return writer, nil
}

type credentialSource struct {
	projection *projection
}

func (source *credentialSource) Credential(ctx context.Context) (string, error) {
	if source == nil || source.projection == nil || ctx == nil || ctx.Err() != nil {
		return "", ErrCredential
	}
	document, err := source.projection.readFile(ctx, TokenFileName, maxTokenBytes, tokenFilePolicy)
	if err != nil {
		return "", ErrCredential
	}
	token := string(document)
	if strings.HasSuffix(token, "\n") {
		token = strings.TrimSuffix(token, "\n")
	}
	if !canonicalJWT(token) {
		return "", ErrCredential
	}
	return token, nil
}

type projection struct {
	root string
}

func openProjection(root string) (*projection, error) {
	if root == "" || strings.TrimSpace(root) != root || !filepath.IsAbs(root) ||
		filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, ErrConfig
	}
	info, err := os.Lstat(root)
	if err != nil || !safeDirectory(info) {
		return nil, ErrConfig
	}
	return &projection{root: root}, nil
}

type filePolicy int

const (
	tokenFilePolicy filePolicy = iota
	caFilePolicy
)

func (projection *projection) readFile(
	ctx context.Context,
	name string,
	limit int64,
	policy filePolicy,
) ([]byte, error) {
	if projection == nil || ctx == nil || ctx.Err() != nil ||
		(name != TokenFileName && name != CAFileName) || limit <= 0 {
		return nil, ErrConfig
	}
	rootBefore, err := os.Lstat(projection.root)
	if err != nil || !safeDirectory(rootBefore) {
		return nil, ErrConfig
	}
	requestedPath := filepath.Join(projection.root, name)
	requestedBefore, err := os.Lstat(requestedPath)
	if err != nil {
		return nil, err
	}
	resolvedPath := requestedPath
	projectedFile := requestedBefore.Mode()&os.ModeSymlink != 0
	var generationPath string
	var generationBefore os.FileInfo
	var dataTarget string
	if projectedFile {
		resolvedPath, generationPath, generationBefore, dataTarget, err = projection.resolveAtomicWriterFile(name)
		if err != nil {
			return nil, err
		}
	} else if !requestedBefore.Mode().IsRegular() {
		return nil, ErrConfig
	}
	fileBefore, err := os.Lstat(resolvedPath)
	if err != nil || !safeFile(fileBefore, policy) {
		return nil, ErrConfig
	}
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !sameFileSnapshot(fileBefore, opened) {
		return nil, ErrConfig
	}
	document, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(document)) > limit {
		return nil, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rootAfter, err := os.Lstat(projection.root)
	if err != nil || !safeDirectory(rootAfter) || !sameFileSnapshot(rootBefore, rootAfter) {
		return nil, ErrConfig
	}
	if projectedFile {
		currentResolved, currentGenerationPath, generationAfter, currentDataTarget, resolveErr :=
			projection.resolveAtomicWriterFile(name)
		if resolveErr != nil || currentResolved != resolvedPath || currentGenerationPath != generationPath ||
			currentDataTarget != dataTarget || !sameFileSnapshot(generationBefore, generationAfter) {
			return nil, ErrConfig
		}
	} else {
		requestedAfter, statErr := os.Lstat(requestedPath)
		if statErr != nil || !safeFile(requestedAfter, policy) || !sameFileSnapshot(requestedBefore, requestedAfter) {
			return nil, ErrConfig
		}
	}
	current, err := os.Lstat(resolvedPath)
	if err != nil || !safeFile(current, policy) || !sameFileSnapshot(opened, current) {
		return nil, ErrConfig
	}
	return document, nil
}

// resolveAtomicWriterFile accepts only Kubernetes' two-link layout:
//
//	<name> -> ..data/<name>
//	..data -> ..<generation>
//
// The generation must be a real in-root directory. This permits kubelet's
// canonical 0755 generation directories but never follows an arbitrary link.
func (projection *projection) resolveAtomicWriterFile(
	name string,
) (resolvedPath string, generationPath string, generationInfo os.FileInfo, dataTarget string, err error) {
	requestedPath := filepath.Join(projection.root, name)
	requestedTarget, err := os.Readlink(requestedPath)
	if err != nil || requestedTarget != filepath.Join("..data", name) {
		return "", "", nil, "", ErrConfig
	}
	dataPath := filepath.Join(projection.root, "..data")
	dataInfo, err := os.Lstat(dataPath)
	if err != nil || dataInfo.Mode()&os.ModeSymlink == 0 {
		return "", "", nil, "", ErrConfig
	}
	dataTarget, err = os.Readlink(dataPath)
	if err != nil || filepath.IsAbs(dataTarget) || filepath.Clean(dataTarget) != dataTarget ||
		filepath.Base(dataTarget) != dataTarget || dataTarget == ".." || !strings.HasPrefix(dataTarget, "..") {
		return "", "", nil, "", ErrConfig
	}
	generationPath = filepath.Join(projection.root, dataTarget)
	generationInfo, err = os.Lstat(generationPath)
	if err != nil || !safeDirectory(generationInfo) {
		return "", "", nil, "", ErrConfig
	}
	resolvedPath = filepath.Join(generationPath, name)
	return resolvedPath, generationPath, generationInfo, dataTarget, nil
}

func (projection *projection) loadCAPool(ctx context.Context) (*x509.CertPool, error) {
	if projection == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrTrust
	}
	document, err := projection.readFile(ctx, CAFileName, maxCABundleBytes, caFilePolicy)
	if err != nil {
		return nil, ErrTrust
	}
	roots := x509.NewCertPool()
	rest := document
	count := 0
	for {
		rest = bytes.TrimSpace(rest)
		if len(rest) == 0 {
			break
		}
		block, remaining := pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, ErrTrust
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.BasicConstraintsValid || !certificate.IsCA {
			return nil, ErrTrust
		}
		roots.AddCert(certificate)
		count++
		if count > maxCACertificates {
			return nil, ErrTrust
		}
		rest = remaining
	}
	if count == 0 {
		return nil, ErrTrust
	}
	return roots, nil
}

type rotatingCATransport struct {
	projection        *projection
	expectedAuthority string
	expectedCellKey   string
	collectionPath    string
	itemPath          string
	requestTimeout    time.Duration
	handshakeTimeout  time.Duration
}

func (transport *rotatingCATransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport == nil || transport.projection == nil || request == nil || request.URL == nil || request.Context() == nil ||
		(request.Method != http.MethodPost && request.Method != http.MethodPut) || request.Body == nil || request.Body == http.NoBody ||
		request.GetBody == nil || request.ContentLength <= 0 || request.ContentLength > secretwriter.MaximumRequestBytes ||
		len(request.TransferEncoding) != 0 || len(request.Trailer) != 0 || !request.Close || !safeRequestHeaders(request.Header) ||
		request.URL.Scheme != "https" || request.URL.Host != transport.expectedAuthority ||
		(request.Host != "" && request.Host != transport.expectedAuthority) || request.URL.User != nil || request.URL.Opaque != "" ||
		(request.URL.Path != transport.collectionPath && request.URL.Path != transport.itemPath) || request.URL.RawPath != "" ||
		request.URL.EscapedPath() != request.URL.Path || request.URL.RawQuery != secretwriter.DryRunRawQuery ||
		request.URL.ForceQuery || request.URL.Fragment != "" ||
		!boundedDuration(transport.requestTimeout, minimumOperationDuration, secretwriter.MaximumRequestTimeout) ||
		!boundedDuration(transport.handshakeTimeout, minimumOperationDuration, maximumHandshakeTimeout) ||
		transport.handshakeTimeout > transport.requestTimeout {
		return nil, ErrTrust
	}
	document, err := io.ReadAll(io.LimitReader(request.Body, secretwriter.MaximumRequestBytes+1))
	closeErr := request.Body.Close()
	if err != nil || closeErr != nil || len(document) == 0 || int64(len(document)) != request.ContentLength ||
		int64(len(document)) > secretwriter.MaximumRequestBytes ||
		secretwriter.ValidateTransportRequest(request.Method, request.URL.Path, request.URL.RawQuery, transport.expectedCellKey, document) != nil {
		return nil, ErrTrust
	}
	if err := request.Context().Err(); err != nil {
		return nil, ErrTrust
	}
	request.Body = io.NopCloser(bytes.NewReader(document))
	request.GetBody = nil
	request.ContentLength = int64(len(document))
	roots, err := transport.projection.loadCAPool(request.Context())
	if err != nil {
		return nil, ErrTrust
	}
	dialer := &net.Dialer{Timeout: transport.handshakeTimeout, KeepAlive: -1}
	requestTransport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		ForceAttemptHTTP2:      true,
		DisableCompression:     true,
		DisableKeepAlives:      true,
		MaxIdleConns:           0,
		MaxIdleConnsPerHost:    1,
		MaxConnsPerHost:        1,
		IdleConnTimeout:        0,
		TLSHandshakeTimeout:    transport.handshakeTimeout,
		ResponseHeaderTimeout:  transport.requestTimeout,
		ExpectContinueTimeout:  0,
		MaxResponseHeaderBytes: maxResponseHeaderBytes,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: request.URL.Hostname(),
		},
	}
	response, err := requestTransport.RoundTrip(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		requestTransport.CloseIdleConnections()
		return nil, ErrTrust
	}
	if response == nil || response.Body == nil {
		requestTransport.CloseIdleConnections()
		return nil, ErrTrust
	}
	response.Body = &closingBody{ReadCloser: response.Body, closeIdle: requestTransport.CloseIdleConnections}
	return response, nil
}

func safeRequestHeaders(header http.Header) bool {
	if len(header) != 6 || header.Get("Accept") != "application/json" ||
		header.Get("Accept-Encoding") != "identity" || header.Get("Cache-Control") != "no-store" ||
		header.Get("Content-Type") != "application/json" || header.Get("User-Agent") != secretwriter.RequestUserAgent {
		return false
	}
	authorization := header.Get("Authorization")
	if !strings.HasPrefix(authorization, "Bearer ") || !canonicalJWT(strings.TrimPrefix(authorization, "Bearer ")) {
		return false
	}
	for name, values := range header {
		if name != "Accept" && name != "Accept-Encoding" && name != "Authorization" && name != "Cache-Control" &&
			name != "Content-Type" && name != "User-Agent" {
			return false
		}
		if len(values) != 1 || values[0] == "" {
			return false
		}
	}
	return true
}

type closingBody struct {
	io.ReadCloser
	closeIdle func()
}

func (body *closingBody) Close() error {
	if body == nil || body.ReadCloser == nil {
		return nil
	}
	err := body.ReadCloser.Close()
	if body.closeIdle != nil {
		body.closeIdle()
	}
	return err
}

func canonicalHTTPSAuthority(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return "", ErrConfig
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.String() != raw ||
		strings.HasSuffix(parsed.Host, ":") {
		return "", ErrConfig
	}
	if port := parsed.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return "", ErrConfig
		}
	}
	return parsed.Host, nil
}

func canonicalJWT(token string) bool {
	if token == "" || len(token) > int(maxTokenBytes) || strings.TrimSpace(token) != token || strings.ContainsAny(token, "\r\n") {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, character := range part {
			if !((character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
				(character >= '0' && character <= '9') || character == '_' || character == '-') {
				return false
			}
		}
	}
	return true
}

func boundedDuration(value, minimum, maximum time.Duration) bool {
	return value >= minimum && value <= maximum && value%time.Millisecond == 0
}

func safeDirectory(info os.FileInfo) bool {
	return info != nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 &&
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) == 0 && info.Mode().Perm()&0o022 == 0
}

func safeFile(info os.FileInfo, policy filePolicy) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return false
	}
	permissions := info.Mode().Perm()
	switch policy {
	case tokenFilePolicy:
		return permissions == 0o400 || permissions == 0o440 || permissions == 0o600 || permissions == 0o640
	case caFilePolicy:
		return permissions&0o133 == 0 && permissions&0o400 != 0
	default:
		return false
	}
}

func sameFileSnapshot(before, after os.FileInfo) bool {
	return before != nil && after != nil && os.SameFile(before, after) && before.Mode() == after.Mode() &&
		before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}
