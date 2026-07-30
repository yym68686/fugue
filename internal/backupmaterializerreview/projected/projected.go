// Package projected binds the backup materializer TokenReview adapter to one
// Kubernetes atomic-writer projection. It reloads the API caller credential
// and CA roots for every review without exposing filesystem or TLS capability
// to the identity policy itself.
package projected

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/backupmaterializerreview"
)

const (
	TokenFileName = "token"
	CAFileName    = "ca.crt"

	maxTokenBytes           = int64(16 << 10)
	maxCABundleBytes        = int64(256 << 10)
	maxCACertificates       = 32
	defaultRequestTimeout   = 5 * time.Second
	defaultHandshakeTimeout = 5 * time.Second
)

var (
	ErrConfig     = errors.New("backup materializer projected TokenReview configuration invalid")
	ErrCredential = errors.New("backup materializer projected API credential unavailable")
	ErrTrust      = errors.New("backup materializer projected API trust unavailable")
)

// Config describes one dedicated read-only projection containing the exact
// Kubernetes API caller token and CA bundle names. APIServerURL must be an
// HTTPS origin with no path, query, fragment, or embedded credentials.
type Config struct {
	APIServerURL     string
	ProjectionRoot   string
	RequestTimeout   time.Duration
	HandshakeTimeout time.Duration
	MaxResponseBytes int64
}

// New validates the complete projection once, then returns a reviewer that
// rereads both projected files for every TokenReview. No credential, CA pool,
// HTTP connection, or response is cached across reviews.
func New(config Config) (*backupmaterializerreview.Reviewer, error) {
	authority, err := canonicalAPIServerAuthority(config.APIServerURL)
	if err != nil {
		return nil, ErrConfig
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.RequestTimeout < time.Second || config.RequestTimeout > 15*time.Second ||
		config.RequestTimeout%time.Millisecond != 0 {
		return nil, ErrConfig
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = defaultHandshakeTimeout
		if config.HandshakeTimeout > config.RequestTimeout {
			config.HandshakeTimeout = config.RequestTimeout
		}
	}
	if config.HandshakeTimeout < time.Second || config.HandshakeTimeout > config.RequestTimeout ||
		config.HandshakeTimeout%time.Millisecond != 0 {
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
	client := &http.Client{Transport: &rotatingCATransport{
		projection:        projection,
		expectedAuthority: authority,
		handshakeTimeout:  config.HandshakeTimeout,
	}}
	reviewer, err := backupmaterializerreview.New(backupmaterializerreview.Config{
		APIServerURL:     config.APIServerURL,
		CredentialSource: source,
		HTTPClient:       client,
		RequestTimeout:   config.RequestTimeout,
		MaxResponseBytes: config.MaxResponseBytes,
	})
	if err != nil {
		return nil, ErrConfig
	}
	return reviewer, nil
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
	if !canonicalToken(token) {
		return "", ErrCredential
	}
	return token, nil
}

type projection struct {
	root string
}

func openProjection(root string) (*projection, error) {
	if root == "" || strings.TrimSpace(root) != root || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
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
	projected := requestedBefore.Mode()&os.ModeSymlink != 0
	var generationPath string
	var generationBefore os.FileInfo
	var dataTarget string
	if projected {
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
	if err != nil {
		return nil, err
	}
	if int64(len(document)) > limit {
		return nil, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rootAfter, err := os.Lstat(projection.root)
	if err != nil || !safeDirectory(rootAfter) || !sameFileSnapshot(rootBefore, rootAfter) {
		return nil, ErrConfig
	}
	if projected {
		currentResolved, currentGenerationPath, generationAfter, currentDataTarget, resolveErr := projection.resolveAtomicWriterFile(name)
		if resolveErr != nil || currentResolved != resolvedPath || currentGenerationPath != generationPath ||
			currentDataTarget != dataTarget || !sameFileSnapshot(generationBefore, generationAfter) {
			return nil, ErrConfig
		}
	} else {
		requestedAfter, statErr := os.Lstat(requestedPath)
		if statErr != nil || !requestedAfter.Mode().IsRegular() || requestedAfter.Mode()&os.ModeSymlink != 0 ||
			!sameFileSnapshot(requestedBefore, requestedAfter) {
			return nil, ErrConfig
		}
	}
	current, err := os.Stat(resolvedPath)
	if err != nil || !sameFileSnapshot(opened, current) {
		return nil, ErrConfig
	}
	return document, nil
}

// resolveAtomicWriterFile accepts only the Kubernetes layout:
//
//	<name> -> ..data/<name>
//	..data -> ..<generation>
//
// The generation itself must be a non-symlink directory under the configured
// projection root, so neither link can redirect reads outside the volume.
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
	handshakeTimeout  time.Duration
}

func (transport *rotatingCATransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport == nil || transport.projection == nil || request == nil || request.URL == nil ||
		request.URL.Scheme != "https" || request.URL.Host != transport.expectedAuthority || request.URL.User != nil {
		return nil, ErrTrust
	}
	roots, err := transport.projection.loadCAPool(request.Context())
	if err != nil {
		return nil, ErrTrust
	}
	requestTransport := http.DefaultTransport.(*http.Transport).Clone()
	requestTransport.Proxy = nil
	requestTransport.DisableCompression = true
	requestTransport.DisableKeepAlives = true
	requestTransport.ForceAttemptHTTP2 = true
	requestTransport.MaxIdleConns = 0
	requestTransport.MaxIdleConnsPerHost = 1
	requestTransport.MaxConnsPerHost = 1
	requestTransport.IdleConnTimeout = 0
	requestTransport.TLSHandshakeTimeout = transport.handshakeTimeout
	requestTransport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
	}
	response, err := requestTransport.RoundTrip(request)
	if err != nil {
		requestTransport.CloseIdleConnections()
		return nil, ErrTrust
	}
	if response == nil || response.Body == nil {
		requestTransport.CloseIdleConnections()
		return nil, ErrTrust
	}
	response.Body = &closingBody{
		ReadCloser: response.Body,
		closeIdle:  requestTransport.CloseIdleConnections,
	}
	return response, nil
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

func canonicalAPIServerAuthority(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return "", ErrConfig
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", ErrConfig
	}
	return parsed.Host, nil
}

func canonicalToken(token string) bool {
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

func safeDirectory(info os.FileInfo) bool {
	return info != nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 &&
		info.Mode()&(os.ModeSetuid|os.ModeSetgid) == 0 && info.Mode().Perm()&0o022 == 0
}

func safeFile(info os.FileInfo, policy filePolicy) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return false
	}
	switch policy {
	case tokenFilePolicy:
		return info.Mode().Perm()&0o137 == 0
	case caFilePolicy:
		return info.Mode().Perm()&0o133 == 0
	default:
		return false
	}
}

func sameFileSnapshot(before os.FileInfo, after os.FileInfo) bool {
	return before != nil && after != nil && os.SameFile(before, after) &&
		before.Mode() == after.Mode() && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}
