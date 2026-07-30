package backupobserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
)

const (
	defaultObservationRequestTimeout = 10 * time.Second
	maxObservationRequestTimeout     = 30 * time.Second
	defaultObservationResponseBytes  = int64(64 << 10)
	maxObservationResponseBytes      = int64(1 << 20)
	maxObservationTokenBytes         = 16 << 10
)

var (
	ErrObservationAPI       = errors.New("backup observation API failed")
	ErrObservationTransport = errors.New("backup observation transport unavailable")
)

// ObservationAPIStatusError exposes only an HTTP status. Remote bodies are
// deliberately discarded so a legacy error message can never enter local
// status, logs, or health responses.
type ObservationAPIStatusError struct {
	StatusCode int
}

func (err *ObservationAPIStatusError) Error() string {
	if err == nil {
		return ErrObservationAPI.Error()
	}
	return fmt.Sprintf("%s: HTTP %d", ErrObservationAPI, err.StatusCode)
}

func (err *ObservationAPIStatusError) Unwrap() error {
	return ErrObservationAPI
}

func (err *ObservationAPIStatusError) Retryable() bool {
	if err == nil {
		return false
	}
	return err.StatusCode == http.StatusRequestTimeout || err.StatusCode == http.StatusTooEarly ||
		err.StatusCode == http.StatusTooManyRequests || err.StatusCode >= http.StatusInternalServerError
}

type HTTPObservationSourceConfig struct {
	BaseURL                   string
	BearerToken               string
	RequestTimeout            time.Duration
	MaxResponseBytes          int64
	AllowInsecureHTTPForTests bool
	Client                    *http.Client
}

// HTTPObservationSource is a GET-only adapter for the future fixed-purpose
// legacy observation bridge. It has no method capable of scheduling,
// claiming, executing, deleting, restoring, or otherwise mutating a backup.
type HTTPObservationSource struct {
	baseURL          *url.URL
	bearerToken      string
	requestTimeout   time.Duration
	maxResponseBytes int64
	client           *http.Client
}

func NewHTTPObservationSource(cfg HTTPObservationSourceConfig) (*HTTPObservationSource, error) {
	baseURL, err := validateObservationBaseURL(cfg.BaseURL, cfg.AllowInsecureHTTPForTests)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(cfg.BearerToken)
	if token == "" || len(token) > maxObservationTokenBytes || token != cfg.BearerToken || strings.ContainsAny(token, "\r\n") {
		return nil, fmt.Errorf("%w: bearer credential is not canonical", ErrObservationAPI)
	}
	requestTimeout := cfg.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultObservationRequestTimeout
	}
	if requestTimeout <= 0 || requestTimeout > maxObservationRequestTimeout {
		return nil, fmt.Errorf("%w: request timeout must be between 1ns and %s", ErrObservationAPI, maxObservationRequestTimeout)
	}
	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultObservationResponseBytes
	}
	if maxResponseBytes <= 0 || maxResponseBytes > maxObservationResponseBytes {
		return nil, fmt.Errorf("%w: response limit must be between 1 and %d bytes", ErrObservationAPI, maxObservationResponseBytes)
	}
	client := &http.Client{}
	if cfg.Client != nil {
		*client = *cfg.Client
	}
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPObservationSource{
		baseURL:          baseURL,
		bearerToken:      token,
		requestTimeout:   requestTimeout,
		maxResponseBytes: maxResponseBytes,
		client:           client,
	}, nil
}

func (source *HTTPObservationSource) Observe(ctx context.Context, spec backupcontrol.BackupRunSpec) (backupcontrol.BackupRunStatus, error) {
	if source == nil || ctx == nil {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("%w: source or context is nil", ErrObservationAPI)
	}
	if err := backupcontrol.ValidateBackupRunSpec(spec); err != nil {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("%w: invalid desired spec", ErrObservationAPI)
	}
	requestCtx, cancel := context.WithTimeout(ctx, source.requestTimeout)
	defer cancel()
	endpoint := *source.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/v1/backup-control/runs/" + url.PathEscape(spec.RunID) + "/observation"
	endpoint.RawPath = ""
	query := endpoint.Query()
	query.Set("spec_digest", spec.Digest)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("%w: construct request", ErrObservationAPI)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+source.bearerToken)
	response, err := source.client.Do(request)
	if err != nil {
		if requestCtx.Err() != nil {
			return backupcontrol.BackupRunStatus{}, requestCtx.Err()
		}
		return backupcontrol.BackupRunStatus{}, ErrObservationTransport
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return backupcontrol.BackupRunStatus{}, &ObservationAPIStatusError{StatusCode: response.StatusCode}
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || !hasCacheControlDirective(response.Header.Values("Cache-Control"), "private") ||
		!hasCacheControlDirective(response.Header.Values("Cache-Control"), "no-store") {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("%w: unsafe response metadata", ErrObservationAPI)
	}
	document, err := io.ReadAll(io.LimitReader(response.Body, source.maxResponseBytes+1))
	if err != nil {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("%w: read response", ErrObservationAPI)
	}
	if int64(len(document)) > source.maxResponseBytes {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("%w: response exceeds %d bytes", ErrObservationAPI, source.maxResponseBytes)
	}
	status, err := backupcontrol.DecodeBackupRunStatus(spec, document)
	if err != nil {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("%w: invalid status contract", ErrObservationAPI)
	}
	return status, nil
}

func validateObservationBaseURL(raw string, allowInsecureHTTP bool) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || raw == "" || !parsed.IsAbs() || parsed.Host == "" || parsed.User != nil || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return nil, fmt.Errorf("%w: API base URL is invalid", ErrObservationAPI)
	}
	if parsed.Scheme != "https" && !(allowInsecureHTTP && parsed.Scheme == "http") {
		return nil, fmt.Errorf("%w: API base URL must use HTTPS", ErrObservationAPI)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	if parsed.Path != "" && path.Clean(parsed.Path) != parsed.Path {
		return nil, fmt.Errorf("%w: API base URL path is not canonical", ErrObservationAPI)
	}
	parsed.RawPath = ""
	return parsed, nil
}

func hasCacheControlDirective(values []string, expected string) bool {
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(strings.SplitN(directive, "=", 2)[0]), expected) {
				return true
			}
		}
	}
	return false
}
