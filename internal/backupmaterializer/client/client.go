// Package client implements the default-off, GET-only transport used by a
// future backup materializer to fetch one exact observer input generation. It
// owns no filesystem, Kubernetes, datastore, signing, Secret, or process
// capability. Credentials and TLS transport are supplied by narrower adapters.
package client

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	materializercontract "fugue/internal/backupmaterializer/contract"
)

const (
	APIVersion = "backup-materializer-client.fugue.dev/v1"

	defaultRequestTimeout  = 5 * time.Second
	maxRequestTimeout      = 30 * time.Second
	maxBundleDeliveryAge   = time.Minute
	defaultMaxResponse     = int64(materializercontract.MaxObserverInputBundleBytes)
	minimumResponseLimit   = int64(1024)
	maxWorkloadTokenBytes  = 16 << 10
	observerInputRouteHead = "/v1/backup-control/runs/"
	observerInputRouteTail = "/observer-input-bundle"
)

var (
	ErrConfig                = errors.New("backup materializer client configuration invalid")
	ErrDisabled              = errors.New("backup materializer client disabled")
	ErrCredentialUnavailable = errors.New("backup materializer client credential unavailable")
	ErrInputRejected         = errors.New("backup materializer input request rejected")
	ErrInputNotFound         = errors.New("backup materializer input not found")
	ErrInputConflict         = errors.New("backup materializer input inconsistent")
	ErrInputUnavailable      = errors.New("backup materializer input unavailable")
	ErrInputResponse         = errors.New("backup materializer input response invalid")

	canonicalCellKey = regexp.MustCompile(`^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$`)
	canonicalRunID   = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
)

// CredentialSource supplies the materializer's audience-bound workload JWT.
// Implementations must reread their source for each request so kubelet token
// rotation is observed. The client never stores or returns the credential.
type CredentialSource interface {
	Credential(context.Context) (string, error)
}

type Config struct {
	Enabled                   bool
	BaseURL                   string
	ExpectedCellKey           string
	ExpectedRunID             string
	CredentialSource          CredentialSource
	HTTPClient                *http.Client
	RequestTimeout            time.Duration
	MaxResponseBytes          int64
	AllowInsecureHTTPForTests bool
	Now                       func() time.Time
}

func (config Config) String() string {
	return "backup materializer client configuration [REDACTED]"
}

func (config Config) GoString() string {
	return config.String()
}

type Client struct {
	enabled         bool
	endpoint        string
	expectedCellKey string
	expectedRunID   string
	credential      CredentialSource
	httpClient      *http.Client
	requestTimeout  time.Duration
	maxResponse     int64
	now             func() time.Time
}

// New ignores and retains none of the supplied capabilities while disabled.
// Enabling requires one exact cell/run plus an injected credential source and
// HTTPS client boundary; construction itself performs no I/O.
func New(config Config) (*Client, error) {
	if !config.Enabled {
		return &Client{}, nil
	}
	if !canonicalCellKey.MatchString(config.ExpectedCellKey) ||
		!canonicalRunID.MatchString(config.ExpectedRunID) || nilInterface(config.CredentialSource) || config.HTTPClient == nil {
		return nil, ErrConfig
	}
	baseURL, err := canonicalBaseURL(config.BaseURL, config.AllowInsecureHTTPForTests)
	if err != nil {
		return nil, ErrConfig
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.RequestTimeout < time.Second || config.RequestTimeout > maxRequestTimeout ||
		config.RequestTimeout%time.Millisecond != 0 {
		return nil, ErrConfig
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponse
	}
	if config.MaxResponseBytes < minimumResponseLimit ||
		config.MaxResponseBytes > int64(materializercontract.MaxObserverInputBundleBytes) {
		return nil, ErrConfig
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	httpClient := &http.Client{}
	*httpClient = *config.HTTPClient
	httpClient.Timeout = config.RequestTimeout
	httpClient.Jar = nil
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	endpoint := *baseURL
	endpoint.Path = observerInputRouteHead + config.ExpectedRunID + observerInputRouteTail
	return &Client{
		enabled:         true,
		endpoint:        endpoint.String(),
		expectedCellKey: config.ExpectedCellKey,
		expectedRunID:   config.ExpectedRunID,
		credential:      config.CredentialSource,
		httpClient:      httpClient,
		requestTimeout:  config.RequestTimeout,
		maxResponse:     config.MaxResponseBytes,
		now:             config.Now,
	}, nil
}

func (client *Client) Enabled() bool {
	return client != nil && client.enabled && client.endpoint != "" && client.credential != nil &&
		client.httpClient != nil && client.requestTimeout > 0 && client.maxResponse > 0 && client.now != nil
}

func (client *Client) String() string {
	if client == nil {
		return "backup materializer client <nil>"
	}
	return "backup materializer client [REDACTED]"
}

func (client *Client) GoString() string {
	return client.String()
}

// Fetch reads the credential immediately before issuing one bounded GET. It
// accepts only the private response metadata and exact cell/run generation.
// The returned observer bearer token is intentionally present in the private
// result, but neither it nor the workload credential can enter an error.
func (client *Client) Fetch(ctx context.Context) (materializercontract.ObserverInputBundle, error) {
	if client == nil {
		return materializercontract.ObserverInputBundle{}, ErrConfig
	}
	if !client.Enabled() {
		return materializercontract.ObserverInputBundle{}, ErrDisabled
	}
	if ctx == nil {
		return materializercontract.ObserverInputBundle{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return materializercontract.ObserverInputBundle{}, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, client.requestTimeout)
	defer cancel()
	credential, err := client.credential.Credential(requestCtx)
	if err != nil || !canonicalWorkloadJWT(credential) {
		if requestCtx.Err() != nil {
			return materializercontract.ObserverInputBundle{}, requestCtx.Err()
		}
		return materializercontract.ObserverInputBundle{}, ErrCredentialUnavailable
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, client.endpoint, nil)
	if err != nil {
		return materializercontract.ObserverInputBundle{}, ErrConfig
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Pragma", "no-cache")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if requestCtx.Err() != nil {
			return materializercontract.ObserverInputBundle{}, requestCtx.Err()
		}
		return materializercontract.ObserverInputBundle{}, ErrInputUnavailable
	}
	if response == nil || response.Body == nil {
		return materializercontract.ObserverInputBundle{}, ErrInputUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return materializercontract.ObserverInputBundle{}, statusError(response.StatusCode)
	}
	if !safeResponseMetadata(response) || response.ContentLength < -1 || response.ContentLength > client.maxResponse {
		return materializercontract.ObserverInputBundle{}, ErrInputResponse
	}
	document, err := io.ReadAll(io.LimitReader(response.Body, client.maxResponse+1))
	if err != nil || len(document) == 0 || int64(len(document)) > client.maxResponse ||
		(response.ContentLength >= 0 && response.ContentLength != int64(len(document))) {
		return materializercontract.ObserverInputBundle{}, ErrInputResponse
	}
	now := client.now()
	if now.IsZero() {
		return materializercontract.ObserverInputBundle{}, ErrInputResponse
	}
	now = now.UTC().Truncate(time.Second)
	bundle, err := materializercontract.DecodeObserverInputBundleEnvelope(document, now)
	if err != nil || bundle.CellKey != client.expectedCellKey || bundle.RunID != client.expectedRunID ||
		bundle.DesiredSpec.CellKey != client.expectedCellKey || bundle.DesiredSpec.RunID != client.expectedRunID ||
		bundle.IssuedAt.Before(now.Add(-maxBundleDeliveryAge)) || !bundle.RenewAfter.After(now) {
		return materializercontract.ObserverInputBundle{}, ErrInputResponse
	}
	return bundle, nil
}

func canonicalBaseURL(raw string, allowInsecureHTTP bool) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, ErrConfig
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" ||
		parsed.ForceQuery || parsed.Fragment != "" {
		return nil, ErrConfig
	}
	if parsed.Scheme != "https" && !(allowInsecureHTTP && parsed.Scheme == "http") {
		return nil, ErrConfig
	}
	if port := parsed.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return nil, ErrConfig
		}
	}
	return parsed, nil
}

func canonicalWorkloadJWT(token string) bool {
	if token == "" || len(token) > maxWorkloadTokenBytes || strings.TrimSpace(token) != token || strings.ContainsAny(token, "\r\n") {
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

func safeResponseMetadata(response *http.Response) bool {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	contentEncoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	return err == nil && mediaType == "application/json" &&
		(contentEncoding == "" || strings.EqualFold(contentEncoding, "identity")) &&
		safeCacheControl(response.Header.Values("Cache-Control")) &&
		hasHeaderToken(response.Header.Values("Pragma"), "no-cache") &&
		hasHeaderToken(response.Header.Values("Vary"), "Authorization") &&
		strings.EqualFold(strings.TrimSpace(response.Header.Get("X-Content-Type-Options")), "nosniff")
}

func safeCacheControl(values []string) bool {
	private := false
	noStore := false
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			parts := strings.SplitN(strings.TrimSpace(directive), "=", 2)
			name := strings.ToLower(strings.TrimSpace(parts[0]))
			switch name {
			case "private":
				private = true
			case "no-store":
				if len(parts) != 1 {
					return false
				}
				noStore = true
			case "max-age":
				if len(parts) != 2 || strings.Trim(strings.TrimSpace(parts[1]), `"`) != "0" {
					return false
				}
			case "public", "s-maxage", "immutable":
				return false
			}
		}
	}
	return private && noStore
}

func hasHeaderToken(values []string, expected string) bool {
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), expected) {
				return true
			}
		}
	}
	return false
}

func statusError(status int) error {
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized:
		return ErrInputRejected
	case http.StatusNotFound:
		return ErrInputNotFound
	case http.StatusConflict:
		return ErrInputConflict
	default:
		return ErrInputUnavailable
	}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
