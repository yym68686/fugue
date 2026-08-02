package releasecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	// ComponentPlanAPIContractV1 is the transport contract used by the
	// release-control component. The /v1 path and request marker are versioned
	// independently of the internal Go package; typed response invariants reject
	// incompatible semantics while additive response fields remain compatible.
	ComponentPlanAPIContractV1            = "component-plan-api.fugue.dev/v1"
	componentPlanAPIUserAgent             = "fugue-release-control/" + ComponentPlanAPIContractV1
	defaultComponentPlanAPIRequestTimeout = 10 * time.Second
	maxComponentPlanAPIRequestTimeout     = 30 * time.Second
	defaultComponentPlanAPIResponseBytes  = 2 << 20
	maxComponentPlanAPIResponseBytes      = 8 << 20
	maxComponentPlanAPIRequestBytes       = 128 << 10
)

var (
	ErrComponentPlanAPI        = errors.New("component plan API request failed")
	componentPlanArtifactIDRE  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$`)
	componentPlanIdempotencyRE = regexp.MustCompile(`^component-shadow/[0-9a-f]{64}$`)
)

// HTTPComponentPlanStoreConfig describes the only network capability exposed
// to the release-control reconciler. It is intentionally limited to the
// existing versioned control-plane API; no database or Kubernetes client is
// accepted here.
type HTTPComponentPlanStoreConfig struct {
	BaseURL          string
	BearerToken      string
	Client           *http.Client
	RequestTimeout   time.Duration
	MaxResponseBytes int64
}

// HTTPComponentPlanStore is a context-aware API adapter for the shadow
// component-plan ledger. Its release method is permanently constrained to
// the shadow channel, even if a caller accidentally supplies a production
// capable request.
type HTTPComponentPlanStore struct {
	baseURL         *url.URL
	bearerToken     string
	client          *http.Client
	maxResponseSize int64
}

// ComponentPlanAPIStatusError preserves a bounded, credential-redacted remote
// status and exposes whether lane-local recovery may safely retry it.
type ComponentPlanAPIStatusError struct {
	StatusCode int
	Code       string
	Message    string
}

func (err *ComponentPlanAPIStatusError) Error() string {
	if err == nil {
		return ErrComponentPlanAPI.Error()
	}
	if err.Code != "" {
		return fmt.Sprintf("%s: HTTP %d (%s): %s", ErrComponentPlanAPI, err.StatusCode, err.Code, err.Message)
	}
	return fmt.Sprintf("%s: HTTP %d: %s", ErrComponentPlanAPI, err.StatusCode, err.Message)
}

func (err *ComponentPlanAPIStatusError) Unwrap() error {
	return ErrComponentPlanAPI
}

func (err *ComponentPlanAPIStatusError) Retryable() bool {
	if err == nil {
		return false
	}
	return err.StatusCode == http.StatusRequestTimeout ||
		err.StatusCode == http.StatusTooEarly ||
		err.StatusCode == http.StatusTooManyRequests ||
		err.StatusCode >= http.StatusInternalServerError
}

// NewHTTPComponentPlanStore validates the transport boundary and copies the
// supplied client before installing a no-redirect policy. This prevents a
// bearer token from following a redirect to an untrusted host.
func NewHTTPComponentPlanStore(cfg HTTPComponentPlanStoreConfig) (*HTTPComponentPlanStore, error) {
	rawBaseURL := strings.TrimSpace(cfg.BaseURL)
	if rawBaseURL == "" {
		return nil, fmt.Errorf("%w: base URL is required", ErrComponentPlanAPI)
	}
	baseURL, err := url.Parse(rawBaseURL)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("%w: base URL must be an absolute HTTP(S) URL without credentials, query, or fragment", ErrComponentPlanAPI)
	}
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return nil, fmt.Errorf("%w: base URL scheme must be http or https", ErrComponentPlanAPI)
	}
	if strings.ContainsAny(baseURL.Path, "\r\n") || strings.Contains(baseURL.Path, "..") {
		return nil, fmt.Errorf("%w: base URL path is invalid", ErrComponentPlanAPI)
	}
	token := strings.TrimSpace(cfg.BearerToken)
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, fmt.Errorf("%w: bearer token is required", ErrComponentPlanAPI)
	}
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = defaultComponentPlanAPIRequestTimeout
	}
	if timeout < 0 || timeout > maxComponentPlanAPIRequestTimeout {
		return nil, fmt.Errorf("%w: request timeout must be between 1ns and %s", ErrComponentPlanAPI, maxComponentPlanAPIRequestTimeout)
	}
	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultComponentPlanAPIResponseBytes
	}
	if maxResponseBytes < 1 || maxResponseBytes > maxComponentPlanAPIResponseBytes {
		return nil, fmt.Errorf("%w: max response size must be between 1 and %d bytes", ErrComponentPlanAPI, maxComponentPlanAPIResponseBytes)
	}

	var client *http.Client
	if cfg.Client != nil {
		if cfg.Client.Transport == nil {
			return nil, fmt.Errorf("%w: injected HTTP client must define an explicit transport", ErrComponentPlanAPI)
		}
		copy := *cfg.Client
		client = &copy
	} else {
		client = &http.Client{Transport: &http.Transport{
			Proxy:                 nil,
			DisableCompression:    true,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          8,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: time.Second,
		}}
	}
	client.Timeout = timeout
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &HTTPComponentPlanStore{
		baseURL:         baseURL,
		bearerToken:     token,
		client:          client,
		maxResponseSize: maxResponseBytes,
	}, nil
}

// GetPlatformArtifact reads one immutable artifact by ID. The response is
// checked for identity before being returned so a misrouted API cannot make a
// reconciler act on a different generation.
func (s *HTTPComponentPlanStore) GetPlatformArtifact(ctx context.Context, id string) (model.PlatformArtifact, error) {
	if s == nil {
		return model.PlatformArtifact{}, fmt.Errorf("%w: store is nil", ErrComponentPlanAPI)
	}
	id = strings.TrimSpace(id)
	if !componentPlanArtifactIDRE.MatchString(id) {
		return model.PlatformArtifact{}, fmt.Errorf("%w: artifact ID is invalid", ErrComponentPlanAPI)
	}
	var response model.PlatformArtifactResponse
	if err := s.doJSON(ctx, http.MethodGet, s.endpoint("v1", "admin", "artifacts", id), nil, http.StatusOK, &response); err != nil {
		return model.PlatformArtifact{}, err
	}
	if response.Artifact.ID != id {
		return model.PlatformArtifact{}, fmt.Errorf("%w: API returned artifact %q for %q", ErrComponentPlanAPI, response.Artifact.ID, id)
	}
	return response.Artifact, nil
}

// ReleasePlatformArtifact writes exactly one shadow observation. The
// restrictions here intentionally duplicate the reconciler and API kernel:
// an adapter bug must not turn a shadow component into a serving publisher.
func (s *HTTPComponentPlanStore) ReleasePlatformArtifact(
	ctx context.Context,
	id string,
	req model.PlatformArtifactReleaseRequest,
	principal model.Principal,
) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	if s == nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, fmt.Errorf("%w: store is nil", ErrComponentPlanAPI)
	}
	id = strings.TrimSpace(id)
	if !componentPlanArtifactIDRE.MatchString(id) {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, fmt.Errorf("%w: artifact ID is invalid", ErrComponentPlanAPI)
	}
	if !componentPlanPrincipalAuthorized(principal) || strings.TrimSpace(principal.ActorType) == "" || strings.TrimSpace(principal.ActorID) == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, fmt.Errorf("%w: component plan observer identity is required", ErrComponentPlanAPI)
	}
	if req.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		req.CanaryRuleRef != "" || req.SoftOverride || req.ForcePublish || req.KernelBreakGlass != nil ||
		req.Reason != componentPlanReleaseReason || !componentPlanIdempotencyRE.MatchString(req.IdempotencyKey) {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, fmt.Errorf("%w: only the exact shadow release request is permitted", ErrComponentPlanAPI)
	}
	var response model.PlatformArtifactReleaseResponse
	body, err := json.Marshal(req)
	if err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, fmt.Errorf("%w: encode release request: %v", ErrComponentPlanAPI, err)
	}
	if int64(len(body)) > maxComponentPlanAPIRequestBytes {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, fmt.Errorf("%w: release request is too large", ErrComponentPlanAPI)
	}
	if err := s.doJSON(ctx, http.MethodPost, s.endpoint("v1", "admin", "artifacts", id, "release"), body, http.StatusOK, &response); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, err
	}
	if response.Artifact.ID != id || response.Release.ArtifactID != id || response.Message.ArtifactID != id {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, fmt.Errorf("%w: API release response is bound to a different artifact", ErrComponentPlanAPI)
	}
	return response.Artifact, response.Release, response.Message, response.LKG, nil
}

// ResolvePrincipal obtains the API-authenticated identity used by the
// reconciler. It makes a rolling deployment fail closed when a token is
// accidentally mounted with an overprivileged or mismatched identity.
func (s *HTTPComponentPlanStore) ResolvePrincipal(ctx context.Context) (model.Principal, error) {
	if s == nil {
		return model.Principal{}, fmt.Errorf("%w: store is nil", ErrComponentPlanAPI)
	}
	var response componentPlanAuthContextResponse
	if err := s.doJSON(ctx, http.MethodGet, s.endpoint("v1", "auth", "context"), nil, http.StatusOK, &response); err != nil {
		return model.Principal{}, err
	}
	actorType := strings.TrimSpace(response.Principal.ActorType)
	actorID := strings.TrimSpace(response.Principal.ActorID)
	if actorType == "" || actorID == "" {
		return model.Principal{}, fmt.Errorf("%w: API auth context has no component identity", ErrComponentPlanAPI)
	}
	scopes := make(map[string]struct{}, len(response.Principal.Scopes))
	for _, scope := range response.Principal.Scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			return model.Principal{}, fmt.Errorf("%w: API auth context contains an empty scope", ErrComponentPlanAPI)
		}
		if _, duplicate := scopes[scope]; duplicate {
			return model.Principal{}, fmt.Errorf("%w: API auth context contains duplicate scope %q", ErrComponentPlanAPI, scope)
		}
		scopes[scope] = struct{}{}
	}
	principal := model.Principal{
		ActorType: actorType,
		ActorID:   actorID,
		TenantID:  strings.TrimSpace(response.Principal.TenantID),
		ProjectID: strings.TrimSpace(response.Principal.ProjectID),
		AppID:     strings.TrimSpace(response.Principal.AppID),
		Scopes:    scopes,
	}
	if response.Principal.PlatformAdmin != principal.IsPlatformAdmin() || !componentPlanPrincipalAuthorized(principal) {
		return model.Principal{}, fmt.Errorf("%w: API auth context is not an authorized component plan observer", ErrComponentPlanAPI)
	}
	return principal, nil
}

type componentPlanAuthContextResponse struct {
	Principal componentPlanAuthPrincipal `json:"principal"`
}

type componentPlanAuthPrincipal struct {
	ActorType     string   `json:"actor_type"`
	ActorID       string   `json:"actor_id"`
	TenantID      string   `json:"tenant_id"`
	ProjectID     string   `json:"project_id"`
	AppID         string   `json:"app_id"`
	Scopes        []string `json:"scopes"`
	PlatformAdmin bool     `json:"platform_admin"`
}

func (s *HTTPComponentPlanStore) endpoint(parts ...string) string {
	joined, err := url.JoinPath(s.baseURL.String(), parts...)
	if err != nil {
		// All parts are validated before use. Keep a defensive fallback so a
		// future caller cannot turn an endpoint construction failure into an
		// empty URL.
		return ""
	}
	return joined
}

func (s *HTTPComponentPlanStore) doJSON(
	ctx context.Context,
	method string,
	endpoint string,
	body []byte,
	expectedStatus int,
	destination any,
) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrComponentPlanAPI)
	}
	if endpoint == "" {
		return fmt.Errorf("%w: endpoint construction failed", ErrComponentPlanAPI)
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("%w: build request: %v", ErrComponentPlanAPI, err)
	}
	request.Header.Set("Authorization", "Bearer "+s.bearerToken)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Fugue-Contract-Version", ComponentPlanAPIContractV1)
	request.Header.Set("User-Agent", componentPlanAPIUserAgent)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := s.client.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", ErrComponentPlanAPI, err)
		}
		return fmt.Errorf("%w: transport: %s", ErrComponentPlanAPI, sanitizeComponentPlanErrorField(err.Error(), s.bearerToken, 512))
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, s.maxResponseSize+1))
	if readErr != nil {
		return fmt.Errorf("%w: read response: %v", ErrComponentPlanAPI, readErr)
	}
	if int64(len(data)) > s.maxResponseSize {
		return fmt.Errorf("%w: response exceeds %d bytes", ErrComponentPlanAPI, s.maxResponseSize)
	}
	if response.StatusCode != expectedStatus {
		return remoteComponentPlanAPIError(response.StatusCode, data, s.bearerToken)
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return fmt.Errorf("%w: response content type must be application/json", ErrComponentPlanAPI)
	}
	if err := decodeSingleJSON(data, destination); err != nil {
		return fmt.Errorf("%w: decode response: %v", ErrComponentPlanAPI, err)
	}
	return nil
}

func decodeSingleJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("response contains multiple JSON documents")
		}
		return err
	}
	return nil
}

func remoteComponentPlanAPIError(status int, data []byte, bearerToken string) error {
	var payload struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	message := strings.TrimSpace(http.StatusText(status))
	code := ""
	if len(data) > 0 && json.Unmarshal(data, &payload) == nil {
		if strings.TrimSpace(payload.Error) != "" {
			message = strings.TrimSpace(payload.Error)
		}
		code = strings.TrimSpace(payload.Code)
	}
	return &ComponentPlanAPIStatusError{
		StatusCode: status,
		Code:       sanitizeComponentPlanErrorField(code, bearerToken, 128),
		Message:    sanitizeComponentPlanErrorField(message, bearerToken, 512),
	}
}

func sanitizeComponentPlanErrorField(value, bearerToken string, maximumRunes int) string {
	value = strings.Map(func(character rune) rune {
		if character < 0x20 || character == 0x7f {
			return ' '
		}
		return character
	}, value)
	if bearerToken != "" {
		value = strings.ReplaceAll(value, bearerToken, "[REDACTED]")
	}
	runes := []rune(value)
	if maximumRunes > 0 && len(runes) > maximumRunes {
		value = string(runes[:maximumRunes])
	}
	return strings.TrimSpace(value)
}
