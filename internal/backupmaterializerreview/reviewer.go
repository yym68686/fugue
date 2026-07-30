// Package backupmaterializerreview adapts the Kubernetes
// authentication.k8s.io/v1 TokenReview API to the minimal, token-free result
// consumed by backupmaterializeridentity. It has no client-go, informer,
// Kubernetes mutation, filesystem, datastore, or signing capability.
package backupmaterializerreview

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
	"strings"
	"time"

	"fugue/internal/backupmaterializeridentity"

	authenticationv1 "k8s.io/api/authentication/v1"
)

const (
	defaultRequestTimeout = 5 * time.Second
	defaultMaxResponse    = int64(64 << 10)
	maxBearerTokenBytes   = 16 << 10
	tokenReviewPath       = "/apis/authentication.k8s.io/v1/tokenreviews"
	reviewDeniedMarker    = "review denied"
)

var (
	ErrReviewerConfig      = errors.New("backup materializer TokenReview configuration invalid")
	ErrReviewerUnavailable = errors.New("backup materializer TokenReview unavailable")
	ErrReviewerResponse    = errors.New("backup materializer TokenReview response invalid")
)

// CredentialSource supplies the API caller's own projected ServiceAccount
// credential. Implementations must reread their source so kubelet rotation is
// observed; the reviewer never stores or returns this credential.
type CredentialSource interface {
	Credential(context.Context) (string, error)
}

type Config struct {
	APIServerURL     string
	CredentialSource CredentialSource
	HTTPClient       *http.Client
	RequestTimeout   time.Duration
	MaxResponseBytes int64
}

type Reviewer struct {
	endpoint         string
	credentialSource CredentialSource
	client           *http.Client
	maxResponseBytes int64
}

func New(config Config) (*Reviewer, error) {
	baseURL, err := canonicalAPIServerURL(config.APIServerURL)
	if err != nil || config.CredentialSource == nil {
		return nil, ErrReviewerConfig
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.RequestTimeout < time.Second || config.RequestTimeout > 15*time.Second || config.RequestTimeout%time.Millisecond != 0 {
		return nil, ErrReviewerConfig
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponse
	}
	if config.MaxResponseBytes < 1024 || config.MaxResponseBytes > 1<<20 {
		return nil, ErrReviewerConfig
	}
	client := &http.Client{}
	if config.HTTPClient != nil {
		*client = *config.HTTPClient
	}
	client.Timeout = config.RequestTimeout
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Reviewer{
		endpoint:         baseURL + tokenReviewPath,
		credentialSource: config.CredentialSource,
		client:           client,
		maxResponseBytes: config.MaxResponseBytes,
	}, nil
}

// ReviewToken performs one bounded, audience-exact review. The presented
// materializer token and the API caller token are never included in returned
// values or error strings, and the echoed TokenReview spec is discarded.
func (reviewer *Reviewer) ReviewToken(
	ctx context.Context,
	presentedToken string,
	audiences []string,
) (backupmaterializeridentity.ReviewResult, error) {
	if reviewer == nil || ctx == nil || reviewer.client == nil || reviewer.credentialSource == nil ||
		!canonicalJWT(presentedToken) ||
		len(audiences) != 1 || audiences[0] != backupmaterializeridentity.Audience {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerConfig
	}
	if err := ctx.Err(); err != nil {
		return backupmaterializeridentity.ReviewResult{}, fmt.Errorf("%w: request canceled", ErrReviewerUnavailable)
	}
	apiToken, err := reviewer.credentialSource.Credential(ctx)
	if err != nil {
		return backupmaterializeridentity.ReviewResult{}, fmt.Errorf("%w: caller credential unavailable", ErrReviewerUnavailable)
	}
	if !canonicalJWT(apiToken) || apiToken == presentedToken {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerConfig
	}
	review := authenticationv1.TokenReview{}
	review.APIVersion = "authentication.k8s.io/v1"
	review.Kind = "TokenReview"
	review.Spec.Token = presentedToken
	review.Spec.Audiences = []string{backupmaterializeridentity.Audience}
	body, err := json.Marshal(review)
	if err != nil {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerConfig
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, reviewer.endpoint, bytes.NewReader(body))
	if err != nil {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerConfig
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Authorization", "Bearer "+apiToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := reviewer.client.Do(request)
	if err != nil {
		return backupmaterializeridentity.ReviewResult{}, fmt.Errorf("%w: request failed", ErrReviewerUnavailable)
	}
	defer response.Body.Close()
	document, err := io.ReadAll(io.LimitReader(response.Body, reviewer.maxResponseBytes+1))
	if err != nil {
		return backupmaterializeridentity.ReviewResult{}, fmt.Errorf("%w: response read failed", ErrReviewerUnavailable)
	}
	if response.StatusCode != http.StatusCreated {
		return backupmaterializeridentity.ReviewResult{}, fmt.Errorf("%w: unexpected HTTP status", ErrReviewerUnavailable)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || len(document) == 0 || int64(len(document)) > reviewer.maxResponseBytes {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerResponse
	}
	var reviewed authenticationv1.TokenReview
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reviewed); err != nil {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerResponse
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerResponse
	}
	if reviewed.APIVersion != "authentication.k8s.io/v1" || reviewed.Kind != "TokenReview" ||
		reviewed.Spec.Token != presentedToken || len(reviewed.Spec.Audiences) != 1 ||
		reviewed.Spec.Audiences[0] != backupmaterializeridentity.Audience {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerResponse
	}
	statusError := ""
	if reviewed.Status.Error != "" {
		statusError = reviewDeniedMarker
	}
	result := backupmaterializeridentity.ReviewResult{
		Authenticated: reviewed.Status.Authenticated,
		Audiences:     append([]string(nil), reviewed.Status.Audiences...),
		Username:      reviewed.Status.User.Username,
		UID:           reviewed.Status.User.UID,
		Groups:        append([]string(nil), reviewed.Status.User.Groups...),
		Extra:         make(map[string][]string, len(reviewed.Status.User.Extra)),
		Error:         statusError,
	}
	for key, values := range reviewed.Status.User.Extra {
		result.Extra[key] = append([]string(nil), values...)
	}
	if reviewResultContainsCredential(result, presentedToken, apiToken) {
		return backupmaterializeridentity.ReviewResult{}, ErrReviewerResponse
	}
	return result, nil
}

func reviewResultContainsCredential(
	result backupmaterializeridentity.ReviewResult,
	credentials ...string,
) bool {
	containsCredential := func(value string) bool {
		for _, credential := range credentials {
			if credential != "" && strings.Contains(value, credential) {
				return true
			}
		}
		return false
	}
	values := []string{result.Username, result.UID, result.Error}
	values = append(values, result.Audiences...)
	values = append(values, result.Groups...)
	for _, value := range values {
		if containsCredential(value) {
			return true
		}
	}
	for key, values := range result.Extra {
		if containsCredential(key) {
			return true
		}
		for _, value := range values {
			if containsCredential(value) {
				return true
			}
		}
	}
	return false
}

func canonicalAPIServerURL(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return "", ErrReviewerConfig
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" ||
		parsed.ForceQuery || parsed.Fragment != "" {
		return "", ErrReviewerConfig
	}
	if parsed.Scheme != "https" {
		return "", ErrReviewerConfig
	}
	return parsed.String(), nil
}

func canonicalJWT(token string) bool {
	if token == "" || len(token) > maxBearerTokenBytes || strings.TrimSpace(token) != token {
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
