// Package secretwriter performs a server-side dry run of one exact,
// cell-local backup observer input Secret create or resourceVersion-CAS
// replacement. It can never issue a live mutation: every request contains the
// fixed dryRun=All query, redirects and retries are disabled, delete/patch are
// absent, and returned status remains explicitly non-executable.
package secretwriter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/secretdryrunrequest"
)

const (
	APIVersion = secretdryrunrequest.ReceiptAPIVersion
	Kind       = secretdryrunrequest.ReceiptKind
	Policy     = secretdryrunrequest.ReceiptPolicy

	RequestUserAgent = "fugue-backup-materializer-secret-dry-run/1"
	FieldManager     = secretdryrunrequest.FieldManager
	DryRunRawQuery   = secretdryrunrequest.DryRunRawQuery

	DefaultRequestTimeout = 5 * time.Second
	MaximumRequestTimeout = 30 * time.Second
	DefaultMaxResponse    = int64(256 << 10)
	MaximumResponse       = int64(1 << 20)
	MaximumDecisionAge    = secretdryrunrequest.MaximumDecisionAge
	MaximumRequestBytes   = secretdryrunrequest.MaximumRequestBytes

	minimumResponseBytes = int64(4 << 10)
	maxBearerTokenBytes  = 16 << 10
	secretCollectionPath = secretdryrunrequest.SecretCollectionPath
)

var (
	ErrConfig                = errors.New("backup materializer Secret dry-run configuration invalid")
	ErrDisabled              = errors.New("backup materializer Secret dry-run disabled")
	ErrIntent                = errors.New("backup materializer Secret dry-run intent invalid")
	ErrCredentialUnavailable = errors.New("backup materializer Secret dry-run credential unavailable")
	ErrRejected              = errors.New("backup materializer Secret dry-run rejected")
	ErrConflict              = errors.New("backup materializer Secret dry-run conflict")
	ErrUnavailable           = errors.New("backup materializer Secret dry-run unavailable")
	ErrResponse              = errors.New("backup materializer Secret dry-run response invalid")
)

// CredentialSource supplies one Kubernetes API credential per dry-run
// request. Implementations must reread rotating projections and must never
// return the credential through formatting or error text.
type CredentialSource interface {
	Credential(context.Context) (string, error)
}

type Config struct {
	Enabled                   bool
	APIServerURL              string
	ExpectedCellKey           string
	CredentialSource          CredentialSource
	HTTPClient                *http.Client
	RequestTimeout            time.Duration
	MaxResponseBytes          int64
	Now                       func() time.Time
	AllowInsecureHTTPForTests bool
}

func (config Config) String() string {
	return "backup materializer Secret dry-run configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

type Writer struct {
	enabled        bool
	collectionURL  string
	itemURL        string
	expectedCell   string
	expectedCellID string
	expectedName   string
	credential     CredentialSource
	client         *http.Client
	requestTimeout time.Duration
	maxResponse    int64
	now            func() time.Time
}

// Result remains an alias for source compatibility while the versioned,
// secret-free receipt belongs to the pure request/response contract package.
type Result = secretdryrunrequest.Receipt

// New performs no filesystem or network operation. Disabled construction
// ignores and retains none of the supplied endpoint, identity, credential,
// client, timing, or clock capabilities.
func New(config Config) (*Writer, error) {
	if !config.Enabled {
		return &Writer{}, nil
	}
	identity, err := materialization.SecretIdentityForCell(config.ExpectedCellKey)
	if err != nil || nilInterface(config.CredentialSource) || config.HTTPClient == nil {
		return nil, ErrConfig
	}
	baseURL, err := canonicalAPIServerURL(config.APIServerURL, config.AllowInsecureHTTPForTests)
	if err != nil {
		return nil, ErrConfig
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = DefaultRequestTimeout
	}
	if requestTimeout < time.Second || requestTimeout > MaximumRequestTimeout || requestTimeout%time.Millisecond != 0 {
		return nil, ErrConfig
	}
	maxResponse := config.MaxResponseBytes
	if maxResponse == 0 {
		maxResponse = DefaultMaxResponse
	}
	if maxResponse < minimumResponseBytes || maxResponse > MaximumResponse {
		return nil, ErrConfig
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	client := &http.Client{}
	*client = *config.HTTPClient
	client.Timeout = requestTimeout
	client.Jar = nil
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	collection := *baseURL
	collection.Path = secretCollectionPath
	collection.RawQuery = dryRunQuery()
	item := collection
	item.Path += "/" + identity.SecretName
	return &Writer{
		enabled:        true,
		collectionURL:  collection.String(),
		itemURL:        item.String(),
		expectedCell:   identity.CellKey,
		expectedCellID: identity.CellID,
		expectedName:   identity.SecretName,
		credential:     config.CredentialSource,
		client:         client,
		requestTimeout: requestTimeout,
		maxResponse:    maxResponse,
		now:            now,
	}, nil
}

func (writer *Writer) Enabled() bool {
	return writer != nil && writer.enabled && writer.collectionURL != "" && writer.itemURL != "" &&
		writer.expectedCell != "" && writer.expectedCellID != "" && writer.expectedName != "" &&
		writer.credential != nil && writer.client != nil && writer.requestTimeout > 0 && writer.maxResponse > 0 && writer.now != nil
}

func (writer *Writer) String() string {
	if writer == nil {
		return "backup materializer Secret dry-run <nil>"
	}
	return "backup materializer Secret dry-run [REDACTED]"
}

func (writer *Writer) GoString() string { return writer.String() }

// DryRun sends exactly one POST create or PUT resourceVersion-CAS update with
// dryRun=All. It never retries an uncertain response. Callers must perform a
// fresh read/reconcile cycle after every result or error.
func (writer *Writer) DryRun(
	ctx context.Context,
	plan materialization.Plan,
	decision reconcile.Decision,
) (Result, error) {
	if writer == nil {
		return Result{}, ErrConfig
	}
	if !writer.Enabled() {
		return Result{}, ErrDisabled
	}
	if ctx == nil {
		return Result{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	now := writer.now().UTC().Truncate(time.Second)
	sealedRequest, document, endpoint, err := buildRequest(writer, plan, decision, now)
	if err != nil {
		return Result{}, ErrIntent
	}
	requestCtx, cancel := context.WithTimeout(ctx, writer.requestTimeout)
	defer cancel()
	credential, err := writer.credential.Credential(requestCtx)
	if err != nil || !canonicalJWT(credential) {
		if requestCtx.Err() != nil {
			return Result{}, requestCtx.Err()
		}
		return Result{}, ErrCredentialUnavailable
	}
	request, err := http.NewRequestWithContext(requestCtx, sealedRequest.Method, endpoint, bytes.NewReader(document))
	if err != nil {
		return Result{}, ErrConfig
	}
	request.Close = true
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", RequestUserAgent)
	response, err := writer.client.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if requestCtx.Err() != nil {
			return Result{}, requestCtx.Err()
		}
		return Result{}, ErrUnavailable
	}
	if response == nil || response.Body == nil {
		return Result{}, ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != sealedRequest.ExpectedStatus {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return Result{}, classifyStatus(response.StatusCode)
	}
	if !safeResponseMetadata(response) || response.ContentLength < -1 || response.ContentLength > writer.maxResponse {
		return Result{}, ErrResponse
	}
	responseDocument, err := io.ReadAll(io.LimitReader(response.Body, writer.maxResponse+1))
	if err != nil || len(responseDocument) == 0 || int64(len(responseDocument)) > writer.maxResponse ||
		(response.ContentLength >= 0 && response.ContentLength != int64(len(responseDocument))) {
		if requestCtx.Err() != nil {
			return Result{}, requestCtx.Err()
		}
		return Result{}, ErrResponse
	}
	if !validDryRunResponse(responseDocument, plan, decision, now) {
		return Result{}, ErrResponse
	}
	result := Result{
		APIVersion:                APIVersion,
		Kind:                      Kind,
		Policy:                    Policy,
		Namespace:                 plan.Namespace,
		SecretName:                plan.SecretName,
		CellKey:                   plan.CellKey,
		CellID:                    plan.CellID,
		Action:                    decision.Action,
		PlanDigest:                plan.Digest,
		DecisionDigest:            decision.Digest,
		RequestDigest:             sealedRequest.RequestDigest,
		IdempotencyKey:            sealedRequest.IdempotencyKey,
		ValidatedAt:               now,
		Accepted:                  true,
		ServerSideDryRun:          true,
		Persisted:                 false,
		DeleteAllowed:             false,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
	}
	result.Digest = DigestResult(result)
	if ValidateResult(result) != nil {
		return Result{}, ErrResponse
	}
	return result, nil
}

func ValidateResult(result Result) error {
	if secretdryrunrequest.ValidateReceipt(result) != nil {
		return ErrResponse
	}
	return nil
}

// ValidateTransportRequest independently proves that a request handed to a
// capability-bearing transport is still the one-cell, server-side dry-run
// shape emitted by Writer. It authenticates the complete sealed Secret body
// through the same recovery contract used by the reader, but grants no
// network or execution capability and does not replace Writer's freshness
// check.
func ValidateTransportRequest(method, path, rawQuery, expectedCellKey string, document []byte) error {
	if secretdryrunrequest.ValidateTransportRequest(method, path, rawQuery, expectedCellKey, document) != nil {
		return ErrIntent
	}
	return nil
}

func DigestResult(result Result) string {
	return secretdryrunrequest.DigestReceipt(result)
}

type secretDocument struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   secretMetadata    `json:"metadata"`
	Immutable  *bool             `json:"immutable"`
	Type       string            `json:"type"`
	Data       map[string]string `json:"data"`
	StringData map[string]string `json:"stringData,omitempty"`
}

type secretMetadata struct {
	Name                       string            `json:"name"`
	GenerateName               string            `json:"generateName,omitempty"`
	Namespace                  string            `json:"namespace"`
	SelfLink                   string            `json:"selfLink,omitempty"`
	UID                        string            `json:"uid,omitempty"`
	ResourceVersion            string            `json:"resourceVersion,omitempty"`
	Generation                 int64             `json:"generation,omitempty"`
	CreationTimestamp          json.RawMessage   `json:"creationTimestamp,omitempty"`
	DeletionTimestamp          json.RawMessage   `json:"deletionTimestamp,omitempty"`
	DeletionGracePeriodSeconds *int64            `json:"deletionGracePeriodSeconds,omitempty"`
	Labels                     map[string]string `json:"labels"`
	Annotations                map[string]string `json:"annotations"`
	OwnerReferences            []json.RawMessage `json:"ownerReferences,omitempty"`
	Finalizers                 []string          `json:"finalizers,omitempty"`
	ManagedFields              []json.RawMessage `json:"managedFields,omitempty"`
}

func buildRequest(
	writer *Writer,
	plan materialization.Plan,
	decision reconcile.Decision,
	now time.Time,
) (secretdryrunrequest.Evidence, []byte, string, error) {
	if writer == nil {
		return secretdryrunrequest.Evidence{}, nil, "", ErrIntent
	}
	prepared, err := secretdryrunrequest.Prepare(writer.expectedCell, plan, decision, now)
	if err != nil {
		return secretdryrunrequest.Evidence{}, nil, "", ErrIntent
	}
	evidence, document, err := prepared.Open(writer.expectedCell, now)
	if err != nil {
		return secretdryrunrequest.Evidence{}, nil, "", ErrIntent
	}
	endpoint := writer.collectionURL
	if evidence.Method == http.MethodPut {
		endpoint = writer.itemURL
	}
	if endpoint == "" {
		return secretdryrunrequest.Evidence{}, nil, "", ErrIntent
	}
	return evidence, document, endpoint, nil
}

func validDryRunResponse(document []byte, plan materialization.Plan, decision reconcile.Decision, now time.Time) bool {
	var response secretDocument
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false
	}
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		return false
	}
	data, err := plan.Data(now)
	if err != nil {
		return false
	}
	metadata := response.Metadata
	if response.APIVersion != "v1" || response.Kind != "Secret" || response.Type != manifest.SecretType ||
		response.Immutable == nil || *response.Immutable || len(response.StringData) != 0 ||
		metadata.Name != manifest.SecretName || metadata.Namespace != manifest.Namespace || metadata.GenerateName != "" ||
		metadata.SelfLink != "" || metadata.Generation != 0 || metadata.DeletionGracePeriodSeconds != nil ||
		len(metadata.OwnerReferences) != 0 || len(metadata.Finalizers) != 0 ||
		!validOptionalOpaque(metadata.UID) || !validOptionalOpaque(metadata.ResourceVersion) ||
		!validOptionalTimestamp(metadata.CreationTimestamp) || !nullOrAbsent(metadata.DeletionTimestamp) ||
		!containsRequired(metadata.Labels, manifest.Labels) || !containsRequired(metadata.Annotations, manifest.Annotations) ||
		len(response.Data) != 2 ||
		response.Data[data.SpecKey] != base64.StdEncoding.EncodeToString(data.SpecDocument) ||
		response.Data[data.TokenKey] != base64.StdEncoding.EncodeToString(data.ObserverToken) {
		return false
	}
	if decision.Action == reconcile.ActionReplaceResourceVersionCAS &&
		(decision.ExpectedUID == "" || decision.ExpectedResourceVersion == "") {
		return false
	}
	return true
}

func dryRunQuery() string {
	query := make(url.Values, 3)
	query.Set("dryRun", "All")
	query.Set("fieldManager", FieldManager)
	query.Set("fieldValidation", "Strict")
	encoded := query.Encode()
	if encoded != DryRunRawQuery {
		return ""
	}
	return encoded
}

func canonicalAPIServerURL(raw string, allowInsecureHTTP bool) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, ErrConfig
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.Hostname() == "" ||
		(parsed.Scheme != "https" && !(allowInsecureHTTP && parsed.Scheme == "http")) ||
		parsed.User != nil || parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.String() != raw ||
		strings.HasSuffix(parsed.Host, ":") {
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

func classifyStatus(status int) error {
	switch {
	case status == http.StatusConflict:
		return ErrConflict
	case status == http.StatusTooManyRequests || status >= 500 && status <= 599:
		return ErrUnavailable
	case status >= 400 && status <= 499:
		return ErrRejected
	default:
		return ErrResponse
	}
}

func safeResponseMetadata(response *http.Response) bool {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	contentEncoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	return err == nil && mediaType == "application/json" &&
		(contentEncoding == "" || strings.EqualFold(contentEncoding, "identity"))
}

func canonicalJWT(token string) bool {
	if token == "" || len(token) > maxBearerTokenBytes || strings.TrimSpace(token) != token || strings.ContainsAny(token, "\r\n") {
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

func validOptionalOpaque(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e || character == '\\' || character == '"' {
			return false
		}
	}
	return true
}

func validOptionalTimestamp(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return true
	}
	var timestamp string
	if json.Unmarshal(trimmed, &timestamp) != nil || timestamp == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, timestamp)
	return err == nil && !parsed.IsZero()
}

func nullOrAbsent(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func containsRequired(actual, required map[string]string) bool {
	if len(actual) < len(required) {
		return false
	}
	for key, value := range required {
		if actual[key] != value {
			return false
		}
	}
	return true
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
