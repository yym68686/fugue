// Package secretwriter performs a server-side dry run of one exact,
// cell-local backup observer input Secret create or resourceVersion-CAS
// replacement. It can never issue a live mutation: every request contains the
// fixed dryRun=All query, redirects and retries are disabled, delete/patch are
// absent, and returned status remains explicitly non-executable.
package secretwriter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
)

const (
	APIVersion = "backup-materializer-secret-dry-run.fugue.dev/v1"
	Kind       = "BackupObserverSecretDryRunResult"
	Policy     = "single-secret-create-or-resource-version-cas-dry-run-v1"

	RequestUserAgent = "fugue-backup-materializer-secret-dry-run/1"
	FieldManager     = "fugue-backup-materializer"
	DryRunRawQuery   = "dryRun=All&fieldManager=fugue-backup-materializer&fieldValidation=Strict"

	DefaultRequestTimeout = 5 * time.Second
	MaximumRequestTimeout = 30 * time.Second
	DefaultMaxResponse    = int64(256 << 10)
	MaximumResponse       = int64(1 << 20)
	MaximumDecisionAge    = 5 * time.Second
	MaximumRequestBytes   = int64(128 << 10)

	minimumResponseBytes = int64(4 << 10)
	maxBearerTokenBytes  = 16 << 10
	secretCollectionPath = "/api/v1/namespaces/" + materialization.SecretNamespace + "/secrets"
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

// Result is a secret-free receipt that the API server accepted the exact
// request through admission, validation, and conflict checks without storage.
// Kubernetes-generated response fields are intentionally omitted because
// dry-run values are not stable evidence of a future persisted object.
type Result struct {
	APIVersion                string           `json:"apiVersion"`
	Kind                      string           `json:"kind"`
	Policy                    string           `json:"policy"`
	Namespace                 string           `json:"namespace"`
	SecretName                string           `json:"secretName"`
	CellKey                   string           `json:"cellKey"`
	CellID                    string           `json:"cellId"`
	Action                    reconcile.Action `json:"action"`
	PlanDigest                string           `json:"planDigest"`
	DecisionDigest            string           `json:"decisionDigest"`
	RequestDigest             string           `json:"requestDigest"`
	IdempotencyKey            string           `json:"idempotencyKey"`
	ValidatedAt               time.Time        `json:"validatedAt"`
	Accepted                  bool             `json:"accepted"`
	ServerSideDryRun          bool             `json:"serverSideDryRun"`
	Persisted                 bool             `json:"persisted"`
	DeleteAllowed             bool             `json:"deleteAllowed"`
	ExecutionAllowed          bool             `json:"executionAllowed"`
	ProductionMutationAllowed bool             `json:"productionMutationAllowed"`
	Digest                    string           `json:"digest"`
}

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
	if !validIntent(writer, plan, decision, now) {
		return Result{}, ErrIntent
	}
	document, method, endpoint, expectedStatus, err := buildRequest(writer, plan, decision, now)
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
	request, err := http.NewRequestWithContext(requestCtx, method, endpoint, bytes.NewReader(document))
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
	if response.StatusCode != expectedStatus {
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
		RequestDigest:             digestBytes(document),
		IdempotencyKey:            dryRunIdempotencyKey(plan.CellID, decision.Digest),
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
	identity, err := materialization.SecretIdentityForCell(result.CellKey)
	if err != nil || result.APIVersion != APIVersion || result.Kind != Kind || result.Policy != Policy ||
		result.Namespace != identity.Namespace || result.SecretName != identity.SecretName || result.CellID != identity.CellID ||
		(result.Action != reconcile.ActionCreateIfAbsent && result.Action != reconcile.ActionReplaceResourceVersionCAS) ||
		!validDigest(result.PlanDigest) || !validDigest(result.DecisionDigest) || !validDigest(result.RequestDigest) ||
		result.IdempotencyKey != dryRunIdempotencyKey(result.CellID, result.DecisionDigest) ||
		!canonicalTime(result.ValidatedAt) || !result.Accepted || !result.ServerSideDryRun || result.Persisted ||
		result.DeleteAllowed || result.ExecutionAllowed || result.ProductionMutationAllowed || result.Digest != DigestResult(result) {
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
	identity, err := materialization.SecretIdentityForCell(expectedCellKey)
	if err != nil || rawQuery != DryRunRawQuery || len(document) == 0 || int64(len(document)) > MaximumRequestBytes {
		return ErrIntent
	}
	var request secretDocument
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return ErrIntent
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrIntent
	}
	canonicalDocument, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonicalDocument, document) {
		return ErrIntent
	}
	metadata := request.Metadata
	if request.APIVersion != "v1" || request.Kind != "Secret" || request.Type != reconcile.SecretTypeOpaque ||
		request.Immutable == nil || *request.Immutable || len(request.StringData) != 0 ||
		metadata.Name != identity.SecretName || metadata.Namespace != identity.Namespace || metadata.GenerateName != "" ||
		metadata.SelfLink != "" || metadata.Generation != 0 || len(metadata.CreationTimestamp) != 0 ||
		len(metadata.DeletionTimestamp) != 0 || metadata.DeletionGracePeriodSeconds != nil ||
		len(metadata.OwnerReferences) != 0 || len(metadata.Finalizers) != 0 || len(metadata.ManagedFields) != 0 ||
		len(request.Data) != 2 {
		return ErrIntent
	}
	uid := metadata.UID
	resourceVersion := metadata.ResourceVersion
	switch method {
	case http.MethodPost:
		if path != secretCollectionPath || uid != "" || resourceVersion != "" {
			return ErrIntent
		}
		uid = "transport-validation"
		resourceVersion = "1"
	case http.MethodPut:
		if path != secretCollectionPath+"/"+identity.SecretName || uid == "" || resourceVersion == "" {
			return ErrIntent
		}
	default:
		return ErrIntent
	}
	data := make(map[string][]byte, len(request.Data))
	for key, encoded := range request.Data {
		decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
			return ErrIntent
		}
		data[key] = decoded
	}
	observation, err := reconcile.ObserveExisting(expectedCellKey, reconcile.SecretEvidence{
		Namespace: identity.Namespace, SecretName: identity.SecretName, UID: uid, ResourceVersion: resourceVersion,
		SecretType: request.Type, Labels: request.Metadata.Labels, Annotations: request.Metadata.Annotations,
		Data: data, Immutable: false, DeletionPending: false, OwnerReferenceCount: 0,
	})
	if err != nil || observation.State != reconcile.StateManaged {
		return ErrIntent
	}
	return nil
}

func DigestResult(result Result) string {
	result.Digest = ""
	document, err := json.Marshal(result)
	if err != nil {
		return ""
	}
	return digestBytes(document)
}

func (result Result) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretDryRunResult{cell=%q action=%q accepted=%t persisted=false executionAllowed=false digest=%q}",
		result.CellKey,
		result.Action,
		result.Accepted,
		result.Digest,
	)
}

func (result Result) GoString() string { return result.String() }

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

func validIntent(writer *Writer, plan materialization.Plan, decision reconcile.Decision, now time.Time) bool {
	if writer == nil || !canonicalTime(now) || reconcile.ValidateDecision(decision) != nil ||
		materialization.Validate(plan, now) != nil || plan.CellKey != writer.expectedCell ||
		plan.CellID != writer.expectedCellID || plan.SecretName != writer.expectedName ||
		decision.Namespace != plan.Namespace || decision.SecretName != plan.SecretName ||
		decision.CellKey != plan.CellKey || decision.CellID != plan.CellID ||
		decision.DesiredPlanDigest != plan.Digest || !decision.MutationCandidate || decision.Blocked ||
		decision.ExecutionAllowed || decision.ProductionMutationAllowed || decision.DeleteAllowed ||
		decision.DecidedAt.After(now) || now.Sub(decision.DecidedAt) > MaximumDecisionAge {
		return false
	}
	return decision.Action == reconcile.ActionCreateIfAbsent || decision.Action == reconcile.ActionReplaceResourceVersionCAS
}

func buildRequest(
	writer *Writer,
	plan materialization.Plan,
	decision reconcile.Decision,
	now time.Time,
) ([]byte, string, string, int, error) {
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		return nil, "", "", 0, ErrIntent
	}
	data, err := plan.Data(now)
	if err != nil {
		return nil, "", "", 0, ErrIntent
	}
	immutable := false
	document := secretDocument{
		APIVersion: "v1",
		Kind:       "Secret",
		Metadata: secretMetadata{
			Name:        manifest.SecretName,
			Namespace:   manifest.Namespace,
			Labels:      cloneStringMap(manifest.Labels),
			Annotations: cloneStringMap(manifest.Annotations),
		},
		Immutable: &immutable,
		Type:      manifest.SecretType,
		Data: map[string]string{
			data.SpecKey:  base64.StdEncoding.EncodeToString(data.SpecDocument),
			data.TokenKey: base64.StdEncoding.EncodeToString(data.ObserverToken),
		},
	}
	method := http.MethodPost
	endpoint := writer.collectionURL
	expectedStatus := http.StatusCreated
	if decision.Action == reconcile.ActionReplaceResourceVersionCAS {
		document.Metadata.UID = decision.ExpectedUID
		document.Metadata.ResourceVersion = decision.ExpectedResourceVersion
		method = http.MethodPut
		endpoint = writer.itemURL
		expectedStatus = http.StatusOK
	}
	encoded, err := json.Marshal(document)
	if err != nil || len(encoded) == 0 || int64(len(encoded)) > MaximumRequestBytes {
		return nil, "", "", 0, ErrIntent
	}
	return encoded, method, endpoint, expectedStatus, nil
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

func cloneStringMap(value map[string]string) map[string]string {
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func dryRunIdempotencyKey(cellID, decisionDigest string) string {
	if cellID == "" || !validDigest(decisionDigest) {
		return ""
	}
	return "backup-materializer-secret-dry-run/" + cellID + "/" + strings.TrimPrefix(decisionDigest, "sha256:")
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && strings.ToLower(value) == value
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func canonicalTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
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
