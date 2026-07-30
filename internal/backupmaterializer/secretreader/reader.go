// Package secretreader implements the default-off, GET-only Kubernetes API
// boundary for observing one cell-local backup observer input Secret. It has
// no create, update, patch, delete, watch, filesystem, datastore, signer, or
// process capability.
package secretreader

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
)

const (
	APIVersion = "backup-materializer-secret-reader.fugue.dev/v1"

	DefaultRequestTimeout = 5 * time.Second
	MaximumRequestTimeout = 30 * time.Second
	DefaultMaxResponse    = int64(128 << 10)
	MaximumResponse       = int64(256 << 10)

	minimumResponseBytes = int64(4 << 10)
	maxBearerTokenBytes  = 16 << 10
	secretPathPrefix     = "/api/v1/namespaces/"
)

var (
	ErrConfig                = errors.New("backup materializer Secret reader configuration invalid")
	ErrDisabled              = errors.New("backup materializer Secret reader disabled")
	ErrCredentialUnavailable = errors.New("backup materializer Secret reader credential unavailable")
	ErrSecretUnavailable     = errors.New("backup materializer Secret observation unavailable")
	ErrSecretResponse        = errors.New("backup materializer Secret response invalid")
)

// CredentialSource supplies the reader's Kubernetes-API audience credential.
// Implementations must reread their source for every observation so projected
// token rotation is visible. The reader never returns or formats it.
type CredentialSource interface {
	Credential(context.Context) (string, error)
}

type Config struct {
	Enabled          bool
	APIServerURL     string
	ExpectedCellKey  string
	CredentialSource CredentialSource
	HTTPClient       *http.Client
	RequestTimeout   time.Duration
	MaxResponseBytes int64
}

func (config Config) String() string {
	return "backup materializer Secret reader configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

type Reader struct {
	enabled         bool
	endpoint        string
	expectedCellKey string
	expectedName    string
	credential      CredentialSource
	client          *http.Client
	requestTimeout  time.Duration
	maxResponse     int64
}

// New retains none of the supplied URL, cell, credential, or HTTP capability
// while disabled. Enabled construction validates one exact target but performs
// no credential access or network request.
func New(config Config) (*Reader, error) {
	if !config.Enabled {
		return &Reader{}, nil
	}
	identity, err := materialization.SecretIdentityForCell(config.ExpectedCellKey)
	if err != nil || nilInterface(config.CredentialSource) || config.HTTPClient == nil {
		return nil, ErrConfig
	}
	baseURL, err := canonicalAPIServerURL(config.APIServerURL)
	if err != nil {
		return nil, ErrConfig
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = DefaultRequestTimeout
	}
	if config.RequestTimeout < time.Second || config.RequestTimeout > MaximumRequestTimeout ||
		config.RequestTimeout%time.Millisecond != 0 {
		return nil, ErrConfig
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = DefaultMaxResponse
	}
	if config.MaxResponseBytes < minimumResponseBytes || config.MaxResponseBytes > MaximumResponse {
		return nil, ErrConfig
	}
	client := &http.Client{}
	*client = *config.HTTPClient
	client.Timeout = config.RequestTimeout
	client.Jar = nil
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	endpoint := *baseURL
	endpoint.Path = secretPathPrefix + identity.Namespace + "/secrets/" + identity.SecretName
	return &Reader{
		enabled:         true,
		endpoint:        endpoint.String(),
		expectedCellKey: identity.CellKey,
		expectedName:    identity.SecretName,
		credential:      config.CredentialSource,
		client:          client,
		requestTimeout:  config.RequestTimeout,
		maxResponse:     config.MaxResponseBytes,
	}, nil
}

func (reader *Reader) Enabled() bool {
	return reader != nil && reader.enabled && reader.endpoint != "" && reader.expectedCellKey != "" &&
		reader.expectedName != "" && reader.credential != nil && reader.client != nil &&
		reader.requestTimeout > 0 && reader.maxResponse > 0
}

func (reader *Reader) String() string {
	if reader == nil {
		return "backup materializer Secret reader <nil>"
	}
	return "backup materializer Secret reader [REDACTED]"
}

func (reader *Reader) GoString() string { return reader.String() }

// Observe performs exactly one bounded GET of the configured Secret. A proven
// Kubernetes NotFound becomes an absent observation; a valid object becomes a
// managed/foreign/malformed observation through the pure reconcile contract.
func (reader *Reader) Observe(ctx context.Context) (reconcile.Observation, error) {
	if reader == nil {
		return reconcile.Observation{}, ErrConfig
	}
	if !reader.Enabled() {
		return reconcile.Observation{}, ErrDisabled
	}
	if ctx == nil {
		return reconcile.Observation{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return reconcile.Observation{}, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, reader.requestTimeout)
	defer cancel()
	credential, err := reader.credential.Credential(requestCtx)
	if err != nil || !canonicalJWT(credential) {
		if requestCtx.Err() != nil {
			return reconcile.Observation{}, requestCtx.Err()
		}
		return reconcile.Observation{}, ErrCredentialUnavailable
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, reader.endpoint, nil)
	if err != nil {
		return reconcile.Observation{}, ErrConfig
	}
	request.Close = true
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Cache-Control", "no-store")
	response, err := reader.client.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if requestCtx.Err() != nil {
			return reconcile.Observation{}, requestCtx.Err()
		}
		return reconcile.Observation{}, ErrSecretUnavailable
	}
	if response == nil || response.Body == nil {
		return reconcile.Observation{}, ErrSecretUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return reconcile.Observation{}, ErrSecretUnavailable
	}
	if !safeResponseMetadata(response) || response.ContentLength < -1 || response.ContentLength > reader.maxResponse {
		return reconcile.Observation{}, ErrSecretResponse
	}
	document, err := io.ReadAll(io.LimitReader(response.Body, reader.maxResponse+1))
	if err != nil || len(document) == 0 || int64(len(document)) > reader.maxResponse ||
		(response.ContentLength >= 0 && response.ContentLength != int64(len(document))) {
		if requestCtx.Err() != nil {
			return reconcile.Observation{}, requestCtx.Err()
		}
		return reconcile.Observation{}, ErrSecretResponse
	}
	if response.StatusCode == http.StatusNotFound {
		if !validNotFound(document, reader.expectedName) {
			return reconcile.Observation{}, ErrSecretResponse
		}
		observation, err := reconcile.ObserveAbsent(reader.expectedCellKey)
		if err != nil {
			return reconcile.Observation{}, ErrSecretResponse
		}
		return observation, nil
	}
	evidence, forceMalformed, err := decodeSecret(document)
	if err != nil {
		return reconcile.Observation{}, ErrSecretResponse
	}
	if forceMalformed {
		evidence.SecretType = ""
	}
	observation, err := reconcile.ObserveExisting(reader.expectedCellKey, evidence)
	if err != nil {
		return reconcile.Observation{}, ErrSecretResponse
	}
	return observation, nil
}

type secretDocument struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   secretMetadata    `json:"metadata"`
	Immutable  *bool             `json:"immutable,omitempty"`
	Data       map[string]string `json:"data"`
	StringData map[string]string `json:"stringData,omitempty"`
	Type       string            `json:"type"`
}

type secretMetadata struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	UID               string            `json:"uid"`
	ResourceVersion   string            `json:"resourceVersion"`
	Labels            map[string]string `json:"labels"`
	Annotations       map[string]string `json:"annotations"`
	DeletionTimestamp json.RawMessage   `json:"deletionTimestamp"`
	OwnerReferences   []map[string]any  `json:"ownerReferences"`
}

func decodeSecret(document []byte) (reconcile.SecretEvidence, bool, error) {
	var wire secretDocument
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&wire); err != nil {
		return reconcile.SecretEvidence{}, false, ErrSecretResponse
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return reconcile.SecretEvidence{}, false, ErrSecretResponse
	}
	data := make(map[string][]byte, len(wire.Data))
	forceMalformed := wire.APIVersion != "v1" || wire.Kind != "Secret" || len(wire.StringData) != 0
	for key, encoded := range wire.Data {
		decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
			forceMalformed = true
			data = nil
			break
		}
		data[key] = decoded
	}
	immutable := wire.Immutable != nil && *wire.Immutable
	deletionTimestamp := bytes.TrimSpace(wire.Metadata.DeletionTimestamp)
	deletionPending := len(deletionTimestamp) != 0 && !bytes.Equal(deletionTimestamp, []byte("null"))
	return reconcile.SecretEvidence{
		Namespace:           wire.Metadata.Namespace,
		SecretName:          wire.Metadata.Name,
		UID:                 wire.Metadata.UID,
		ResourceVersion:     wire.Metadata.ResourceVersion,
		SecretType:          wire.Type,
		Labels:              wire.Metadata.Labels,
		Annotations:         wire.Metadata.Annotations,
		Data:                data,
		Immutable:           immutable,
		DeletionPending:     deletionPending,
		OwnerReferenceCount: len(wire.Metadata.OwnerReferences),
	}, forceMalformed, nil
}

type statusDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Status     string `json:"status"`
	Reason     string `json:"reason"`
	Code       int32  `json:"code"`
	Details    struct {
		Name  string `json:"name"`
		Kind  string `json:"kind"`
		Group string `json:"group"`
	} `json:"details"`
}

func validNotFound(document []byte, expectedName string) bool {
	var status statusDocument
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&status); err != nil {
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false
	}
	return status.APIVersion == "v1" && status.Kind == "Status" && status.Status == "Failure" &&
		status.Reason == "NotFound" && status.Code == http.StatusNotFound &&
		status.Details.Name == expectedName && status.Details.Kind == "secrets" && status.Details.Group == ""
}

func canonicalAPIServerURL(raw string) (*url.URL, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return nil, ErrConfig
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" ||
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

func safeResponseMetadata(response *http.Response) bool {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	contentEncoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	return err == nil && mediaType == "application/json" &&
		(contentEncoding == "" || strings.EqualFold(contentEncoding, "identity"))
}

func canonicalJWT(token string) bool {
	if token == "" || len(token) > maxBearerTokenBytes || strings.TrimSpace(token) != token ||
		strings.ContainsAny(token, "\r\n") {
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
