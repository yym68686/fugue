// Package httpapi defines the private HTTP read boundary for issuing one
// backup observer input bundle. It owns no route registration, datastore,
// signing key, Kubernetes client, filesystem, or mutation capability.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	"fugue/internal/backupmaterializeridentity"
	"fugue/internal/backupmaterializeridentity/httpauth"
)

const RoutePath = "/v1/backup-control/runs/{run}/observer-input-bundle"

var (
	ErrConfig           = errors.New("backup materializer input HTTP configuration invalid")
	ErrInputNotFound    = errors.New("backup materializer input not found")
	ErrInputConflict    = errors.New("backup materializer input inconsistent")
	ErrInputUnavailable = errors.New("backup materializer input unavailable")

	canonicalRunID    = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	canonicalTenantID = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	canonicalTokenID  = regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`)
)

// ReadRequest is the exact token-free ownership precondition given to the
// data owner. The source must filter by both values and the handler validates
// the returned spec a second time before invoking the signer.
type ReadRequest struct {
	RunID   string
	CellKey string
}

// DesiredInput is the already redacted desired state returned by a future
// data-owner adapter. TenantID is used only to bind the short-lived observer
// identity and is never written as a separate response field.
type DesiredInput struct {
	Spec     backupcontrol.BackupRunSpec
	TenantID string
}

// Source performs the bounded read for one exact run. Implementations must
// return ErrInputNotFound, ErrInputConflict, or ErrInputUnavailable without
// placing secret-bearing backend data in the returned error.
type Source interface {
	ReadDesiredInput(context.Context, ReadRequest) (DesiredInput, error)
}

// Issuer owns the observer signing capability. It is invoked only after the
// current spec has been validated and bound to the authenticated caller cell.
type Issuer interface {
	IssueObserverInputBundle(context.Context, backupcontrol.BackupRunSpec, string, time.Time) (backupmaterializer.ObserverInputBundle, error)
}

type Handler struct {
	source Source
	issuer Issuer
	now    func() time.Time
}

func New(source Source, issuer Issuer, now func() time.Time) (*Handler, error) {
	if nilInterface(source) || nilInterface(issuer) || now == nil {
		return nil, ErrConfig
	}
	return &Handler{source: source, issuer: issuer, now: now}, nil
}

// ServeHTTP serves only the fixed GET resource. A generated route and the
// materializer HTTP identity middleware must eventually wrap this handler;
// the direct guards keep an accidental unwrapped registration fail-closed.
func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setPrivateHeaders(writer.Header())
	if request == nil || request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if handler == nil || nilInterface(handler.source) || nilInterface(handler.issuer) || handler.now == nil {
		writeUnavailable(writer)
		return
	}
	claims, ok := httpauth.ClaimsFromContext(request.Context())
	if !ok || backupmaterializeridentity.ValidateClaims(claims) != nil {
		writeError(writer, http.StatusUnauthorized, "auth_required")
		return
	}
	runID := request.PathValue("run")
	if request.URL == nil || request.URL.RawQuery != "" || request.ContentLength != 0 || len(request.TransferEncoding) != 0 ||
		!canonicalRunID.MatchString(runID) || !exactRoutePath(request.URL, runID) {
		writeError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	now := handler.now()
	if now.IsZero() {
		writeUnavailable(writer)
		return
	}
	input, err := handler.source.ReadDesiredInput(request.Context(), ReadRequest{RunID: runID, CellKey: claims.CellKey})
	if err != nil {
		writeSourceError(writer, err)
		return
	}
	if !validDesiredInput(input, runID) {
		writeError(writer, http.StatusConflict, "input_inconsistent")
		return
	}
	if claims.CellKey != input.Spec.CellKey {
		// Keep a foreign-cell run indistinguishable from an absent run.
		writeError(writer, http.StatusNotFound, "input_not_found")
		return
	}
	bundle, err := handler.issuer.IssueObserverInputBundle(
		request.Context(),
		input.Spec,
		input.TenantID,
		now,
	)
	if err != nil || !validIssuedBundle(bundle, input.Spec, now) {
		writeUnavailable(writer)
		return
	}
	document, err := json.Marshal(bundle)
	if err != nil || len(document) == 0 || len(document) > backupmaterializer.MaxObserverInputBundleBytes {
		writeUnavailable(writer)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Length", strconv.Itoa(len(document)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(document)
}

func validDesiredInput(input DesiredInput, runID string) bool {
	tenantID := strings.TrimSpace(input.TenantID)
	return backupcontrol.ValidateBackupRunSpec(input.Spec) == nil &&
		input.Spec.RunID == runID &&
		input.TenantID == tenantID &&
		(tenantID == "" || canonicalTenantID.MatchString(tenantID))
}

func validIssuedBundle(
	bundle backupmaterializer.ObserverInputBundle,
	spec backupcontrol.BackupRunSpec,
	now time.Time,
) bool {
	issuedAt := now.UTC().Truncate(time.Second)
	return bundle.APIVersion == backupmaterializer.ObserverInputBundleAPIVersion &&
		bundle.Kind == backupmaterializer.ObserverInputBundleKind &&
		bundle.Policy == backupmaterializer.ObserverInputBundlePolicy &&
		bundle.CellKey == spec.CellKey &&
		bundle.RunID == spec.RunID &&
		bundle.SpecDigest == spec.Digest &&
		bundle.CredentialID == backupidentity.CredentialIDForCell(spec.CellKey) &&
		canonicalTokenID.MatchString(bundle.TokenID) &&
		bundle.DesiredSpec == spec &&
		bundle.IssuedAt == issuedAt &&
		bundle.RenewAfter == issuedAt.Add(backupmaterializer.ObserverIdentityRenewAfter) &&
		bundle.ExpiresAt == issuedAt.Add(backupmaterializer.ObserverIdentityTTL) &&
		bundle.ObservationOnly && !bundle.ProductionMutationAllowed &&
		strings.HasPrefix(bundle.ObserverToken, "fugue_bo_v1.") &&
		strings.TrimSpace(bundle.ObserverToken) == bundle.ObserverToken &&
		bundle.Digest != "" && bundle.Digest == backupmaterializer.DigestObserverInputBundle(bundle)
}

func exactRoutePath(requestURL *url.URL, runID string) bool {
	if requestURL == nil || requestURL.RawPath != "" {
		return false
	}
	want := strings.Replace(RoutePath, "{run}", runID, 1)
	return requestURL.Path == want && requestURL.EscapedPath() == want
}

func writeSourceError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInputNotFound):
		writeError(writer, http.StatusNotFound, "input_not_found")
	case errors.Is(err, ErrInputConflict):
		writeError(writer, http.StatusConflict, "input_inconsistent")
	default:
		writeUnavailable(writer)
	}
}

func writeUnavailable(writer http.ResponseWriter) {
	writer.Header().Set("Retry-After", "1")
	writeError(writer, http.StatusServiceUnavailable, "input_unavailable")
}

type errorResponse struct {
	Code string `json:"code"`
}

func writeError(writer http.ResponseWriter, status int, code string) {
	setPrivateHeaders(writer.Header())
	document, _ := json.Marshal(errorResponse{Code: code})
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Length", strconv.Itoa(len(document)))
	writer.WriteHeader(status)
	_, _ = writer.Write(document)
}

func setPrivateHeaders(header http.Header) {
	header.Set("Cache-Control", "private, no-store, max-age=0")
	header.Set("Pragma", "no-cache")
	header.Set("X-Content-Type-Options", "nosniff")
	for _, value := range header.Values("Vary") {
		for _, field := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(field), "Authorization") {
				return
			}
		}
	}
	header.Add("Vary", "Authorization")
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
