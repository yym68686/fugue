// Package httpauth exposes the backup materializer workload identity as a
// claims-only, GET-only HTTP boundary. It owns no route, store, signer,
// Kubernetes client, or mutation capability.
package httpauth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"time"

	"fugue/internal/backupmaterializeridentity"
)

const (
	privateCacheControl = "private, no-store, max-age=0"
	materializerRealm   = `Bearer realm="fugue-backup-materializer"`
)

var ErrConfig = errors.New("backup materializer HTTP identity configuration invalid")

type clock func() time.Time

type Middleware struct {
	reviewer backupmaterializeridentity.TokenReviewer
	now      clock
}

// New constructs an authentication-only boundary. The reviewer and clock are
// injected so this package remains independent of Kubernetes transport,
// filesystem, server configuration, and global process state.
func New(
	reviewer backupmaterializeridentity.TokenReviewer,
	now func() time.Time,
) (*Middleware, error) {
	if nilInterface(reviewer) || now == nil {
		return nil, ErrConfig
	}
	return &Middleware{reviewer: reviewer, now: now}, nil
}

type claimsContextKey struct{}

// ClaimsFromContext returns only the already validated, token-free workload
// identity. The reviewed bearer credential is never placed in context.
func ClaimsFromContext(ctx context.Context) (backupmaterializeridentity.Claims, bool) {
	if ctx == nil {
		return backupmaterializeridentity.Claims{}, false
	}
	claims, ok := ctx.Value(claimsContextKey{}).(backupmaterializeridentity.Claims)
	return claims, ok
}

// RequireGET rejects every non-GET request before credential parsing or
// external review. On success it derives the exact cell from TokenReview,
// strips authentication headers from the cloned downstream request, and adds
// only validated Claims to context.
func (middleware *Middleware) RequireGET(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		privateWriter := &noStoreWriter{ResponseWriter: writer}
		setPrivateHeaders(privateWriter.Header())
		if request == nil || request.Method != http.MethodGet {
			privateWriter.Header().Set("Allow", http.MethodGet)
			writeError(privateWriter, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		if middleware == nil || nilInterface(middleware.reviewer) || middleware.now == nil || nilInterface(next) {
			writeUnavailable(privateWriter)
			return
		}
		now := middleware.now()
		if now.IsZero() {
			writeUnavailable(privateWriter)
			return
		}
		token, ok := exactBearerToken(request)
		if !ok {
			writeUnauthorized(privateWriter)
			return
		}
		claims, err := backupmaterializeridentity.AuthenticateReviewedCell(
			request.Context(),
			middleware.reviewer,
			token,
			now,
		)
		if err != nil {
			if errors.Is(err, backupmaterializeridentity.ErrReviewerUnavailable) {
				writeUnavailable(privateWriter)
				return
			}
			writeUnauthorized(privateWriter)
			return
		}
		ctx := context.WithValue(request.Context(), claimsContextKey{}, claims)
		sanitized := request.Clone(ctx)
		sanitized.Header = request.Header.Clone()
		sanitized.Header.Del("Authorization")
		sanitized.Header.Del("Proxy-Authorization")
		next.ServeHTTP(privateWriter, sanitized)
		setPrivateHeaders(privateWriter.Header())
	})
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

func exactBearerToken(request *http.Request) (string, bool) {
	if request == nil {
		return "", false
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	value := values[0]
	if strings.TrimSpace(value) != value || !strings.HasPrefix(value, "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(value, "Bearer ")
	if token == "" || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t,\r\n") {
		return "", false
	}
	return token, true
}

type errorResponse struct {
	Code string `json:"code"`
}

func writeUnauthorized(writer http.ResponseWriter) {
	writer.Header().Set("WWW-Authenticate", materializerRealm)
	writeError(writer, http.StatusUnauthorized, "auth_required")
}

func writeUnavailable(writer http.ResponseWriter) {
	writer.Header().Set("Retry-After", "1")
	writeError(writer, http.StatusServiceUnavailable, "identity_unavailable")
}

func writeError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(errorResponse{Code: code})
}

type noStoreWriter struct {
	http.ResponseWriter
}

func (writer *noStoreWriter) Unwrap() http.ResponseWriter {
	if writer == nil {
		return nil
	}
	return writer.ResponseWriter
}

func (writer *noStoreWriter) WriteHeader(status int) {
	setPrivateHeaders(writer.Header())
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *noStoreWriter) Write(document []byte) (int, error) {
	setPrivateHeaders(writer.Header())
	return writer.ResponseWriter.Write(document)
}

func setPrivateHeaders(header http.Header) {
	header.Set("Cache-Control", privateCacheControl)
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
