package api

import (
	"net/http"
	"reflect"
	"strings"

	"fugue/internal/httpx"
)

// BackupMaterializerEndpoint is a private, versioned composition injected by
// the process root. The endpoint itself must own the projected workload
// identity boundary; the generated route gate owns only default-off
// availability and cannot authenticate or read backup state.
type BackupMaterializerEndpoint interface {
	http.Handler
	Enabled() bool
	BackupMaterializerEndpointV1()
}

// requireBackupMaterializerEndpoint keeps the generated OpenAPI route private
// and indistinguishable from an absent resource until a complete v1 endpoint
// is explicitly injected. It never falls through to another Fugue auth mode.
func (s *Server) requireBackupMaterializerEndpoint(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		setBackupMaterializerNoStoreHeaders(writer.Header())
		if !s.backupMaterializerEndpointEnabled() || nilHTTPHandler(next) {
			httpx.WriteError(writer, http.StatusNotFound, "not found")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

// handleGetBackupObserverInputBundle delegates the exact generated route to
// the injected composition. The repeated enablement guard keeps direct method
// use fail-closed and narrows a state change between route admission and
// dispatch to a private 404.
func (s *Server) handleGetBackupObserverInputBundle(writer http.ResponseWriter, request *http.Request) {
	setBackupMaterializerNoStoreHeaders(writer.Header())
	if !s.backupMaterializerEndpointEnabled() {
		httpx.WriteError(writer, http.StatusNotFound, "not found")
		return
	}
	s.backupMaterializerEndpoint.ServeHTTP(writer, request)
}

func (s *Server) backupMaterializerEndpointEnabled() bool {
	if s == nil || nilInterfaceValue(s.backupMaterializerEndpoint) {
		return false
	}
	return s.backupMaterializerEndpoint.Enabled()
}

func nilHTTPHandler(handler http.Handler) bool {
	return nilInterfaceValue(handler)
}

func nilInterfaceValue(value any) bool {
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

func setBackupMaterializerNoStoreHeaders(header http.Header) {
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
