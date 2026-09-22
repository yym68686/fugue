package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Only historical equivalence tests use these adapters. The production router
// retains authenticated 410 responses for the removed comparison APIs.
func performLegacyComparisonRequest(t *testing.T, s *Server, method, target, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var handler http.HandlerFunc
	if strings.HasPrefix(target, "/v1/admin/platform-config/dns/compare") {
		handler = s.handleComparePlatformDNSMigrationForTest
	}
	if strings.HasPrefix(target, "/v1/admin/platform-config/routes/compare") {
		handler = s.handleComparePlatformRouteMigrationForTest
	}
	if handler == nil {
		return performJSONRequest(t, s, method, target, token, body)
	}
	if method != http.MethodGet || body != nil {
		t.Fatal("legacy comparison adapter is read-only")
	}
	req := httptest.NewRequest(method, target, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	r := httptest.NewRecorder()
	s.auth.RequireAPI(handler).ServeHTTP(r, req)
	return r
}
