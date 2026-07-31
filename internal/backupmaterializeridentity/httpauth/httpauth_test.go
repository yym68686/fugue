package httpauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupmaterializeridentity"
)

const (
	testCellKey = "backup/app-database/0123456789abcdef"
	testToken   = "test-header.test-materializer-payload.test-materializer-signature"
)

type reviewerStub struct {
	result    backupmaterializeridentity.ReviewResult
	err       error
	calls     int
	token     string
	audiences []string
}

type downstreamStub struct{}

func (*downstreamStub) ServeHTTP(http.ResponseWriter, *http.Request) {
	panic("typed nil downstream handler was invoked")
}

func (reviewer *reviewerStub) ReviewToken(
	_ context.Context,
	token string,
	audiences []string,
) (backupmaterializeridentity.ReviewResult, error) {
	reviewer.calls++
	reviewer.token = token
	reviewer.audiences = append([]string(nil), audiences...)
	return reviewer.result, reviewer.err
}

func TestRequireGETPassesOnlyTokenFreeCellClaims(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 13, 0, 0, 123, time.UTC)
	reviewer := &reviewerStub{result: validReviewResult(testCellKey)}
	middleware, err := New(reviewer, func() time.Time { return now })
	if err != nil {
		t.Fatalf("create materializer HTTP identity: %v", err)
	}
	downstreamCalled := false
	handler := middleware.RequireGET(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		downstreamCalled = true
		claims, ok := ClaimsFromContext(request.Context())
		if !ok || claims.CellKey != testCellKey || claims.ServiceAccountName != backupmaterializeridentity.ServiceAccountNameForCell(testCellKey) ||
			claims.ReviewedAt != now.Truncate(time.Second) || claims.Permission != backupmaterializeridentity.PermissionReadBundle {
			t.Fatalf("downstream claims missing or drifted: claims=%+v ok=%t", claims, ok)
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("Proxy-Authorization") != "" {
			t.Fatalf("downstream request retained authentication headers: %v", request.Header)
		}
		rendered := fmt.Sprintf("%+v", claims)
		if strings.Contains(rendered, testToken) || strings.Contains(rendered, "test-header") {
			t.Fatalf("claims retained reviewed bearer material: %s", rendered)
		}
		writer.Header().Set("Cache-Control", "public, max-age=3600")
		writer.Header().Del("Pragma")
		writer.Header().Set("Vary", "Accept-Encoding")
		writer.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/future-private-bundle", nil)
	request.Header.Set("Authorization", "Bearer "+testToken)
	request.Header.Set("Proxy-Authorization", "Bearer proxy-secret")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || !downstreamCalled {
		t.Fatalf("authenticated request status=%d downstream=%t body=%s", recorder.Code, downstreamCalled, recorder.Body.String())
	}
	if reviewer.calls != 1 || reviewer.token != testToken || !reflect.DeepEqual(reviewer.audiences, []string{backupmaterializeridentity.Audience}) {
		t.Fatalf("TokenReview request drifted: calls=%d token=%q audiences=%v", reviewer.calls, reviewer.token, reviewer.audiences)
	}
	assertPrivateResponse(t, recorder)
	if !headerContainsToken(recorder.Header().Values("Vary"), "Authorization") || !headerContainsToken(recorder.Header().Values("Vary"), "Accept-Encoding") {
		t.Fatalf("private response Vary header drifted: %v", recorder.Header().Values("Vary"))
	}
}

func TestRequireGETReassertsPrivacyForImplicitSuccess(t *testing.T) {
	t.Parallel()
	reviewer := &reviewerStub{result: validReviewResult(testCellKey)}
	middleware, err := New(reviewer, func() time.Time { return time.Date(2026, 7, 30, 13, 5, 0, 0, time.UTC) })
	if err != nil {
		t.Fatalf("create materializer HTTP identity: %v", err)
	}
	handler := middleware.RequireGET(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, ok := ClaimsFromContext(request.Context()); !ok {
			t.Fatal("implicit response is missing materializer claims")
		}
		writer.Header().Set("Cache-Control", "public, max-age=3600")
		writer.Header().Del("Pragma")
		writer.Header().Del("Vary")
	}))
	request := httptest.NewRequest(http.MethodGet, "/future-private-bundle", nil)
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if response := recorder.Result(); response.StatusCode != http.StatusOK {
		t.Fatalf("implicit success status=%d, want 200", response.StatusCode)
	}
	assertPrivateResponse(t, recorder)
}

func TestRequireGETRejectsMalformedAndForeignCredentialsUniformly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC)
	invalidReview := validReviewResult(testCellKey)
	invalidReview.Username = "system:serviceaccount:default:" + backupmaterializeridentity.ServiceAccountNameForCell(testCellKey)
	tests := map[string]struct {
		headers  []string
		reviewer *reviewerStub
	}{
		"missing":          {reviewer: &reviewerStub{result: validReviewResult(testCellKey)}},
		"empty":            {headers: []string{""}, reviewer: &reviewerStub{result: validReviewResult(testCellKey)}},
		"lowercase scheme": {headers: []string{"bearer " + testToken}, reviewer: &reviewerStub{result: validReviewResult(testCellKey)}},
		"leading space":    {headers: []string{" Bearer " + testToken}, reviewer: &reviewerStub{result: validReviewResult(testCellKey)}},
		"embedded space":   {headers: []string{"Bearer header.pay load.signature"}, reviewer: &reviewerStub{result: validReviewResult(testCellKey)}},
		"multiple":         {headers: []string{"Bearer " + testToken, "Bearer other.payload.signature"}, reviewer: &reviewerStub{result: validReviewResult(testCellKey)}},
		"opaque":           {headers: []string{"Bearer tenant-api-key"}, reviewer: &reviewerStub{result: validReviewResult(testCellKey)}},
		"foreign account":  {headers: []string{"Bearer " + testToken}, reviewer: &reviewerStub{result: invalidReview}},
	}
	var canonicalBody string
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			middleware, err := New(test.reviewer, func() time.Time { return now })
			if err != nil {
				t.Fatalf("create materializer HTTP identity: %v", err)
			}
			handler := middleware.RequireGET(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("invalid materializer identity reached downstream handler")
			}))
			request := httptest.NewRequest(http.MethodGet, "/future-private-bundle", nil)
			for _, header := range test.headers {
				request.Header.Add("Authorization", header)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized || recorder.Header().Get("WWW-Authenticate") != materializerRealm {
				t.Fatalf("invalid identity status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
			}
			assertPrivateResponse(t, recorder)
			if canonicalBody == "" {
				canonicalBody = recorder.Body.String()
			}
			if recorder.Body.String() != canonicalBody || strings.Contains(recorder.Body.String(), testToken) {
				t.Fatalf("invalid identity response became an oracle: %q want=%q", recorder.Body.String(), canonicalBody)
			}
			wantCalls := 0
			if name == "foreign account" {
				wantCalls = 1
			}
			if test.reviewer.calls != wantCalls {
				t.Fatalf("invalid identity reviewer calls=%d want=%d", test.reviewer.calls, wantCalls)
			}
		})
	}
}

func TestRequireGETDistinguishesReviewerOutageWithoutDetails(t *testing.T) {
	t.Parallel()
	reviewer := &reviewerStub{err: errors.New("remote detail contained " + testToken)}
	middleware, err := New(reviewer, func() time.Time { return time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatalf("create materializer HTTP identity: %v", err)
	}
	handler := middleware.RequireGET(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("reviewer outage reached downstream handler")
	}))
	request := httptest.NewRequest(http.MethodGet, "/future-private-bundle", nil)
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" ||
		strings.Contains(recorder.Body.String(), testToken) || strings.Contains(recorder.Body.String(), "remote detail") {
		t.Fatalf("reviewer outage response drifted: status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	assertPrivateResponse(t, recorder)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	reviewer = &reviewerStub{result: validReviewResult(testCellKey)}
	middleware, _ = New(reviewer, time.Now)
	handler = middleware.RequireGET(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("canceled request reached downstream handler")
	}))
	request = httptest.NewRequest(http.MethodGet, "/future-private-bundle", nil).WithContext(canceled)
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || reviewer.calls != 0 {
		t.Fatalf("canceled review status=%d calls=%d body=%s", recorder.Code, reviewer.calls, recorder.Body.String())
	}
}

func TestRequireGETRejectsMethodsBeforeReview(t *testing.T) {
	t.Parallel()
	for _, method := range []string{http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			reviewer := &reviewerStub{result: validReviewResult(testCellKey)}
			middleware, _ := New(reviewer, time.Now)
			handler := middleware.RequireGET(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("non-GET request reached downstream handler")
			}))
			request := httptest.NewRequest(method, "/future-private-bundle", nil)
			request.Header.Set("Authorization", "Bearer "+testToken)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet || reviewer.calls != 0 {
				t.Fatalf("method=%s status=%d allow=%q reviewerCalls=%d", method, recorder.Code, recorder.Header().Get("Allow"), reviewer.calls)
			}
			assertPrivateResponse(t, recorder)
		})
	}
}

func TestHTTPIdentityConfigurationFailsClosed(t *testing.T) {
	t.Parallel()
	if _, err := New(nil, time.Now); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil reviewer config error = %v, want ErrConfig", err)
	}
	var typedNilReviewer *reviewerStub
	if _, err := New(typedNilReviewer, time.Now); !errors.Is(err, ErrConfig) {
		t.Fatalf("typed nil reviewer config error = %v, want ErrConfig", err)
	}
	if _, err := New(&reviewerStub{}, nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil clock config error = %v, want ErrConfig", err)
	}
	reviewer := &reviewerStub{result: validReviewResult(testCellKey)}
	middleware, _ := New(reviewer, func() time.Time { return time.Time{} })
	validMiddleware, _ := New(reviewer, func() time.Time { return time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC) })
	var typedNilDownstream *downstreamStub
	for name, handler := range map[string]http.Handler{
		"zero clock": middleware.RequireGET(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("zero clock reached downstream handler")
		})),
		"nil downstream":       validMiddleware.RequireGET(nil),
		"typed nil downstream": validMiddleware.RequireGET(typedNilDownstream),
		"nil middleware": (*Middleware)(nil).RequireGET(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("nil middleware reached downstream handler")
		})),
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/future-private-bundle", nil)
			request.Header.Set("Authorization", "Bearer "+testToken)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" {
				t.Fatalf("configuration failure status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
			}
			assertPrivateResponse(t, recorder)
		})
	}
	if reviewer.calls != 0 {
		t.Fatalf("invalid middleware configuration invoked reviewer %d time(s)", reviewer.calls)
	}
}

func TestHTTPIdentityDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list HTTP identity dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	if !reflect.DeepEqual(local, []string{"fugue/internal/backupmaterializeridentity"}) {
		t.Fatalf("HTTP identity dependency boundary widened: %v", local)
	}
	for _, forbidden := range []string{"k8s.io/", "database/sql", "os", "os/exec"} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("HTTP identity gained forbidden dependency %q", forbidden)
		}
	}
}

func validReviewResult(cellKey string) backupmaterializeridentity.ReviewResult {
	serviceAccountName := backupmaterializeridentity.ServiceAccountNameForCell(cellKey)
	return backupmaterializeridentity.ReviewResult{
		Authenticated: true,
		Audiences:     []string{backupmaterializeridentity.Audience},
		Username: "system:serviceaccount:" + backupmaterializeridentity.ServiceAccountNamespace + ":" +
			serviceAccountName,
		UID: "11111111-1111-4111-8111-111111111111",
		Groups: []string{
			"system:serviceaccounts",
			"system:serviceaccounts:" + backupmaterializeridentity.ServiceAccountNamespace,
			"system:authenticated",
		},
		Extra: map[string][]string{
			"authentication.kubernetes.io/credential-id": {"JTI=33333333-3333-4333-8333-333333333333"},
			"authentication.kubernetes.io/pod-name":      {serviceAccountName + "-6d4f7c8b9f-abcde"},
			"authentication.kubernetes.io/pod-uid":       {"22222222-2222-4222-8222-222222222222"},
		},
	}
}

func assertPrivateResponse(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Header().Get("Cache-Control") != privateCacheControl || recorder.Header().Get("Pragma") != "no-cache" ||
		recorder.Header().Get("X-Content-Type-Options") != "nosniff" || !headerContainsToken(recorder.Header().Values("Vary"), "Authorization") {
		t.Fatalf("materializer HTTP identity response is not private: %v", recorder.Header())
	}
	if recorder.Code < http.StatusOK || recorder.Code >= http.StatusMultipleChoices {
		var response errorResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Code == "" ||
			recorder.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("materializer HTTP identity error is not strict JSON: response=%+v err=%v body=%q", response, err, recorder.Body.String())
		}
	}
}

func headerContainsToken(values []string, token string) bool {
	for _, value := range values {
		for _, field := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(field), token) {
				return true
			}
		}
	}
	return false
}
