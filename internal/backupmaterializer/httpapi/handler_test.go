package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	"fugue/internal/backupmaterializeridentity"
	"fugue/internal/backupmaterializeridentity/httpauth"
)

const testWorkloadToken = "test-header.test-materializer-payload.test-materializer-signature"

type sourceStub struct {
	input DesiredInput
	err   error
	calls int
	read  ReadRequest
}

func (source *sourceStub) ReadDesiredInput(_ context.Context, request ReadRequest) (DesiredInput, error) {
	source.calls++
	source.read = request
	return source.input, source.err
}

type issuerStub struct {
	keyring backupidentity.Keyring
	err     error
	mutate  func(*backupmaterializer.ObserverInputBundle)
	calls   int
	spec    backupcontrol.BackupRunSpec
	tenant  string
	now     time.Time
}

func (issuer *issuerStub) IssueObserverInputBundle(
	_ context.Context,
	spec backupcontrol.BackupRunSpec,
	tenantID string,
	now time.Time,
) (backupmaterializer.ObserverInputBundle, error) {
	issuer.calls++
	issuer.spec = spec
	issuer.tenant = tenantID
	issuer.now = now
	if issuer.err != nil {
		return backupmaterializer.ObserverInputBundle{}, issuer.err
	}
	bundle, err := backupmaterializer.IssueObserverInputBundle(issuer.keyring, spec, tenantID, now)
	if err != nil {
		return backupmaterializer.ObserverInputBundle{}, err
	}
	if issuer.mutate != nil {
		issuer.mutate(&bundle)
		bundle.Digest = backupmaterializer.DigestObserverInputBundle(bundle)
	}
	return bundle, nil
}

type reviewerStub struct {
	result backupmaterializeridentity.ReviewResult
	err    error
	calls  int
}

func (reviewer *reviewerStub) ReviewToken(
	_ context.Context,
	_ string,
	_ []string,
) (backupmaterializeridentity.ReviewResult, error) {
	reviewer.calls++
	return reviewer.result, reviewer.err
}

func TestObserverInputBundleHTTPBoundaryIssuesOnlyAfterCellBinding(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 14, 0, 0, 123, time.UTC)
	spec := testDesiredSpec(t)
	keyring := testIssuerKeyring()
	source := &sourceStub{input: DesiredInput{Spec: spec, TenantID: "tenant-1"}}
	issuer := &issuerStub{keyring: keyring}
	reviewer := &reviewerStub{result: validReviewResult(spec.CellKey)}
	handler := authenticatedHandler(t, source, issuer, reviewer, func() time.Time { return now })

	request := validRequest(spec.RunID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || source.calls != 1 || source.read != (ReadRequest{RunID: spec.RunID, CellKey: spec.CellKey}) || issuer.calls != 1 || reviewer.calls != 1 {
		t.Fatalf("bundle request status=%d source=%d/%+v issuer=%d reviewer=%d body=%s", recorder.Code, source.calls, source.read, issuer.calls, reviewer.calls, recorder.Body.String())
	}
	if issuer.spec != spec || issuer.tenant != "tenant-1" || issuer.now != now {
		t.Fatalf("issuer input escaped validated source: spec=%+v tenant=%q now=%s", issuer.spec, issuer.tenant, issuer.now)
	}
	assertPrivateResponse(t, recorder)
	if recorder.Header().Get("Content-Type") != "application/json" || recorder.Header().Get("Content-Length") != strconv.Itoa(recorder.Body.Len()) {
		t.Fatalf("bundle response framing drifted: headers=%v bytes=%d", recorder.Header(), recorder.Body.Len())
	}
	bundle, err := backupmaterializer.DecodeObserverInputBundle(recorder.Body.Bytes(), keyring, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("decode issued observer input bundle: %v body=%s", err, recorder.Body.String())
	}
	if bundle.CellKey != spec.CellKey || bundle.RunID != spec.RunID || bundle.SpecDigest != spec.Digest ||
		bundle.DesiredSpec != spec || !strings.HasPrefix(bundle.ObserverToken, "fugue_bo_v1.") {
		t.Fatalf("issued bundle drifted: %#v", bundle)
	}
	if strings.Contains(recorder.Body.String(), testWorkloadToken) {
		t.Fatal("bundle response retained the materializer workload credential")
	}
}

func TestObserverInputBundleHTTPBoundaryHidesForeignAndMissingRuns(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 14, 5, 0, 0, time.UTC)
	spec := testDesiredSpec(t)
	tests := map[string]struct {
		source     *sourceStub
		cellKey    string
		wantSource int
		wantIssuer int
	}{
		"missing": {
			source:  &sourceStub{err: fmt.Errorf("%w: private backend name", ErrInputNotFound)},
			cellKey: spec.CellKey, wantSource: 1,
		},
		"foreign cell": {
			source:  &sourceStub{input: DesiredInput{Spec: spec, TenantID: "tenant-1"}},
			cellKey: "backup/registry/ffffffffffffffff", wantSource: 1,
		},
	}
	var canonicalBody string
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			issuer := &issuerStub{keyring: testIssuerKeyring()}
			reviewer := &reviewerStub{result: validReviewResult(test.cellKey)}
			handler := authenticatedHandler(t, test.source, issuer, reviewer, func() time.Time { return now })
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, validRequest(spec.RunID))
			if recorder.Code != http.StatusNotFound || test.source.calls != test.wantSource || issuer.calls != test.wantIssuer {
				t.Fatalf("hidden run status=%d source=%d issuer=%d body=%s", recorder.Code, test.source.calls, issuer.calls, recorder.Body.String())
			}
			assertPrivateResponse(t, recorder)
			if canonicalBody == "" {
				canonicalBody = recorder.Body.String()
			}
			if recorder.Body.String() != canonicalBody || strings.Contains(recorder.Body.String(), "private backend") {
				t.Fatalf("foreign/missing response became an oracle: got=%q want=%q", recorder.Body.String(), canonicalBody)
			}
		})
	}
}

func TestObserverInputBundleHTTPBoundaryRejectsAmbiguousRequestsBeforeSource(t *testing.T) {
	t.Parallel()
	spec := testDesiredSpec(t)
	tests := map[string]func(*http.Request){
		"missing run":   func(request *http.Request) { request.SetPathValue("run", "") },
		"uppercase run": func(request *http.Request) { request.SetPathValue("run", "Run-1") },
		"query":         func(request *http.Request) { request.URL.RawQuery = "spec_digest=" + spec.Digest },
		"other path":    func(request *http.Request) { request.URL.Path = "/other" },
		"encoded path": func(request *http.Request) {
			request.URL.RawPath = strings.Replace(request.URL.Path, "run-materializer-1", "run-materializer%2D1", 1)
		},
		"body": func(request *http.Request) {
			request.Body = http.NoBody
			request.ContentLength = 1
		},
		"transfer encoding": func(request *http.Request) { request.TransferEncoding = []string{"chunked"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			source := &sourceStub{input: DesiredInput{Spec: spec, TenantID: "tenant-1"}}
			issuer := &issuerStub{keyring: testIssuerKeyring()}
			reviewer := &reviewerStub{result: validReviewResult(spec.CellKey)}
			handler := authenticatedHandler(t, source, issuer, reviewer, time.Now)
			request := validRequest(spec.RunID)
			mutate(request)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || source.calls != 0 || issuer.calls != 0 || reviewer.calls != 1 {
				t.Fatalf("ambiguous request status=%d source=%d issuer=%d reviewer=%d body=%s", recorder.Code, source.calls, issuer.calls, reviewer.calls, recorder.Body.String())
			}
			assertPrivateResponse(t, recorder)
		})
	}
}

func TestObserverInputBundleHTTPBoundaryRejectsNonGETBeforeReview(t *testing.T) {
	t.Parallel()
	spec := testDesiredSpec(t)
	for _, method := range []string{http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			source := &sourceStub{input: DesiredInput{Spec: spec, TenantID: "tenant-1"}}
			issuer := &issuerStub{keyring: testIssuerKeyring()}
			reviewer := &reviewerStub{result: validReviewResult(spec.CellKey)}
			handler := authenticatedHandler(t, source, issuer, reviewer, time.Now)
			request := validRequest(spec.RunID)
			request.Method = method
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet ||
				reviewer.calls != 0 || source.calls != 0 || issuer.calls != 0 {
				t.Fatalf("method=%s status=%d allow=%q reviewer=%d source=%d issuer=%d", method, recorder.Code, recorder.Header().Get("Allow"), reviewer.calls, source.calls, issuer.calls)
			}
			assertPrivateResponse(t, recorder)
		})
	}
}

func TestObserverInputBundleHTTPBoundaryFailsClosedOnSourceAndIssuerDrift(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 14, 10, 0, 0, time.UTC)
	spec := testDesiredSpec(t)
	driftedSpec := spec
	driftedSpec.Digest = "sha256:" + strings.Repeat("f", 64)
	invalidInputs := map[string]DesiredInput{
		"spec digest": {Spec: driftedSpec, TenantID: "tenant-1"},
		"wrong run": func() DesiredInput {
			value := DesiredInput{Spec: spec, TenantID: "tenant-1"}
			value.Spec.RunID = "other-run"
			return value
		}(),
		"tenant whitespace": {Spec: spec, TenantID: " tenant-1"},
		"tenant syntax":     {Spec: spec, TenantID: "tenant/1"},
	}
	for name, input := range invalidInputs {
		t.Run("source "+name, func(t *testing.T) {
			source := &sourceStub{input: input}
			issuer := &issuerStub{keyring: testIssuerKeyring()}
			handler := authenticatedHandler(t, source, issuer, &reviewerStub{result: validReviewResult(spec.CellKey)}, func() time.Time { return now })
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, validRequest(spec.RunID))
			if recorder.Code != http.StatusConflict || source.calls != 1 || issuer.calls != 0 {
				t.Fatalf("invalid source status=%d source=%d issuer=%d body=%s", recorder.Code, source.calls, issuer.calls, recorder.Body.String())
			}
		})
	}
	issuerMutations := map[string]func(*backupmaterializer.ObserverInputBundle){
		"cell": func(bundle *backupmaterializer.ObserverInputBundle) {
			bundle.CellKey = "backup/registry/ffffffffffffffff"
		},
		"run": func(bundle *backupmaterializer.ObserverInputBundle) { bundle.RunID = "other-run" },
		"time": func(bundle *backupmaterializer.ObserverInputBundle) {
			bundle.IssuedAt = bundle.IssuedAt.Add(time.Second)
		},
		"timezone": func(bundle *backupmaterializer.ObserverInputBundle) {
			bundle.IssuedAt = bundle.IssuedAt.In(time.FixedZone("offset", 3600))
		},
		"credential": func(bundle *backupmaterializer.ObserverInputBundle) {
			bundle.CredentialID = "backup-observer:other"
		},
		"token id": func(bundle *backupmaterializer.ObserverInputBundle) {
			bundle.TokenID = strings.Repeat("x", 21)
		},
		"token": func(bundle *backupmaterializer.ObserverInputBundle) {
			bundle.ObserverToken = "wrong-domain.value.signature"
		},
	}
	for name, mutate := range issuerMutations {
		t.Run("issuer "+name, func(t *testing.T) {
			source := &sourceStub{input: DesiredInput{Spec: spec, TenantID: "tenant-1"}}
			issuer := &issuerStub{keyring: testIssuerKeyring(), mutate: mutate}
			handler := authenticatedHandler(t, source, issuer, &reviewerStub{result: validReviewResult(spec.CellKey)}, func() time.Time { return now })
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, validRequest(spec.RunID))
			if recorder.Code != http.StatusServiceUnavailable || source.calls != 1 || issuer.calls != 1 || recorder.Header().Get("Retry-After") != "1" {
				t.Fatalf("invalid issuer status=%d source=%d issuer=%d headers=%v body=%s", recorder.Code, source.calls, issuer.calls, recorder.Header(), recorder.Body.String())
			}
		})
	}
}

func TestObserverInputBundleHTTPBoundaryMapsFailuresWithoutDetails(t *testing.T) {
	t.Parallel()
	spec := testDesiredSpec(t)
	tests := map[string]struct {
		sourceErr error
		issuerErr error
		status    int
		code      string
	}{
		"source conflict":    {sourceErr: fmt.Errorf("%w: secret conflict detail", ErrInputConflict), status: http.StatusConflict, code: "input_inconsistent"},
		"source unavailable": {sourceErr: fmt.Errorf("%w: database DSN detail", ErrInputUnavailable), status: http.StatusServiceUnavailable, code: "input_unavailable"},
		"unknown source":     {sourceErr: errors.New("private bucket detail"), status: http.StatusServiceUnavailable, code: "input_unavailable"},
		"issuer unavailable": {issuerErr: errors.New("signing key detail"), status: http.StatusServiceUnavailable, code: "input_unavailable"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			source := &sourceStub{input: DesiredInput{Spec: spec, TenantID: "tenant-1"}, err: test.sourceErr}
			issuer := &issuerStub{keyring: testIssuerKeyring(), err: test.issuerErr}
			handler := authenticatedHandler(t, source, issuer, &reviewerStub{result: validReviewResult(spec.CellKey)}, time.Now)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, validRequest(spec.RunID))
			var response errorResponse
			if recorder.Code != test.status || json.Unmarshal(recorder.Body.Bytes(), &response) != nil || response.Code != test.code {
				t.Fatalf("failure status=%d response=%+v body=%s", recorder.Code, response, recorder.Body.String())
			}
			for _, forbidden := range []string{"secret conflict", "database DSN", "private bucket", "signing key"} {
				if strings.Contains(recorder.Body.String(), forbidden) {
					t.Fatalf("failure response leaked %q: %s", forbidden, recorder.Body.String())
				}
			}
			if test.status == http.StatusServiceUnavailable && recorder.Header().Get("Retry-After") != "1" {
				t.Fatalf("unavailable response lacks retry bound: %v", recorder.Header())
			}
		})
	}
}

func TestObserverInputBundleHTTPBoundaryConfigurationAndDirectUseFailClosed(t *testing.T) {
	t.Parallel()
	var typedNilSource *sourceStub
	var typedNilIssuer *issuerStub
	validSource := &sourceStub{}
	validIssuer := &issuerStub{}
	for name, construct := range map[string]func() error{
		"nil source":       func() error { _, err := New(nil, validIssuer, time.Now); return err },
		"typed nil source": func() error { _, err := New(typedNilSource, validIssuer, time.Now); return err },
		"nil issuer":       func() error { _, err := New(validSource, nil, time.Now); return err },
		"typed nil issuer": func() error { _, err := New(validSource, typedNilIssuer, time.Now); return err },
		"nil clock":        func() error { _, err := New(validSource, validIssuer, nil); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := construct(); !errors.Is(err, ErrConfig) {
				t.Fatalf("config error=%v, want ErrConfig", err)
			}
		})
	}

	spec := testDesiredSpec(t)
	source := &sourceStub{input: DesiredInput{Spec: spec, TenantID: "tenant-1"}}
	issuer := &issuerStub{keyring: testIssuerKeyring()}
	handler, _ := New(source, issuer, time.Now)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, validRequest(spec.RunID))
	if recorder.Code != http.StatusUnauthorized || source.calls != 0 || issuer.calls != 0 {
		t.Fatalf("unwrapped handler status=%d source=%d issuer=%d body=%s", recorder.Code, source.calls, issuer.calls, recorder.Body.String())
	}

	reviewer := &reviewerStub{result: validReviewResult(spec.CellKey)}
	zeroClock, _ := New(source, issuer, func() time.Time { return time.Time{} })
	middleware, _ := httpauth.New(reviewer, time.Now)
	recorder = httptest.NewRecorder()
	middleware.RequireGET(zeroClock).ServeHTTP(recorder, validRequest(spec.RunID))
	if recorder.Code != http.StatusServiceUnavailable || source.calls != 0 || issuer.calls != 0 {
		t.Fatalf("zero-clock handler status=%d source=%d issuer=%d body=%s", recorder.Code, source.calls, issuer.calls, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	(*Handler)(nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, RoutePath, nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil handler status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestObserverInputBundleHTTPDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list input HTTP dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupidentity",
		"fugue/internal/backupmaterializer",
		"fugue/internal/backupmaterializeridentity",
		"fugue/internal/backupmaterializeridentity/httpauth",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("input HTTP dependency boundary widened: got=%v want=%v", local, want)
	}
	for _, forbidden := range []string{"fugue/internal/api", "fugue/internal/store", "fugue/internal/model", "database/sql", "k8s.io/", "os", "os/exec"} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("input HTTP boundary gained forbidden dependency %q", forbidden)
		}
	}
}

func authenticatedHandler(
	t *testing.T,
	source Source,
	issuer Issuer,
	reviewer backupmaterializeridentity.TokenReviewer,
	now func() time.Time,
) http.Handler {
	t.Helper()
	handler, err := New(source, issuer, now)
	if err != nil {
		t.Fatalf("create input bundle handler: %v", err)
	}
	middleware, err := httpauth.New(reviewer, now)
	if err != nil {
		t.Fatalf("create materializer HTTP identity: %v", err)
	}
	return middleware.RequireGET(handler)
}

func validRequest(runID string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, strings.Replace(RoutePath, "{run}", runID, 1), nil)
	request.SetPathValue("run", runID)
	request.Header.Set("Authorization", "Bearer "+testWorkloadToken)
	return request
}

func testDesiredSpec(t *testing.T) backupcontrol.BackupRunSpec {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		"run-materializer-1",
		"run-materializer-1",
		backupcontrol.BackupTarget{Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry"},
		"backend-1",
		"sha256:"+strings.Repeat("a", 64),
		4,
		120,
		1800,
	)
	if err != nil {
		t.Fatalf("build desired input spec: %v", err)
	}
	return spec
}

func testIssuerKeyring() backupidentity.Keyring {
	return backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
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
	if recorder.Header().Get("Cache-Control") != "private, no-store, max-age=0" ||
		recorder.Header().Get("Pragma") != "no-cache" || recorder.Header().Get("X-Content-Type-Options") != "nosniff" ||
		!headerContainsToken(recorder.Header().Values("Vary"), "Authorization") {
		t.Fatalf("input bundle response is not private: %v", recorder.Header())
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
