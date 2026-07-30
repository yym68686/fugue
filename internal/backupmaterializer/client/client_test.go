package client

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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	materializercontract "fugue/internal/backupmaterializer/contract"
	materializerhttpapi "fugue/internal/backupmaterializer/httpapi"
)

const (
	testWorkloadToken        = "test-header.test-materializer-payload.test-signature"
	testRotatedWorkloadToken = "rotated-header.rotated-materializer-payload.rotated-signature"
)

type testCredentialSource struct {
	mu     sync.Mutex
	tokens []string
	err    error
	calls  int
}

func (source *testCredentialSource) Credential(context.Context) (string, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.calls++
	if source.err != nil {
		return "", source.err
	}
	index := source.calls - 1
	if index >= len(source.tokens) {
		index = len(source.tokens) - 1
	}
	if index < 0 {
		return "", nil
	}
	return source.tokens[index], nil
}

func (source *testCredentialSource) callCount() int {
	if source == nil {
		return 0
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.calls
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type deadlineCredentialSource struct {
	mu       sync.Mutex
	deadline time.Time
	ok       bool
}

func (source *deadlineCredentialSource) Credential(ctx context.Context) (string, error) {
	source.mu.Lock()
	source.deadline, source.ok = ctx.Deadline()
	source.mu.Unlock()
	return testWorkloadToken, nil
}

func (source *deadlineCredentialSource) snapshot() (time.Time, bool) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.deadline, source.ok
}

func TestClientIsInertAndRetainsNoCapabilitiesWhileDisabled(t *testing.T) {
	t.Parallel()
	var networkCalls atomic.Int64
	var typedNilSource *testCredentialSource
	client, err := New(Config{
		Enabled:          false,
		BaseURL:          "not a URL",
		ExpectedCellKey:  "not a cell",
		ExpectedRunID:    "not a run",
		CredentialSource: typedNilSource,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			networkCalls.Add(1)
			return nil, errors.New("unexpected network access")
		})},
		Now: func() time.Time { panic("disabled client read the clock") },
	})
	if err != nil {
		t.Fatalf("create disabled client: %v", err)
	}
	if client.Enabled() {
		t.Fatal("disabled client reported enabled")
	}
	if _, err := client.Fetch(nil); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled fetch error = %v, want disabled", err)
	}
	if typedNilSource.callCount() != 0 || networkCalls.Load() != 0 {
		t.Fatalf("disabled client touched a capability: credential=%d network=%d", typedNilSource.callCount(), networkCalls.Load())
	}
	for _, rendered := range []string{fmt.Sprint(client), fmt.Sprintf("%#v", client)} {
		if strings.Contains(rendered, "not a URL") || !strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("disabled client formatting exposed retained configuration: %q", rendered)
		}
	}
}

func TestClientRouteMatchesTheRegisteredMaterializerBoundary(t *testing.T) {
	t.Parallel()
	if got := observerInputRouteHead + "{run}" + observerInputRouteTail; got != materializerhttpapi.RoutePath {
		t.Fatalf("materializer client route = %q, registered route = %q", got, materializerhttpapi.RoutePath)
	}
}

func TestClientFetchesExactPrivateBundleAndRereadsWorkloadCredential(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	keyring := testKeyring()
	spec := testSpec(t, "run-1", backupcontrol.TargetAppDatabase, "app/app-1/database")
	source := &testCredentialSource{tokens: []string{testWorkloadToken, testRotatedWorkloadToken}}
	servedBundle := issueBundle(t, keyring, spec, now)
	servedDocument := encodeBundle(t, servedBundle)
	var mu sync.Mutex
	authorizations := []string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		mu.Unlock()
		if request.Method != http.MethodGet || request.URL.RequestURI() != observerInputRouteHead+spec.RunID+observerInputRouteTail ||
			request.ContentLength != 0 || request.Header.Get("Accept") != "application/json" ||
			request.Header.Get("Accept-Encoding") != "identity" || request.Header.Get("Cache-Control") != "no-store" ||
			request.Header.Get("Pragma") != "no-cache" {
			t.Errorf("materializer request boundary drifted: method=%s uri=%s contentLength=%d headers=%v", request.Method, request.URL.RequestURI(), request.ContentLength, request.Header)
		}
		writePrivateDocument(writer, servedDocument)
	}))
	defer server.Close()
	client, err := New(Config{
		Enabled: true, BaseURL: server.URL, ExpectedCellKey: spec.CellKey, ExpectedRunID: spec.RunID,
		CredentialSource: source, HTTPClient: server.Client(), Now: func() time.Time { return now.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("create materializer client: %v", err)
	}
	var last backupmaterializer.ObserverInputBundle
	for range 2 {
		last, err = client.Fetch(context.Background())
		if err != nil {
			t.Fatalf("fetch materializer bundle: %v", err)
		}
		if last.CellKey != spec.CellKey || last.RunID != spec.RunID || last.DesiredSpec != spec ||
			last.ObserverToken == "" || backupmaterializer.ValidateObserverInputBundle(last, keyring, now.Add(time.Minute)) != nil {
			t.Fatalf("fetched bundle drifted: %#v", last)
		}
	}
	mu.Lock()
	wantAuthorizations := []string{"Bearer " + testWorkloadToken, "Bearer " + testRotatedWorkloadToken}
	if !reflect.DeepEqual(authorizations, wantAuthorizations) || source.callCount() != 2 {
		t.Fatalf("workload credential did not rotate: auth=%v calls=%d", authorizations, source.callCount())
	}
	mu.Unlock()
	for _, rendered := range []string{fmt.Sprint(client), fmt.Sprintf("%#v", client)} {
		if strings.Contains(rendered, testWorkloadToken) || strings.Contains(rendered, last.ObserverToken) ||
			strings.Contains(rendered, server.URL) || !strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("client formatting exposed private state: %q", rendered)
		}
	}
}

func TestClientUsesOneBoundedDeadlineForCredentialAndHTTP(t *testing.T) {
	t.Parallel()
	spec := testSpec(t, "run-1", backupcontrol.TargetAppDatabase, "app/app-1/database")
	source := &deadlineCredentialSource{}
	var requestDeadline time.Time
	client, err := New(Config{
		Enabled: true, BaseURL: "https://api.example.test", ExpectedCellKey: spec.CellKey, ExpectedRunID: spec.RunID,
		CredentialSource: source,
		RequestTimeout:   time.Second,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requestDeadline, _ = request.Context().Deadline()
			return nil, errors.New("synthetic transport outage")
		})},
	})
	if err != nil {
		t.Fatalf("create deadline client: %v", err)
	}
	started := time.Now()
	if _, err := client.Fetch(context.Background()); !errors.Is(err, ErrInputUnavailable) {
		t.Fatalf("bounded fetch error = %v, want unavailable", err)
	}
	credentialDeadline, ok := source.snapshot()
	if !ok || credentialDeadline.IsZero() || requestDeadline.IsZero() || !credentialDeadline.Equal(requestDeadline) ||
		credentialDeadline.Before(started) || credentialDeadline.After(started.Add(2*time.Second)) {
		t.Fatalf("credential and HTTP did not share one bounded deadline: credential=%v ok=%t request=%v", credentialDeadline, ok, requestDeadline)
	}
}

func TestClientRejectsResponseMetadataContractAndBindingDrift(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	keyring := testKeyring()
	spec := testSpec(t, "run-1", backupcontrol.TargetAppDatabase, "app/app-1/database")
	validBundle := issueBundle(t, keyring, spec, now)
	validDocument := encodeBundle(t, validBundle)

	unknown := append([]byte(nil), validDocument[:len(validDocument)-1]...)
	unknown = append(unknown, []byte(`,"unexpected":true}`)...)
	otherCell := issueBundle(t, keyring, testSpec(t, "run-1", backupcontrol.TargetRegistry, "platform/registry"), now)
	otherRun := issueBundle(t, keyring, testSpec(t, "other-run", backupcontrol.TargetAppDatabase, "app/app-1/database"), now)
	replayed := issueBundle(t, keyring, spec, now.Add(-2*materializercontract.MaxObserverInputDeliveryAge))
	staleRenewal := issueBundle(t, keyring, spec, now.Add(-backupmaterializer.ObserverIdentityRenewAfter))
	future := issueBundle(t, keyring, spec, now.Add(backupidentity.FutureSkew+time.Second))
	claimDrift := validBundle
	claimDrift.TokenID = strings.Repeat("A", 22)
	claimDrift.Digest = backupmaterializer.DigestObserverInputBundle(claimDrift)

	tests := map[string]struct {
		document []byte
		headers  func(http.Header)
	}{
		"wrong content type":   {document: validDocument, headers: func(header http.Header) { header.Set("Content-Type", "text/plain") }},
		"missing private":      {document: validDocument, headers: func(header http.Header) { header.Set("Cache-Control", "no-store") }},
		"missing no-store":     {document: validDocument, headers: func(header http.Header) { header.Set("Cache-Control", "private") }},
		"public cache policy":  {document: validDocument, headers: func(header http.Header) { header.Set("Cache-Control", "private, no-store, public") }},
		"valued no-store":      {document: validDocument, headers: func(header http.Header) { header.Set("Cache-Control", "private, no-store=true") }},
		"positive max age":     {document: validDocument, headers: func(header http.Header) { header.Set("Cache-Control", "private, no-store, max-age=60") }},
		"missing pragma":       {document: validDocument, headers: func(header http.Header) { header.Del("Pragma") }},
		"missing vary":         {document: validDocument, headers: func(header http.Header) { header.Del("Vary") }},
		"missing nosniff":      {document: validDocument, headers: func(header http.Header) { header.Del("X-Content-Type-Options") }},
		"encoded response":     {document: validDocument, headers: func(header http.Header) { header.Set("Content-Encoding", "gzip") }},
		"empty":                {document: nil},
		"unknown field":        {document: unknown},
		"trailing document":    {document: append(append([]byte(nil), validDocument...), []byte(` {}`)...)},
		"oversized":            {document: []byte(strings.Repeat("x", backupmaterializer.MaxObserverInputBundleBytes+1))},
		"foreign cell":         {document: encodeBundle(t, otherCell)},
		"foreign run":          {document: encodeBundle(t, otherRun)},
		"replayed generation":  {document: encodeBundle(t, replayed)},
		"renewal already due":  {document: encodeBundle(t, staleRenewal)},
		"future issue time":    {document: encodeBundle(t, future)},
		"token claim mismatch": {document: encodeBundle(t, claimDrift)},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				setPrivateResponseHeaders(writer.Header())
				if test.headers != nil {
					test.headers(writer.Header())
				}
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write(test.document)
			}))
			defer server.Close()
			client, err := New(Config{
				Enabled: true, BaseURL: server.URL, ExpectedCellKey: spec.CellKey, ExpectedRunID: spec.RunID,
				CredentialSource: &testCredentialSource{tokens: []string{testWorkloadToken}},
				HTTPClient:       server.Client(), Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatalf("create materializer client: %v", err)
			}
			_, err = client.Fetch(context.Background())
			assertSecretFreeError(t, err, ErrInputResponse, testWorkloadToken, validBundle.ObserverToken)
		})
	}
}

func TestClientMapsOnlyFixedStatusOutcomesAndDiscardsRemoteBodies(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	spec := testSpec(t, "run-1", backupcontrol.TargetAppDatabase, "app/app-1/database")
	observerToken := issueBundle(t, testKeyring(), spec, now).ObserverToken
	for name, test := range map[string]struct {
		status int
		want   error
	}{
		"bad request":  {status: http.StatusBadRequest, want: ErrInputRejected},
		"unauthorized": {status: http.StatusUnauthorized, want: ErrInputRejected},
		"not found":    {status: http.StatusNotFound, want: ErrInputNotFound},
		"conflict":     {status: http.StatusConflict, want: ErrInputConflict},
		"unavailable":  {status: http.StatusServiceUnavailable, want: ErrInputUnavailable},
		"unexpected":   {status: http.StatusTeapot, want: ErrInputUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(test.status)
				_, _ = writer.Write([]byte("remote detail " + request.Header.Get("Authorization") + " " + observerToken))
			}))
			defer server.Close()
			client, err := New(Config{
				Enabled: true, BaseURL: server.URL, ExpectedCellKey: spec.CellKey, ExpectedRunID: spec.RunID,
				CredentialSource: &testCredentialSource{tokens: []string{testWorkloadToken}}, HTTPClient: server.Client(),
			})
			if err != nil {
				t.Fatalf("create materializer client: %v", err)
			}
			_, err = client.Fetch(context.Background())
			assertSecretFreeError(t, err, test.want, testWorkloadToken, observerToken, "remote detail")
		})
	}
}

func TestClientRejectsRedirectCancellationAndCredentialFailureBeforeUnsafeWork(t *testing.T) {
	t.Parallel()
	spec := testSpec(t, "run-1", backupcontrol.TargetAppDatabase, "app/app-1/database")
	var redirectTargetCalled atomic.Bool
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectTargetCalled.Store(true)
	}))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL)
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	redirectClient, err := New(Config{
		Enabled: true, BaseURL: redirect.URL, ExpectedCellKey: spec.CellKey, ExpectedRunID: spec.RunID,
		CredentialSource: &testCredentialSource{tokens: []string{testWorkloadToken}}, HTTPClient: redirect.Client(),
	})
	if err != nil {
		t.Fatalf("create redirect client: %v", err)
	}
	if _, err := redirectClient.Fetch(context.Background()); !errors.Is(err, ErrInputUnavailable) {
		t.Fatalf("redirect error = %v, want unavailable", err)
	}
	if redirectTargetCalled.Load() {
		t.Fatal("redirect forwarded the materializer workload credential")
	}

	source := &testCredentialSource{tokens: []string{testWorkloadToken}}
	client, err := New(baseTestConfig(spec, source))
	if err != nil {
		t.Fatalf("create cancellation client: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Fetch(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fetch error = %v, want context canceled", err)
	}
	if source.callCount() != 0 {
		t.Fatalf("canceled fetch read the credential %d time(s)", source.callCount())
	}

	for name, failureSource := range map[string]*testCredentialSource{
		"source error": {err: errors.New("credential body " + testWorkloadToken)},
		"empty token":  {tokens: []string{""}},
		"opaque token": {tokens: []string{"opaque"}},
		"line break":   {tokens: []string{testWorkloadToken + "\n"}},
	} {
		t.Run(name, func(t *testing.T) {
			var networkCalls atomic.Int64
			config := baseTestConfig(spec, failureSource)
			config.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				networkCalls.Add(1)
				return nil, errors.New("unexpected network access")
			})}
			client, err := New(config)
			if err != nil {
				t.Fatalf("create credential-failure client: %v", err)
			}
			_, err = client.Fetch(context.Background())
			assertSecretFreeError(t, err, ErrCredentialUnavailable, testWorkloadToken, "credential body")
			if networkCalls.Load() != 0 {
				t.Fatalf("invalid credential reached the network %d time(s)", networkCalls.Load())
			}
		})
	}
	if _, err := (*Client)(nil).Fetch(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil client error = %v, want invalid config", err)
	}
}

func TestClientRejectsEnabledConfigurationDrift(t *testing.T) {
	t.Parallel()
	spec := testSpec(t, "run-1", backupcontrol.TargetAppDatabase, "app/app-1/database")
	source := &testCredentialSource{tokens: []string{testWorkloadToken}}
	valid := baseTestConfig(spec, source)
	var typedNil *testCredentialSource
	tests := map[string]func(*Config){
		"empty URL":          func(config *Config) { config.BaseURL = "" },
		"plaintext":          func(config *Config) { config.BaseURL = "http://api.example.test" },
		"URL credentials":    func(config *Config) { config.BaseURL = "https://user@api.example.test" },
		"URL root path":      func(config *Config) { config.BaseURL = "https://api.example.test/" },
		"URL query":          func(config *Config) { config.BaseURL = "https://api.example.test?x=1" },
		"empty URL query":    func(config *Config) { config.BaseURL = "https://api.example.test?" },
		"URL fragment":       func(config *Config) { config.BaseURL = "https://api.example.test#x" },
		"invalid port":       func(config *Config) { config.BaseURL = "https://api.example.test:70000" },
		"invalid cell":       func(config *Config) { config.ExpectedCellKey = "backup/app-database/ABC" },
		"invalid run":        func(config *Config) { config.ExpectedRunID = "run/1" },
		"nil credential":     func(config *Config) { config.CredentialSource = nil },
		"typed nil":          func(config *Config) { config.CredentialSource = typedNil },
		"nil HTTP client":    func(config *Config) { config.HTTPClient = nil },
		"short timeout":      func(config *Config) { config.RequestTimeout = time.Second - time.Millisecond },
		"long timeout":       func(config *Config) { config.RequestTimeout = maxRequestTimeout + time.Second },
		"fractional timeout": func(config *Config) { config.RequestTimeout = time.Second + time.Nanosecond },
		"small response":     func(config *Config) { config.MaxResponseBytes = minimumResponseLimit - 1 },
		"large response": func(config *Config) {
			config.MaxResponseBytes = int64(backupmaterializer.MaxObserverInputBundleBytes) + 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if _, err := New(config); !errors.Is(err, ErrConfig) {
				t.Fatalf("configuration error = %v, want invalid config", err)
			}
		})
	}
	insecure := valid
	insecure.BaseURL = "http://api.example.test"
	insecure.AllowInsecureHTTPForTests = true
	if client, err := New(insecure); err != nil || !client.Enabled() {
		t.Fatalf("explicit test-only HTTP config rejected: client=%v err=%v", client, err)
	}
	secretSource := &testCredentialSource{tokens: []string{testWorkloadToken}}
	config := baseTestConfig(spec, secretSource)
	for _, rendered := range []string{fmt.Sprint(config), fmt.Sprintf("%#v", config)} {
		if strings.Contains(rendered, testWorkloadToken) || strings.Contains(rendered, config.BaseURL) || !strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("configuration formatting exposed private state: %q", rendered)
		}
	}
}

func TestClientSupportsConcurrentCellLocalFetches(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	keyring := testKeyring()
	spec := testSpec(t, "run-1", backupcontrol.TargetAppDatabase, "app/app-1/database")
	servedDocument := encodeBundle(t, issueBundle(t, keyring, spec, now))
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writePrivateDocument(writer, servedDocument)
	}))
	defer server.Close()
	source := &testCredentialSource{tokens: []string{testWorkloadToken}}
	client, err := New(Config{
		Enabled: true, BaseURL: server.URL, ExpectedCellKey: spec.CellKey, ExpectedRunID: spec.RunID,
		CredentialSource: source, HTTPClient: server.Client(), Now: func() time.Time { return now.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("create concurrent client: %v", err)
	}
	const fetches = 24
	errorsFound := make(chan error, fetches)
	var wait sync.WaitGroup
	for range fetches {
		wait.Add(1)
		go func() {
			defer wait.Done()
			bundle, err := client.Fetch(context.Background())
			if err == nil && (bundle.CellKey != spec.CellKey || bundle.RunID != spec.RunID) {
				err = errors.New("bundle binding drifted")
			}
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent fetch failed: %v", err)
		}
	}
	if source.callCount() != fetches {
		t.Fatalf("concurrent fetch credential reads = %d, want %d", source.callCount(), fetches)
	}
}

func TestClientProductionDependencyBoundaryHasNoMutationOrSigningCapability(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list materializer client dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "os/exec", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer",
		} {
			if dependency == forbidden {
				t.Fatalf("materializer client dependency widened to %q", dependency)
			}
		}
		for _, forbiddenPrefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializerreview", "fugue/internal/backupmaterializeridentity",
		} {
			if strings.HasPrefix(dependency, forbiddenPrefix) {
				t.Fatalf("materializer client dependency widened to %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/client",
		"fugue/internal/backupmaterializer/contract",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("materializer client local dependency closure drifted: got=%v want=%v", local, want)
	}
	direct := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := direct.Output()
	if err != nil {
		t.Fatalf("list direct materializer client imports: %v", err)
	}
	for _, forbidden := range []string{
		"os", "os/exec", "database/sql", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer",
	} {
		for _, dependency := range strings.Fields(string(directOutput)) {
			if dependency == forbidden {
				t.Fatalf("materializer client has forbidden direct capability %q", dependency)
			}
		}
	}
	for _, forbiddenPrefix := range []string{"k8s.io/", "fugue/internal/api", "fugue/internal/store"} {
		for _, dependency := range strings.Fields(string(directOutput)) {
			if strings.HasPrefix(dependency, forbiddenPrefix) {
				t.Fatalf("materializer client has forbidden direct capability %q", dependency)
			}
		}
	}
}

func baseTestConfig(spec backupcontrol.BackupRunSpec, source CredentialSource) Config {
	return Config{
		Enabled:          true,
		BaseURL:          "https://api.example.test",
		ExpectedCellKey:  spec.CellKey,
		ExpectedRunID:    spec.RunID,
		CredentialSource: source,
		HTTPClient:       &http.Client{},
	}
}

func testKeyring() backupidentity.Keyring {
	return backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
}

func testSpec(t *testing.T, runID, targetType, scopeKey string) backupcontrol.BackupRunSpec {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID,
		runID,
		backupcontrol.BackupTarget{Type: targetType, ScopeKey: scopeKey},
		"backend-1",
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		4,
		120,
		1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	return spec
}

func issueBundle(
	t *testing.T,
	keyring backupidentity.Keyring,
	spec backupcontrol.BackupRunSpec,
	now time.Time,
) backupmaterializer.ObserverInputBundle {
	t.Helper()
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", now)
	if err != nil {
		t.Fatalf("issue observer input bundle: %v", err)
	}
	return bundle
}

func encodeBundle(t *testing.T, bundle backupmaterializer.ObserverInputBundle) []byte {
	t.Helper()
	document, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("encode observer input bundle: %v", err)
	}
	return document
}

func writePrivateDocument(writer http.ResponseWriter, document []byte) {
	setPrivateResponseHeaders(writer.Header())
	writer.Header().Set("Content-Length", fmt.Sprint(len(document)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(document)
}

func setPrivateResponseHeaders(header http.Header) {
	header.Set("Content-Type", "application/json")
	header.Set("Cache-Control", "private, no-store, max-age=0")
	header.Set("Pragma", "no-cache")
	header.Set("Vary", "Authorization")
	header.Set("X-Content-Type-Options", "nosniff")
}

func assertSecretFreeError(t *testing.T, err error, want error, secrets ...string) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	rendered := fmt.Sprintf("%+v", err)
	for _, secret := range secrets {
		if secret != "" && strings.Contains(rendered, secret) {
			t.Fatalf("error leaked private material %q: %s", secret, rendered)
		}
	}
}
