package secretreader

import (
	"context"
	"encoding/base64"
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
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
)

func TestReaderGetsOnlyExactSecretAndRecoversRotatingObservation(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 2, 1, 0, 0, 0, time.UTC)
	fixture := newFixture(t, issuedAt)
	document := fixture.document(t)
	source := &sequenceCredentialSource{tokens: []string{"aaa.bbb.ccc", "ddd.eee.fff"}}
	var requests atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		call := int(requests.Add(1))
		wantToken := source.tokens[call-1]
		if request.Method != http.MethodGet || request.URL.Path != secretPathPrefix+materialization.SecretNamespace+"/secrets/"+fixture.plan.SecretName ||
			request.URL.RawQuery != "" || request.Header.Get("Authorization") != "Bearer "+wantToken ||
			request.Header.Get("Accept") != "application/json" || request.Header.Get("Accept-Encoding") != "identity" ||
			request.Header.Get("Cache-Control") != "no-store" || request.Header.Get("User-Agent") != RequestUserAgent ||
			request.ContentLength != 0 {
			t.Errorf("request widened: method=%q url=%q headers=%v length=%d", request.Method, request.URL.String(), request.Header, request.ContentLength)
			http.Error(w, "request drift", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(document)
	}))
	defer server.Close()
	reader, err := New(Config{
		Enabled: true, APIServerURL: server.URL, ExpectedCellKey: fixture.plan.CellKey,
		CredentialSource: source, HTTPClient: server.Client(),
	})
	if err != nil || !reader.Enabled() || requests.Load() != 0 || source.Calls() != 0 {
		t.Fatalf("construct reader: enabled=%t requests=%d credentials=%d err=%v", reader != nil && reader.Enabled(), requests.Load(), source.Calls(), err)
	}
	first, err := reader.Observe(context.Background())
	if err != nil || first.State != reconcile.StateManaged || first.CellKey != fixture.plan.CellKey ||
		first.UID != fixture.uid || first.ResourceVersion != fixture.resourceVersion {
		t.Fatalf("first observation drifted: observation=%#v err=%v", first, err)
	}
	second, err := reader.Observe(context.Background())
	if err != nil || !reflect.DeepEqual(second, first) || requests.Load() != 2 || source.Calls() != 2 {
		t.Fatalf("rotated observation drifted: observation=%#v requests=%d credentials=%d err=%v", second, requests.Load(), source.Calls(), err)
	}
	decision, err := reconcile.Decide(fixture.plan.CellKey, nil, second, fixture.plan.RenewAfter.Add(time.Minute))
	if err != nil || decision.Action != reconcile.ActionRetainLastKnownGood || !decision.Stable ||
		!decision.RetainExisting || decision.MutationCandidate || decision.DeleteAllowed {
		t.Fatalf("recovered LKG decision drifted: decision=%#v err=%v", decision, err)
	}
	for _, rendered := range []string{fmt.Sprint(reader), fmt.Sprintf("%#v", reader)} {
		if !strings.Contains(rendered, "[REDACTED]") || strings.Contains(rendered, server.URL) || strings.Contains(rendered, fixture.plan.CellKey) {
			t.Fatalf("reader formatting exposed configuration: %q", rendered)
		}
	}
}

func TestReaderRequiresExactKubernetesNotFoundProof(t *testing.T) {
	t.Parallel()
	fixture := newFixture(t, time.Date(2026, 8, 2, 2, 0, 0, 0, time.UTC))
	valid := map[string]any{
		"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "NotFound", "code": 404,
		"details": map[string]any{"name": fixture.plan.SecretName, "kind": "secrets", "group": ""},
	}
	tests := map[string]struct {
		mutate func(map[string]any)
		ok     bool
	}{
		"exact":    {ok: true},
		"API":      {mutate: func(value map[string]any) { value["apiVersion"] = "status.k8s.io/v1" }},
		"kind":     {mutate: func(value map[string]any) { value["kind"] = "Secret" }},
		"status":   {mutate: func(value map[string]any) { value["status"] = "Success" }},
		"reason":   {mutate: func(value map[string]any) { value["reason"] = "Forbidden" }},
		"code":     {mutate: func(value map[string]any) { value["code"] = 200 }},
		"name":     {mutate: func(value map[string]any) { value["details"].(map[string]any)["name"] = "other" }},
		"resource": {mutate: func(value map[string]any) { value["details"].(map[string]any)["kind"] = "configmaps" }},
		"group":    {mutate: func(value map[string]any) { value["details"].(map[string]any)["group"] = "core" }},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			status := cloneJSONMap(t, valid)
			if test.mutate != nil {
				test.mutate(status)
			}
			document, err := json.Marshal(status)
			if err != nil {
				t.Fatalf("marshal status: %v", err)
			}
			server := jsonServer(t, http.StatusNotFound, document, nil)
			defer server.Close()
			reader := mustReader(t, fixture.plan.CellKey, server, &sequenceCredentialSource{tokens: []string{"aaa.bbb.ccc"}})
			observation, observeErr := reader.Observe(context.Background())
			if test.ok {
				if observeErr != nil || observation.State != reconcile.StateAbsent {
					t.Fatalf("absent observation drifted: observation=%#v err=%v", observation, observeErr)
				}
			} else if !errors.Is(observeErr, ErrSecretResponse) || observation.Digest != "" {
				t.Fatalf("invalid absence proof error = %v observation=%#v", observeErr, observation)
			}
		})
	}
}

func TestReaderClassifiesForeignAndMalformedObjectsWithoutAdoption(t *testing.T) {
	t.Parallel()
	fixture := newFixture(t, time.Date(2026, 8, 2, 3, 0, 0, 0, time.UTC))
	tests := map[string]struct {
		mutate func(map[string]any)
		state  reconcile.CurrentState
	}{
		"future top-level metadata is compatible": {
			mutate: func(value map[string]any) { value["futureReadOnlyField"] = map[string]any{"value": true} },
			state:  reconcile.StateManaged,
		},
		"foreign owner": {
			mutate: func(value map[string]any) {
				metadata(value)["labels"].(map[string]any)[reconcile.LabelManagedBy] = "other-controller"
			},
			state: reconcile.StateForeign,
		},
		"invalid base64": {
			mutate: func(value map[string]any) { value["data"].(map[string]any)[materialization.TokenDataKey] = "***" },
			state:  reconcile.StateMalformed,
		},
		"noncanonical base64": {
			mutate: func(value map[string]any) { value["data"].(map[string]any)[materialization.TokenDataKey] = "YQ==\n" },
			state:  reconcile.StateMalformed,
		},
		"wrong API": {
			mutate: func(value map[string]any) { value["apiVersion"] = "v2" },
			state:  reconcile.StateMalformed,
		},
		"wrong kind": {
			mutate: func(value map[string]any) { value["kind"] = "ConfigMap" },
			state:  reconcile.StateMalformed,
		},
		"string data": {
			mutate: func(value map[string]any) { value["stringData"] = map[string]any{"token": "private"} },
			state:  reconcile.StateMalformed,
		},
		"immutable": {
			mutate: func(value map[string]any) { value["immutable"] = true },
			state:  reconcile.StateMalformed,
		},
		"deleting": {
			mutate: func(value map[string]any) { metadata(value)["deletionTimestamp"] = "2026-08-02T03:01:00Z" },
			state:  reconcile.StateMalformed,
		},
		"owner reference": {
			mutate: func(value map[string]any) {
				metadata(value)["ownerReferences"] = []any{map[string]any{"uid": "owner-uid"}}
			},
			state: reconcile.StateMalformed,
		},
		"extra data": {
			mutate: func(value map[string]any) {
				value["data"].(map[string]any)["other"] = base64.StdEncoding.EncodeToString([]byte("value"))
			},
			state: reconcile.StateMalformed,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			object := cloneJSONMap(t, fixture.object)
			test.mutate(object)
			document, err := json.Marshal(object)
			if err != nil {
				t.Fatalf("marshal Secret: %v", err)
			}
			server := jsonServer(t, http.StatusOK, document, nil)
			defer server.Close()
			reader := mustReader(t, fixture.plan.CellKey, server, &sequenceCredentialSource{tokens: []string{"aaa.bbb.ccc"}})
			observation, observeErr := reader.Observe(context.Background())
			if observeErr != nil || observation.State != test.state {
				t.Fatalf("classification drifted: observation=%#v err=%v", observation, observeErr)
			}
			if test.state != reconcile.StateManaged {
				decision, err := reconcile.Decide(fixture.plan.CellKey, &fixture.plan, observation, fixture.applyAt)
				if err != nil || decision.Action != reconcile.ActionBlock || !decision.Blocked ||
					decision.MutationCandidate || decision.RetainExisting || decision.DeleteAllowed {
					t.Fatalf("obstruction escaped block: decision=%#v err=%v", decision, err)
				}
			}
		})
	}
}

func TestReaderFailsClosedOnTransportAndResponseDriftWithoutLeaks(t *testing.T) {
	t.Parallel()
	fixture := newFixture(t, time.Date(2026, 8, 2, 4, 0, 0, 0, time.UTC))
	credential := "private.jwt.credential"
	secretMarker := "private-response-marker"
	tests := map[string]struct {
		status      int
		body        []byte
		contentType string
		encoding    string
		want        error
	}{
		"unauthorized": {status: http.StatusUnauthorized, body: []byte(secretMarker), contentType: "text/plain", want: ErrSecretUnavailable},
		"server error": {status: http.StatusServiceUnavailable, body: []byte(secretMarker), contentType: "text/plain", want: ErrSecretUnavailable},
		"content type": {status: http.StatusOK, body: fixture.document(t), contentType: "text/plain", want: ErrSecretResponse},
		"encoding":     {status: http.StatusOK, body: fixture.document(t), contentType: "application/json", encoding: "gzip", want: ErrSecretResponse},
		"empty":        {status: http.StatusOK, body: nil, contentType: "application/json", want: ErrSecretResponse},
		"malformed":    {status: http.StatusOK, body: []byte(`{"apiVersion":`), contentType: "application/json", want: ErrSecretResponse},
		"trailing":     {status: http.StatusOK, body: append(fixture.document(t), []byte(` {}`)...), contentType: "application/json", want: ErrSecretResponse},
		"oversized":    {status: http.StatusOK, body: []byte(`{"value":"` + strings.Repeat("x", int(minimumResponseBytes)) + `"}`), contentType: "application/json", want: ErrSecretResponse},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server := jsonServer(t, test.status, test.body, func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", test.contentType)
				if test.encoding != "" {
					w.Header().Set("Content-Encoding", test.encoding)
				}
			})
			defer server.Close()
			reader, err := New(Config{
				Enabled: true, APIServerURL: server.URL, ExpectedCellKey: fixture.plan.CellKey,
				CredentialSource: &sequenceCredentialSource{tokens: []string{credential}}, HTTPClient: server.Client(),
				MaxResponseBytes: minimumResponseBytes,
			})
			if err != nil {
				t.Fatalf("construct reader: %v", err)
			}
			observation, observeErr := reader.Observe(context.Background())
			if !errors.Is(observeErr, test.want) || observation.Digest != "" ||
				strings.Contains(fmt.Sprint(observeErr), credential) || strings.Contains(fmt.Sprint(observeErr), secretMarker) {
				t.Fatalf("response error drifted or leaked: observation=%#v err=%v", observation, observeErr)
			}
		})
	}

	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	redirectReader := mustReader(t, fixture.plan.CellKey, redirect, &sequenceCredentialSource{tokens: []string{credential}})
	if _, err := redirectReader.Observe(context.Background()); !errors.Is(err, ErrSecretUnavailable) || redirected.Load() != 0 {
		t.Fatalf("redirect was followed or misclassified: redirected=%d err=%v", redirected.Load(), err)
	}
}

func TestReaderDisabledConfigDeadlinesAndValidationAreFailClosed(t *testing.T) {
	t.Parallel()
	source := &sequenceCredentialSource{tokens: []string{"aaa.bbb.ccc"}}
	reader, err := New(Config{
		Enabled: false, APIServerURL: "http://private.invalid/path?secret=value", ExpectedCellKey: "invalid",
		CredentialSource: source, HTTPClient: &http.Client{}, RequestTimeout: -1, MaxResponseBytes: -1,
	})
	if err != nil || reader.Enabled() || source.Calls() != 0 {
		t.Fatalf("disabled construction retained capability: reader=%#v calls=%d err=%v", reader, source.Calls(), err)
	}
	if _, err := reader.Observe(context.Background()); !errors.Is(err, ErrDisabled) || source.Calls() != 0 {
		t.Fatalf("disabled observation accessed credential: calls=%d err=%v", source.Calls(), err)
	}
	if _, err := (*Reader)(nil).Observe(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil reader error = %v, want config", err)
	}
	if _, err := reader.Observe(nil); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled nil-context error = %v, want disabled", err)
	}

	fixture := newFixture(t, time.Date(2026, 8, 2, 5, 0, 0, 0, time.UTC))
	var typedNil *sequenceCredentialSource
	invalid := []Config{
		{Enabled: true, APIServerURL: "http://kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc/path", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc?x=1", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}},
		{Enabled: true, APIServerURL: "https://user@kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc:0", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc", ExpectedCellKey: "backup/other/invalid", CredentialSource: source, HTTPClient: &http.Client{}},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: typedNil, HTTPClient: &http.Client{}},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}, RequestTimeout: time.Second - time.Millisecond},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}, RequestTimeout: MaximumRequestTimeout + time.Millisecond},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}, MaxResponseBytes: minimumResponseBytes - 1},
		{Enabled: true, APIServerURL: "https://kubernetes.default.svc", ExpectedCellKey: fixture.plan.CellKey, CredentialSource: source, HTTPClient: &http.Client{}, MaxResponseBytes: MaximumResponse + 1},
	}
	for index, config := range invalid {
		if _, err := New(config); !errors.Is(err, ErrConfig) {
			t.Fatalf("invalid config %d error = %v, want config", index, err)
		}
	}

	server := jsonServer(t, http.StatusOK, fixture.document(t), nil)
	defer server.Close()
	invalidCredential := mustReader(t, fixture.plan.CellKey, server, &sequenceCredentialSource{tokens: []string{"private invalid credential"}})
	if _, err := invalidCredential.Observe(context.Background()); !errors.Is(err, ErrCredentialUnavailable) || strings.Contains(fmt.Sprint(err), "private invalid") {
		t.Fatalf("invalid credential error drifted or leaked: %v", err)
	}
	blocking := &blockingCredentialSource{}
	deadlineReader := mustReader(t, fixture.plan.CellKey, server, blocking)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := deadlineReader.Observe(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared credential deadline error = %v, want deadline", err)
	}
}

func TestReaderConcurrentObservationsAreCellLocalAndDeterministic(t *testing.T) {
	t.Parallel()
	fixture := newFixture(t, time.Date(2026, 8, 2, 6, 0, 0, 0, time.UTC))
	server := jsonServer(t, http.StatusOK, fixture.document(t), nil)
	defer server.Close()
	reader := mustReader(t, fixture.plan.CellKey, server, constantCredentialSource("aaa.bbb.ccc"))
	baseline, err := reader.Observe(context.Background())
	if err != nil {
		t.Fatalf("baseline observation: %v", err)
	}
	const workers = 32
	var wait sync.WaitGroup
	errorsFound := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			observation, observeErr := reader.Observe(context.Background())
			if observeErr == nil && !reflect.DeepEqual(observation, baseline) {
				observeErr = errors.New("observation drifted")
			}
			errorsFound <- observeErr
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent observation failed: %v", err)
		}
	}
}

func TestSecretReaderProductionDependencyBoundaryIsReadOnly(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list Secret reader dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{"database/sql", "os/exec", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer"} {
			if dependency == forbidden {
				t.Fatalf("Secret reader dependency widened to %q", dependency)
			}
		}
		for _, prefix := range []string{"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model"} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("Secret reader dependency widened to %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/secretreader",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("Secret reader local dependency closure drifted: got=%v want=%v", local, want)
	}
	directCommand := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := directCommand.Output()
	if err != nil {
		t.Fatalf("list direct Secret reader dependencies: %v", err)
	}
	direct := strings.Fields(string(directOutput))
	sort.Strings(direct)
	wantDirect := []string{
		"bytes", "context", "encoding/base64", "encoding/json", "errors",
		"fugue/internal/backupmaterializer/materialization", "fugue/internal/backupmaterializer/reconcile",
		"io", "mime", "net/http", "net/url", "reflect", "strconv", "strings", "time",
	}
	if !reflect.DeepEqual(direct, wantDirect) {
		t.Fatalf("Secret reader direct dependency boundary widened: got=%v want=%v", direct, wantDirect)
	}
}

type fixture struct {
	plan            materialization.Plan
	applyAt         time.Time
	uid             string
	resourceVersion string
	object          map[string]any
}

func newFixture(t *testing.T, issuedAt time.Time) fixture {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		"run-1", "run-1",
		backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"},
		"backend-1", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 4, 120, 1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	keyring := backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", issuedAt)
	if err != nil {
		t.Fatalf("issue observer bundle: %v", err)
	}
	applyAt := issuedAt.Add(30 * time.Second)
	plan, err := materialization.Build(bundle, applyAt)
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	manifest, err := reconcile.BuildManifest(plan, applyAt)
	if err != nil {
		t.Fatalf("build Secret manifest: %v", err)
	}
	data, err := plan.Data(applyAt)
	if err != nil {
		t.Fatalf("read private Secret data: %v", err)
	}
	uid := "01234567-89ab-cdef-0123-456789abcdef"
	resourceVersion := "42"
	labels := make(map[string]any, len(manifest.Labels))
	for key, value := range manifest.Labels {
		labels[key] = value
	}
	annotations := make(map[string]any, len(manifest.Annotations))
	for key, value := range manifest.Annotations {
		annotations[key] = value
	}
	object := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":              plan.SecretName,
			"namespace":         plan.Namespace,
			"uid":               uid,
			"resourceVersion":   resourceVersion,
			"labels":            labels,
			"annotations":       annotations,
			"creationTimestamp": issuedAt.Format(time.RFC3339),
			"managedFields":     []any{},
		},
		"type": reconcile.SecretTypeOpaque,
		"data": map[string]any{
			data.SpecKey:  base64.StdEncoding.EncodeToString(data.SpecDocument),
			data.TokenKey: base64.StdEncoding.EncodeToString(data.ObserverToken),
		},
	}
	return fixture{plan: plan, applyAt: applyAt, uid: uid, resourceVersion: resourceVersion, object: object}
}

func (value fixture) document(t *testing.T) []byte {
	t.Helper()
	document, err := json.Marshal(value.object)
	if err != nil {
		t.Fatalf("marshal Secret: %v", err)
	}
	return document
}

func metadata(value map[string]any) map[string]any {
	return value["metadata"].(map[string]any)
}

func cloneJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	document, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal clone: %v", err)
	}
	var cloned map[string]any
	if err := json.Unmarshal(document, &cloned); err != nil {
		t.Fatalf("unmarshal clone: %v", err)
	}
	return cloned
}

func jsonServer(t *testing.T, status int, body []byte, headers func(http.ResponseWriter)) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if headers != nil {
			headers(w)
		} else {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
}

func mustReader(t *testing.T, cellKey string, server *httptest.Server, source CredentialSource) *Reader {
	t.Helper()
	reader, err := New(Config{
		Enabled: true, APIServerURL: server.URL, ExpectedCellKey: cellKey,
		CredentialSource: source, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("construct reader: %v", err)
	}
	return reader
}

type sequenceCredentialSource struct {
	mu     sync.Mutex
	tokens []string
	calls  int
}

func (source *sequenceCredentialSource) Credential(context.Context) (string, error) {
	if source == nil {
		return "", errors.New("nil source")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.calls >= len(source.tokens) {
		return "", errors.New("no credential")
	}
	token := source.tokens[source.calls]
	source.calls++
	return token, nil
}

func (source *sequenceCredentialSource) Calls() int {
	if source == nil {
		return 0
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.calls
}

type constantCredentialSource string

func (source constantCredentialSource) Credential(context.Context) (string, error) {
	return string(source), nil
}

type blockingCredentialSource struct{}

func (*blockingCredentialSource) Credential(ctx context.Context) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}
