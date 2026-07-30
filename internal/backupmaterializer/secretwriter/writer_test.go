package secretwriter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
)

const testWorkloadToken = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.signature"

type credentialStub struct {
	token string
	err   error
	calls atomic.Int64
}

func (stub *credentialStub) Credential(ctx context.Context) (string, error) {
	stub.calls.Add(1)
	if ctx == nil || ctx.Err() != nil {
		return "", ctx.Err()
	}
	return stub.token, stub.err
}

func TestDisabledWriterIgnoresEveryCapability(t *testing.T) {
	t.Parallel()
	var typedNil *credentialStub
	writer, err := New(Config{
		Enabled:                   false,
		APIServerURL:              "private invalid URL",
		ExpectedCellKey:           "private invalid cell",
		CredentialSource:          typedNil,
		HTTPClient:                &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { panic("network used") })},
		RequestTimeout:            -time.Second,
		MaxResponseBytes:          -1,
		Now:                       func() time.Time { panic("clock used") },
		AllowInsecureHTTPForTests: true,
	})
	if err != nil || writer.Enabled() || writer.collectionURL != "" || writer.itemURL != "" || writer.credential != nil ||
		writer.client != nil || writer.now != nil {
		t.Fatalf("disabled construction retained capability: writer=%#v err=%v", writer, err)
	}
	if _, err := writer.DryRun(context.Background(), materialization.Plan{}, reconcile.Decision{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled DryRun error = %v, want ErrDisabled", err)
	}
	if !strings.Contains(fmt.Sprintf("%v", writer), "[REDACTED]") || strings.Contains(fmt.Sprintf("%#v", writer), "private") {
		t.Fatalf("writer formatting leaked disabled inputs: %v %#v", writer, writer)
	}
}

func TestCreateDryRunUsesExactNonPersistingRequestAndReturnsRedactedReceipt(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-create", now)
	decision := testCreateDecision(t, plan, now)
	credential := &credentialStub{token: testWorkloadToken}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		wire := assertDryRunRequest(t, request, http.MethodPost, secretCollectionPath, plan, decision, now)
		wire.Metadata.UID = "dry-run-generated-uid"
		wire.Metadata.ResourceVersion = "dry-run-generated-version"
		wire.Metadata.CreationTimestamp = json.RawMessage(`"2026-07-31T07:00:00Z"`)
		wire.Metadata.Labels["admission.example.test/validated"] = "true"
		wire.Metadata.Annotations["admission.example.test/generation"] = "dry-run"
		wire.Metadata.ManagedFields = []json.RawMessage{json.RawMessage(`{"manager":"fugue-backup-materializer"}`)}
		writeSecretResponse(t, response, http.StatusCreated, wire)
	}))
	defer server.Close()

	writer := testWriter(t, server, plan.CellKey, credential, now)
	result, err := writer.DryRun(context.Background(), plan, decision)
	if err != nil {
		t.Fatalf("create dry-run: %v", err)
	}
	if calls.Load() != 1 || credential.calls.Load() != 1 || ValidateResult(result) != nil ||
		result.Action != reconcile.ActionCreateIfAbsent || result.PlanDigest != plan.Digest ||
		result.DecisionDigest != decision.Digest || result.RequestDigest == "" || result.Digest == "" ||
		!result.Accepted || !result.ServerSideDryRun || result.Persisted || result.DeleteAllowed ||
		result.ExecutionAllowed || result.ProductionMutationAllowed {
		t.Fatalf("create receipt drifted: calls=%d credential=%d result=%#v err=%v", calls.Load(), credential.calls.Load(), result, ValidateResult(result))
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	data, _ := plan.Data(now)
	for _, sensitive := range []string{testWorkloadToken, string(data.ObserverToken), string(data.SpecDocument)} {
		if strings.Contains(string(encoded), sensitive) || strings.Contains(fmt.Sprintf("%#v", result), sensitive) {
			t.Fatalf("dry-run receipt leaked private material %q", sensitive)
		}
	}
}

func TestReplaceDryRunUsesExactUIDAndResourceVersionCAS(t *testing.T) {
	t.Parallel()
	now := testNow()
	previous := testPlan(t, "run-previous", now)
	desired := testPlan(t, "run-replacement", now)
	decision := testReplaceDecision(t, previous, desired, now)
	credential := &credentialStub{token: testWorkloadToken}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		wire := assertDryRunRequest(t, request, http.MethodPut, secretCollectionPath+"/"+desired.SecretName, desired, decision, now)
		if wire.Metadata.UID != decision.ExpectedUID || wire.Metadata.ResourceVersion != decision.ExpectedResourceVersion {
			t.Fatalf("replace body lost CAS preconditions: metadata=%+v decision=%+v", wire.Metadata, decision)
		}
		// Generated dry-run fields are deliberately not treated as durable
		// evidence; only the accepted CAS request and exact desired body are.
		wire.Metadata.UID = "different-dry-run-generated-uid"
		wire.Metadata.ResourceVersion = "different-dry-run-generated-version"
		writeSecretResponse(t, response, http.StatusOK, wire)
	}))
	defer server.Close()

	result, err := testWriter(t, server, desired.CellKey, credential, now).DryRun(context.Background(), desired, decision)
	if err != nil || ValidateResult(result) != nil || result.Action != reconcile.ActionReplaceResourceVersionCAS ||
		result.PlanDigest != desired.Digest || result.DecisionDigest != decision.Digest {
		t.Fatalf("replace dry-run result = %#v err=%v validation=%v", result, err, ValidateResult(result))
	}
}

func TestDryRunRejectsStaleMismatchedAndNonMutationIntentBeforeCapabilities(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-intent", now)
	create := testCreateDecision(t, plan, now)
	managed := testManagedObservation(t, plan, now)
	noop, err := reconcile.Decide(plan.CellKey, &plan, managed, now)
	if err != nil {
		t.Fatalf("build no-op decision: %v", err)
	}
	otherPlan := testPlanForTarget(t, "run-other", backupcontrol.BackupTarget{
		Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry",
	}, now)
	otherDecision := testCreateDecision(t, otherPlan, now)

	tests := []struct {
		name     string
		cell     string
		clock    time.Time
		plan     materialization.Plan
		decision reconcile.Decision
	}{
		{name: "no-op", cell: plan.CellKey, clock: now, plan: plan, decision: noop},
		{name: "different plan", cell: plan.CellKey, clock: now, plan: otherPlan, decision: create},
		{name: "different decision", cell: plan.CellKey, clock: now, plan: plan, decision: otherDecision},
		{name: "writer cell mismatch", cell: otherPlan.CellKey, clock: now, plan: plan, decision: create},
		{name: "stale decision", cell: plan.CellKey, clock: now.Add(MaximumDecisionAge + time.Second), plan: plan, decision: create},
		{name: "future decision", cell: plan.CellKey, clock: now.Add(-time.Second), plan: plan, decision: create},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credential := &credentialStub{token: testWorkloadToken}
			var calls atomic.Int64
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("network must not run")
			})}
			writer, err := New(Config{
				Enabled: true, APIServerURL: "https://kubernetes.example.test", ExpectedCellKey: test.cell,
				CredentialSource: credential, HTTPClient: client, Now: func() time.Time { return test.clock },
			})
			if err != nil {
				t.Fatalf("construct writer: %v", err)
			}
			if _, err := writer.DryRun(context.Background(), test.plan, test.decision); !errors.Is(err, ErrIntent) {
				t.Fatalf("DryRun error = %v, want ErrIntent", err)
			}
			if calls.Load() != 0 || credential.calls.Load() != 0 {
				t.Fatalf("invalid intent crossed capability boundary: network=%d credential=%d", calls.Load(), credential.calls.Load())
			}
		})
	}
}

func TestDryRunCredentialNetworkStatusAndRedirectFailuresAreFixedAndSingleAttempt(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-errors", now)
	decision := testCreateDecision(t, plan, now)

	t.Run("credential unavailable", func(t *testing.T) {
		credential := &credentialStub{token: "private.invalid.token", err: errors.New("private credential detail")}
		writer, err := New(Config{
			Enabled: true, APIServerURL: "https://kubernetes.example.test", ExpectedCellKey: plan.CellKey,
			CredentialSource: credential, HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { panic("network used") })},
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("construct writer: %v", err)
		}
		_, err = writer.DryRun(context.Background(), plan, decision)
		if !errors.Is(err, ErrCredentialUnavailable) || strings.Contains(err.Error(), "private") || credential.calls.Load() != 1 {
			t.Fatalf("credential failure = %v calls=%d", err, credential.calls.Load())
		}
	})

	t.Run("transport is not retried", func(t *testing.T) {
		credential := &credentialStub{token: testWorkloadToken}
		var calls atomic.Int64
		writer, err := New(Config{
			Enabled: true, APIServerURL: "https://kubernetes.example.test", ExpectedCellKey: plan.CellKey,
			CredentialSource: credential,
			HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("private transport detail")
			})},
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("construct writer: %v", err)
		}
		_, err = writer.DryRun(context.Background(), plan, decision)
		if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "private") || calls.Load() != 1 || credential.calls.Load() != 1 {
			t.Fatalf("transport failure = %v network=%d credential=%d", err, calls.Load(), credential.calls.Load())
		}
	})

	for _, test := range []struct {
		name   string
		status int
		want   error
	}{
		{name: "conflict", status: http.StatusConflict, want: ErrConflict},
		{name: "forbidden", status: http.StatusForbidden, want: ErrRejected},
		{name: "not found", status: http.StatusNotFound, want: ErrRejected},
		{name: "rate limited", status: http.StatusTooManyRequests, want: ErrUnavailable},
		{name: "server error", status: http.StatusServiceUnavailable, want: ErrUnavailable},
		{name: "unexpected success", status: http.StatusOK, want: ErrResponse},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				response.WriteHeader(test.status)
				_, _ = response.Write([]byte(`{"private":"response detail"}`))
			}))
			defer server.Close()
			_, err := testWriter(t, server, plan.CellKey, &credentialStub{token: testWorkloadToken}, now).DryRun(context.Background(), plan, decision)
			if !errors.Is(err, test.want) || strings.Contains(err.Error(), "private") {
				t.Fatalf("status %d error = %v, want %v", test.status, err, test.want)
			}
		})
	}

	t.Run("redirect does not receive body", func(t *testing.T) {
		var sinkCalls atomic.Int64
		sink := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { sinkCalls.Add(1) }))
		defer sink.Close()
		source := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			response.Header().Set("Location", sink.URL)
			response.WriteHeader(http.StatusTemporaryRedirect)
		}))
		defer source.Close()
		_, err := testWriter(t, source, plan.CellKey, &credentialStub{token: testWorkloadToken}, now).DryRun(context.Background(), plan, decision)
		if !errors.Is(err, ErrResponse) || sinkCalls.Load() != 0 {
			t.Fatalf("redirect error=%v sinkCalls=%d", err, sinkCalls.Load())
		}
	})
}

func TestDryRunRejectsMalformedOrMutatedSuccessResponses(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-response", now)
	decision := testCreateDecision(t, plan, now)
	tests := []struct {
		name   string
		header func(http.Header)
		mutate func(*secretDocument)
		raw    []byte
	}{
		{name: "missing content type", header: func(header http.Header) { header.Del("Content-Type") }},
		{name: "encoded response", header: func(header http.Header) { header.Set("Content-Encoding", "gzip") }},
		{name: "wrong API", mutate: func(value *secretDocument) { value.APIVersion = "v2" }},
		{name: "wrong kind", mutate: func(value *secretDocument) { value.Kind = "ConfigMap" }},
		{name: "wrong name", mutate: func(value *secretDocument) { value.Metadata.Name = "other" }},
		{name: "wrong namespace", mutate: func(value *secretDocument) { value.Metadata.Namespace = "default" }},
		{name: "generate name", mutate: func(value *secretDocument) { value.Metadata.GenerateName = "generated-" }},
		{name: "self link", mutate: func(value *secretDocument) { value.Metadata.SelfLink = "/private" }},
		{name: "generation", mutate: func(value *secretDocument) { value.Metadata.Generation = 1 }},
		{name: "invalid UID", mutate: func(value *secretDocument) { value.Metadata.UID = "invalid uid" }},
		{name: "invalid resource version", mutate: func(value *secretDocument) { value.Metadata.ResourceVersion = "invalid\\version" }},
		{name: "invalid creation timestamp", mutate: func(value *secretDocument) {
			value.Metadata.CreationTimestamp = json.RawMessage(`"not-a-time"`)
		}},
		{name: "deletion grace", mutate: func(value *secretDocument) {
			seconds := int64(1)
			value.Metadata.DeletionGracePeriodSeconds = &seconds
		}},
		{name: "owner reference", mutate: func(value *secretDocument) {
			value.Metadata.OwnerReferences = []json.RawMessage{json.RawMessage(`{"uid":"x"}`)}
		}},
		{name: "finalizer", mutate: func(value *secretDocument) { value.Metadata.Finalizers = []string{"retain"} }},
		{name: "deleting", mutate: func(value *secretDocument) {
			value.Metadata.DeletionTimestamp = json.RawMessage(`"2026-07-31T07:00:00Z"`)
		}},
		{name: "immutable", mutate: func(value *secretDocument) { immutable := true; value.Immutable = &immutable }},
		{name: "missing immutable", mutate: func(value *secretDocument) { value.Immutable = nil }},
		{name: "wrong type", mutate: func(value *secretDocument) { value.Type = "kubernetes.io/tls" }},
		{name: "string data", mutate: func(value *secretDocument) { value.StringData = map[string]string{"token": "private"} }},
		{name: "missing label", mutate: func(value *secretDocument) { delete(value.Metadata.Labels, reconcile.LabelManagedBy) }},
		{name: "mutated annotation", mutate: func(value *secretDocument) {
			value.Metadata.Annotations[reconcile.AnnotationPlanDigest] = strings.Repeat("0", 64)
		}},
		{name: "mutated data", mutate: func(value *secretDocument) { value.Data[materialization.TokenDataKey] = "cHJpdmF0ZQ==" }},
		{name: "extra data", mutate: func(value *secretDocument) { value.Data["extra"] = "eA==" }},
		{name: "unknown top level", raw: []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{},"immutable":false,"type":"Opaque","data":{},"unknown":true}`)},
		{name: "trailing JSON", raw: []byte(`{} {}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				wire := assertDryRunRequest(t, request, http.MethodPost, secretCollectionPath, plan, decision, now)
				if test.mutate != nil {
					test.mutate(&wire)
				}
				response.Header().Set("Content-Type", "application/json")
				if test.header != nil {
					test.header(response.Header())
				}
				response.WriteHeader(http.StatusCreated)
				if test.raw != nil {
					_, _ = response.Write(test.raw)
					return
				}
				_ = json.NewEncoder(response).Encode(wire)
			}))
			defer server.Close()
			_, err := testWriter(t, server, plan.CellKey, &credentialStub{token: testWorkloadToken}, now).DryRun(context.Background(), plan, decision)
			if !errors.Is(err, ErrResponse) {
				t.Fatalf("malformed response error = %v, want ErrResponse", err)
			}
		})
	}
}

func TestWriterRejectsInvalidConfigurationAndCopiesHTTPClient(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-config", now)
	credential := &credentialStub{token: testWorkloadToken}
	typedNil := (*credentialStub)(nil)
	base := Config{
		Enabled: true, APIServerURL: "https://kubernetes.example.test", ExpectedCellKey: plan.CellKey,
		CredentialSource: credential, HTTPClient: &http.Client{}, Now: func() time.Time { return now },
	}
	tests := map[string]func(*Config){
		"plaintext":          func(value *Config) { value.APIServerURL = "http://kubernetes.example.test" },
		"URL path":           func(value *Config) { value.APIServerURL = "https://kubernetes.example.test/api" },
		"URL query":          func(value *Config) { value.APIServerURL = "https://kubernetes.example.test?dryRun=" },
		"URL credentials":    func(value *Config) { value.APIServerURL = "https://user@kubernetes.example.test" },
		"URL fragment":       func(value *Config) { value.APIServerURL = "https://kubernetes.example.test#fragment" },
		"invalid port":       func(value *Config) { value.APIServerURL = "https://kubernetes.example.test:70000" },
		"invalid cell":       func(value *Config) { value.ExpectedCellKey = "backup/all/invalid" },
		"nil credential":     func(value *Config) { value.CredentialSource = nil },
		"typed nil":          func(value *Config) { value.CredentialSource = typedNil },
		"nil client":         func(value *Config) { value.HTTPClient = nil },
		"short timeout":      func(value *Config) { value.RequestTimeout = time.Second - time.Millisecond },
		"long timeout":       func(value *Config) { value.RequestTimeout = MaximumRequestTimeout + time.Millisecond },
		"fractional timeout": func(value *Config) { value.RequestTimeout = time.Second + time.Nanosecond },
		"small response":     func(value *Config) { value.MaxResponseBytes = minimumResponseBytes - 1 },
		"large response":     func(value *Config) { value.MaxResponseBytes = MaximumResponse + 1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			if _, err := New(config); !errors.Is(err, ErrConfig) {
				t.Fatalf("New error = %v, want ErrConfig", err)
			}
		})
	}

	callerClient := &http.Client{Timeout: 29 * time.Second}
	writer, err := New(Config{
		Enabled: true, APIServerURL: "http://kubernetes.example.test", ExpectedCellKey: plan.CellKey,
		CredentialSource: credential, HTTPClient: callerClient, Now: func() time.Time { return now }, AllowInsecureHTTPForTests: true,
	})
	if err != nil {
		t.Fatalf("construct test writer: %v", err)
	}
	if writer.client == callerClient || writer.client.Timeout != DefaultRequestTimeout || callerClient.Timeout != 29*time.Second ||
		writer.client.CheckRedirect == nil || writer.collectionURL != "http://kubernetes.example.test"+secretCollectionPath+"?"+dryRunQuery() ||
		writer.itemURL != "http://kubernetes.example.test"+secretCollectionPath+"/"+plan.SecretName+"?"+dryRunQuery() {
		t.Fatalf("writer client or endpoints drifted: writer=%#v caller=%#v", writer, callerClient)
	}
	if strings.Contains(fmt.Sprintf("%#v", base), base.APIServerURL) || !strings.Contains(fmt.Sprintf("%#v", base), "[REDACTED]") {
		t.Fatalf("config formatting leaked endpoint: %#v", base)
	}
}

func TestDryRunHonorsCanceledContextWithoutCapabilityUse(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-canceled", now)
	decision := testCreateDecision(t, plan, now)
	credential := &credentialStub{token: testWorkloadToken}
	writer, err := New(Config{
		Enabled: true, APIServerURL: "https://kubernetes.example.test", ExpectedCellKey: plan.CellKey,
		CredentialSource: credential, HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { panic("network used") })},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct writer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := writer.DryRun(ctx, plan, decision); !errors.Is(err, context.Canceled) || credential.calls.Load() != 0 {
		t.Fatalf("canceled DryRun error=%v credentialCalls=%d", err, credential.calls.Load())
	}
}

func TestWriterAndResultRejectNilAndPublicContractDrift(t *testing.T) {
	t.Parallel()
	if (*Writer)(nil).Enabled() || !strings.Contains((*Writer)(nil).String(), "<nil>") ||
		!strings.Contains((*Writer)(nil).GoString(), "<nil>") {
		t.Fatal("nil writer formatting or enablement drifted")
	}
	if _, err := (*Writer)(nil).DryRun(context.Background(), materialization.Plan{}, reconcile.Decision{}); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil writer error = %v, want ErrConfig", err)
	}

	now := testNow()
	plan := testPlan(t, "run-result-contract", now)
	decision := testCreateDecision(t, plan, now)
	valid := Result{
		APIVersion: APIVersion, Kind: Kind, Policy: Policy, Namespace: plan.Namespace, SecretName: plan.SecretName,
		CellKey: plan.CellKey, CellID: plan.CellID, Action: decision.Action, PlanDigest: plan.Digest,
		DecisionDigest: decision.Digest, RequestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		IdempotencyKey: dryRunIdempotencyKey(plan.CellID, decision.Digest), ValidatedAt: now,
		Accepted: true, ServerSideDryRun: true,
	}
	valid.Digest = DigestResult(valid)
	if err := ValidateResult(valid); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	tests := map[string]func(*Result){
		"API":                 func(value *Result) { value.APIVersion = "v2" },
		"kind":                func(value *Result) { value.Kind = "Other" },
		"policy":              func(value *Result) { value.Policy = "live-write" },
		"namespace":           func(value *Result) { value.Namespace = "default" },
		"name":                func(value *Result) { value.SecretName = "other" },
		"cell":                func(value *Result) { value.CellKey = "backup/all/invalid" },
		"cell ID":             func(value *Result) { value.CellID = "other" },
		"action":              func(value *Result) { value.Action = reconcile.ActionNoop },
		"plan digest":         func(value *Result) { value.PlanDigest = "invalid" },
		"decision digest":     func(value *Result) { value.DecisionDigest = "invalid" },
		"request digest":      func(value *Result) { value.RequestDigest = "invalid" },
		"idempotency":         func(value *Result) { value.IdempotencyKey = "other" },
		"noncanonical time":   func(value *Result) { value.ValidatedAt = value.ValidatedAt.In(time.FixedZone("other", 0)) },
		"not accepted":        func(value *Result) { value.Accepted = false },
		"not dry-run":         func(value *Result) { value.ServerSideDryRun = false },
		"persisted":           func(value *Result) { value.Persisted = true },
		"delete":              func(value *Result) { value.DeleteAllowed = true },
		"execution":           func(value *Result) { value.ExecutionAllowed = true },
		"production mutation": func(value *Result) { value.ProductionMutationAllowed = true },
		"digest":              func(value *Result) { value.Digest = strings.Repeat("0", 64) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if name != "digest" {
				value.Digest = DigestResult(value)
			}
			if err := ValidateResult(value); !errors.Is(err, ErrResponse) {
				t.Fatalf("ValidateResult error = %v, want ErrResponse", err)
			}
		})
	}
}

func TestDryRunRejectsInvalidCredentialAndBoundedResponseFailures(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-bounds", now)
	decision := testCreateDecision(t, plan, now)

	for _, token := range []string{"", "not-a-jwt", "a.b.c\n", "a..c", strings.Repeat("a", maxBearerTokenBytes+1)} {
		t.Run("credential "+fmt.Sprintf("%d", len(token)), func(t *testing.T) {
			credential := &credentialStub{token: token}
			writer, err := New(Config{
				Enabled: true, APIServerURL: "https://kubernetes.example.test", ExpectedCellKey: plan.CellKey,
				CredentialSource: credential, HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { panic("network used") })},
				Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatalf("construct writer: %v", err)
			}
			if _, err := writer.DryRun(context.Background(), plan, decision); !errors.Is(err, ErrCredentialUnavailable) {
				t.Fatalf("invalid credential error = %v", err)
			}
		})
	}

	for _, test := range []struct {
		name string
		body []byte
	}{
		{name: "empty", body: nil},
		{name: "oversized", body: bytes.Repeat([]byte{'x'}, int(DefaultMaxResponse)+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				response.WriteHeader(http.StatusCreated)
				_, _ = response.Write(test.body)
			}))
			defer server.Close()
			_, err := testWriter(t, server, plan.CellKey, &credentialStub{token: testWorkloadToken}, now).DryRun(context.Background(), plan, decision)
			if !errors.Is(err, ErrResponse) {
				t.Fatalf("bounded response error = %v, want ErrResponse", err)
			}
		})
	}

	writer, err := New(Config{
		Enabled: true, APIServerURL: "https://kubernetes.example.test", ExpectedCellKey: plan.CellKey,
		CredentialSource: &credentialStub{token: testWorkloadToken},
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusCreated, Header: http.Header{"Content-Type": []string{"application/json"}},
				Body: &failingReadCloser{}, ContentLength: -1,
			}, nil
		})},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct reader-failure writer: %v", err)
	}
	if _, err := writer.DryRun(context.Background(), plan, decision); !errors.Is(err, ErrResponse) {
		t.Fatalf("response read error = %v, want ErrResponse", err)
	}
}

func TestSecretWriterProductionDependencyBoundary(t *testing.T) {
	directOutput, err := exec.Command("go", "list", "-f", "{{join .Imports \"\\n\"}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("list direct dependencies: %v output=%s", err, directOutput)
	}
	direct := strings.Fields(string(directOutput))
	sort.Strings(direct)
	wantDirect := []string{
		"bytes", "context", "crypto/sha256", "encoding/base64", "encoding/hex", "encoding/json", "errors", "fmt", "fugue/internal/backupmaterializer/materialization", "fugue/internal/backupmaterializer/reconcile", "io", "mime", "net/http", "net/url", "reflect", "strconv", "strings", "time",
	}
	sort.Strings(wantDirect)
	if !reflect.DeepEqual(direct, wantDirect) {
		t.Fatalf("direct dependency boundary widened: got=%v want=%v", direct, wantDirect)
	}
	allOutput, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("list dependency closure: %v output=%s", err, allOutput)
	}
	dependencies := strings.Fields(string(allOutput))
	for _, forbidden := range []string{
		"database/sql", "os/exec", "fugue/internal/api", "fugue/internal/auth", "fugue/internal/backupidentity",
		"fugue/internal/backupmaterializer/composition", "fugue/internal/backupmaterializer/httpapi",
		"fugue/internal/backupmaterializer/localissuer", "fugue/internal/backupmaterializer/storesource",
		"fugue/internal/backupmaterializeridentity", "fugue/internal/backupmaterializerreview",
		"fugue/internal/controller", "fugue/internal/model", "fugue/internal/store", "k8s.io/client-go",
	} {
		for _, dependency := range dependencies {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Fatalf("production dry-run writer gained forbidden dependency %q", dependency)
			}
		}
	}
}

func assertDryRunRequest(
	t *testing.T,
	request *http.Request,
	wantMethod string,
	wantPath string,
	plan materialization.Plan,
	decision reconcile.Decision,
	now time.Time,
) secretDocument {
	t.Helper()
	if request.Method != wantMethod || request.URL.Path != wantPath || request.URL.RawQuery != dryRunQuery() ||
		request.URL.Fragment != "" || request.URL.User != nil || !request.Close {
		t.Fatalf("request boundary drifted: method=%s URL=%s close=%t", request.Method, request.URL.String(), request.Close)
	}
	for name, want := range map[string]string{
		"Accept":          "application/json",
		"Accept-Encoding": "identity",
		"Authorization":   "Bearer " + testWorkloadToken,
		"Cache-Control":   "no-store",
		"Content-Type":    "application/json",
		"User-Agent":      RequestUserAgent,
	} {
		if got := request.Header.Get(name); got != want {
			t.Fatalf("request header %s = %q, want %q", name, got, want)
		}
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumRequestBytes+1))
	if err != nil || len(body) == 0 || int64(len(body)) > maximumRequestBytes {
		t.Fatalf("read bounded request: bytes=%d err=%v", len(body), err)
	}
	var wire secretDocument
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		t.Fatalf("decode request body: %v body=%s", err, body)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("request contains trailing JSON: %v", err)
	}
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	if wire.APIVersion != "v1" || wire.Kind != "Secret" || wire.Metadata.Name != plan.SecretName ||
		wire.Metadata.Namespace != plan.Namespace || wire.Type != reconcile.SecretTypeOpaque || wire.Immutable == nil || *wire.Immutable ||
		!reflect.DeepEqual(wire.Metadata.Labels, manifest.Labels) || !reflect.DeepEqual(wire.Metadata.Annotations, manifest.Annotations) ||
		len(wire.Data) != 2 || wire.Data[data.SpecKey] == "" || wire.Data[data.TokenKey] == "" || len(wire.StringData) != 0 ||
		len(wire.Metadata.OwnerReferences) != 0 || len(wire.Metadata.Finalizers) != 0 {
		t.Fatalf("request Secret drifted: %+v", wire)
	}
	if decision.Action == reconcile.ActionCreateIfAbsent && (wire.Metadata.UID != "" || wire.Metadata.ResourceVersion != "") {
		t.Fatalf("create request unexpectedly contains CAS values: %+v", wire.Metadata)
	}
	return wire
}

func writeSecretResponse(t *testing.T, response http.ResponseWriter, status int, wire secretDocument) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(status)
	if err := json.NewEncoder(response).Encode(wire); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func testWriter(t *testing.T, server *httptest.Server, cellKey string, credential CredentialSource, now time.Time) *Writer {
	t.Helper()
	writer, err := New(Config{
		Enabled: true, APIServerURL: server.URL, ExpectedCellKey: cellKey, CredentialSource: credential,
		HTTPClient: server.Client(), Now: func() time.Time { return now }, AllowInsecureHTTPForTests: true,
	})
	if err != nil {
		t.Fatalf("construct writer: %v", err)
	}
	return writer
}

func testCreateDecision(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	absent, err := reconcile.ObserveAbsent(plan.CellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	decision, err := reconcile.Decide(plan.CellKey, &plan, absent, now)
	if err != nil {
		t.Fatalf("decide create: %v", err)
	}
	return decision
}

func testReplaceDecision(t *testing.T, previous, desired materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	current := testManagedObservation(t, previous, now)
	decision, err := reconcile.Decide(desired.CellKey, &desired, current, now)
	if err != nil {
		t.Fatalf("decide replace: %v", err)
	}
	return decision
}

func testManagedObservation(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Observation {
	t.Helper()
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	snapshot, err := reconcile.SealCurrent(plan, reconcile.SecretEvidence{
		Namespace: plan.Namespace, SecretName: plan.SecretName, UID: "01234567-89ab-cdef-0123-456789abcdef",
		ResourceVersion: "42", SecretType: manifest.SecretType, Labels: cloneStringMap(manifest.Labels),
		Annotations: cloneStringMap(manifest.Annotations), Data: map[string][]byte{
			data.SpecKey: append([]byte(nil), data.SpecDocument...), data.TokenKey: append([]byte(nil), data.ObserverToken...),
		},
	})
	if err != nil {
		t.Fatalf("seal current: %v", err)
	}
	observation, err := reconcile.ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe managed: %v", err)
	}
	return observation
}

func testPlan(t *testing.T, runID string, now time.Time) materialization.Plan {
	t.Helper()
	return testPlanForTarget(t, runID, backupcontrol.BackupTarget{
		Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database",
	}, now)
}

func testPlanForTarget(t *testing.T, runID string, target backupcontrol.BackupTarget, now time.Time) materialization.Plan {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID, runID, target, "backend-1",
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 4, 120, 1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	issuedAt := now.Add(-30 * time.Second)
	tokenID := "AAAAAAAAAAAAAAAAAAAAAA"
	claims := struct {
		Version       string `json:"v"`
		CredentialID  string `json:"credential_id"`
		TokenID       string `json:"token_id"`
		RunID         string `json:"run_id"`
		TenantID      string `json:"tenant_id"`
		CellKey       string `json:"cell_key"`
		SpecDigest    string `json:"spec_digest"`
		Permission    string `json:"permission"`
		IssuedAtUnix  int64  `json:"issued_at"`
		ExpiresAtUnix int64  `json:"expires_at"`
	}{
		Version: "v1", CredentialID: materializercontract.CredentialIDForCell(spec.CellKey), TokenID: tokenID,
		RunID: spec.RunID, TenantID: "tenant-1", CellKey: spec.CellKey, SpecDigest: spec.Digest,
		Permission: materializercontract.ObserverIdentityPermission, IssuedAtUnix: issuedAt.Unix(),
		ExpiresAtUnix: issuedAt.Add(materializercontract.ObserverIdentityTTL).Unix(),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal test claims: %v", err)
	}
	observerToken := "fugue_bo_v1." +
		base64.RawURLEncoding.EncodeToString([]byte("backup-key-1")) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{'s'}, 32))
	bundle := materializercontract.ObserverInputBundle{
		APIVersion: materializercontract.ObserverInputBundleAPIVersion, Kind: materializercontract.ObserverInputBundleKind,
		Policy: materializercontract.ObserverInputBundlePolicy, CellKey: spec.CellKey, RunID: spec.RunID,
		SpecDigest: spec.Digest, CredentialID: claims.CredentialID, TokenID: tokenID, DesiredSpec: spec,
		ObserverToken: observerToken, IssuedAt: issuedAt,
		RenewAfter: issuedAt.Add(materializercontract.ObserverIdentityRenewAfter),
		ExpiresAt:  issuedAt.Add(materializercontract.ObserverIdentityTTL), ObservationOnly: true,
		ProductionMutationAllowed: false,
	}
	bundle.Digest = materializercontract.DigestObserverInputBundle(bundle)
	plan, err := materialization.Build(bundle, now)
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	return plan
}

func testNow() time.Time {
	return time.Date(2026, 7, 31, 7, 0, 0, 0, time.UTC)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type failingReadCloser struct{}

func (*failingReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("private response read detail")
}
func (*failingReadCloser) Close() error { return nil }
