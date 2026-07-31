package secretdryrunrequest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
)

func TestPrepareSealsImmutableCreateAndReplaceRequests(t *testing.T) {
	t.Parallel()
	now := requestTestNow()
	previous := requestTestPlan(t, "request-previous", now)
	desired := requestTestPlan(t, "request-desired", now)
	create := requestTestCreateDecision(t, previous, now)
	replace := requestTestReplaceDecision(t, previous, desired, now)

	tests := []struct {
		name           string
		plan           materialization.Plan
		decision       reconcile.Decision
		method         string
		path           string
		expectedStatus int
	}{
		{name: "create", plan: previous, decision: create, method: CreateMethod, path: SecretCollectionPath, expectedStatus: CreateExpectedStatus},
		{name: "replace", plan: desired, decision: replace, method: ReplaceMethod, path: SecretCollectionPath + "/" + desired.SecretName, expectedStatus: ReplaceExpectedStatus},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request, err := Prepare(test.plan.CellKey, test.plan, test.decision, now)
			if err != nil || Validate(request, test.plan.CellKey, now) != nil {
				t.Fatalf("prepare request: err=%v validation=%v", err, Validate(request, test.plan.CellKey, now))
			}
			evidence, document, err := request.Open(test.plan.CellKey, now)
			if err != nil || evidence.APIVersion != APIVersion || evidence.Kind != Kind || evidence.Policy != Policy ||
				evidence.CellKey != test.plan.CellKey || evidence.CellID != test.plan.CellID ||
				evidence.Namespace != test.plan.Namespace || evidence.SecretName != test.plan.SecretName ||
				evidence.RunID != test.plan.RunID || evidence.Action != test.decision.Action ||
				evidence.PlanDigest != test.plan.Digest || evidence.DecisionDigest != test.decision.Digest ||
				evidence.Method != test.method || evidence.Path != test.path || evidence.RawQuery != DryRunRawQuery ||
				evidence.ExpectedStatus != test.expectedStatus || evidence.RequestDigest != digestBytes(document) ||
				evidence.DecidedAt != test.decision.DecidedAt || evidence.PreparedAt != now ||
				evidence.ExpiresAt != test.decision.DecidedAt.Add(MaximumDecisionAge) || !evidence.OneShot ||
				evidence.RetriesAllowed || !evidence.ServerSideDryRun || evidence.Persisted || evidence.DeleteAllowed ||
				evidence.ExecutionAllowed || evidence.ProductionMutationAllowed || evidence.Digest != DigestEvidence(evidence) {
				t.Fatalf("sealed evidence drifted: evidence=%#v bytes=%d err=%v", evidence, len(document), err)
			}
			if test.decision.Action == reconcile.ActionCreateIfAbsent {
				if !evidence.RequireAbsent || evidence.RequireUIDMatch || evidence.RequireResourceVersionCAS ||
					evidence.RetainExisting || evidence.ExpectedUID != "" || evidence.ExpectedResourceVersion != "" {
					t.Fatalf("create preconditions drifted: %#v", evidence)
				}
			} else if evidence.RequireAbsent || !evidence.RequireUIDMatch || !evidence.RequireResourceVersionCAS ||
				!evidence.RetainExisting || evidence.ExpectedUID != test.decision.ExpectedUID ||
				evidence.ExpectedResourceVersion != test.decision.ExpectedResourceVersion {
				t.Fatalf("replace preconditions drifted: %#v", evidence)
			}
			if got := request.Evidence(); !reflect.DeepEqual(got, evidence) {
				t.Fatalf("Evidence projection drifted: got=%#v want=%#v", got, evidence)
			}
			restored, err := Restore(evidence, document, test.plan.CellKey, now)
			if err != nil || Validate(restored, test.plan.CellKey, now) != nil {
				t.Fatalf("restore sealed handoff: err=%v validation=%v", err, Validate(restored, test.plan.CellKey, now))
			}
			_, restoredDocument, err := restored.Open(test.plan.CellKey, now)
			if err != nil || !bytes.Equal(restoredDocument, document) {
				t.Fatalf("restored document drifted: equal=%t err=%v", bytes.Equal(restoredDocument, document), err)
			}

			// Every public return is a copy; neither evidence nor a body returned
			// by Open can mutate the sealed request.
			evidence.CellKey = "backup/app-database/0000000000000000"
			document[0] = 'x'
			secondEvidence, secondDocument, err := request.Open(test.plan.CellKey, now)
			if err != nil || secondEvidence.CellKey != test.plan.CellKey || secondDocument[0] == 'x' {
				t.Fatalf("caller mutated sealed request: evidence=%#v firstByte=%q err=%v", secondEvidence, secondDocument[0], err)
			}

			// Default JSON and diagnostic formatting expose only evidence.
			encodedRequest, err := json.Marshal(request)
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			var decodedRequest Request
			if err := json.Unmarshal(encodedRequest, &decodedRequest); !errors.Is(err, ErrRequest) {
				t.Fatalf("public evidence JSON reconstructed a private request: %v", err)
			}
			data, err := test.plan.Data(now)
			if err != nil {
				t.Fatalf("open plan fixture: %v", err)
			}
			public := string(encodedRequest) + fmt.Sprintf(" %v %#v", request, request)
			for _, sensitive := range []string{
				string(data.SpecDocument), string(data.ObserverToken),
				base64.StdEncoding.EncodeToString(data.SpecDocument),
				base64.StdEncoding.EncodeToString(data.ObserverToken),
			} {
				if strings.Contains(public, sensitive) {
					t.Fatalf("public request surface leaked private material %q", sensitive)
				}
			}
			if Validate(request, test.plan.CellKey, evidence.ExpiresAt) != nil ||
				!errors.Is(Validate(request, test.plan.CellKey, evidence.ExpiresAt.Add(time.Second)), ErrRequest) {
				t.Fatalf("request replay deadline drifted")
			}
		})
	}
}

func TestTransportValidatorRejectsEveryNonCanonicalMutationShape(t *testing.T) {
	t.Parallel()
	now := requestTestNow()
	plan := requestTestPlan(t, "request-transport", now)
	request, err := Prepare(plan.CellKey, plan, requestTestCreateDecision(t, plan, now), now)
	if err != nil {
		t.Fatalf("prepare transport fixture: %v", err)
	}
	evidence, document, err := request.Open(plan.CellKey, now)
	if err != nil {
		t.Fatalf("open transport fixture: %v", err)
	}
	if err := ValidateTransportRequest(evidence.Method, evidence.Path, evidence.RawQuery, plan.CellKey, document); err != nil {
		t.Fatalf("canonical transport rejected: %v", err)
	}
	if _, _, err := (Request{}).Open(plan.CellKey, now); !errors.Is(err, ErrRequest) {
		t.Fatalf("zero request opened: %v", err)
	}

	tests := map[string]struct {
		method   string
		path     string
		query    string
		cellKey  string
		document []byte
		mutate   func(*secretDocument)
	}{
		"wrong method":       {method: "DELETE"},
		"wrong path":         {path: evidence.Path + "/other"},
		"wrong query":        {query: "dryRun=All"},
		"wrong cell":         {cellKey: "backup/registry/0000000000000000"},
		"empty body":         {document: []byte{}},
		"oversized body":     {document: bytes.Repeat([]byte{'x'}, int(MaximumRequestBytes)+1)},
		"leading whitespace": {document: append([]byte{' '}, document...)},
		"trailing JSON":      {document: append(append([]byte(nil), document...), []byte(` {}`)...)},
		"unknown field":      {document: bytes.Replace(document, []byte(`{"apiVersion":"v1"`), []byte(`{"unknown":true,"apiVersion":"v1"`), 1)},
		"immutable": {mutate: func(value *secretDocument) {
			immutable := true
			value.Immutable = &immutable
		}},
		"owner reference": {mutate: func(value *secretDocument) {
			value.Metadata.OwnerReferences = []json.RawMessage{json.RawMessage(`{"uid":"other"}`)}
		}},
		"managed fields": {mutate: func(value *secretDocument) {
			value.Metadata.ManagedFields = []json.RawMessage{json.RawMessage(`{"manager":"other"}`)}
		}},
		"non-canonical base64": {mutate: func(value *secretDocument) {
			value.Data[materialization.TokenDataKey] = "***"
		}},
		"missing owned label": {mutate: func(value *secretDocument) {
			delete(value.Metadata.Labels, reconcile.LabelName)
			value.Metadata.Labels["replacement.example.test/name"] = "other"
		}},
		"missing owned annotation": {mutate: func(value *secretDocument) {
			delete(value.Metadata.Annotations, reconcile.AnnotationExpiresAt)
			value.Metadata.Annotations["replacement.example.test/expires"] = "other"
		}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			method := evidence.Method
			if test.method != "" {
				method = test.method
			}
			path := evidence.Path
			if test.path != "" {
				path = test.path
			}
			query := evidence.RawQuery
			if test.query != "" {
				query = test.query
			}
			cellKey := plan.CellKey
			if test.cellKey != "" {
				cellKey = test.cellKey
			}
			candidate := append([]byte(nil), document...)
			if test.document != nil {
				candidate = append([]byte(nil), test.document...)
			} else if test.mutate != nil {
				var decoded secretDocument
				if err := json.Unmarshal(candidate, &decoded); err != nil {
					t.Fatalf("decode fixture: %v", err)
				}
				test.mutate(&decoded)
				var err error
				candidate, err = json.Marshal(decoded)
				if err != nil {
					t.Fatalf("encode mutation: %v", err)
				}
			}
			if err := ValidateTransportRequest(method, path, query, cellKey, candidate); !errors.Is(err, ErrRequest) {
				t.Fatalf("transport mutation accepted: %v", err)
			}
		})
	}
}

func TestPrepareRejectsInvalidOrAgedIntentBeforeSealing(t *testing.T) {
	t.Parallel()
	now := requestTestNow()
	plan := requestTestPlan(t, "request-invalid", now)
	create := requestTestCreateDecision(t, plan, now)
	other := requestTestPlanForTarget(t, "request-other", backupcontrol.BackupTarget{
		Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry",
	}, now)
	managed := requestTestManagedObservation(t, plan, now)
	noop, err := reconcile.Decide(plan.CellKey, &plan, managed, now)
	if err != nil {
		t.Fatalf("build no-op decision: %v", err)
	}
	oldDecisionAt := now.Add(-MaximumDecisionAge - time.Second)
	oldCreate, err := reconcile.Decide(plan.CellKey, &plan, mustObserveAbsent(t, plan.CellKey), oldDecisionAt)
	if err != nil {
		t.Fatalf("build old decision: %v", err)
	}
	futureCreate, err := reconcile.Decide(plan.CellKey, &plan, mustObserveAbsent(t, plan.CellKey), now.Add(time.Second))
	if err != nil {
		t.Fatalf("build future decision: %v", err)
	}
	tests := []struct {
		name     string
		cellKey  string
		plan     materialization.Plan
		decision reconcile.Decision
		now      time.Time
	}{
		{name: "wrong expected cell", cellKey: other.CellKey, plan: plan, decision: create, now: now},
		{name: "different plan", cellKey: plan.CellKey, plan: other, decision: create, now: now},
		{name: "no-op", cellKey: plan.CellKey, plan: plan, decision: noop, now: now},
		{name: "aged decision", cellKey: plan.CellKey, plan: plan, decision: oldCreate, now: now},
		{name: "future decision", cellKey: plan.CellKey, plan: plan, decision: futureCreate, now: now},
		{name: "zero clock", cellKey: plan.CellKey, plan: plan, decision: create, now: time.Time{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Prepare(test.cellKey, test.plan, test.decision, test.now); !errors.Is(err, ErrRequest) {
				t.Fatalf("Prepare error = %v, want ErrRequest", err)
			}
		})
	}
}

func TestRestoreRejectsEvidenceAndDocumentDriftEvenWhenRedigested(t *testing.T) {
	t.Parallel()
	now := requestTestNow()
	previous := requestTestPlan(t, "request-restore-previous", now)
	desired := requestTestPlan(t, "request-restore-desired", now)
	createRequest, err := Prepare(previous.CellKey, previous, requestTestCreateDecision(t, previous, now), now)
	if err != nil {
		t.Fatalf("prepare create: %v", err)
	}
	createEvidence, createDocument, err := createRequest.Open(previous.CellKey, now)
	if err != nil {
		t.Fatalf("open create: %v", err)
	}

	evidenceTests := map[string]func(*Evidence){
		"live query":                func(value *Evidence) { value.RawQuery = "fieldManager=" + FieldManager },
		"wrong method":              func(value *Evidence) { value.Method = ReplaceMethod },
		"wrong status":              func(value *Evidence) { value.ExpectedStatus = ReplaceExpectedStatus },
		"wrong path":                func(value *Evidence) { value.Path += "/" + value.SecretName },
		"extended deadline":         func(value *Evidence) { value.ExpiresAt = value.ExpiresAt.Add(time.Second) },
		"future preparation":        func(value *Evidence) { value.PreparedAt = value.PreparedAt.Add(time.Second) },
		"retry permission":          func(value *Evidence) { value.RetriesAllowed = true },
		"production mutation":       func(value *Evidence) { value.ProductionMutationAllowed = true },
		"lost absence precondition": func(value *Evidence) { value.RequireAbsent = false },
		"body digest":               func(value *Evidence) { value.RequestDigest = "sha256:" + strings.Repeat("0", 64) },
		"plan binding":              func(value *Evidence) { value.PlanDigest = "sha256:" + strings.Repeat("0", 64) },
		"uppercase digest":          func(value *Evidence) { value.PlanDigest = "sha256:" + strings.Repeat("A", 64) },
	}
	for name, mutate := range evidenceTests {
		t.Run(name, func(t *testing.T) {
			evidence := createEvidence
			mutate(&evidence)
			evidence.Digest = DigestEvidence(evidence)
			if _, err := Restore(evidence, createDocument, previous.CellKey, now); !errors.Is(err, ErrRequest) {
				t.Fatalf("redigested evidence drift accepted: err=%v evidence=%#v", err, evidence)
			}
		})
	}

	documentTests := map[string]func(*secretDocument){
		"extra label": func(value *secretDocument) {
			value.Metadata.Labels["extra.example.test/value"] = "extra"
		},
		"extra annotation": func(value *secretDocument) {
			value.Metadata.Annotations["extra.example.test/value"] = "extra"
		},
		"generated name": func(value *secretDocument) { value.Metadata.GenerateName = "unexpected-" },
		"extra data":     func(value *secretDocument) { value.Data["extra"] = "eA==" },
		"changed token": func(value *secretDocument) {
			value.Data[materialization.TokenDataKey] = base64.StdEncoding.EncodeToString([]byte("different"))
		},
	}
	for name, mutate := range documentTests {
		t.Run(name, func(t *testing.T) {
			var document secretDocument
			if err := json.Unmarshal(createDocument, &document); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			mutate(&document)
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatalf("encode mutation: %v", err)
			}
			evidence := createEvidence
			evidence.RequestDigest = digestBytes(encoded)
			evidence.Digest = DigestEvidence(evidence)
			if _, err := Restore(evidence, encoded, previous.CellKey, now); !errors.Is(err, ErrRequest) {
				t.Fatalf("redigested body drift accepted: err=%v", err)
			}
		})
	}

	replaceRequest, err := Prepare(desired.CellKey, desired, requestTestReplaceDecision(t, previous, desired, now), now)
	if err != nil {
		t.Fatalf("prepare replace: %v", err)
	}
	replaceEvidence, replaceDocument, err := replaceRequest.Open(desired.CellKey, now)
	if err != nil {
		t.Fatalf("open replace: %v", err)
	}
	var withoutCAS secretDocument
	if err := json.Unmarshal(replaceDocument, &withoutCAS); err != nil {
		t.Fatalf("decode replace: %v", err)
	}
	withoutCAS.Metadata.UID = ""
	withoutCAS.Metadata.ResourceVersion = ""
	encodedWithoutCAS, err := json.Marshal(withoutCAS)
	if err != nil {
		t.Fatalf("encode replace without CAS: %v", err)
	}
	replaceEvidence.ExpectedUID = ""
	replaceEvidence.ExpectedResourceVersion = ""
	replaceEvidence.RequestDigest = digestBytes(encodedWithoutCAS)
	replaceEvidence.Digest = DigestEvidence(replaceEvidence)
	if _, err := Restore(replaceEvidence, encodedWithoutCAS, desired.CellKey, now); !errors.Is(err, ErrRequest) {
		t.Fatalf("replace without CAS accepted: %v", err)
	}
	for name, invalidUID := range map[string]string{
		"leading whitespace": " invalid", "newline": "invalid\n", "too long": strings.Repeat("x", 257),
	} {
		t.Run("replace UID "+name, func(t *testing.T) {
			evidence := replaceRequest.Evidence()
			evidence.ExpectedUID = invalidUID
			evidence.Digest = DigestEvidence(evidence)
			if _, err := Restore(evidence, replaceDocument, desired.CellKey, now); !errors.Is(err, ErrRequest) {
				t.Fatalf("invalid UID accepted: %v", err)
			}
		})
	}

	if _, err := Restore(createEvidence, append([]byte{' '}, createDocument...), previous.CellKey, now); !errors.Is(err, ErrRequest) {
		t.Fatalf("non-canonical document accepted: %v", err)
	}
}

func TestSecretDryRunRequestProductionDependencyBoundary(t *testing.T) {
	directOutput, err := exec.Command("go", "list", "-f", "{{join .Imports \"\\n\"}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("list direct dependencies: %v output=%s", err, directOutput)
	}
	direct := strings.Fields(string(directOutput))
	sort.Strings(direct)
	wantDirect := []string{
		"bytes", "crypto/sha256", "encoding/base64", "encoding/hex", "encoding/json", "errors", "fmt",
		"fugue/internal/backupmaterializer/materialization", "fugue/internal/backupmaterializer/reconcile",
		"io", "strings", "time",
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
		"context", "database/sql", "net", "net/http", "os/exec",
		"fugue/internal/api", "fugue/internal/auth", "fugue/internal/backupidentity",
		"fugue/internal/backupmaterializer/composition", "fugue/internal/backupmaterializer/httpapi",
		"fugue/internal/backupmaterializer/localissuer", "fugue/internal/backupmaterializer/secretwriter",
		"fugue/internal/backupmaterializer/storesource", "fugue/internal/backupmaterializeridentity",
		"fugue/internal/backupmaterializerreview", "fugue/internal/controller", "fugue/internal/model",
		"fugue/internal/store", "k8s.io/client-go",
	} {
		for _, dependency := range dependencies {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Fatalf("pure dry-run request gained forbidden dependency %q", dependency)
			}
		}
	}
}

func requestTestCreateDecision(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	decision, err := reconcile.Decide(plan.CellKey, &plan, mustObserveAbsent(t, plan.CellKey), now)
	if err != nil {
		t.Fatalf("decide create: %v", err)
	}
	return decision
}

func requestTestReplaceDecision(t *testing.T, previous, desired materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	decision, err := reconcile.Decide(desired.CellKey, &desired, requestTestManagedObservation(t, previous, now), now)
	if err != nil {
		t.Fatalf("decide replace: %v", err)
	}
	return decision
}

func mustObserveAbsent(t *testing.T, cellKey string) reconcile.Observation {
	t.Helper()
	observation, err := reconcile.ObserveAbsent(cellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	return observation
}

func requestTestManagedObservation(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Observation {
	t.Helper()
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("open plan data: %v", err)
	}
	snapshot, err := reconcile.SealCurrent(plan, reconcile.SecretEvidence{
		Namespace: plan.Namespace, SecretName: plan.SecretName,
		UID: "01234567-89ab-cdef-0123-456789abcdef", ResourceVersion: "42",
		SecretType: manifest.SecretType, Labels: cloneStringMap(manifest.Labels),
		Annotations: cloneStringMap(manifest.Annotations), Data: map[string][]byte{
			data.SpecKey:  append([]byte(nil), data.SpecDocument...),
			data.TokenKey: append([]byte(nil), data.ObserverToken...),
		},
	})
	if err != nil {
		t.Fatalf("seal managed observation: %v", err)
	}
	observation, err := reconcile.ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe managed: %v", err)
	}
	return observation
}

func requestTestPlan(t *testing.T, runID string, now time.Time) materialization.Plan {
	t.Helper()
	return requestTestPlanForTarget(t, runID, backupcontrol.BackupTarget{
		Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database",
	}, now)
}

func requestTestPlanForTarget(t *testing.T, runID string, target backupcontrol.BackupTarget, now time.Time) materialization.Plan {
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
		t.Fatalf("marshal claims: %v", err)
	}
	observerToken := "fugue_bo_v1." +
		base64.RawURLEncoding.EncodeToString([]byte("backup-key-1")) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{'s'}, 32))
	bundle := materializercontract.ObserverInputBundle{
		APIVersion: materializercontract.ObserverInputBundleAPIVersion,
		Kind:       materializercontract.ObserverInputBundleKind, Policy: materializercontract.ObserverInputBundlePolicy,
		CellKey: spec.CellKey, RunID: spec.RunID, SpecDigest: spec.Digest,
		CredentialID: claims.CredentialID, TokenID: tokenID, DesiredSpec: spec, ObserverToken: observerToken,
		IssuedAt: issuedAt, RenewAfter: issuedAt.Add(materializercontract.ObserverIdentityRenewAfter),
		ExpiresAt: issuedAt.Add(materializercontract.ObserverIdentityTTL), ObservationOnly: true,
		ProductionMutationAllowed: false,
	}
	bundle.Digest = materializercontract.DigestObserverInputBundle(bundle)
	plan, err := materialization.Build(bundle, now)
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	return plan
}

func requestTestNow() time.Time {
	return time.Date(2026, 7, 31, 7, 0, 0, 0, time.UTC)
}
