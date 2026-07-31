package dryrungatewaycontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/secretdryrunrequest"
)

func TestRequestWireRoundTripsCanonicalCreateAndReplace(t *testing.T) {
	t.Parallel()
	now := gatewayTestNow()
	previous := gatewayTestPlan(t, "gateway-previous", gatewayTestTarget(), now)
	desired := gatewayTestPlan(t, "gateway-desired", gatewayTestTarget(), now)
	tests := []struct {
		name     string
		plan     materialization.Plan
		decision reconcile.Decision
	}{
		{name: "create", plan: previous, decision: gatewayCreateDecision(t, previous, now)},
		{name: "replace", plan: desired, decision: gatewayReplaceDecision(t, previous, desired, now)},
	}
	if RoutePath != "/v1/secret-dry-run" || MediaType != "application/json" ||
		MaximumRequestBytes <= int(secretdryrunrequest.MaximumRequestBytes) || MaximumReceiptBytes < 1024 {
		t.Fatalf("gateway constants drifted: route=%q media=%q request=%d receipt=%d", RoutePath, MediaType, MaximumRequestBytes, MaximumReceiptBytes)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request, err := secretdryrunrequest.Prepare(test.plan.CellKey, test.plan, test.decision, now)
			if err != nil {
				t.Fatalf("prepare request: %v", err)
			}
			evidence, secretDocument, err := request.Open(test.plan.CellKey, now)
			if err != nil {
				t.Fatalf("open request: %v", err)
			}
			first, err := EncodeRequest(request, test.plan.CellKey, now)
			if err != nil {
				t.Fatalf("encode request: %v", err)
			}
			second, err := EncodeRequest(request, test.plan.CellKey, now)
			if err != nil || !bytes.Equal(first, second) || len(first) > MaximumRequestBytes {
				t.Fatalf("wire request is not bounded and deterministic: equal=%t bytes=%d err=%v", bytes.Equal(first, second), len(first), err)
			}
			wire := gatewayDecodeWire(t, first)
			if wire.APIVersion != APIVersion || wire.Kind != Kind || wire.Policy != Policy ||
				wire.CellKey != evidence.CellKey || wire.CellID != evidence.CellID ||
				!reflect.DeepEqual(wire.Request, evidence) || !bytes.Equal(wire.SecretDocument, secretDocument) ||
				wire.IdempotencyKey != evidence.IdempotencyKey || !wire.OneShot || wire.RetriesAllowed ||
				wire.ProductionMutationAllowed || wire.Digest != digestWire(wire) {
				t.Fatalf("wire envelope drifted: %#v", wire)
			}
			decoded, err := DecodeRequest(first, test.plan.CellKey, now)
			if err != nil {
				t.Fatalf("decode request: %v", err)
			}
			decodedEvidence, decodedSecret, err := decoded.Open(test.plan.CellKey, now)
			if err != nil || !reflect.DeepEqual(decodedEvidence, evidence) || !bytes.Equal(decodedSecret, secretDocument) {
				t.Fatalf("decoded request drifted: evidenceEqual=%t bodyEqual=%t err=%v", reflect.DeepEqual(decodedEvidence, evidence), bytes.Equal(decodedSecret, secretDocument), err)
			}

			// The private wire must carry the exact Secret, while all default
			// request and receipt formatting surfaces remain secret-free.
			data, err := test.plan.Data(now)
			if err != nil {
				t.Fatalf("open plan data: %v", err)
			}
			encodedSpec := base64.StdEncoding.EncodeToString(data.SpecDocument)
			encodedToken := base64.StdEncoding.EncodeToString(data.ObserverToken)
			if !bytes.Contains(first, []byte(encodedSpec)) || !bytes.Contains(first, []byte(encodedToken)) {
				t.Fatal("private gateway envelope omitted the exact Secret data")
			}
			publicJSON, err := json.Marshal(decoded)
			if err != nil {
				t.Fatalf("marshal public request projection: %v", err)
			}
			public := string(publicJSON) + fmt.Sprintf(" %v %#v", decoded, decoded)
			for _, sensitive := range []string{string(data.SpecDocument), string(data.ObserverToken), encodedSpec, encodedToken} {
				if strings.Contains(public, sensitive) {
					t.Fatalf("public request surface leaked private material %q", sensitive)
				}
			}

			// Both egress and ingress own copies rather than aliases supplied by
			// their callers.
			first[0] = 'x'
			again, err := EncodeRequest(request, test.plan.CellKey, now)
			if err != nil || !bytes.Equal(again, second) {
				t.Fatalf("caller mutated encoded request or source request: equal=%t err=%v", bytes.Equal(again, second), err)
			}
			again[0] = 'x'
			_, reopened, err := decoded.Open(test.plan.CellKey, now)
			if err != nil || !bytes.Equal(reopened, secretDocument) {
				t.Fatalf("caller mutated decoded sealed request: equal=%t err=%v", bytes.Equal(reopened, secretDocument), err)
			}

			receipt := gatewayReceipt(request, now)
			receiptDocument, err := EncodeReceipt(receipt, request, test.plan.CellKey, now)
			if err != nil || len(receiptDocument) > MaximumReceiptBytes {
				t.Fatalf("encode receipt: bytes=%d err=%v", len(receiptDocument), err)
			}
			decodedReceipt, err := DecodeReceipt(receiptDocument, request, test.plan.CellKey, now)
			if err != nil || !reflect.DeepEqual(decodedReceipt, receipt) {
				t.Fatalf("receipt round trip drifted: equal=%t err=%v", reflect.DeepEqual(decodedReceipt, receipt), err)
			}
			for _, sensitive := range []string{string(data.SpecDocument), string(data.ObserverToken), encodedSpec, encodedToken} {
				if strings.Contains(string(receiptDocument)+fmt.Sprintf(" %v %#v", decodedReceipt, decodedReceipt), sensitive) {
					t.Fatalf("receipt leaked private material %q", sensitive)
				}
			}

			boundaryReceipt := receipt
			boundaryReceipt.ValidatedAt = evidence.ExpiresAt
			boundaryReceipt.Digest = secretdryrunrequest.DigestReceipt(boundaryReceipt)
			boundaryDocument, err := EncodeReceipt(boundaryReceipt, request, test.plan.CellKey, evidence.ExpiresAt)
			if err != nil {
				t.Fatalf("receipt at exact request deadline rejected: %v", err)
			}
			if _, err := DecodeReceipt(boundaryDocument, request, test.plan.CellKey, evidence.ExpiresAt.Add(time.Second)); !errors.Is(err, ErrContract) {
				t.Fatalf("expired request replay accepted: %v", err)
			}
		})
	}
}

func TestRequestWireRejectsCanonicalRedigestedSemanticDrift(t *testing.T) {
	t.Parallel()
	now := gatewayTestNow()
	plan := gatewayTestPlan(t, "gateway-mutation", gatewayTestTarget(), now)
	request, err := secretdryrunrequest.Prepare(plan.CellKey, plan, gatewayCreateDecision(t, plan, now), now)
	if err != nil {
		t.Fatalf("prepare request: %v", err)
	}
	document, err := EncodeRequest(request, plan.CellKey, now)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	mutations := map[string]func(*wireRequest){
		"API version": func(value *wireRequest) { value.APIVersion = "v2" },
		"kind":        func(value *wireRequest) { value.Kind = "Other" },
		"policy":      func(value *wireRequest) { value.Policy = "live-write" },
		"cell key":    func(value *wireRequest) { value.CellKey = "backup/registry/0000000000000000" },
		"cell ID":     func(value *wireRequest) { value.CellID += "-other" },
		"request cell key": func(value *wireRequest) {
			value.Request.CellKey = "backup/registry/0000000000000000"
			gatewayRedigestEvidence(value)
		},
		"request cell ID":      func(value *wireRequest) { value.Request.CellID += "-other"; gatewayRedigestEvidence(value) },
		"envelope idempotency": func(value *wireRequest) { value.IdempotencyKey += "-other" },
		"request idempotency": func(value *wireRequest) {
			value.Request.IdempotencyKey += "-other"
			value.IdempotencyKey = value.Request.IdempotencyKey
			gatewayRedigestEvidence(value)
		},
		"retry permission":    func(value *wireRequest) { value.RetriesAllowed = true },
		"lost one shot":       func(value *wireRequest) { value.OneShot = false },
		"production mutation": func(value *wireRequest) { value.ProductionMutationAllowed = true },
		"empty Secret":        func(value *wireRequest) { value.SecretDocument = nil },
		"plan binding": func(value *wireRequest) {
			value.Request.PlanDigest = "sha256:" + strings.Repeat("0", 64)
			gatewayRedigestEvidence(value)
		},
		"extended deadline": func(value *wireRequest) {
			value.Request.ExpiresAt = value.Request.ExpiresAt.Add(time.Second)
			gatewayRedigestEvidence(value)
		},
		"redigested Secret drift": func(value *wireRequest) {
			var secret map[string]any
			if err := json.Unmarshal(value.SecretDocument, &secret); err != nil {
				t.Fatalf("decode Secret fixture: %v", err)
			}
			secret["unexpected"] = true
			encoded, err := json.Marshal(secret)
			if err != nil {
				t.Fatalf("encode Secret mutation: %v", err)
			}
			value.SecretDocument = encoded
			value.Request.RequestDigest = gatewayDigest(encoded)
			gatewayRedigestEvidence(value)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			wire := gatewayDecodeWire(t, document)
			mutate(&wire)
			wire.Digest = digestWire(wire)
			candidate, err := json.Marshal(wire)
			if err != nil {
				t.Fatalf("marshal mutation: %v", err)
			}
			if _, err := DecodeRequest(candidate, plan.CellKey, now); !errors.Is(err, ErrContract) {
				t.Fatalf("redigested gateway mutation accepted: %v", err)
			}
		})
	}
}

func TestRequestWireRejectsNonCanonicalAndUnboundInput(t *testing.T) {
	t.Parallel()
	now := gatewayTestNow()
	plan := gatewayTestPlan(t, "gateway-boundary", gatewayTestTarget(), now)
	request, err := secretdryrunrequest.Prepare(plan.CellKey, plan, gatewayCreateDecision(t, plan, now), now)
	if err != nil {
		t.Fatalf("prepare request: %v", err)
	}
	document, err := EncodeRequest(request, plan.CellKey, now)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, document, "", "  "); err != nil {
		t.Fatalf("indent request: %v", err)
	}
	unknown := bytes.Replace(document, []byte(`{"apiVersion":`), []byte(`{"unknown":true,"apiVersion":`), 1)
	duplicate := bytes.Replace(document, []byte(`{"apiVersion":`), []byte(`{"apiVersion":"duplicate","apiVersion":`), 1)
	badDigest := gatewayDecodeWire(t, document)
	badDigest.Digest = "sha256:" + strings.Repeat("0", 64)
	badDigestDocument, err := json.Marshal(badDigest)
	if err != nil {
		t.Fatalf("marshal bad digest: %v", err)
	}
	uppercaseDigest := gatewayDecodeWire(t, document)
	uppercaseDigest.Digest = strings.ToUpper(uppercaseDigest.Digest)
	uppercaseDocument, err := json.Marshal(uppercaseDigest)
	if err != nil {
		t.Fatalf("marshal uppercase digest: %v", err)
	}
	tests := map[string][]byte{
		"empty":               nil,
		"JSON null":           []byte("null"),
		"leading whitespace":  append([]byte{' '}, document...),
		"trailing whitespace": append(append([]byte(nil), document...), '\n'),
		"trailing JSON":       append(append([]byte(nil), document...), []byte(`{}`)...),
		"indented":            indented.Bytes(),
		"unknown field":       unknown,
		"duplicate field":     duplicate,
		"wrong digest":        badDigestDocument,
		"uppercase digest":    uppercaseDocument,
		"oversized":           bytes.Repeat([]byte{'x'}, MaximumRequestBytes+1),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRequest(candidate, plan.CellKey, now); !errors.Is(err, ErrContract) {
				t.Fatalf("invalid wire input accepted: %v", err)
			}
		})
	}
	otherCell := gatewayTestPlan(t, "gateway-other-cell", backupcontrol.BackupTarget{
		Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry",
	}, now).CellKey
	for name, call := range map[string]func() error{
		"encode zero request": func() error { _, err := EncodeRequest(secretdryrunrequest.Request{}, plan.CellKey, now); return err },
		"encode cross cell":   func() error { _, err := EncodeRequest(request, otherCell, now); return err },
		"decode cross cell":   func() error { _, err := DecodeRequest(document, otherCell, now); return err },
		"encode expired": func() error {
			_, err := EncodeRequest(request, plan.CellKey, request.Evidence().ExpiresAt.Add(time.Second))
			return err
		},
		"decode expired": func() error {
			_, err := DecodeRequest(document, plan.CellKey, request.Evidence().ExpiresAt.Add(time.Second))
			return err
		},
		"decode zero clock": func() error { _, err := DecodeRequest(document, plan.CellKey, time.Time{}); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrContract) {
				t.Fatalf("unbound request accepted: %v", err)
			}
		})
	}
	for _, value := range []string{"", "sha256:0", "sha256:" + strings.Repeat("g", 64), "SHA256:" + strings.Repeat("0", 64)} {
		if validDigest(value) {
			t.Fatalf("invalid digest accepted: %q", value)
		}
	}
}

func TestReceiptWireRejectsCanonicalMismatchesAndNonCanonicalJSON(t *testing.T) {
	t.Parallel()
	now := gatewayTestNow()
	previous := gatewayTestPlan(t, "receipt-previous", gatewayTestTarget(), now)
	desired := gatewayTestPlan(t, "receipt-desired", gatewayTestTarget(), now)
	request, err := secretdryrunrequest.Prepare(previous.CellKey, previous, gatewayCreateDecision(t, previous, now), now)
	if err != nil {
		t.Fatalf("prepare create request: %v", err)
	}
	receipt := gatewayReceipt(request, now)
	document, err := EncodeReceipt(receipt, request, previous.CellKey, now)
	if err != nil {
		t.Fatalf("encode receipt: %v", err)
	}
	mutations := map[string]func(*secretdryrunrequest.Receipt){
		"plan digest": func(value *secretdryrunrequest.Receipt) {
			value.PlanDigest = "sha256:" + strings.Repeat("0", 64)
		},
		"decision digest": func(value *secretdryrunrequest.Receipt) {
			value.DecisionDigest = "sha256:" + strings.Repeat("1", 64)
			value.IdempotencyKey = secretdryrunrequest.IdempotencyKey(value.CellID, value.DecisionDigest)
		},
		"request digest": func(value *secretdryrunrequest.Receipt) {
			value.RequestDigest = "sha256:" + strings.Repeat("2", 64)
		},
		"validated before preparation": func(value *secretdryrunrequest.Receipt) {
			value.ValidatedAt = request.Evidence().PreparedAt.Add(-time.Second)
		},
		"validated in future": func(value *secretdryrunrequest.Receipt) {
			value.ValidatedAt = now.Add(time.Second)
		},
		"retry-shaped rejection": func(value *secretdryrunrequest.Receipt) { value.Accepted = false },
		"persisted":              func(value *secretdryrunrequest.Receipt) { value.Persisted = true },
		"production mutation":    func(value *secretdryrunrequest.Receipt) { value.ProductionMutationAllowed = true },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := receipt
			mutate(&candidate)
			candidate.Digest = secretdryrunrequest.DigestReceipt(candidate)
			encoded, err := json.Marshal(candidate)
			if err != nil {
				t.Fatalf("marshal receipt mutation: %v", err)
			}
			if _, err := DecodeReceipt(encoded, request, previous.CellKey, now); !errors.Is(err, ErrContract) {
				t.Fatalf("mismatched receipt accepted: %v", err)
			}
			if _, err := EncodeReceipt(candidate, request, previous.CellKey, now); !errors.Is(err, ErrContract) {
				t.Fatalf("mismatched receipt encoded: %v", err)
			}
		})
	}

	replaceRequest, err := secretdryrunrequest.Prepare(desired.CellKey, desired, gatewayReplaceDecision(t, previous, desired, now), now)
	if err != nil {
		t.Fatalf("prepare replace request: %v", err)
	}
	if _, err := DecodeReceipt(mustEncodeReceipt(t, gatewayReceipt(replaceRequest, now)), request, previous.CellKey, now); !errors.Is(err, ErrContract) {
		t.Fatalf("valid receipt from another attempt accepted: %v", err)
	}
	otherPlan := gatewayTestPlan(t, "receipt-other-cell", backupcontrol.BackupTarget{
		Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry",
	}, now)
	otherRequest, err := secretdryrunrequest.Prepare(otherPlan.CellKey, otherPlan, gatewayCreateDecision(t, otherPlan, now), now)
	if err != nil {
		t.Fatalf("prepare other-cell request: %v", err)
	}
	if _, err := DecodeReceipt(mustEncodeReceipt(t, gatewayReceipt(otherRequest, now)), request, previous.CellKey, now); !errors.Is(err, ErrContract) {
		t.Fatalf("valid receipt from another cell accepted: %v", err)
	}

	unknown := bytes.Replace(document, []byte(`{"apiVersion":`), []byte(`{"unknown":true,"apiVersion":`), 1)
	badDigest := receipt
	badDigest.Digest = "sha256:" + strings.Repeat("0", 64)
	badDigestDocument := mustEncodeReceipt(t, badDigest)
	tests := map[string][]byte{
		"empty":              nil,
		"leading whitespace": append([]byte{' '}, document...),
		"trailing JSON":      append(append([]byte(nil), document...), []byte(`{}`)...),
		"unknown field":      unknown,
		"bad digest":         badDigestDocument,
		"oversized":          bytes.Repeat([]byte{'x'}, MaximumReceiptBytes+1),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeReceipt(candidate, request, previous.CellKey, now); !errors.Is(err, ErrContract) {
				t.Fatalf("invalid receipt JSON accepted: %v", err)
			}
		})
	}
	if _, err := EncodeReceipt(receipt, request, "backup/registry/0000000000000000", now); !errors.Is(err, ErrContract) {
		t.Fatalf("receipt encoded across cell boundary: %v", err)
	}
}

func gatewayDecodeWire(t *testing.T, document []byte) wireRequest {
	t.Helper()
	var wire wireRequest
	if err := json.Unmarshal(document, &wire); err != nil {
		t.Fatalf("decode wire fixture: %v", err)
	}
	return wire
}

func gatewayRedigestEvidence(wire *wireRequest) {
	wire.Request.Digest = secretdryrunrequest.DigestEvidence(wire.Request)
}

func gatewayReceipt(request secretdryrunrequest.Request, validatedAt time.Time) secretdryrunrequest.Receipt {
	evidence := request.Evidence()
	receipt := secretdryrunrequest.Receipt{
		APIVersion: secretdryrunrequest.ReceiptAPIVersion,
		Kind:       secretdryrunrequest.ReceiptKind,
		Policy:     secretdryrunrequest.ReceiptPolicy,
		Namespace:  evidence.Namespace,
		SecretName: evidence.SecretName,
		CellKey:    evidence.CellKey,
		CellID:     evidence.CellID,
		Action:     evidence.Action,
		PlanDigest: evidence.PlanDigest, DecisionDigest: evidence.DecisionDigest,
		RequestDigest: evidence.RequestDigest, IdempotencyKey: evidence.IdempotencyKey,
		ValidatedAt: validatedAt, Accepted: true, ServerSideDryRun: true,
		Persisted: false, DeleteAllowed: false, ExecutionAllowed: false, ProductionMutationAllowed: false,
	}
	receipt.Digest = secretdryrunrequest.DigestReceipt(receipt)
	return receipt
}

func mustEncodeReceipt(t *testing.T, receipt secretdryrunrequest.Receipt) []byte {
	t.Helper()
	document, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("marshal receipt fixture: %v", err)
	}
	return document
}

func gatewayCreateDecision(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	observation, err := reconcile.ObserveAbsent(plan.CellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	decision, err := reconcile.Decide(plan.CellKey, &plan, observation, now)
	if err != nil {
		t.Fatalf("decide create: %v", err)
	}
	return decision
}

func gatewayReplaceDecision(t *testing.T, previous, desired materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	manifest, err := reconcile.BuildManifest(previous, now)
	if err != nil {
		t.Fatalf("build previous manifest: %v", err)
	}
	data, err := previous.Data(now)
	if err != nil {
		t.Fatalf("open previous plan: %v", err)
	}
	snapshot, err := reconcile.SealCurrent(previous, reconcile.SecretEvidence{
		Namespace: previous.Namespace, SecretName: previous.SecretName,
		UID: "01234567-89ab-cdef-0123-456789abcdef", ResourceVersion: "42",
		SecretType: manifest.SecretType, Labels: gatewayCloneMap(manifest.Labels),
		Annotations: gatewayCloneMap(manifest.Annotations), Data: map[string][]byte{
			data.SpecKey: append([]byte(nil), data.SpecDocument...), data.TokenKey: append([]byte(nil), data.ObserverToken...),
		},
	})
	if err != nil {
		t.Fatalf("seal previous observation: %v", err)
	}
	observation, err := reconcile.ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe previous generation: %v", err)
	}
	decision, err := reconcile.Decide(desired.CellKey, &desired, observation, now)
	if err != nil {
		t.Fatalf("decide replace: %v", err)
	}
	return decision
}

func gatewayTestPlan(t *testing.T, runID string, target backupcontrol.BackupTarget, now time.Time) materialization.Plan {
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
		t.Fatalf("marshal observer claims: %v", err)
	}
	observerToken := "fugue_bo_v1." +
		base64.RawURLEncoding.EncodeToString([]byte("backup-key-1")) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{'s'}, 32))
	bundle := materializercontract.ObserverInputBundle{
		APIVersion: materializercontract.ObserverInputBundleAPIVersion,
		Kind:       materializercontract.ObserverInputBundleKind,
		Policy:     materializercontract.ObserverInputBundlePolicy,
		CellKey:    spec.CellKey, RunID: spec.RunID, SpecDigest: spec.Digest,
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

func gatewayTestTarget() backupcontrol.BackupTarget {
	return backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"}
}

func gatewayCloneMap(source map[string]string) map[string]string {
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func gatewayDigest(document []byte) string {
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func gatewayTestNow() time.Time {
	return time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
}
