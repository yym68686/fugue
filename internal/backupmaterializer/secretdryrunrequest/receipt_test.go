package secretdryrunrequest

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
)

func TestReceiptIsVersionedSecretFreeAndRoundTrips(t *testing.T) {
	t.Parallel()
	now := requestTestNow()
	previous := requestTestPlan(t, "receipt-previous", now)
	desired := requestTestPlan(t, "receipt-desired", now)
	tests := []struct {
		name     string
		plan     materialization.Plan
		decision reconcile.Decision
	}{
		{name: "create", plan: previous, decision: requestTestCreateDecision(t, previous, now)},
		{name: "replace", plan: desired, decision: requestTestReplaceDecision(t, previous, desired, now)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			prepared, err := Prepare(test.plan.CellKey, test.plan, test.decision, now)
			if err != nil {
				t.Fatalf("prepare receipt source: %v", err)
			}
			evidence := prepared.Evidence()
			receipt := Receipt{
				APIVersion: ReceiptAPIVersion, Kind: ReceiptKind, Policy: ReceiptPolicy,
				Namespace: evidence.Namespace, SecretName: evidence.SecretName,
				CellKey: evidence.CellKey, CellID: evidence.CellID, Action: evidence.Action,
				PlanDigest: evidence.PlanDigest, DecisionDigest: evidence.DecisionDigest,
				RequestDigest: evidence.RequestDigest, IdempotencyKey: evidence.IdempotencyKey,
				ValidatedAt: now, Accepted: true, ServerSideDryRun: true,
			}
			receipt.Digest = DigestReceipt(receipt)
			if err := ValidateReceipt(receipt); err != nil {
				t.Fatalf("valid receipt rejected: %v receipt=%#v", err, receipt)
			}
			document, err := json.Marshal(receipt)
			if err != nil {
				t.Fatalf("marshal receipt: %v", err)
			}
			var decoded Receipt
			decoder := json.NewDecoder(strings.NewReader(string(document)))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&decoded); err != nil || !reflect.DeepEqual(decoded, receipt) || ValidateReceipt(decoded) != nil {
				t.Fatalf("receipt round trip drifted: decoded=%#v err=%v validation=%v", decoded, err, ValidateReceipt(decoded))
			}
			data, err := test.plan.Data(now)
			if err != nil {
				t.Fatalf("open plan fixture: %v", err)
			}
			public := string(document) + fmt.Sprintf(" %v %#v", receipt, receipt)
			for _, sensitive := range []string{
				string(data.SpecDocument), string(data.ObserverToken),
				base64.StdEncoding.EncodeToString(data.SpecDocument),
				base64.StdEncoding.EncodeToString(data.ObserverToken),
			} {
				if strings.Contains(public, sensitive) {
					t.Fatalf("receipt leaked private material %q", sensitive)
				}
			}
			if receipt.Persisted || receipt.DeleteAllowed || receipt.ExecutionAllowed || receipt.ProductionMutationAllowed ||
				!strings.Contains(receipt.String(), "persisted=false") || receipt.GoString() != receipt.String() {
				t.Fatalf("receipt safety/formatting drifted: %v %#v", receipt, receipt)
			}
		})
	}
}

func TestReceiptRejectsRedigestedSemanticDrift(t *testing.T) {
	t.Parallel()
	now := requestTestNow()
	plan := requestTestPlan(t, "receipt-invalid", now)
	decision := requestTestCreateDecision(t, plan, now)
	prepared, err := Prepare(plan.CellKey, plan, decision, now)
	if err != nil {
		t.Fatalf("prepare receipt source: %v", err)
	}
	evidence := prepared.Evidence()
	valid := Receipt{
		APIVersion: ReceiptAPIVersion, Kind: ReceiptKind, Policy: ReceiptPolicy,
		Namespace: evidence.Namespace, SecretName: evidence.SecretName,
		CellKey: evidence.CellKey, CellID: evidence.CellID, Action: evidence.Action,
		PlanDigest: evidence.PlanDigest, DecisionDigest: evidence.DecisionDigest,
		RequestDigest: evidence.RequestDigest, IdempotencyKey: evidence.IdempotencyKey,
		ValidatedAt: now, Accepted: true, ServerSideDryRun: true,
	}
	valid.Digest = DigestReceipt(valid)
	tests := map[string]func(*Receipt){
		"API":                 func(value *Receipt) { value.APIVersion = "v2" },
		"kind":                func(value *Receipt) { value.Kind = "Other" },
		"policy":              func(value *Receipt) { value.Policy = "live-write" },
		"namespace":           func(value *Receipt) { value.Namespace = "default" },
		"Secret":              func(value *Receipt) { value.SecretName += "-other" },
		"cell":                func(value *Receipt) { value.CellKey = "invalid" },
		"cell ID":             func(value *Receipt) { value.CellID += "-other" },
		"action":              func(value *Receipt) { value.Action = reconcile.ActionNoop },
		"plan digest":         func(value *Receipt) { value.PlanDigest = "sha256:invalid" },
		"decision digest":     func(value *Receipt) { value.DecisionDigest = "sha256:invalid" },
		"request digest":      func(value *Receipt) { value.RequestDigest = "sha256:invalid" },
		"idempotency":         func(value *Receipt) { value.IdempotencyKey += "-other" },
		"time":                func(value *Receipt) { value.ValidatedAt = time.Time{} },
		"time precision":      func(value *Receipt) { value.ValidatedAt = value.ValidatedAt.Add(time.Nanosecond) },
		"not accepted":        func(value *Receipt) { value.Accepted = false },
		"not dry-run":         func(value *Receipt) { value.ServerSideDryRun = false },
		"persisted":           func(value *Receipt) { value.Persisted = true },
		"delete":              func(value *Receipt) { value.DeleteAllowed = true },
		"execution":           func(value *Receipt) { value.ExecutionAllowed = true },
		"production mutation": func(value *Receipt) { value.ProductionMutationAllowed = true },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			value.Digest = DigestReceipt(value)
			if err := ValidateReceipt(value); !errors.Is(err, ErrReceipt) {
				t.Fatalf("redigested semantic drift accepted: err=%v receipt=%#v", err, value)
			}
		})
	}
	badDigest := valid
	badDigest.Digest = "sha256:" + strings.Repeat("0", 64)
	if err := ValidateReceipt(badDigest); !errors.Is(err, ErrReceipt) {
		t.Fatalf("bad digest accepted: %v", err)
	}
	if IdempotencyKey("", decision.Digest) != "" || IdempotencyKey(plan.CellID, "sha256:invalid") != "" {
		t.Fatal("invalid idempotency input produced a key")
	}
}
