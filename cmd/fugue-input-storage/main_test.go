package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRetiredMaintenanceModesRejectBeforeDatabaseAccess(t *testing.T) {
	for _, mode := range []string{"archive-retirement-plan", "archive-retire", "archive-copy", "archive-batch", "archive-route-batch", "", "inspect, collect"} {
		if validCompilerInputStorageMode(mode) {
			t.Fatalf("unsupported maintenance mode accepted: %q", mode)
		}
	}
	for _, mode := range []string{"inspect", "collect", "archive-hot-restore", "encoded-backup", "release-backup"} {
		if !validCompilerInputStorageMode(mode) {
			t.Fatalf("supported maintenance mode removed: %q", mode)
		}
	}
}

func TestRootObligationDiagnosticCannotAuthorizeOrAcceptWriteFlags(t *testing.T) {
	selected := map[string]bool{"mode": true, "root-obligation-targets": true}
	if !validRootObligationFlags(selected) {
		t.Fatal("read-only target invocation rejected")
	}
	selected["apply"] = true
	if validRootObligationFlags(selected) {
		t.Fatal("write flag admitted")
	}
	var output bytes.Buffer
	if err := writeRootObligations(&output, store.CompilerInputHotObligations{AllSourcesComplete: true, BodyVerification: true, RetirementAuthorized: true, PlanSHA256: "must-not-be-proof"}); err != nil {
		t.Fatal(err)
	}
	var got store.CompilerInputHotObligations
	if err := json.Unmarshal(output.Bytes(), &got); err != nil || got.AllSourcesComplete || got.BodyVerification || got.RetirementAuthorized || got.PlanSHA256 != "" {
		t.Fatal("diagnostic became an authority", got, err)
	}
}

func TestHotInventoryFailureOutputIsAggregateAndCannotAuthorizeRetirement(t *testing.T) {
	report := store.CompilerInputHotInventory{CoherentMetadata: true, RootInventoryComplete: true,
		RootCounts: map[string]int{"current-lkg": 2}, Progress: inputarchive.HotClosureProgress{Phase: "resolve-identities", ResolvedNodes: 7},
		Cost:                 store.CompilerInputHotCost{RootInventoryUS: 11, Artifacts: store.CompilerInputHotReadCost{Queries: 2, Rows: 3, DecodeUS: 17}},
		RetirementAuthorized: true, Closure: inputarchive.HotClosure{PlanSHA256: "private-proof", MetadataComplete: true}}
	var output bytes.Buffer
	if err := writeHotInventoryFailure(&output, report, fmt.Errorf("private SQL URL token: %w", context.DeadlineExceeded)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "private") {
		t.Fatal("partial proof or arbitrary error leaked")
	}
	var failure map[string]any
	if err := json.Unmarshal(output.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"inspection_complete", "metadata_complete", "local_copies_verified", "retirement_authorized"} {
		if failure[field] != false {
			t.Fatal("incomplete observation granted authority", field)
		}
	}
	if failure["plan_sha256"] != "" || failure["reason"] != "deadline" || failure["category"] != "deadline" || failure["progress"].(map[string]any)["resolved_nodes"] != float64(7) {
		t.Fatal("bounded diagnostic category/count missing", failure)
	}
	if failure["cost"].(map[string]any)["artifacts"].(map[string]any)["decode_us"] != float64(17) {
		t.Fatal("failed inspection lost aggregate cost")
	}
}

func TestMaintenanceErrorCategoryDoesNotExposeErrorDetails(t *testing.T) {
	for _, item := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("private diagnostic: %w", &pgconn.PgError{Code: "55P03", Message: "synthetic-private-value"}), "55P03"},
		{&pgconn.PgError{Code: "private-value"}, "unclassified"},
		{&pgconn.PgError{Code: "12\n34"}, "unclassified"},
		{context.DeadlineExceeded, "deadline"},
		{context.Canceled, "canceled"},
		{fmt.Errorf("private diagnostic: %w", store.ErrConflict), "conflict"},
		{store.ErrInvalidInput, "invalid-input"},
		{inputarchive.ErrInvalid, "invalid-input"},
		{inputarchive.ErrIntegrity, "integrity"},
		{inputarchive.ErrBudget, "budget"},
		{&url.Error{Op: "Get", URL: "https://synthetic.invalid/private-value", Err: errors.New("synthetic-private-value")}, "network"},
		{errors.New("synthetic-private-value"), "unclassified"},
	} {
		if got := maintenanceErrorCategory(item.err); got != item.want {
			t.Fatalf("unexpected bounded category: %q, want %q", got, item.want)
		}
	}
}

func TestRetentionInventoryFailurePreservesSafeCategoryAndWithholdsComplement(t *testing.T) {
	report := store.CompilerInputRetentionInventory{Schema: "fugue.compiler-input-retention-inventory.v1",
		UniverseComplete: true, ClassificationComplete: true, ExecutionAuthorized: true, AuditSHA256: "private-proof",
		Entries: []store.CompilerInputRetentionEntry{{Digest: "private-input", Class: "U"}},
		Totals:  map[string]store.CompilerInputRetentionTotal{"U": {Inputs: 1}}}
	for _, code := range []string{"53400", "57014", "55P03", "private-invalid-code"} {
		var output bytes.Buffer
		err := fmt.Errorf("private SQL and URL: %w", &pgconn.PgError{Code: code, Message: "private-driver-message", Detail: "private-value"})
		if e := writeRetentionInventoryReport(&output, report, err); e != nil {
			t.Fatal(e)
		}
		var got map[string]any
		if err := json.Unmarshal(output.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		want := code
		if code == "private-invalid-code" {
			want = "unclassified"
		}
		failure := got["failure"].(map[string]any)
		if failure["stage"] != "connection-or-snapshot" || failure["category"] != want || strings.Contains(output.String(), "private") || got["entries"] != nil || got["totals"] != nil || got["audit_sha256"] != "" {
			t.Fatal("refusal lost safe diagnosis or exposed a partial complement", output.String())
		}
		for _, field := range []string{"universe_complete", "classification_complete", "execution_authorized"} {
			if got[field] != false {
				t.Fatal("failed audit retained a complete/authority flag", field)
			}
		}
	}
	var success bytes.Buffer
	if err := writeRetentionInventoryReport(&success, report, nil); err != nil {
		t.Fatal(err)
	}
	original, _ := json.Marshal(report)
	if strings.TrimSpace(success.String()) != string(original) || strings.Contains(success.String(), `"failure"`) {
		t.Fatal("successful inventory shape changed", success.String())
	}
}

func TestTaskPreparationContractIsStrictAndApplyRequiresExactPlan(t *testing.T) {
	c := store.NodeTaskInputPreparationContract{Version: 1, TaskID: "neutral-task", UpdaterID: "neutral-updater", OperationKey: "neutral-operation", MaxInputs: 1, MaxRawBytes: 1024, MaxStoredBytes: 1024, MaxGetBytes: 65536, MaxGetRequests: 1, MaxSeconds: 60}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeTaskInputPreparationContract(bytes.NewReader(raw), false); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeTaskInputPreparationContract(bytes.NewReader(raw), true); err == nil {
		t.Fatal("apply accepted without plan")
	}
	c.ExpectedPlanSHA256 = strings.Repeat("a", 64)
	raw, _ = json.Marshal(c)
	if _, err := decodeTaskInputPreparationContract(bytes.NewReader(raw), true); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{string(raw) + "{}", strings.TrimSuffix(string(raw), "}") + `,"unreviewed":true}`, `{}`, strings.Repeat(" ", 16385) + string(raw), string(raw) + strings.Repeat(" ", 16385)} {
		if _, err := decodeTaskInputPreparationContract(strings.NewReader(bad), true); err == nil {
			t.Fatal("non-strict contract accepted")
		}
	}
	for _, change := range []func(*store.NodeTaskInputPreparationContract){
		func(c *store.NodeTaskInputPreparationContract) { c.MaxInputs = 33 },
		func(c *store.NodeTaskInputPreparationContract) { c.MaxRawBytes = 129 << 20 },
		func(c *store.NodeTaskInputPreparationContract) { c.MaxStoredBytes = 129 << 20 },
		func(c *store.NodeTaskInputPreparationContract) { c.MaxGetBytes = 449 << 20 },
		func(c *store.NodeTaskInputPreparationContract) { c.MaxGetRequests = 32769 },
		func(c *store.NodeTaskInputPreparationContract) { c.MaxSeconds = 541 },
		func(c *store.NodeTaskInputPreparationContract) { c.OperationKey = strings.Repeat("x", 49) },
	} {
		bad := c
		change(&bad)
		if bad.Validate(true) == nil {
			t.Fatal("budget contract accepted", bad)
		}
	}
}

func TestTaskPreparationPlanDecoderStrictPublicEnvelope(t *testing.T) {
	now := time.Now().UTC()
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	key := base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey))
	// Construct through JSON: tuple/manifest details stay internal to store.
	raw := map[string]any{
		"version": 1, "contract": store.NodeTaskInputPreparationContract{Version: 1, TaskID: "neutral-task", UpdaterID: "neutral-updater", OperationKey: "neutral-op", ExpectedPlanSHA256: strings.Repeat("a", 64), MaxInputs: 1, MaxRawBytes: 1024, MaxStoredBytes: 1024, MaxGetBytes: 65536, MaxGetRequests: 1, MaxSeconds: 30},
		"backend_hash": "sha256:" + strings.Repeat("b", 64), "closure_sha256": strings.Repeat("c", 64), "epoch": 0, "horizon": 1,
		"issued_at": now, "expires_at": now.Add(10 * time.Minute), "inputs": []any{},
		"tuples": []any{map[string]any{"table": "fugue_node_update_tasks", "key": "neutral-task", "xmin": "1", "ctid": "(1,1)", "relation": 1, "filenode": 1}}, "signature": "",
	}
	data, _ := json.Marshal(raw)
	var ticket store.NodeTaskInputPreparationTicket
	if err := json.Unmarshal(data, &ticket); err != nil {
		t.Fatal(err)
	}
	// Sign the typed canonical payload, not map serialization.
	data, _ = json.Marshal(ticket)
	ticket.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, data))
	plan := store.NodeTaskInputPreparationPlan{Ticket: ticket, VerificationKey: key}
	data, _ = json.Marshal(plan)
	if _, err := decodeTaskInputPreparationPlan(bytes.NewReader(data), now); err != nil {
		t.Fatal("valid public plan", err)
	}
	for _, bad := range []string{string(data) + "{}", strings.TrimSuffix(string(data), "}") + `,"unreviewed":true}`, strings.Replace(string(data), `"epoch":0`, `"epoch":1`, 1), strings.Repeat(" ", (513<<10)+1) + string(data), `{}`} {
		if _, err := decodeTaskInputPreparationPlan(strings.NewReader(bad), now); err == nil {
			t.Fatal("non-strict or unsigned change accepted")
		}
	}
}
