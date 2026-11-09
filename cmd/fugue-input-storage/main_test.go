package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

type sourceFailure struct{ cause error }

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

func (s sourceFailure) Read([]byte) (int, error) { return 0, s.cause }

type unexpectedObjects struct{}

func (unexpectedObjects) Put(context.Context, string, []byte) error {
	return errors.New("unexpected PUT")
}
func (unexpectedObjects) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("unexpected GET")
}

func TestCopyFailureReceiptPreservesSQLStateWithoutPrivateDetails(t *testing.T) {
	plan := inputarchive.Manifest{Format: inputarchive.Format, Digest: "sha256:" + strings.Repeat("a", 64), RawBytes: 65536, SegmentBytes: 65536, Prefix: "archive/source-check"}
	cause := &pgconn.PgError{Code: "55P03", Message: "synthetic-private-value"}
	_, err := inputarchive.Copy(t.Context(), unexpectedObjects{}, io.MultiReader(bytes.NewBufferString("partial"), sourceFailure{cause}), plan, func(inputarchive.Segment) error { return nil })
	var output bytes.Buffer
	if encodeErr := writeArchiveCopyReport(&output, store.CompilerInputArchiveCopyReport{Action: "archive-copy"}, err); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	var receipt struct {
		Failure struct {
			Stage, Category string
			Ordinal         int `json:"segment_ordinal"`
			Received        int `json:"segment_bytes_read"`
		}
	}
	if decodeErr := json.Unmarshal(output.Bytes(), &receipt); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if strings.Contains(output.String(), "private") || receipt.Failure.Stage != "source-read" || receipt.Failure.Category != "55P03" || receipt.Failure.Ordinal != 0 || receipt.Failure.Received != 7 || !errors.Is(err, inputarchive.ErrIntegrity) {
		t.Fatal("copy failure lost bounded diagnosis or exposed driver details", output.String())
	}
	output.Reset()
	if err := writeArchiveCopyReport(&output, store.CompilerInputArchiveCopyReport{State: "verified"}, nil); err != nil || strings.Contains(output.String(), "failure") {
		t.Fatal("successful receipt has failure fields", output.String(), err)
	}
}

func TestRetirementRefusalReportsMissingProofsWithoutDriverDetails(t *testing.T) {
	var output bytes.Buffer
	writeRetirementError(&output, fmt.Errorf("private connection details: %w", store.ErrCompilerInputArchiveRetirementSafetyUnavailable))
	if strings.Contains(output.String(), "private") || !strings.Contains(output.String(), "before database/object access") || !strings.Contains(output.String(), "reactivation admission") {
		t.Fatal("retirement refusal omitted its safety reason or leaked details", output.String())
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
