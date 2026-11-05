package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

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
		{&url.Error{Op: "Get", URL: "https://synthetic.invalid/private-value", Err: errors.New("synthetic-private-value")}, "network"},
		{errors.New("synthetic-private-value"), "unclassified"},
	} {
		if got := maintenanceErrorCategory(item.err); got != item.want {
			t.Fatalf("unexpected bounded category: %q, want %q", got, item.want)
		}
	}
}
