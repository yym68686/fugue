package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

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
