package imagetracking

import (
	"context"
	"database/sql"
	"strings"

	"fugue/internal/model"
)

// Rows is the image-tracking portion of the JSON transaction. LookupApp is
// read-only and observes the same snapshot; no unrelated tables are exposed.
type Rows struct {
	Trackings []model.AppImageTracking
	Checks    []model.AppImageTrackingCheck
	LookupApp func(string) (model.App, bool)
}

// Repository owns image tracking persistence. The store composition root binds
// it to the existing connection and file lock, including same-transaction app
// lookup. No database handle is exposed to its callers.
type Repository struct {
	db              *sql.DB
	postgres        bool
	file            func(bool, func(*Rows) error) error
	lookupApp       func(context.Context, *sql.Tx, string) (model.App, error)
	cloneAppSpec    func(*model.AppSpec) *model.AppSpec
	createOperation func(model.Operation) (model.Operation, error)
	mapError        func(error) error
	invalidInput    error
	notFound        error
}

type OperationWriter struct {
	CloneSpec func(*model.AppSpec) *model.AppSpec
	Create    func(model.Operation) (model.Operation, error)
}

type Errors struct {
	InvalidInput, NotFound error
	MapDatabase            func(error) error
}

func NewFile(tx func(bool, func(*Rows) error) error, operations OperationWriter, errs Errors) Repository {
	return Repository{file: tx, cloneAppSpec: operations.CloneSpec, createOperation: operations.Create,
		invalidInput: errs.InvalidInput, notFound: errs.NotFound}
}

func NewPostgres(db *sql.DB, lookupApp func(context.Context, *sql.Tx, string) (model.App, error),
	operations OperationWriter, errs Errors) Repository {
	return Repository{db: db, postgres: true, lookupApp: lookupApp, mapError: errs.MapDatabase,
		cloneAppSpec: operations.CloneSpec, createOperation: operations.Create,
		invalidInput: errs.InvalidInput, notFound: errs.NotFound}
}

type sqlScanner interface {
	Scan(...any) error
}

func stripImageNUL(value string) string {
	return strings.ReplaceAll(value, "\x00", "")
}

func normalizeImageDigest(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "@sha256:") {
		return raw[1:]
	}
	return raw
}
