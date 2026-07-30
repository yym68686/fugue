package storesource

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"

	"fugue/internal/backupmaterializer/legacysource"
	"fugue/internal/model"
	"fugue/internal/store"
)

var _ ReadStore = (*store.Store)(nil)

type readStoreStub struct {
	run            model.BackupRun
	runErr         error
	backend        store.BackupBackendObservation
	backendErr     error
	runCalls       int
	backendCalls   int
	runID          string
	runTenant      string
	runAdmin       bool
	backendID      string
	backendTenant  string
	backendAdmin   bool
	afterRunLookup func()
}

func (stub *readStoreStub) GetBackupRun(runID, tenantID string, platformAdmin bool) (model.BackupRun, error) {
	stub.runCalls++
	stub.runID = runID
	stub.runTenant = tenantID
	stub.runAdmin = platformAdmin
	if stub.afterRunLookup != nil {
		stub.afterRunLookup()
	}
	return stub.run, stub.runErr
}

func (stub *readStoreStub) GetBackupBackendObservation(
	backendID,
	tenantID string,
	platformAdmin bool,
) (store.BackupBackendObservation, error) {
	stub.backendCalls++
	stub.backendID = backendID
	stub.backendTenant = tenantID
	stub.backendAdmin = platformAdmin
	return stub.backend, stub.backendErr
}

func TestStoreSourceUsesOnlyTwoPlatformReadOperations(t *testing.T) {
	t.Parallel()
	run := validStoreSourceRun()
	backend := validStoreSourceBackend(run)
	stub := &readStoreStub{run: run, backend: backend}
	reader, err := New(stub)
	if err != nil {
		t.Fatalf("construct store reader: %v", err)
	}
	snapshot, err := reader.ReadSnapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("read materializer snapshot: %v", err)
	}
	if snapshot.Run != run || snapshot.BackendGeneration != backend.Generation ||
		stub.runCalls != 1 || stub.backendCalls != 1 ||
		stub.runID != run.ID || stub.runTenant != "" || !stub.runAdmin ||
		stub.backendID != run.BackendID || stub.backendTenant != "" || !stub.backendAdmin {
		t.Fatalf("store read boundary drifted: snapshot=%+v stub=%+v", snapshot, stub)
	}
}

func TestStoreSourceClassifiesErrorsWithoutDetails(t *testing.T) {
	t.Parallel()
	run := validStoreSourceRun()
	tests := map[string]struct {
		runErr     error
		backendErr error
		want       error
	}{
		"run missing":     {runErr: fmt.Errorf("%w: private run detail", store.ErrNotFound), want: legacysource.ErrSnapshotNotFound},
		"run conflict":    {runErr: fmt.Errorf("%w: private run detail", store.ErrConflict), want: legacysource.ErrSnapshotConflict},
		"run invalid":     {runErr: fmt.Errorf("%w: private run detail", store.ErrInvalidInput), want: legacysource.ErrSnapshotConflict},
		"run unavailable": {runErr: errors.New("database DSN detail"), want: legacysource.ErrSnapshotUnavailable},
		"backend missing": {backendErr: fmt.Errorf("%w: bucket detail", store.ErrNotFound), want: legacysource.ErrSnapshotNotFound},
		"backend conflict": {backendErr: fmt.Errorf("%w: ciphertext detail", store.ErrConflict),
			want: legacysource.ErrSnapshotConflict},
		"backend unavailable": {backendErr: errors.New("secret access key detail"), want: legacysource.ErrSnapshotUnavailable},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			stub := &readStoreStub{run: run, runErr: test.runErr, backend: validStoreSourceBackend(run), backendErr: test.backendErr}
			reader, _ := New(stub)
			snapshot, err := reader.ReadSnapshot(context.Background(), run.ID)
			if !errors.Is(err, test.want) || snapshot != (legacysource.Snapshot{}) {
				t.Fatalf("store error=%v snapshot=%+v want=%v", err, snapshot, test.want)
			}
			for _, detail := range []string{"private run", "database DSN", "bucket detail", "ciphertext", "secret access key"} {
				if strings.Contains(err.Error(), detail) {
					t.Fatalf("store error leaked %q: %v", detail, err)
				}
			}
			if test.runErr != nil && stub.backendCalls != 0 {
				t.Fatalf("run failure reached backend read %d time(s)", stub.backendCalls)
			}
		})
	}
}

func TestStoreSourceRejectsInconsistentReadResults(t *testing.T) {
	t.Parallel()
	validRun := validStoreSourceRun()
	tests := map[string]func(*readStoreStub){
		"wrong run":                 func(stub *readStoreStub) { stub.run.ID = "run-other" },
		"missing backend":           func(stub *readStoreStub) { stub.run.BackendID = "" },
		"backend whitespace":        func(stub *readStoreStub) { stub.run.BackendID = " backend-1" },
		"tenant whitespace":         func(stub *readStoreStub) { stub.run.TenantID = " tenant-1" },
		"backend identity":          func(stub *readStoreStub) { stub.backend.BackendID = "backend-other" },
		"backend tenant":            func(stub *readStoreStub) { stub.backend.TenantID = "tenant-other" },
		"backend tenant whitespace": func(stub *readStoreStub) { stub.backend.TenantID = " tenant-1" },
		"generation empty":          func(stub *readStoreStub) { stub.backend.Generation = "" },
		"generation whitespace":     func(stub *readStoreStub) { stub.backend.Generation += " " },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			stub := &readStoreStub{run: validRun, backend: validStoreSourceBackend(validRun)}
			mutate(stub)
			reader, _ := New(stub)
			if _, err := reader.ReadSnapshot(context.Background(), validRun.ID); !errors.Is(err, legacysource.ErrSnapshotConflict) {
				t.Fatalf("inconsistent read error=%v, want snapshot conflict", err)
			}
		})
	}
}

func TestStoreSourceConfigurationCancellationAndFormattingFailClosed(t *testing.T) {
	t.Parallel()
	if _, err := New(nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil store config error=%v, want ErrConfig", err)
	}
	var typedNil *readStoreStub
	if _, err := New(typedNil); !errors.Is(err, ErrConfig) {
		t.Fatalf("typed nil store config error=%v, want ErrConfig", err)
	}
	run := validStoreSourceRun()
	stub := &readStoreStub{run: run, backend: validStoreSourceBackend(run)}
	reader, _ := New(stub)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, invoke := range map[string]func() error{
		"nil context": func() error { _, err := reader.ReadSnapshot(nil, run.ID); return err },
		"canceled":    func() error { _, err := reader.ReadSnapshot(canceled, run.ID); return err },
		"nil reader": func() error {
			_, err := (*Reader)(nil).ReadSnapshot(context.Background(), run.ID)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invoke(); !errors.Is(err, legacysource.ErrSnapshotUnavailable) || stub.runCalls != 0 {
				t.Fatalf("closed read error=%v calls=%d", err, stub.runCalls)
			}
		})
	}
	if _, err := reader.ReadSnapshot(context.Background(), "Run-1"); !errors.Is(err, legacysource.ErrSnapshotConflict) || stub.runCalls != 0 {
		t.Fatalf("invalid run error=%v calls=%d", err, stub.runCalls)
	}
	afterRun, cancelAfterRun := context.WithCancel(context.Background())
	stub.afterRunLookup = cancelAfterRun
	if _, err := reader.ReadSnapshot(afterRun, run.ID); !errors.Is(err, legacysource.ErrSnapshotUnavailable) || stub.backendCalls != 0 {
		t.Fatalf("post-run cancellation error=%v backendCalls=%d", err, stub.backendCalls)
	}
	for _, rendered := range []string{reader.String(), reader.GoString(), fmt.Sprint(reader), fmt.Sprintf("%+v", reader), fmt.Sprintf("%#v", reader)} {
		if !strings.Contains(rendered, "[REDACTED]") || strings.Contains(rendered, run.ID) {
			t.Fatalf("reader formatting exposed store state: %q", rendered)
		}
	}
}

func TestStoreSourceDependencyAndCapabilityBoundary(t *testing.T) {
	t.Parallel()
	readStoreType := reflect.TypeOf((*ReadStore)(nil)).Elem()
	if readStoreType.NumMethod() != 2 || readStoreType.Method(0).Name == "" || readStoreType.Method(1).Name == "" {
		t.Fatalf("store source capability widened: %v", readStoreType)
	}
	methodNames := []string{readStoreType.Method(0).Name, readStoreType.Method(1).Name}
	sort.Strings(methodNames)
	if !reflect.DeepEqual(methodNames, []string{"GetBackupBackendObservation", "GetBackupRun"}) {
		t.Fatalf("store source methods drifted: %v", methodNames)
	}
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list store source dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupmaterializer/legacysource",
		"fugue/internal/model",
		"fugue/internal/store",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("store source dependency boundary widened: got=%v want=%v", local, want)
	}
	for _, forbidden := range []string{"fugue/internal/api", "database/sql", "k8s.io/", "net/http", "os", "os/exec"} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("store source gained forbidden dependency %q", forbidden)
		}
	}
}

func validStoreSourceRun() model.BackupRun {
	return model.BackupRun{
		ID: "run-store-source-1", TenantID: "tenant-1", AppID: "app-1", BackendID: "backend-1",
		Trigger: model.BackupRunTriggerManual, Status: model.BackupRunStatusPending, Attempt: 1,
		Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app-1"},
	}
}

func validStoreSourceBackend(run model.BackupRun) store.BackupBackendObservation {
	return store.BackupBackendObservation{
		BackendID:  run.BackendID,
		TenantID:   run.TenantID,
		Generation: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}
