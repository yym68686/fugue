package legacysource

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"

	"fugue/internal/backupadapter"
	"fugue/internal/backupcontrol"
	"fugue/internal/backupmaterializer/httpapi"
	"fugue/internal/model"
)

const testBackendGeneration = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestLegacySourceMapsAllCanonicalCellsFromOneBoundedSnapshot(t *testing.T) {
	t.Parallel()
	tests := map[string]model.BackupRun{
		"control plane": {
			ID: "run-control-plane", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		},
		"registry": {
			ID: "run-registry", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetRegistry},
		},
		"platform component": {
			ID: "run-component", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetPlatformComponent, Component: "image-cache"},
		},
		"app database": {
			ID: "run-app-db", TenantID: "tenant-1", AppID: "app-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app-1"},
		},
		"persistent storage": {
			ID: "run-storage", TenantID: "tenant-1", AppID: "app-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetPersistentStorage, AppID: "app-1"},
		},
		"data workspace": {
			ID: "run-workspace", TenantID: "tenant-1", ProjectID: "project-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetDataWorkspace, ProjectID: "project-1", WorkspaceID: "workspace-1"},
		},
	}
	for name, run := range tests {
		run := run
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			run.Trigger = model.BackupRunTriggerManual
			run.Status = model.BackupRunStatusPending
			run.Attempt = 1
			spec, err := backupadapter.BuildShadowSpec(run, testBackendGeneration)
			if err != nil {
				t.Fatalf("build expected spec: %v", err)
			}
			calls := 0
			seenRunID := ""
			source, err := New(func(_ context.Context, runID string) (Snapshot, error) {
				calls++
				seenRunID = runID
				return Snapshot{Run: run, BackendGeneration: testBackendGeneration}, nil
			})
			if err != nil {
				t.Fatalf("construct legacy source: %v", err)
			}
			input, err := source.ReadDesiredInput(context.Background(), httpapi.ReadRequest{RunID: run.ID, CellKey: spec.CellKey})
			if err != nil {
				t.Fatalf("read desired input: %v", err)
			}
			if calls != 1 || seenRunID != run.ID || input.Spec != spec || input.TenantID != strings.TrimSpace(run.TenantID) ||
				backupcontrol.ValidateBackupRunSpec(input.Spec) != nil {
				t.Fatalf("legacy input drifted: calls=%d seen=%q input=%+v wantSpec=%+v", calls, seenRunID, input, spec)
			}
		})
	}
}

func TestLegacySourceHidesForeignCellsAndClassifiesSnapshotFailures(t *testing.T) {
	t.Parallel()
	run, spec := testLegacyRunAndSpec(t)
	tests := map[string]struct {
		read    SnapshotReader
		cellKey string
		want    error
	}{
		"not found": {
			read: func(context.Context, string) (Snapshot, error) {
				return Snapshot{}, fmt.Errorf("%w: private backend name", ErrSnapshotNotFound)
			},
			cellKey: spec.CellKey, want: httpapi.ErrInputNotFound,
		},
		"foreign cell": {
			read: func(context.Context, string) (Snapshot, error) {
				return Snapshot{Run: run, BackendGeneration: testBackendGeneration}, nil
			},
			cellKey: "backup/registry/ffffffffffffffff", want: httpapi.ErrInputNotFound,
		},
		"conflict": {
			read: func(context.Context, string) (Snapshot, error) {
				return Snapshot{}, fmt.Errorf("%w: ciphertext detail", ErrSnapshotConflict)
			},
			cellKey: spec.CellKey, want: httpapi.ErrInputConflict,
		},
		"declared unavailable": {
			read: func(context.Context, string) (Snapshot, error) {
				return Snapshot{}, fmt.Errorf("%w: DSN detail", ErrSnapshotUnavailable)
			},
			cellKey: spec.CellKey, want: httpapi.ErrInputUnavailable,
		},
		"unknown unavailable": {
			read: func(context.Context, string) (Snapshot, error) {
				return Snapshot{}, errors.New("secret access key detail")
			},
			cellKey: spec.CellKey, want: httpapi.ErrInputUnavailable,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			source, err := New(test.read)
			if err != nil {
				t.Fatalf("construct source: %v", err)
			}
			input, err := source.ReadDesiredInput(context.Background(), httpapi.ReadRequest{RunID: run.ID, CellKey: test.cellKey})
			if !errors.Is(err, test.want) || input != (httpapi.DesiredInput{}) {
				t.Fatalf("snapshot error=%v input=%+v want=%v", err, input, test.want)
			}
			for _, detail := range []string{"private backend", "ciphertext", "DSN", "secret access key"} {
				if strings.Contains(err.Error(), detail) {
					t.Fatalf("snapshot error leaked %q: %v", detail, err)
				}
			}
		})
	}
}

func TestLegacySourceRejectsInconsistentSnapshots(t *testing.T) {
	t.Parallel()
	run, spec := testLegacyRunAndSpec(t)
	tests := map[string]Snapshot{
		"wrong run": func() Snapshot {
			candidate := run
			candidate.ID = "run-other"
			return Snapshot{Run: candidate, BackendGeneration: testBackendGeneration}
		}(),
		"invalid generation": {Run: run, BackendGeneration: "sha256:invalid"},
		"tenant whitespace": func() Snapshot {
			candidate := run
			candidate.TenantID = " tenant-1"
			return Snapshot{Run: candidate, BackendGeneration: testBackendGeneration}
		}(),
		"unsupported target": func() Snapshot {
			candidate := run
			candidate.Target.Type = "unsupported"
			return Snapshot{Run: candidate, BackendGeneration: testBackendGeneration}
		}(),
	}
	for name, snapshot := range tests {
		t.Run(name, func(t *testing.T) {
			source, _ := New(func(context.Context, string) (Snapshot, error) { return snapshot, nil })
			if _, err := source.ReadDesiredInput(context.Background(), httpapi.ReadRequest{RunID: run.ID, CellKey: spec.CellKey}); !errors.Is(err, httpapi.ErrInputConflict) {
				t.Fatalf("inconsistent snapshot error=%v, want input conflict", err)
			}
		})
	}
}

func TestLegacySourceConfigurationAndCancellationFailClosed(t *testing.T) {
	t.Parallel()
	if _, err := New(nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil reader config error=%v, want ErrConfig", err)
	}
	run, spec := testLegacyRunAndSpec(t)
	calls := 0
	source, _ := New(func(context.Context, string) (Snapshot, error) {
		calls++
		return Snapshot{Run: run, BackendGeneration: testBackendGeneration}, nil
	})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.ReadDesiredInput(canceled, httpapi.ReadRequest{RunID: run.ID, CellKey: spec.CellKey}); !errors.Is(err, httpapi.ErrInputUnavailable) || calls != 0 {
		t.Fatalf("pre-canceled source error=%v calls=%d", err, calls)
	}
	if _, err := source.ReadDesiredInput(nil, httpapi.ReadRequest{RunID: run.ID, CellKey: spec.CellKey}); !errors.Is(err, httpapi.ErrInputUnavailable) || calls != 0 {
		t.Fatalf("nil-context source error=%v calls=%d", err, calls)
	}
	if _, err := (*Source)(nil).ReadDesiredInput(context.Background(), httpapi.ReadRequest{RunID: run.ID, CellKey: spec.CellKey}); !errors.Is(err, httpapi.ErrInputUnavailable) {
		t.Fatalf("nil source error=%v", err)
	}
	for name, request := range map[string]httpapi.ReadRequest{
		"bad run":  {RunID: "Run-1", CellKey: spec.CellKey},
		"bad cell": {RunID: run.ID, CellKey: "backup/registry/not-a-cell"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := source.ReadDesiredInput(context.Background(), request); !errors.Is(err, httpapi.ErrInputConflict) || calls != 0 {
				t.Fatalf("invalid request error=%v calls=%d", err, calls)
			}
		})
	}
	afterRead, cancelAfterRead := context.WithCancel(context.Background())
	source, _ = New(func(context.Context, string) (Snapshot, error) {
		cancelAfterRead()
		return Snapshot{Run: run, BackendGeneration: testBackendGeneration}, nil
	})
	if _, err := source.ReadDesiredInput(afterRead, httpapi.ReadRequest{RunID: run.ID, CellKey: spec.CellKey}); !errors.Is(err, httpapi.ErrInputUnavailable) {
		t.Fatalf("post-read cancellation error=%v", err)
	}
}

func TestLegacySourceDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list legacy source dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupadapter",
		"fugue/internal/backupmaterializer/httpapi",
		"fugue/internal/backupmaterializeridentity",
		"fugue/internal/model",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("legacy source dependency boundary widened: got=%v want=%v", local, want)
	}
	for _, forbidden := range []string{"fugue/internal/api", "fugue/internal/store", "database/sql", "k8s.io/", "net/http", "os", "os/exec"} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("legacy source gained forbidden dependency %q", forbidden)
		}
	}
}

func testLegacyRunAndSpec(t *testing.T) (model.BackupRun, backupcontrol.BackupRunSpec) {
	t.Helper()
	run := model.BackupRun{
		ID: "run-source-1", TenantID: "tenant-1", AppID: "app-1", BackendID: "backend-1",
		Trigger: model.BackupRunTriggerManual, Status: model.BackupRunStatusPending, Attempt: 1,
		Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app-1"},
	}
	spec, err := backupadapter.BuildShadowSpec(run, testBackendGeneration)
	if err != nil {
		t.Fatalf("build test source spec: %v", err)
	}
	return run, spec
}
