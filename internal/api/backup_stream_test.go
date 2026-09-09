package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

type backupBrokenReader struct{ err error }

func (r backupBrokenReader) Read([]byte) (int, error) { return 0, r.err }

func TestBackupStreamPublishesOnlyCompleteProducerOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		size   int
		broken bool
	}{
		{"small", 123, false},
		{"exact-part", int(dataMultipartMinPartSize), false},
		{"multiple-parts", int(dataMultipartMinPartSize)*2 + 13, false},
		{"failure-after-full-part", int(dataMultipartMinPartSize), true},
		{"failure-before-first-part", 123, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var published, aborted bool
			var received bytes.Buffer
			fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprint(w, `<CreateMultipartUploadResult><UploadId>stream</UploadId></CreateMultipartUploadResult>`)
				case r.Method == http.MethodPut:
					_, _ = io.Copy(&received, r.Body)
					w.Header().Set("ETag", `"part"`)
					if r.URL.Query().Get("uploadId") == "" {
						published = true
					}
				case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "stream":
					published = true
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>complete</ETag></CompleteMultipartUploadResult>`)
				case r.Method == http.MethodDelete:
					aborted = true
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request: %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer fake.Close()
			backend := newTestDataObjectBackend(t, fake.URL)
			data := bytes.Repeat([]byte{42}, test.size)
			var reader io.Reader = bytes.NewReader(data)
			failure := errors.New("producer failed after writing bytes")
			if test.broken {
				reader = io.MultiReader(reader, backupBrokenReader{failure})
			}
			err := backend.putObjectStream(context.Background(), "backup", reader, dataMultipartMinPartSize)
			if test.broken {
				if !errors.Is(err, failure) || published {
					t.Fatalf("incomplete backup published=%v err=%v", published, err)
				}
				if test.size >= int(dataMultipartMinPartSize) && !aborted {
					t.Fatal("partial multipart upload was not aborted")
				}
			} else if err != nil || !published || !bytes.Equal(received.Bytes(), data) {
				t.Fatalf("backup did not preserve all output: published=%v size=%d err=%v", published, received.Len(), err)
			}
		})
	}
}

func TestControlPlaneBackupDoesNotRequireTemporaryDisk(t *testing.T) {
	fake := newFailedUploadS3(t)
	stateStore, backend := newFailedUploadBackupStore(t, fake.URL, "")
	pgDump := writeFailedUploadPGDump(t)
	t.Setenv("FUGUE_PG_DUMP_BIN", pgDump)
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "nonexistent"))
	server := &Server{store: stateStore, controlPlaneDatabaseURL: "postgres://backup.invalid/database"}
	run, err := stateStore.CreateBackupRun(model.BackupRun{Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, BackendID: backend.ID})
	if err != nil {
		t.Fatal(err)
	}
	run, err = stateStore.ClaimBackupRun(run.ID, "stream-test", time.Now().UTC(), backupRunLeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := server.runControlPlaneDatabaseBackup(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) == 0 {
		t.Fatal("missing backup artifacts")
	}
}

func TestPGDumpFailureAfterOutputDoesNotPublishBackup(t *testing.T) {
	fake := newFailedUploadS3(t)
	backend := newTestDataObjectBackend(t, fake.URL)
	script := filepath.Join(t.TempDir(), "dump")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf 'partial dump'\nprintf 'source connection failed' >&2\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := model.BackupRun{ID: "backup_run_producer_error", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}
	tx := newBackupObjectUploadTransaction(nil, run, backend)
	_, _, err := tx.streamPGDump(context.Background(), script, "postgres://example.invalid/database", "control-plane/2026/01/01/00/backup_run_producer_error/control-plane.dump")
	if err == nil || !strings.Contains(err.Error(), "source connection failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.objectCount() != 0 {
		t.Fatal("failed producer published a backup")
	}
}

func TestBackupUploadFailureStopsStreamingProducer(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<CreateMultipartUploadResult><UploadId>stream</UploadId></CreateMultipartUploadResult>`)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer fake.Close()
	script := filepath.Join(t.TempDir(), "dump")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec cat /dev/zero\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := model.BackupRun{ID: "backup_run_upload_error", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}
	tx := newBackupObjectUploadTransaction(nil, run, newTestDataObjectBackend(t, fake.URL))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := tx.streamPGDump(ctx, script, "postgres://example.invalid/database", "control-plane/2026/01/01/00/backup_run_upload_error/control-plane.dump")
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("unexpected upload result: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("upload failure did not promptly stop the producer")
	}
}
