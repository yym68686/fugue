package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestOrphanManifestAdoptionProtectedReconciliation(t *testing.T) {
	for _, mode := range []string{"safe", "legacy", "wrong-policy", "manifest-changed", "config-changed", "changed-before-insert", "wrong-volume", "bad-block-map", "remote-error", "target-unavailable", "wrong-run-label", "existing-artifact"} {
		t.Run(mode, func(t *testing.T) {
			clearBackupUsageSeedEnv(t)
			const runID = "backup_run_fixture"
			const logicalKey = "apps/platform-database/" + runID + "/manifest.json"
			const physicalKey = "backup-root/" + logicalKey
			const backupURL = "s3://bucket@auto/backup-root/longhorn?backup=backup-test&volume=volume-test"
			created := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
			target := model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Component: "platform-database", Engine: model.BackupEngineLonghornSnapshot}
			manifest := model.BackupManifest{RunID: runID, Target: target, Kind: model.BackupArtifactKindLonghornSnapshot, Format: "longhorn-remote-backup", ManifestObjectKey: logicalKey, SizeBytes: 1000, LogicalBytes: 4194304, CreatedAt: created, Metadata: map[string]string{
				"volume_name": "volume-test", "longhorn_backup_name": "backup-test", "snapshot_name": "snapshot-test", "longhorn_namespace": "longhorn-system", "backup_target_name": "default", "backup_target_url": "s3://bucket@auto/backup-root/longhorn", "longhorn_backup_url": backupURL,
			}}
			if mode == "legacy" {
				delete(manifest.Metadata, "longhorn_namespace")
			}
			if mode == "wrong-policy" {
				manifest.PolicyID = "backup_policy_other"
			}
			config := map[string]any{"Name": "backup-test", "VolumeName": "volume-test", "SnapshotName": "snapshot-test", "CreatedTime": created.Format(time.RFC3339), "SnapshotCreatedAt": created.Add(-time.Hour).Format(time.RFC3339), "Size": "2097152", "CompressionMethod": "lz4", "Parameters": map[string]string{"backup-block-size": "2097152"}, "Blocks": []map[string]any{{"Offset": 0, "BlockChecksum": strings.Repeat("a", 64)}}}
			if mode == "wrong-volume" {
				config["VolumeName"] = "another-volume"
			}
			if mode == "bad-block-map" {
				config["Size"] = "100"
			}
			manifestData, _ := json.Marshal(manifest)
			configData, _ := json.Marshal(config)
			executing, configHeads, providerWrites := false, 0, 0
			fakeS3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet && r.Method != http.MethodHead {
					providerWrites++
					t.Errorf("provider write %s", r.Method)
					w.WriteHeader(500)
					return
				}
				if r.URL.Query().Get("list-type") == "2" {
					fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%s</Key><Size>%d</Size></Contents></ListBucketResult>`, physicalKey, len(manifestData))
					return
				}
				data, etag := manifestData, "manifest-etag"
				if strings.HasSuffix(r.URL.Path, ".cfg") {
					data, etag = configData, "config-etag"
					if r.Method == http.MethodHead {
						configHeads++
					}
					if executing && (mode == "config-changed" || mode == "changed-before-insert" && configHeads >= 3) {
						etag = "replacement-config"
					}
				} else if r.URL.Path != "/bucket/"+physicalKey {
					t.Errorf("unexpected key %s", r.URL)
					w.WriteHeader(404)
					return
				} else if executing && mode == "manifest-changed" {
					etag = "replacement-manifest"
				}
				w.Header().Set("ETag", `"`+etag+`"`)
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				if r.Method == http.MethodGet {
					if r.Header.Get("If-Match") != `"`+etag+`"` {
						w.WriteHeader(412)
						return
					}
					w.Write(data)
				}
			}))
			defer fakeS3.Close()
			fakeKube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("kubernetes mutation %s", r.Method)
					w.WriteHeader(500)
					return
				}
				if strings.Contains(r.URL.Path, "/backuptargets/") {
					json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"backupTargetURL": manifest.Metadata["backup_target_url"]}, "status": map[string]any{"available": mode != "target-unavailable"}})
					return
				}
				state, label := "Completed", runID
				if mode == "remote-error" {
					state = "Error"
				}
				if mode == "wrong-run-label" {
					label = "backup_run_other"
				}
				json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "backup-test", "labels": map[string]any{"fugue.pro/backup-run": label}}, "spec": map[string]any{"snapshotName": "snapshot-test"}, "status": map[string]any{"state": state, "url": backupURL, "volumeName": "volume-test"}})
			}))
			defer fakeKube.Close()
			db := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := db.Init(); err != nil {
				t.Fatal(err)
			}
			backend, err := db.CreateBackupBackend(model.BackupBackend{Name: "reconciliation", Provider: model.DataBackendProviderCloudflareR2, Bucket: "bucket", Prefix: "backup-root", Endpoint: fakeS3.URL, Credentials: model.DataBackendCredentials{AccessKeyID: "test", SecretAccessKey: "secret"}})
			if err != nil {
				t.Fatal(err)
			}
			policyID := ""
			if mode == "legacy" {
				policy, err := db.UpsertBackupPolicy(model.BackupPolicy{Name: "legacy-policy", Target: target, BackendID: backend.ID, Enabled: true, Schedule: "@hourly", RetainCount: 3})
				if err != nil {
					t.Fatal(err)
				}
				policyID = policy.ID
			}
			run, err := db.CreateBackupRun(model.BackupRun{ID: runID, PolicyID: policyID, Target: target, BackendID: backend.ID, Status: model.BackupRunStatusFailed, ErrorCode: "backup_failed", ErrorMessage: "artifact persistence failed", FinishedAt: &created})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "existing-artifact" {
				_, err := db.CreateBackupArtifact(model.BackupArtifact{RunID: runID, Target: target, BackendID: backend.ID, Kind: manifest.Kind, ManifestObjectKey: logicalKey})
				if err != nil {
					t.Fatal(err)
				}
			}
			s := NewServer(db, auth.New(db, "bootstrap-test"), nil, ServerConfig{})
			s.newBackupLonghornClient = func(string) (*kubeLogsClient, error) {
				return &kubeLogsClient{baseURL: fakeKube.URL, client: fakeKube.Client()}, nil
			}
			request := orphanManifestAdoptRequest{RunID: runID, ObjectKey: physicalKey}
			preview, err := s.inspectOrphanManifestAdoption(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			wantEligible := mode == "safe" || mode == "legacy" || strings.Contains(mode, "changed") || mode == "changed-before-insert"
			if preview.CanAdopt != wantEligible {
				t.Fatalf("preview=%+v", preview)
			}
			if mode != "existing-artifact" {
				if exists, _ := db.BackupArtifactExistsForRun(runID); exists {
					t.Fatal("preview created artifact")
				}
			}
			executing = true
			request.Execute, request.ExpectedETag, request.ExpectedConfigETag = true, "manifest-etag", "config-etag"
			payload, _ := json.Marshal(request)
			req := httptest.NewRequest(http.MethodPost, "/v1/admin/storage/orphan-manifests/adopt", strings.NewReader(string(payload)))
			req.Header.Set("Authorization", "Bearer bootstrap-test")
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if mode == "safe" || mode == "legacy" {
				if rec.Code != 200 {
					t.Fatalf("status=%d %s", rec.Code, rec.Body)
				}
				var response struct {
					Adoption orphanManifestAdoptResult `json:"adoption"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				artifact, err := db.GetBackupArtifact(response.Adoption.ArtifactID, "", true)
				if err != nil || !artifact.Protected || artifact.AppID != "" || artifact.Manifest.Metadata["restore_verification"] != "not_run" {
					t.Fatalf("artifact=%+v err=%v", artifact, err)
				}
				if artifact.PolicyID != run.PolicyID || artifact.Manifest.Metadata["longhorn_namespace"] != "longhorn-system" {
					t.Fatalf("legacy ownership was not reconciled: %+v", artifact)
				}
				updated, _ := db.GetBackupRun(runID, "", true)
				if updated.Status != run.Status || updated.ErrorMessage != run.ErrorMessage || updated.ArtifactCount != 1 {
					t.Fatalf("run history rewritten: %+v", updated)
				}
				if _, err := db.MarkBackupArtifactDeleted(artifact.ID, "", true); err == nil {
					t.Fatal("protected artifact was deletable")
				}
				if _, err := s.inspectOrphanManifestAdoption(context.Background(), request); err != nil {
					t.Fatal(err)
				}
				if again, _ := s.inspectOrphanManifestAdoption(context.Background(), request); again.CanAdopt {
					t.Fatal("duplicate adoption allowed")
				}
			} else if rec.Code != 409 {
				t.Fatalf("unsafe mode %s status=%d %s", mode, rec.Code, rec.Body)
			}
			if providerWrites != 0 {
				t.Fatal("reconciliation modified provider objects")
			}
		})
	}
}

func TestOrphanManifestAdoptionRequiresPlatformAdmin(t *testing.T) {
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("test")
	_, token, _ := db.CreateAPIKey(tenant.ID, "storage-admin", []string{"storage.admin"})
	s := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/storage/orphan-manifests/adopt", strings.NewReader(`{"run_id":"backup_run_test","object_key":"root/manifest.json"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d %s", rec.Code, rec.Body)
	}
}
