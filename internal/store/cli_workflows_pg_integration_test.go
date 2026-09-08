package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"fugue/internal/model"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCLIWorkflowPostgresIntegration(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable Postgres integration")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("this test requires a disposable loopback fugue_test database")
	}
	s := New("", address)
	if err = s.Init(); err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	suffix := model.NewID("cli_workflow")
	tenant, err := s.CreateTenant(suffix)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_tenants WHERE id=$1`, tenant.ID)
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatal(err)
	}
	rt, _, err := s.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "example/demo:latest", Replicas: 1, RuntimeID: rt.ID})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("immutable source fixture")
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	session, err := s.CreateSourceUploadSession(model.SourceUploadSession{TenantID: tenant.ID, RequestID: suffix, Filename: "source.tgz", SizeBytes: int64(len(data)), SHA256: hash})
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_source_upload_sessions WHERE id=$1`, session.ID)
	if _, err = s.PutSourceUploadChunk(session.ID, 0, hash, data); err != nil {
		t.Fatal(err)
	}
	session, err = s.CompleteSourceUploadSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, started, err := s.BeginSourceUploadSubmission(session.ID, hash); err != nil || !started {
		t.Fatal(started, err)
	}
	source := model.AppSource{Type: model.AppSourceTypeUpload, UploadID: session.UploadID}
	op, err := s.CreateSourceSessionOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: &app.Spec, DesiredSource: &source}, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	session, err = s.GetSourceUploadRequest(tenant.ID, suffix)
	if err != nil || len(session.OperationIDs) != 1 || session.OperationIDs[0] != op.ID {
		t.Fatal(session, err)
	}
	if _, err = s.FinishSourceUploadSubmission(session.ID, 202); err != nil {
		t.Fatal(err)
	}
	if err = s.CleanupSourceUploadChunks(); err != nil {
		t.Fatal(err)
	}
	ws, err := s.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "dataset", ProjectID: project.ID})
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	tr, err := s.CreateDataTransfer(model.DataTransfer{TenantID: tenant.ID, WorkspaceID: ws.ID, Direction: "prewarm", Status: "planned", Target: rt.ID, ExpiresAt: &expires, Cache: &model.DataPrewarmCache{Node: "node-test", State: "planned", ObservedAt: time.Now().UTC()}})
	if err != nil {
		t.Fatal(err)
	}
	tr.Status = "running"
	tr, err = s.UpdateDataTransfer(tr)
	if err != nil || tr.Cache == nil || tr.Cache.Node != "node-test" {
		t.Fatal(tr, err)
	}
	stale := tr
	canceled, err := s.CancelDataTransfer(tr.ID)
	if err != nil {
		t.Fatal(err)
	}
	stale.Status = "completed"
	if _, err = s.UpdateDataTransfer(stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale worker did not conflict: %v", err)
	}
	blockers, err := s.InspectDataDeletion(ws, nil)
	if err != nil || len(blockers) == 0 {
		t.Fatal(blockers, err)
	}
	canceled.Cache.State = "removed"
	if _, err = s.UpdateDataTransfer(canceled); err != nil {
		t.Fatal(err)
	}
	refs, err := s.ListDataPrewarmsForReconcile(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if ref.ID == tr.ID {
			t.Fatal("reclaimed cache scheduled again")
		}
	}
	images, err := s.ListImages(model.ImageFilter{TenantID: tenant.ID, ProjectID: project.ID})
	if err != nil || len(images) != 0 {
		t.Fatal(images, err)
	}
	if _, err = s.ListImageReplicationTasks(model.ImageReplicationTaskFilter{TenantID: tenant.ID, ProjectID: project.ID}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	desired := app.Spec
	desired.Data = &model.AppDataMaterializationSpec{Workspaces: []model.AppDataWorkspaceMaterialization{{WorkspaceID: ws.ID}}}
	if err = lockDataReferencesTx(context.Background(), tx, tenant.ID, &desired); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	deleted := make(chan error, 1)
	go func() { _, e := s.DeleteDataWorkspace(ws.ID, tenant.ID, false); deleted <- e }()
	select {
	case e := <-deleted:
		tx.Rollback()
		t.Fatalf("delete bypassed an in-flight reference: %v", e)
	case <-time.After(50 * time.Millisecond):
	}
	app.Spec = desired
	if err = s.pgUpdateAppTx(context.Background(), tx, app); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if e := <-deleted; !errors.Is(e, ErrConflict) {
		t.Fatalf("committed reference did not block waiting deletion: %v", e)
	}
	if _, err = s.db.Exec(`UPDATE fugue_apps SET spec_json=spec_json-'data' WHERE id=$1`, app.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DeleteDataWorkspace(ws.ID, tenant.ID, false); err != nil {
		t.Fatal(err)
	}
}
