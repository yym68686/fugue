package imagetracking

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestFileRepositoryKeepsTenantAndDeploymentBoundaries(t *testing.T) {
	app := model.App{ID: "app", TenantID: "tenant"}
	rows := Rows{LookupApp: func(id string) (model.App, bool) { return app, id == app.ID }}
	var writes int
	notFound := errors.New("app or tracking not found")
	repo := NewFile(func(write bool, apply func(*Rows) error) error {
		if write {
			writes++
		}
		return apply(&rows)
	}, OperationWriter{}, Errors{NotFound: notFound})
	ref := "example.invalid/app:stable"
	if _, err := repo.UpsertAppImageTracking(model.AppImageTracking{AppID: app.ID, TenantID: "other", ImageRef: ref}); !errors.Is(err, notFound) {
		t.Fatalf("cross-tenant write: %v", err)
	}
	if len(rows.Trackings) != 0 {
		t.Fatal("cross-tenant write changed rows")
	}
	tracking, err := repo.UpsertAppImageTracking(model.AppImageTracking{AppID: app.ID, TenantID: app.TenantID, ImageRef: ref, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetAppImageTracking("other", false, app.ID); !errors.Is(err, notFound) {
		t.Fatalf("cross-tenant read: %v", err)
	}
	now := time.Now().UTC()
	RecordDeployment(&rows, model.Operation{
		ID: "deploy", AppID: app.ID, Type: model.OperationTypeDeploy,
		DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: ref, ResolvedImageRef: ref + "@sha256:deployed"},
	}, now)
	got, err := repo.GetAppImageTracking(app.TenantID, false, app.ID)
	if err != nil || got.ID != tracking.ID || got.LastDeployedDigest != "sha256:deployed" || got.LastOperationID != "deploy" {
		t.Fatalf("deployed marker: %+v, %v", got, err)
	}
	// Deployment was applied to the caller's transaction, not via another lock.
	if writes != 2 {
		t.Fatalf("expected two upsert transaction entries, got %d", writes)
	}
	rows.DeleteApp(app.ID)
	if len(rows.Trackings) != 0 {
		t.Fatal("application deletion left tracking rows")
	}
}
