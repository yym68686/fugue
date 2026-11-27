package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestDiscardNewSourceUploadRequiresExactCapabilityAndRefusesReferences(t *testing.T) {
	t.Parallel()

	stateStore, app, _, upload := appDatabaseImportLifecycleFixture(t)
	for _, testCase := range []struct {
		name  string
		proof model.SourceUpload
	}{
		{
			name: "wrong tenant",
			proof: func() model.SourceUpload {
				proof := upload
				proof.TenantID = "tenant_other"
				return proof
			}(),
		},
		{
			name: "wrong token",
			proof: func() model.SourceUpload {
				proof := upload
				proof.DownloadToken = "fugue_upload_wrong"
				return proof
			}(),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := stateStore.DiscardNewSourceUpload(testCase.proof); !errors.Is(err, ErrNotFound) {
				t.Fatalf("DiscardNewSourceUpload error = %v, want not found", err)
			}
			if _, archive, err := stateStore.GetSourceUploadArchive(upload.ID); err != nil || string(archive) != "select 1;" {
				t.Fatalf("invalid proof removed upload: archive=%q err=%v", archive, err)
			}
		})
	}

	if _, err := stateStore.CreateAppDatabaseImportJob(appDatabaseImportLifecycleJob(app, upload)); err != nil {
		t.Fatalf("create upload reference: %v", err)
	}
	if err := stateStore.DiscardNewSourceUpload(upload); !errors.Is(err, ErrConflict) {
		t.Fatalf("DiscardNewSourceUpload referenced error = %v, want conflict", err)
	}
	if _, archive, err := stateStore.GetSourceUploadArchive(upload.ID); err != nil || string(archive) != "select 1;" {
		t.Fatalf("referenced upload was removed: archive=%q err=%v", archive, err)
	}
}
