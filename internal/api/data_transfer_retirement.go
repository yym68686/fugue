package api

import (
	"context"
	"errors"
	"fugue/internal/model"
	"fugue/internal/store"
	"time"
)

func (s *Server) retireIdleDataTransfers(ctx context.Context, execute bool) (map[string]any, error) {
	now := time.Now().UTC()
	ids, err := s.store.ListIdleDataTransferIDs(ctx, now.Add(-30*24*time.Hour), now)
	result := map[string]any{"transfer_ids": ids, "idle_days": 30, "preview": !execute, "retired": 0}
	if err != nil || !execute {
		return result, err
	}
	retired := 0
	for _, id := range ids {
		t, err := s.store.GetDataTransfer(id)
		if err != nil {
			return result, err
		}
		if t.Status != model.DataTransferStatusCanceled {
			if !t.UpdatedAt.Before(now.Add(-30*24*time.Hour)) || (t.ExpiresAt != nil && !t.ExpiresAt.Before(now)) {
				continue
			}
			t.Status = model.DataTransferStatusCanceled
			t.ErrorCode = "idle_retired"
			t.ErrorMessage = "retired after 30 days without activity; authorization expiry alone does not expire a task"
			t.FinishedAt = &now
			t, err = s.store.UpdateDataTransfer(t)
			if errors.Is(err, store.ErrConflict) {
				continue
			}
			if err != nil {
				return result, err
			}
		}
		if len(t.PlanBlobs) > 0 {
			w, err := s.store.GetDataWorkspace(t.WorkspaceID, "", true)
			if err != nil {
				return result, err
			}
			b, err := s.store.GetDataBackendForUse(w.StorageBackendID, "", true)
			if err != nil {
				return result, err
			}
			if dataBackendSupportsDirectObjectStorage(b) {
				objects, err := newDataObjectBackend(b)
				if err != nil {
					return result, err
				}
				for _, blob := range t.PlanBlobs {
					if blob.UploadID != "" {
						if err := objects.abortMultipartUpload(ctx, blob.ObjectKey, blob.UploadID); err != nil {
							return result, err
						}
					}
				}
			}
			t.PlanBlobs = []model.DataTransferPlanBlob{}
			if _, err = s.store.UpdateDataTransfer(t); err != nil {
				return result, err
			}
		}
		retired++
	}
	result["retired"] = retired
	return result, nil
}
