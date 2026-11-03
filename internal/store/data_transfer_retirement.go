package store

import (
	"context"
	"fugue/internal/model"
	"sort"
	"time"
)

func (s *Store) ListIdleDataTransferIDs(ctx context.Context, before, now time.Time) ([]string, error) {
	out := []string{}
	if s.usingDatabase() {
		rows, err := s.db.QueryContext(ctx, `SELECT id FROM fugue_data_transfers WHERE
   (status IN ('planned','running') AND direction IN ('upload','download') AND updated_at<$1 AND (expires_at IS NULL OR expires_at<$2))
   OR (status='canceled' AND error_code='idle_retired' AND plan_blobs_json<>'[]'::jsonb)
   ORDER BY updated_at,id LIMIT 20`, before, now)
		if err != nil {
			return nil, mapDBErr(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			out = append(out, id)
		}
		return out, mapDBErr(rows.Err())
	}
	err := s.withLockedState(false, func(st *model.State) error {
		for _, t := range st.DataTransfers {
			if ((t.Status == "planned" || t.Status == "running") && (t.Direction == "upload" || t.Direction == "download") && t.UpdatedAt.Before(before) && (t.ExpiresAt == nil || t.ExpiresAt.Before(now))) || (t.Status == "canceled" && t.ErrorCode == "idle_retired" && len(t.PlanBlobs) > 0) {
				out = append(out, t.ID)
			}
		}
		sort.Strings(out)
		if len(out) > 20 {
			out = out[:20]
		}
		return nil
	})
	return out, err
}
