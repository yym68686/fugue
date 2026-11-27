package store

import (
	"context"
	"time"
)

func (s *Store) ListIdleDataTransferIDs(ctx context.Context, before, now time.Time) ([]string, error) {
	out := []string{}
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
