package store

import (
	"context"
	"encoding/json"
	"fugue/internal/model"
	"time"
)

func (s *Store) ListStorageMaintenanceJobs() ([]model.StorageMaintenanceJob, error) {
	out := []model.StorageMaintenanceJob{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Include every actionable job even when newer history fills the display.
	rows, err := s.db.QueryContext(ctx, `SELECT body FROM fugue_storage_maintenance_jobs
   WHERE status IN ('queued','running','retrying')
      OR id IN (SELECT id FROM fugue_storage_maintenance_jobs ORDER BY updated_at DESC,id DESC LIMIT 50)
      OR id IN (SELECT DISTINCT ON (kind,scope_id,(body->>'execute')) id FROM fugue_storage_maintenance_jobs ORDER BY kind,scope_id,(body->>'execute'),updated_at DESC,id DESC)
   ORDER BY updated_at DESC,id DESC`)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var v model.StorageMaintenanceJob
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, mapDBErr(rows.Err())
}

func (s *Store) GetStorageMaintenanceJob(id string) (model.StorageMaintenanceJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	var v model.StorageMaintenanceJob
	if err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_storage_maintenance_jobs WHERE id=$1", id).Scan(&raw); err != nil {
		return v, mapDBErr(err)
	}
	err := json.Unmarshal(raw, &v)
	return v, err
}

func (s *Store) CreateStorageMaintenanceJob(v model.StorageMaintenanceJob) (model.StorageMaintenanceJob, error) {
	v.ID = model.NewID("storage_job")
	v.Status = "queued"
	v.CreatedAt = time.Now().UTC()
	v.UpdatedAt = v.CreatedAt
	raw, err := json.Marshal(v)
	if err != nil {
		return v, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = s.db.ExecContext(ctx, "INSERT INTO fugue_storage_maintenance_jobs(id,kind,scope_id,status,updated_at,body) VALUES($1,$2,$3,$4,$5,$6)", v.ID, v.Kind, v.ScopeID, v.Status, v.UpdatedAt, raw)
	return v, mapDBErr(err)
}

// Compare-and-swap prevents an expired worker from overwriting newer evidence.
func (s *Store) SaveStorageMaintenanceJob(v model.StorageMaintenanceJob, observed time.Time) error {
	v.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := s.db.ExecContext(ctx, "UPDATE fugue_storage_maintenance_jobs SET status=$2,updated_at=$3,body=$4 WHERE id=$1 AND updated_at=$5", v.ID, v.Status, v.UpdatedAt, raw, observed)
	if err != nil {
		return mapDBErr(err)
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return ErrConflict
	}
	return err
}
