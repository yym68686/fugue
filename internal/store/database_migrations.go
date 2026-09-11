package store

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) CreateDatabaseMigration(m model.DatabaseMigration) (model.DatabaseMigration, error) {
	if strings.TrimSpace(m.ID) == "" || strings.TrimSpace(m.Kind) == "" || strings.TrimSpace(m.ResourceID) == "" || strings.TrimSpace(m.TargetStorageClassName) == "" {
		return model.DatabaseMigration{}, ErrInvalidInput
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	m.UpdatedAt = m.CreatedAt
	if m.Status == "" {
		m.Status = model.DatabaseMigrationStatusPending
	}
	if m.TemporaryReplicaCount <= 0 {
		m.TemporaryReplicaCount = 1
	}
	if s.usingDatabase() {
		_, err := s.db.Exec(`INSERT INTO fugue_database_migrations (id,kind,resource_id,namespace,cluster_name,cluster_uid,initial_instances,initial_system_id,source_storage_class_name,target_storage_class_name,storage_size,temporary_replica_count,status,phase,error_message,result_message,requested_by_type,requested_by_id,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`, m.ID, m.Kind, m.ResourceID, m.Namespace, m.ClusterName, m.ClusterUID, m.InitialInstances, m.InitialSystemID, m.SourceStorageClassName, m.TargetStorageClassName, m.StorageSize, m.TemporaryReplicaCount, m.Status, m.Phase, m.ErrorMessage, m.ResultMessage, m.RequestedByType, m.RequestedByID, m.CreatedAt, m.UpdatedAt)
		if err != nil {
			return model.DatabaseMigration{}, mapDBErr(err)
		}
		return m, nil
	}
	err := s.withLockedState(true, func(state *model.State) error {
		for _, existing := range state.DatabaseMigrations {
			if existing.Kind == m.Kind && existing.ResourceID == m.ResourceID && (existing.Status == model.DatabaseMigrationStatusPending || existing.Status == model.DatabaseMigrationStatusRunning) {
				return ErrConflict
			}
		}
		state.DatabaseMigrations = append(state.DatabaseMigrations, m)
		return nil
	})
	return m, err
}

func (s *Store) GetDatabaseMigration(id string) (model.DatabaseMigration, error) {
	if s.usingDatabase() {
		var m model.DatabaseMigration
		err := s.db.QueryRow(`SELECT id,kind,resource_id,namespace,cluster_name,cluster_uid,initial_instances,initial_system_id,source_storage_class_name,target_storage_class_name,storage_size,temporary_replica_count,status,phase,error_message,result_message,requested_by_type,requested_by_id,created_at,updated_at,started_at,completed_at FROM fugue_database_migrations WHERE id=$1`, id).Scan(&m.ID, &m.Kind, &m.ResourceID, &m.Namespace, &m.ClusterName, &m.ClusterUID, &m.InitialInstances, &m.InitialSystemID, &m.SourceStorageClassName, &m.TargetStorageClassName, &m.StorageSize, &m.TemporaryReplicaCount, &m.Status, &m.Phase, &m.ErrorMessage, &m.ResultMessage, &m.RequestedByType, &m.RequestedByID, &m.CreatedAt, &m.UpdatedAt, &m.StartedAt, &m.CompletedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return model.DatabaseMigration{}, ErrNotFound
		}
		return m, mapDBErr(err)
	}
	var out model.DatabaseMigration
	err := s.withLockedState(false, func(state *model.State) error {
		for _, m := range state.DatabaseMigrations {
			if m.ID == id {
				out = m
				return nil
			}
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) ListPendingDatabaseMigrations() ([]model.DatabaseMigration, error) {
	if s.usingDatabase() {
		rows, err := s.db.Query(`SELECT id,kind,resource_id,namespace,cluster_name,cluster_uid,initial_instances,initial_system_id,source_storage_class_name,target_storage_class_name,storage_size,temporary_replica_count,status,phase,error_message,result_message,requested_by_type,requested_by_id,created_at,updated_at,started_at,completed_at FROM fugue_database_migrations WHERE status IN ('pending','running') ORDER BY created_at`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []model.DatabaseMigration
		for rows.Next() {
			var m model.DatabaseMigration
			if err := rows.Scan(&m.ID, &m.Kind, &m.ResourceID, &m.Namespace, &m.ClusterName, &m.ClusterUID, &m.InitialInstances, &m.InitialSystemID, &m.SourceStorageClassName, &m.TargetStorageClassName, &m.StorageSize, &m.TemporaryReplicaCount, &m.Status, &m.Phase, &m.ErrorMessage, &m.ResultMessage, &m.RequestedByType, &m.RequestedByID, &m.CreatedAt, &m.UpdatedAt, &m.StartedAt, &m.CompletedAt); err != nil {
				return nil, err
			}
			out = append(out, m)
		}
		return out, rows.Err()
	}
	var out []model.DatabaseMigration
	err := s.withLockedState(false, func(state *model.State) error {
		for _, m := range state.DatabaseMigrations {
			if m.Status == model.DatabaseMigrationStatusPending || m.Status == model.DatabaseMigrationStatusRunning {
				out = append(out, m)
			}
		}
		return nil
	})
	return out, err
}

func (s *Store) UpdateDatabaseMigration(m model.DatabaseMigration) error {
	m.UpdatedAt = time.Now().UTC()
	if s.usingDatabase() {
		_, err := s.db.Exec(`UPDATE fugue_database_migrations SET cluster_uid=$2,initial_instances=$3,initial_system_id=$4,source_storage_class_name=$5,target_storage_class_name=$6,storage_size=$7,temporary_replica_count=$8,status=$9,phase=$10,error_message=$11,result_message=$12,updated_at=$13,started_at=$14,completed_at=$15 WHERE id=$1`, m.ID, m.ClusterUID, m.InitialInstances, m.InitialSystemID, m.SourceStorageClassName, m.TargetStorageClassName, m.StorageSize, m.TemporaryReplicaCount, m.Status, m.Phase, m.ErrorMessage, m.ResultMessage, m.UpdatedAt, m.StartedAt, m.CompletedAt)
		return mapDBErr(err)
	}
	return s.withLockedState(true, func(state *model.State) error {
		for i := range state.DatabaseMigrations {
			if state.DatabaseMigrations[i].ID == m.ID {
				state.DatabaseMigrations[i] = m
				return nil
			}
		}
		return ErrNotFound
	})
}
