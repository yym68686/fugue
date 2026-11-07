package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

// Claim consumes prior durable full-readback evidence; it never restores a
// cold archive or performs body/network work under the activation locks.
// Release, rollback and consumer admission remain separate work.
func (s *Store) prepareNodeTaskInputAdmission(ctx context.Context, taskID string) (*compilerInputActivationProof, []compilerInputPinBinding, error) {
	proof, err := s.prepareOrdinaryCompilerInputProof(ctx, nodeTaskActivationRoots(taskID), nil, nil, nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("task dependency closure unavailable: %w", errors.Join(err, ErrConflict))
	}
	return s.prepareCompilerInputActivationPins(ctx, proof)
}

func (s *Store) prepareNodeTaskCreateAdmission(ctx context.Context, task model.NodeUpdateTask) (*compilerInputActivationProof, error) {
	proof, err := s.prepareOrdinaryCompilerInputProof(ctx, nodeTaskActivationRoots(task.ID), &task, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("new task dependency closure unavailable: %w", errors.Join(err, ErrConflict))
	}
	return proof, nil
}

func (s *Store) prepareCompilerInputActivationPins(ctx context.Context, proof *compilerInputActivationProof) (*compilerInputActivationProof, []compilerInputPinBinding, error) {
	if len(proof.inputs) == 0 {
		return proof, nil, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	for _, q := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return nil, nil, err
		}
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return nil, nil, err
	}
	if err := schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
		return nil, nil, err
	}
	pins := make([]compilerInputPinBinding, 0, len(proof.inputs))
	for _, digest := range proof.inputs {
		binding := compilerInputPinBinding{Digest: digest}
		// Selection is not proof. The current writable transaction later
		// locks and rechecks the exact token, epoch and representation for
		// EVERY selected pin together with every prepared graph tuple.
		if err := tx.QueryRowContext(ctx, `SELECT p.pin_key,p.token FROM fugue_platform_input_local_pins p
 JOIN fugue_platform_input_manifests m ON m.content_hash=p.content_hash
 JOIN fugue_platform_input_archive_control c ON c.singleton
 WHERE p.content_hash=$1 AND p.protected_content_hash=$1 AND p.state='verified' AND p.verified_at IS NOT NULL
 AND p.source_epoch=c.epoch AND (p.source_format,p.source_location,p.raw_bytes,p.chunk_size,p.chunk_count,p.source_created_at)
 =(m.format,m.storage_location,m.size_bytes,m.chunk_size,m.chunk_count,m.created_at)
 LIMIT 1`, digest).Scan(&binding.Key, &binding.Token); err != nil {
			return nil, nil, fmt.Errorf("independent input readback not prepared: %w", errors.Join(err, ErrConflict))
		}
		pins = append(pins, binding)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	return proof, pins, nil
}
