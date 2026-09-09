package api

import (
	"errors"

	"fugue/internal/model"
)

var errBackupPolicyDisabled = errors.New("backup_policy_disabled: queued automatic backup policy is no longer active")

// A queued retry can outlive its policy. Recheck after claiming the run, before
// starting any export or upload. Already-running exports are not canceled by a
// schedule change, and explicit manual runs keep their existing semantics.
func (s *Server) validateAutomaticBackupPolicy(run model.BackupRun) error {
	if run.PolicyID == "" || run.Trigger == model.BackupRunTriggerManual {
		return nil
	}
	policy, err := s.store.GetBackupPolicy(run.PolicyID, "", true)
	if err != nil {
		return err
	}
	if !policy.Enabled || policy.Status != model.BackupPolicyStatusActive {
		return errBackupPolicyDisabled
	}
	return nil
}
