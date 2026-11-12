package api

import "fugue/internal/model"

// The cleanup executor shares this Lease with every Longhorn backup, not only
// platform backups. A tenant snapshot writer must not race a native purge.
func backupRunNeedsCoordinationLease(run model.BackupRun) bool {
	target := model.NormalizeBackupTarget(run.Target)
	return target.Type == model.BackupTargetControlPlaneDatabase || target.Engine == model.BackupEngineLonghornSnapshot
}
