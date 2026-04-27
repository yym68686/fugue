package model

import (
	"fmt"
	"strings"
)

func NormalizeAppVolumeReplicationMode(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", AppVolumeReplicationModeDisabled:
		return AppVolumeReplicationModeDisabled, nil
	case AppVolumeReplicationModeManual:
		return AppVolumeReplicationModeManual, nil
	case AppVolumeReplicationModeScheduled:
		return AppVolumeReplicationModeScheduled, nil
	default:
		return "", fmt.Errorf("volume replication mode must be disabled, manual, or scheduled")
	}
}

func AppSpecHasReplicableVolume(spec AppSpec) bool {
	if spec.Workspace != nil {
		return true
	}
	return spec.PersistentStorage != nil && !AppPersistentStorageSpecUsesSharedProjectRWX(spec.PersistentStorage)
}

func EffectiveAppVolumeReplicationMode(spec AppSpec) string {
	if spec.VolumeReplication != nil {
		mode, err := NormalizeAppVolumeReplicationMode(spec.VolumeReplication.Mode)
		if err != nil {
			return ""
		}
		return mode
	}
	if spec.Failover != nil && AppSpecHasReplicableVolume(spec) {
		return AppVolumeReplicationModeScheduled
	}
	return AppVolumeReplicationModeDisabled
}

func AppSpecVolumeReplicationEnabled(spec AppSpec) bool {
	switch EffectiveAppVolumeReplicationMode(spec) {
	case AppVolumeReplicationModeManual, AppVolumeReplicationModeScheduled:
		return AppSpecHasReplicableVolume(spec)
	default:
		return false
	}
}

func EffectiveAppVolumeReplicationSchedule(spec AppSpec) string {
	if spec.VolumeReplication != nil {
		if schedule := strings.TrimSpace(spec.VolumeReplication.Schedule); schedule != "" {
			return schedule
		}
	}
	return DefaultAppVolumeReplicationSchedule
}
