package controller

import (
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
	corev1 "k8s.io/api/core/v1"
)

// A known storage class advertises a rollout capability, not proof that its
// live claims still have that capability. Validate fresh PVC/PV observations
// at the child-object write boundary, including candidate and rollback paths.
func (p *storagePlacement) validateSameNodeOnlineRollout(app model.App, scheduling runtime.SchedulingConstraints, allowNewClaims bool) error {
	if !appUsesOnlineDurableRolloutIntent(app) || !strings.EqualFold(strings.TrimSpace(zeroDowntimeAppStorageClass(app.Spec)), model.AppStorageClassFugueLonghornRWO) {
		return nil
	}
	node := strings.TrimSpace(scheduling.NodeSelector[kubeHostnameLabelKey])
	if len(p.claims) == 0 {
		return &storagePlacementRejection{code: "storage_claims_unobserved", message: "online rollout requires observed claims and a validated current-node placement", detail: "Longhorn online rollout requires observed claims and a validated current-node placement"}
	}
	for _, claim := range p.claims {
		prefix := "Longhorn online rollout claim " + claim.name + ": "
		reject := func(code, message string) error {
			return &storagePlacementRejection{code: code, message: message, detail: prefix + message}
		}
		if claim.class.Provisioner != longhornCSIDriver || (claim.pv != nil && claim.driver != longhornCSIDriver) {
			return reject("storage_driver_incompatible", "live volume and storage class must use the Longhorn CSI driver")
		}
		pvc, pv := claim.pvc, claim.pv
		// Initial creation can carry an online intent after a failed build.
		// An unbound new claim has no mount to overlap. Existing deployments
		// and bound claims still require the full live-volume proof below.
		if allowNewClaims && pv == nil && pvc != nil && (pvc.Name == "" || filesystemRWO(pvc.Spec.AccessModes, pvc.Spec.VolumeMode)) {
			continue
		}
		if pvc == nil || pv == nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != pv.Name || pv.Spec.ClaimRef == nil || pv.Spec.StorageClassName != claim.class.Name {
			return reject("storage_binding_unverified", "a bound matching PVC and PV are required")
		}
		if !filesystemRWO(pvc.Spec.AccessModes, pvc.Spec.VolumeMode) || !filesystemRWO(pv.Spec.AccessModes, pv.Spec.VolumeMode) {
			return reject("storage_access_mode_incompatible", "only filesystem ReadWriteOnce volumes support this online rollout")
		}
		if node == "" {
			return reject("storage_same_node_pin_required", "a validated current-node placement is required")
		}
		for attached := range claim.attachedNodes {
			if attached != node {
				return reject("storage_single_writer_conflict", "volume is in use on a different node")
			}
		}
	}
	return nil
}

func filesystemRWO(modes []corev1.PersistentVolumeAccessMode, mode *corev1.PersistentVolumeMode) bool {
	return len(modes) == 1 && modes[0] == corev1.ReadWriteOnce && (mode == nil || *mode == corev1.PersistentVolumeFilesystem)
}
