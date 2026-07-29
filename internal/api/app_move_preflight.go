package api

import (
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

// managedPostgresMoveEvidence is deliberately read-only.  An app-move
// preflight must report the evidence that exists in the backup ledger; it must
// never manufacture a successful backup/restore result merely because the
// operation chain contains those words.
type managedPostgresMoveEvidence struct {
	backupStatus      string
	restoreStatus     string
	grantVerification string
	backupReady       bool
	restoreReady      bool
	grantsReady       bool
}

func (s *Server) managedPostgresMoveEvidence(app model.App, requiresLocalization bool) managedPostgresMoveEvidence {
	evidence := managedPostgresMoveEvidence{
		backupStatus:      "not_configured",
		restoreStatus:     "not_required",
		grantVerification: "not_required",
	}

	policies, policyErr := s.store.ListBackupPolicies(store.BackupPolicyFilter{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		TargetType:      model.BackupTargetAppDatabase,
		IncludeDisabled: true,
		PlatformAdmin:   true,
		Limit:           100,
	})
	artifacts, artifactErr := s.store.ListBackupArtifacts(store.BackupArtifactFilter{
		TenantID:      app.TenantID,
		AppID:         app.ID,
		TargetType:    model.BackupTargetAppDatabase,
		ActiveOnly:    true,
		PlatformAdmin: true,
		Limit:         100,
	})
	if policyErr != nil || artifactErr != nil {
		evidence.backupStatus = "unavailable"
	} else if policy, ok := preferredBackupPolicyForTarget(policies, model.BackupTargetAppDatabase); !ok || !policy.Enabled || policy.Status != model.BackupPolicyStatusActive {
		evidence.backupStatus = "disabled"
	} else if backupArtifactEvidenceComplete(artifacts) {
		evidence.backupStatus = "ready"
		evidence.backupReady = true
	} else {
		evidence.backupStatus = "missing"
	}

	if !requiresLocalization {
		return evidence
	}

	// A cross-runtime move requires a restore/switchover proof after the
	// database dependency has converged.  Restore runs are tenant-scoped in the
	// store API, so filter them by AppID and the app-database target here.
	plans, planErr := s.store.ListBackupRestorePlans(app.TenantID, true, 100)
	runs, runErr := s.store.ListBackupRestoreRuns(app.TenantID, true, 100)
	if planErr != nil || runErr != nil {
		evidence.restoreStatus = "unavailable"
		evidence.grantVerification = "unavailable"
		return evidence
	}
	for _, run := range runs {
		if strings.TrimSpace(run.AppID) != strings.TrimSpace(app.ID) || strings.TrimSpace(run.Status) != model.BackupRestoreStatusSucceeded {
			continue
		}
		for _, plan := range plans {
			if plan.ID != run.PlanID || plan.Target.Type != model.BackupTargetAppDatabase {
				continue
			}
			evidence.restoreStatus = "ready"
			evidence.restoreReady = true
			evidence.grantVerification = "verified"
			evidence.grantsReady = true
			return evidence
		}
	}
	for _, run := range runs {
		if strings.TrimSpace(run.AppID) == strings.TrimSpace(app.ID) &&
			(run.Status == model.BackupRestoreStatusPlanned || run.Status == model.BackupRestoreStatusRunning) {
			evidence.restoreStatus = "pending"
			evidence.grantVerification = "pending"
			return evidence
		}
	}
	evidence.restoreStatus = "missing"
	evidence.grantVerification = "not_verified"
	return evidence
}

func backupArtifactEvidenceComplete(artifacts []model.BackupArtifact) bool {
	for _, artifact := range artifacts {
		if artifact.Target.Type != model.BackupTargetAppDatabase ||
			artifact.Status != model.BackupArtifactStatusActive {
			continue
		}
		if strings.TrimSpace(artifact.ObjectKey) == "" && strings.TrimSpace(artifact.ManifestObjectKey) == "" {
			continue
		}
		if strings.TrimSpace(artifact.SHA256) == "" && strings.TrimSpace(artifact.ManifestDigest) == "" && strings.TrimSpace(artifact.Manifest.SHA256) == "" {
			continue
		}
		return true
	}
	return false
}

// appMoveImageBlobEvidence checks the persisted distributed-image graph when
// one exists.  Registry-only apps intentionally remain compatible: there is
// no local graph to inspect until the image is imported.  Once an image row is
// present, however, a manifest-only row is never considered deployable.
func (s *Server) appMoveImageBlobEvidence(app model.App) (bool, string, error) {
	imageRef := strings.TrimSpace(app.Spec.Image)
	if imageRef == "" {
		return true, "app has no container image", nil
	}
	images, err := s.store.ListImages(model.ImageFilter{
		TenantID:      app.TenantID,
		AppID:         app.ID,
		ImageRef:      imageRef,
		PlatformAdmin: true,
	})
	if err != nil {
		return false, "image metadata lookup failed", err
	}
	if len(images) == 0 {
		// Some externally hosted images are intentionally not represented in the
		// distributed store. Their registry check remains the controller's
		// responsibility; do not invent a local failure here.
		return true, "image is not tracked in the distributed cache", nil
	}
	for _, image := range images {
		if image.LifecycleState != model.ImageLifecycleAvailable {
			continue
		}
		if store.CanonicalImageDigest(image.CanonicalDigest) == "" {
			continue
		}
		if imageManifestHasCompleteBlobGraph(image.ManifestJSON) && image.BlobBytes > 0 {
			return true, "tracked image manifest, config, and layers are present", nil
		}
		manifests, manifestErr := s.store.ListImageCacheManifests(model.ImageCacheManifestFilter{
			Digest:      image.CanonicalDigest,
			PresentOnly: true,
		})
		if manifestErr != nil {
			return false, "image-cache manifest lookup failed", manifestErr
		}
		for _, manifest := range manifests {
			if store.CanonicalImageDigest(manifest.Digest) != store.CanonicalImageDigest(image.CanonicalDigest) ||
				!manifest.Present || manifest.TotalBlobBytes <= 0 || len(manifest.ReferencedBlobs) == 0 {
				continue
			}
			return true, "tracked image cache manifest has complete blob evidence", nil
		}
	}
	return false, fmt.Sprintf("tracked image %q has no complete config/layer blob evidence", imageRef), nil
}

func imageManifestHasCompleteBlobGraph(raw string) bool {
	if strings.TrimSpace(raw) == "" {
		return false
	}
	var manifest map[string]any
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		return false
	}
	digestCount := 0
	if config, ok := manifest["config"].(map[string]any); ok && strings.TrimSpace(stringValue(config["digest"])) != "" {
		digestCount++
	}
	if layers, ok := manifest["layers"].([]any); ok {
		for _, item := range layers {
			if layer, ok := item.(map[string]any); ok && strings.TrimSpace(stringValue(layer["digest"])) != "" {
				digestCount++
			}
		}
	}
	if manifests, ok := manifest["manifests"].([]any); ok {
		for _, item := range manifests {
			if child, ok := item.(map[string]any); ok && strings.TrimSpace(stringValue(child["digest"])) != "" {
				digestCount++
			}
		}
	}
	return digestCount > 0
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
