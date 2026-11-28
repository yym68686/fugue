package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/buildadmission"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

// Reuse is stricter than retry admission: dependencies and selected image
// identity are build inputs too. Keep existing permanent-failure keys stable.
func importBuildReuseFingerprint(source model.AppSource, spec model.AppSpec, policy sourceimport.BuilderPodPolicy) string {
	input := source
	base := buildadmission.InputFingerprint(input, spec, policy)
	if base == "" {
		return ""
	}
	raw, _ := json.Marshal(struct {
		Base, Suffix string
		Dependencies []string
	}{base, source.ImageNameSuffix, source.ComposeDependsOn})
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

// Only hashes and an operation locator live here. Configuration remains in the
// operation, and physical content is reverified against its immutable digest.
type importBuildLocator struct {
	Fingerprint   string   `json:"fingerprint"`
	ResultHash    string   `json:"result_hash"`
	OperationID   string   `json:"operation_id"`
	Digest        string   `json:"digest"`
	CacheEndpoint string   `json:"cache_endpoint"`
	Blobs         []string `json:"blobs,omitempty"`
	BlobBytes     int64    `json:"blob_bytes,omitempty"`
}

func importBuildResultHash(op model.Operation) string {
	model.NormalizeOperationSourceState(&op)
	spec, base := op.DesiredSpec, op.ConfigBaseSpec
	if spec != nil {
		cloned := cloneImportSpec(*spec)
		model.ApplyAppSpecDefaults(&cloned)
		spec = &cloned
	}
	if base != nil {
		cloned := cloneImportSpec(*base)
		model.ApplyAppSpecDefaults(&cloned)
		base = &cloned
	}
	raw, _ := json.Marshal(struct {
		Spec, Base     *model.AppSpec
		Source, Origin *model.AppSource
	}{spec, base, op.DesiredSource, op.DesiredOriginSource})
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

func importBuildLocatorKey(app model.App) string {
	return "import-build-result/v1/" + app.TenantID + "/" + app.ID
}

func (s *Service) saveImportBuildLocator(ctx context.Context, app model.App, op model.Operation, fingerprint string, destination importImageDestination) error {
	if fingerprint == "" || destination.CacheEndpoint == "" || !s.imageStoreDistributedMode() {
		return nil
	}
	if s.verifyDestinationImageCache == nil || op.DesiredSource == nil {
		return fmt.Errorf("build result verifier is unavailable")
	}
	verified, err := s.verifyDestinationImageCache(ctx, destination.CacheEndpoint, op.DesiredSource.ResolvedImageRef)
	if err != nil {
		return fmt.Errorf("verify reusable import result: %w", err)
	}
	if err := validateDestinationImageCacheVerification(verified, op.DesiredSource.ResolvedImageRef); err != nil {
		return err
	}
	digest := store.CanonicalImageDigest(verified.CanonicalDigest)
	locator := importBuildLocator{Fingerprint: fingerprint, ResultHash: importBuildResultHash(op), OperationID: op.ID, Digest: digest, CacheEndpoint: destination.CacheEndpoint, Blobs: verified.ReferencedBlobs, BlobBytes: verified.ReferencedBlobBytes}
	var previous importBuildLocator
	written, err := s.Store.UpdateObservationCheckpoint(ctx, importBuildLocatorKey(app), func(raw []byte) ([]byte, error) { _ = json.Unmarshal(raw, &previous); return json.Marshal(locator) })
	if err == nil && !written {
		return fmt.Errorf("build result locator is concurrently updating")
	}
	if err != nil {
		return err
	}
	shared := 0
	prior := map[string]bool{}
	for _, d := range previous.Blobs {
		prior[d] = true
	}
	for _, d := range verified.ReferencedBlobs {
		if prior[d] {
			shared++
		}
	}
	s.recordOperationEvidenceBestEffort(model.OperationEvidence{TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		Type: "build_storage_footprint", Source: model.OperationEvidenceSourceImportController, ObservedAt: time.Now().UTC(), Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed,
		Summary: "Verified build graph and overlap with previous build", PayloadVersion: 1, RedactionStatus: model.OperationEvidenceRedactionRedacted,
		Payload: map[string]any{"input_fingerprint": fingerprint, "canonical_digest": digest, "referenced_blob_count": len(verified.ReferencedBlobs), "referenced_manifest_count": len(verified.ReferencedManifests), "referenced_blob_bytes": verified.ReferencedBlobBytes, "previous_operation_id": previous.OperationID, "same_inputs_as_previous": previous.Fingerprint == fingerprint, "same_digest_as_previous": previous.Digest == digest, "shared_blob_count": shared, "new_blob_count": len(verified.ReferencedBlobs) - shared}})
	return nil
}

func (s *Service) reusableGitHubBuild(ctx context.Context, app model.App, fingerprint string) (*model.Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	raw, err := s.Store.ReadObservationCheckpoint(ctx, importBuildLocatorKey(app))
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var locator importBuildLocator
	if err = json.Unmarshal(raw, &locator); err != nil {
		return nil, fmt.Errorf("read saved import result: %w", err)
	}
	if fingerprint == "" || locator.Fingerprint != fingerprint {
		return nil, nil
	}
	op, err := s.Store.GetOperation(locator.OperationID)
	if err != nil {
		return nil, fmt.Errorf("read reusable import operation: %w", err)
	}
	if op.AppID != app.ID || op.TenantID != app.TenantID || op.Type != model.OperationTypeImport || op.Status == model.OperationStatusCanceled ||
		op.DesiredSpec == nil || op.DesiredSource == nil || op.ConfigBaseSpec == nil ||
		op.DesiredSource.ResolvedImageRef == "" || !model.IsGitHubAppSourceType(op.DesiredSource.Type) {
		return nil, fmt.Errorf("saved import result has incomplete or mismatched identity")
	}
	if locator.ResultHash == "" || locator.ResultHash != importBuildResultHash(op) {
		return nil, fmt.Errorf("saved import operation result changed")
	}
	ref := imageRefWithDigest(op.DesiredSource.ResolvedImageRef, locator.Digest)
	if ref == "" || locator.CacheEndpoint == "" {
		return nil, fmt.Errorf("saved import result lacks immutable physical identity")
	}
	digest, _, err := s.resolveImportedImageDigestFromDestinationCache(ctx, ref, importImageDestination{CacheEndpoint: locator.CacheEndpoint})
	if err != nil {
		return nil, fmt.Errorf("verify saved import result: %w", err)
	}
	if digest != locator.Digest {
		return nil, fmt.Errorf("saved import digest changed: got=%s expected=%s", digest, locator.Digest)
	}
	copy := op
	spec := cloneImportSpec(*op.DesiredSpec)
	spec.Image = imageRefWithDigest(spec.Image, locator.Digest)
	if spec.Image == "" {
		return nil, fmt.Errorf("saved import result lacks runtime image identity")
	}
	copy.DesiredSpec = &spec
	copy.DesiredSource = model.CloneAppSource(op.DesiredSource)
	copy.DesiredSource.ResolvedImageRef = ref
	return &copy, nil
}

// Changed decisions are emitted once; polling blocked intent never creates a
// new failed operation. Neither credentials nor environment values are recorded.
func (s *Service) recordGitHubReleaseDecision(ctx context.Context, app model.App, commit, fingerprint, decision string, state trackedGitHubCommitState, inventoryCount int) {
	fact := struct {
		Commit           string `json:"commit"`
		Fingerprint      string `json:"input_fingerprint"`
		Decision         string `json:"decision"`
		Failures         int    `json:"consecutive_failures"`
		FailureOperation string `json:"last_failure_operation_id"`
		ManualOperation  string `json:"last_manual_operation_id"`
		InventoryCount   int    `json:"operation_inventory_count"`
	}{commit, fingerprint, decision, state.consecutiveAutomaticFailures, state.lastAutomaticFailure.ID, state.lastManualOperation.ID, inventoryCount}
	encoded, _ := json.Marshal(fact)
	changed := false
	written, err := s.Store.UpdateObservationCheckpoint(ctx, "github-release-decision/v1/"+app.TenantID+"/"+app.ID, func(raw []byte) ([]byte, error) {
		if string(raw) == string(encoded) {
			return nil, nil
		}
		changed = true
		return encoded, nil
	})
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("github release decision persistence failed app=%s: %v", app.ID, err)
		}
		return
	}
	if !written || !changed {
		return
	}
	if s.Logger != nil {
		s.Logger.Printf("github release decision app=%s facts=%s", app.ID, encoded)
	}
	if state.lastAutomaticFailure.ID != "" {
		var payload map[string]any
		_ = json.Unmarshal(encoded, &payload)
		s.recordOperationEvidenceBestEffort(model.OperationEvidence{TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: state.lastAutomaticFailure.ID,
			Type: "source_release_decision", Source: model.OperationEvidenceSourceImportController, ObservedAt: time.Now().UTC(),
			Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed,
			Summary: "Automatic source release: " + decision, Payload: payload, PayloadVersion: 1, RedactionStatus: model.OperationEvidenceRedactionRedacted})
	}
}
