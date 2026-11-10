package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

// importInternalCachedImage resolves and verifies a source, independently of
// application placement. Never publish the missing target as a Present replica
// or send an internal registry hostname to the external image importer.
func (s *Service) importInternalCachedImage(ctx context.Context, app model.App, op model.Operation, ref string, target importImageDestination) (sourceimport.GitHubSourceImportOutput, importImageDestination, error) {
	ctx, cancel := context.WithTimeout(ctx, destinationImageCacheVerifyTimeout)
	defer cancel()
	var output sourceimport.GitHubSourceImportOutput
	if strings.TrimSpace(app.TenantID) == "" {
		return output, target, errors.New("internal image import requires a tenant")
	}
	images, err := s.distributedImagesForRef(app, ref)
	if err != nil {
		return output, target, fmt.Errorf("resolve internal image identity: %w", err)
	}
	locations, err := s.presentImageLocations(app, ref)
	if err != nil {
		return output, target, fmt.Errorf("resolve internal image locations: %w", err)
	}
	// A globally shared cache is a physical witness, not tenant authorization.
	if len(images) == 0 && len(locations) == 0 {
		return output, target, fmt.Errorf("internal image is not available to tenant: %s", ref)
	}
	digests := map[string]struct{}{}
	for _, image := range images {
		if digest := store.CanonicalImageDigest(image.CanonicalDigest); digest != "" {
			digests[digest] = struct{}{}
		}
	}
	for _, location := range locations {
		if digest := store.CanonicalImageDigest(location.Digest); digest != "" {
			digests[digest] = struct{}{}
		}
	}
	expectedDigest := imageDigest(ref)
	if expectedDigest == "" {
		if len(digests) > 1 {
			return output, target, fmt.Errorf("internal image has ambiguous canonical digests; use a digest reference: %s", ref)
		}
		for digest := range digests {
			expectedDigest = digest
		}
	}
	verificationRef := ref
	if expectedDigest != "" {
		verificationRef = imageRefWithDigest(ref, expectedDigest)
	}
	candidates := []importImageDestination{target}
	for _, location := range locations {
		if digest := store.CanonicalImageDigest(location.Digest); expectedDigest != "" && digest != "" && digest != expectedDigest {
			continue
		}
		candidates = append(candidates, importImageDestination{
			CacheEndpoint: location.CacheEndpoint,
			Target:        deployImageTarget{RuntimeID: location.RuntimeID, ClusterNodeName: location.ClusterNodeName},
		})
	}
	seen := map[string]struct{}{}
	var failures []error
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return output, target, err
		}
		endpoint := strings.TrimRight(strings.TrimSpace(candidate.CacheEndpoint), "/")
		if endpoint == "" {
			continue
		}
		if _, exists := seen[endpoint]; exists {
			continue
		}
		if len(seen) >= 8 {
			failures = append(failures, errors.New("internal image cache verification attempt limit reached"))
			break
		}
		seen[endpoint] = struct{}{}
		candidate.CacheEndpoint = endpoint
		s.recordInternalImageCacheEvidence(app, op, target, candidate, verificationRef, "started", time.Now().UTC(), nil)
		started := time.Now()
		verified, verifyErr := s.verifyDestinationImageCache(ctx, endpoint, verificationRef)
		if verifyErr == nil {
			verifyErr = validateDestinationImageCacheVerification(verified, verificationRef)
		}
		if verifyErr != nil {
			s.recordInternalImageCacheEvidence(app, op, target, candidate, verificationRef, "failed", started, verifyErr)
			failures = append(failures, fmt.Errorf("cache %s node=%s: %w", endpoint, candidate.Target.ClusterNodeName, verifyErr))
			continue
		}
		// Pin what was actually verified, including legacy tag-only metadata.
		pinnedRef := imageRefWithDigest(ref, verified.CanonicalDigest)
		output = sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				DetectedProvider: model.AppSourceTypeDockerImage,
				ImageRef:         pinnedRef, DestinationImageRef: cacheEndpointImageRef(endpoint, pinnedRef),
			},
			Source: model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: ref, ResolvedImageRef: pinnedRef, DetectedProvider: model.AppSourceTypeDockerImage},
		}
		s.recordInternalImageCacheEvidence(app, op, target, candidate, pinnedRef, "completed", started, nil)
		return output, candidate, nil
	}
	if len(failures) == 0 {
		return output, target, fmt.Errorf("internal image has no verifiable tenant cache location: %s", ref)
	}
	return output, target, fmt.Errorf("verify internal image on tenant caches: %w", errors.Join(failures...))
}

func (s *Service) recordInternalImageCacheEvidence(app model.App, op model.Operation, target, source importImageDestination, ref, outcome string, started time.Time, cause error) {
	payload := map[string]any{
		"stage": "verify_local_image_graph", "outcome": outcome, "image_ref": ref,
		"target_runtime_id": target.Target.RuntimeID, "target_node": target.Target.ClusterNodeName, "target_cache_endpoint": target.CacheEndpoint,
		"cache_endpoint": source.CacheEndpoint, "cache_node": source.Target.ClusterNodeName, "cache_runtime_id": source.Target.RuntimeID,
		"elapsed_ms": time.Since(started).Milliseconds(),
	}
	severity := model.OperationEvidenceSeverityInfo
	if outcome == "completed" {
		payload["canonical_digest"] = imageDigest(ref)
	} else if cause != nil {
		severity = model.OperationEvidenceSeverityWarning
		payload["error"] = observability.RedactDiagnosticText(cause.Error()).Text
		var httpError *destinationImageCacheHTTPError
		if errors.As(cause, &httpError) {
			payload["http_status"] = httpError.StatusCode
		}
	}
	s.recordOperationEvidenceBestEffort(model.OperationEvidence{
		TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		Type: "image_cache_verification", Source: model.OperationEvidenceSourceImportController,
		Severity: severity, Confidence: model.OperationEvidenceConfidenceConfirmed,
		Summary: "Internal image cache verification " + outcome, NodeName: source.Target.ClusterNodeName,
		Payload: payload, PayloadVersion: 1, RedactionStatus: model.OperationEvidenceRedactionRedacted,
	})
}
