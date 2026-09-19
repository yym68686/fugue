package api

import (
	"context"
	"encoding/hex"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

type platformConfigStoredInputs struct {
	Intent model.PlatformArtifact
	Policy model.PlatformArtifact
}

type platformConfigReferenceError struct{ reason string }

type platformCompilationSource struct {
	PolicyReleaseID string
	SourceDigest    string
}

func (err *platformConfigReferenceError) Error() string { return err.reason }

// materializePlatformCompilation persists deterministic compiler output without
// touching release lanes or LKG. Existing inputs are retained verbatim on replay.
// An interrupted write may leave immutable drafts; a retry ensures the same
// identities and validates the complete parent before returning success.
func (s *Server) materializePlatformCompilation(ctx context.Context, compiled platformconfig.CompileResult, principal model.Principal, inputs *platformConfigStoredInputs, sources ...platformCompilationSource) (platformConfigCompileResponse, error) {
	if binding, present := compiled.InputSnapshot.Facts["configuration_producer"]; present {
		raw, ok := binding.(map[string]any)
		if !ok || len(raw) != 2 {
			return platformConfigCompileResponse{}, store.ErrInvalidInput
		}
		policyID, idOK := raw["policy_release_id"].(string)
		digest, digestOK := raw["source_digest"].(string)
		if !idOK || !digestOK || policyID == "" || !strings.HasPrefix(digest, "sha256:") || len(digest) != 71 {
			return platformConfigCompileResponse{}, store.ErrInvalidInput
		}
		decoded, err := hex.DecodeString(digest[7:])
		if err != nil || hex.EncodeToString(decoded) != digest[7:] {
			return platformConfigCompileResponse{}, store.ErrInvalidInput
		}
		source := platformCompilationSource{PolicyReleaseID: policyID, SourceDigest: digest}
		if len(sources) == 0 {
			sources = append(sources, source)
		} else if sources[0] != source {
			return platformConfigCompileResponse{}, store.ErrConflict
		}
	} else if len(sources) > 0 {
		return platformConfigCompileResponse{}, store.ErrInvalidInput
	}
	if len(sources) > 1 || len(sources) == 1 && (sources[0].PolicyReleaseID == "" || sources[0].SourceDigest == "") {
		return platformConfigCompileResponse{}, store.ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return platformConfigCompileResponse{}, err
	}
	if err := s.store.EnsurePlatformCompilerInput(compiled.InputSnapshot, compiled.Lineage.InputSnapshotDigest); err != nil {
		return platformConfigCompileResponse{}, err
	}
	artifacts := []*model.PlatformArtifact{&compiled.RouteArtifact, &compiled.DNSArtifact, &compiled.TLSArtifact}
	validation := model.PlatformArtifactValidationResult{
		Name: "platform_config.compiler", Pass: true,
		Severity: model.RobustnessSeverityBlockPublish,
		Message:  "deterministic platform configuration compiler passed",
		Evidence: map[string]string{
			"intent_digest": compiled.Lineage.IntentDigest, "policy_digest": compiled.Lineage.PolicyDigest,
			"compiler_version": compiled.Lineage.CompilerVersion,
		},
	}
	parentValidation := model.PlatformArtifactValidationResult{
		Name: "platform_config.release_set", Pass: true,
		Severity: model.RobustnessSeverityBlockPublish,
		Message:  "route, DNS, and TLS artifacts are bound to one immutable release set",
	}
	if inputs == nil {
		artifacts = append([]*model.PlatformArtifact{&compiled.IntentArtifact, &compiled.PolicyArtifact}, artifacts...)
	} else {
		compiled.IntentArtifact, compiled.PolicyArtifact = inputs.Intent, inputs.Policy
		validation.Name, validation.Message = "platform_config.artifact_replay", "deterministic artifact replay passed"
		validation.Evidence = map[string]string{"intent_artifact": inputs.Intent.ID, "policy_artifact": inputs.Policy.ID}
		parentValidation.Name, parentValidation.Message = "platform_config.release_set_replay", "deterministic release set replay passed"
	}
	for _, artifact := range artifacts {
		if err := ctx.Err(); err != nil {
			return platformConfigCompileResponse{}, err
		}
		artifact.CreatedByType, artifact.CreatedByID = strings.TrimSpace(principal.ActorType), strings.TrimSpace(principal.ActorID)
		if err := s.preservePlatformArtifactCreator(artifact); err != nil {
			return platformConfigCompileResponse{}, err
		}
		stored, _, err := s.store.EnsurePlatformArtifact(*artifact)
		if err != nil {
			return platformConfigCompileResponse{}, err
		}
		validated, err := s.store.ValidatePlatformArtifact(stored.ID, []model.PlatformArtifactValidationResult{validation})
		if err != nil {
			return platformConfigCompileResponse{}, err
		}
		*artifact = validated
	}
	if err := ctx.Err(); err != nil {
		return platformConfigCompileResponse{}, err
	}
	compiled.ReleaseSet.ArtifactIDs = []string{compiled.RouteArtifact.ID, compiled.DNSArtifact.ID, compiled.TLSArtifact.ID}
	parent := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, compiled.ReleaseSet.ArtifactIDs, time.Now().UTC())
	if len(sources) == 1 {
		parent.Metadata[platformproducer.PolicyReleaseMetadata] = sources[0].PolicyReleaseID
		parent.Metadata[platformproducer.SourceDigestMetadata] = sources[0].SourceDigest
	}
	parent.CreatedByType, parent.CreatedByID = strings.TrimSpace(principal.ActorType), strings.TrimSpace(principal.ActorID)
	if err := s.preservePlatformArtifactCreator(&parent); err != nil {
		return platformConfigCompileResponse{}, err
	}
	parent, _, err := s.store.EnsurePlatformArtifact(parent)
	if err != nil {
		return platformConfigCompileResponse{}, err
	}
	if result := s.validateReleaseSetReferences(parent); !result.Pass {
		return platformConfigCompileResponse{}, &platformConfigReferenceError{result.Message}
	}
	compiled.ReleaseArtifact, err = s.store.ValidatePlatformArtifact(parent.ID, []model.PlatformArtifactValidationResult{parentValidation})
	if err != nil {
		return platformConfigCompileResponse{}, err
	}
	if inputs == nil {
		s.appendAudit(principal, "platform_config.compiled", "platform_release_set", compiled.ReleaseSet.Generation, "", map[string]string{
			"intent_digest": compiled.Lineage.IntentDigest, "policy_digest": compiled.Lineage.PolicyDigest,
			"compiler_version": compiled.Lineage.CompilerVersion, "release_artifact": compiled.ReleaseArtifact.ID,
		})
	}
	return platformConfigCompileResponse{
		Lineage: compiled.Lineage, ReleaseSet: compiled.ReleaseSet,
		IntentArtifact: compiled.IntentArtifact, PolicyArtifact: compiled.PolicyArtifact,
		RouteArtifact: compiled.RouteArtifact, DNSArtifact: compiled.DNSArtifact,
		TLSArtifact: compiled.TLSArtifact, ReleaseArtifact: compiled.ReleaseArtifact,
	}, nil
}
