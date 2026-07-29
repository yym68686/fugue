package componentmanifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
)

const ShadowArtifactSchemaVersionV1 = "v1"

var exactGitCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// ShadowArtifactEnvelope is the immutable content stored by release-control
// while component planning remains observation-only.  BaseCommit and
// TargetCommit bind the plan to exact Git evidence rather than branch names or
// moving tags.
type ShadowArtifactEnvelope struct {
	SchemaVersion    string                 `json:"schemaVersion"`
	BaseCommit       string                 `json:"baseCommit"`
	TargetCommit     string                 `json:"targetCommit"`
	Manifest         Manifest               `json:"manifest"`
	ChangePlan       ChangePlan             `json:"changePlan"`
	CoordinationPlan ShadowCoordinationPlan `json:"coordinationPlan"`
}

// BuildShadowArtifactEnvelope verifies both plan layers and binds them to
// exact commits.  The returned value contains no credentials or live state.
func BuildShadowArtifactEnvelope(
	manifest Manifest,
	changePlan ChangePlan,
	coordinationPlan ShadowCoordinationPlan,
	baseCommit string,
	targetCommit string,
) (ShadowArtifactEnvelope, error) {
	envelope := ShadowArtifactEnvelope{
		SchemaVersion:    ShadowArtifactSchemaVersionV1,
		BaseCommit:       baseCommit,
		TargetCommit:     targetCommit,
		Manifest:         manifest,
		ChangePlan:       changePlan,
		CoordinationPlan: coordinationPlan,
	}
	if err := envelope.Validate(); err != nil {
		return ShadowArtifactEnvelope{}, err
	}
	return envelope, nil
}

// Validate proves that the envelope is exact, observation-only, and derived
// from the embedded change plan.  Rebuilding the coordination plan avoids
// trusting duplicated scopes, blockers, or mutation flags from input JSON.
func (envelope ShadowArtifactEnvelope) Validate() error {
	if envelope.SchemaVersion != ShadowArtifactSchemaVersionV1 {
		return fmt.Errorf("shadow artifact schemaVersion must be %q", ShadowArtifactSchemaVersionV1)
	}
	if !exactGitCommitPattern.MatchString(envelope.BaseCommit) {
		return fmt.Errorf("shadow artifact baseCommit must be exact lowercase 40-hex")
	}
	if !exactGitCommitPattern.MatchString(envelope.TargetCommit) {
		return fmt.Errorf("shadow artifact targetCommit must be exact lowercase 40-hex")
	}
	if envelope.BaseCommit == envelope.TargetCommit {
		return fmt.Errorf("shadow artifact baseCommit and targetCommit must differ")
	}
	if err := envelope.Manifest.Validate(); err != nil {
		return fmt.Errorf("validate shadow artifact manifest: %w", err)
	}
	if err := envelope.ChangePlan.Validate(); err != nil {
		return fmt.Errorf("validate shadow artifact change plan: %w", err)
	}
	if envelope.ChangePlan.ManifestDigest != envelope.Manifest.Digest() {
		return fmt.Errorf("shadow artifact change plan is not bound to the embedded manifest")
	}
	changedPaths := make([]string, 0, len(envelope.ChangePlan.ChangedPaths))
	for _, changed := range envelope.ChangePlan.ChangedPaths {
		changedPaths = append(changedPaths, changed.Path)
	}
	expectedChangePlan, err := PlanChanges(envelope.Manifest, changedPaths)
	if err != nil {
		return fmt.Errorf("rebuild shadow artifact change plan: %w", err)
	}
	if !reflect.DeepEqual(envelope.ChangePlan, expectedChangePlan) {
		return fmt.Errorf("shadow artifact change plan does not exactly match its manifest and paths")
	}
	expectedCoordination, err := BuildShadowCoordinationPlan(envelope.ChangePlan)
	if err != nil {
		return fmt.Errorf("rebuild shadow artifact coordination plan: %w", err)
	}
	if !reflect.DeepEqual(envelope.CoordinationPlan, expectedCoordination) {
		return fmt.Errorf("shadow artifact coordination plan does not exactly match its change plan")
	}
	if err := envelope.CoordinationPlan.VerifyDigest(); err != nil {
		return fmt.Errorf("validate shadow artifact coordination plan: %w", err)
	}
	return nil
}

// Content returns a detached JSON object suitable for the existing platform
// artifact store.  Mutating the returned map cannot mutate the envelope.
func (envelope ShadowArtifactEnvelope) Content() (map[string]any, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode shadow artifact envelope: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var content map[string]any
	if err := decoder.Decode(&content); err != nil {
		return nil, fmt.Errorf("decode shadow artifact content: %w", err)
	}
	return content, nil
}

// DecodeShadowArtifactContent strictly reconstructs and verifies content read
// from the generic platform artifact store.
func DecodeShadowArtifactContent(content map[string]any) (ShadowArtifactEnvelope, error) {
	if content == nil {
		return ShadowArtifactEnvelope{}, fmt.Errorf("shadow artifact content is nil")
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return ShadowArtifactEnvelope{}, fmt.Errorf("encode stored shadow artifact content: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var envelope ShadowArtifactEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return ShadowArtifactEnvelope{}, fmt.Errorf("decode stored shadow artifact content: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return ShadowArtifactEnvelope{}, fmt.Errorf("decode trailing stored shadow artifact content: %w", err)
		}
		return ShadowArtifactEnvelope{}, fmt.Errorf("stored shadow artifact content must contain exactly one JSON object")
	}
	if err := envelope.Validate(); err != nil {
		return ShadowArtifactEnvelope{}, err
	}
	return envelope, nil
}
