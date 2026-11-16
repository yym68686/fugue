package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"fugue/internal/model"
)

const (
	RuntimeInputConfigurationV1       = "configuration-v1"
	ConfigurationInputCompilerVersion = "platform-config-compiler/v37"
	CompactOnlyInputCompilerVersion   = "platform-config-compiler/v38"
	QualityEvidenceDigestMetadata     = "quality_evidence_digest"
)

var runtimeInputDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// ConfigurationRuntimeInput retains typed operational facts, identities and
// original expiry/observation times. Arbitrary replay facts and physical probe
// transcripts are audit inputs, not a substitute for fresh quality evidence.
// This is a new digest, never a partial verification of the original digest.
func ConfigurationRuntimeInput(in RuntimeSnapshot) RuntimeSnapshot {
	out := in
	out.Facts = nil
	out.DNSSelections = append([]DNSSelectionObservation(nil), in.DNSSelections...)
	for i := range out.DNSSelections {
		out.DNSSelections[i].PhysicalEvidence = nil
		out.DNSSelections[i].RetainedPhysicalEvidence = nil
	}
	return out
}

// ArtifactRuntimeInputDigest resolves the operational dependency declared by a
// compiled artifact. Callers must separately verify the artifact signature and
// full dependency closure. An invalid declaration never falls back to history.
// Version 38 binds the compact input directly. It does not retain the complete
// compiler invocation snapshot; older version 37 artifacts retain both.
func ArtifactRuntimeInputDigest(a model.PlatformArtifact) (string, error) {
	input := a.Metadata["input_snapshot_digest"]
	runtime := a.Metadata["runtime_input_digest"]
	_, runtimeDeclared := a.Metadata["runtime_input_digest"]
	var lineage Lineage
	rawLineage, present := a.Content["lineage"]
	if present {
		raw, err := json.Marshal(rawLineage)
		if err != nil || len(raw) > 16<<10 || bytes.Equal(raw, []byte("null")) {
			return "", fmt.Errorf("invalid runtime input lineage")
		}
		// Legacy lineage remains readable with its established interpretation.
		if err := json.Unmarshal(raw, &lineage); err != nil {
			return "", fmt.Errorf("invalid runtime input lineage")
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) == nil {
			if _, exists := fields["runtime_input_digest"]; exists {
				runtimeDeclared = true
			}
		}
		if input != "" && lineage.InputSnapshotDigest != "" && input != lineage.InputSnapshotDigest {
			return "", fmt.Errorf("input lineage conflicts with metadata")
		}
		if input == "" {
			input = lineage.InputSnapshotDigest
		}
	}
	version := a.Metadata["compiler_version"]
	if version == "" {
		version = lineage.CompilerVersion
	}
	declared := runtimeDeclared || version == ConfigurationInputCompilerVersion || version == CompactOnlyInputCompilerVersion
	if !declared {
		return input, nil
	}
	if !present || runtime == "" || !runtimeInputDigestPattern.MatchString(runtime) ||
		!runtimeInputDigestPattern.MatchString(input) || lineage.RuntimeInputDigest != runtime ||
		lineage.CompilerVersion != version || (version != ConfigurationInputCompilerVersion && version != CompactOnlyInputCompilerVersion) ||
		lineage.IntentGeneration == "" || lineage.PolicyGeneration == "" ||
		!runtimeInputDigestPattern.MatchString(lineage.IntentDigest) || !runtimeInputDigestPattern.MatchString(lineage.PolicyDigest) {
		return "", fmt.Errorf("invalid configuration runtime input contract")
	}
	if version == CompactOnlyInputCompilerVersion && !runtimeInputDigestPattern.MatchString(a.Metadata[QualityEvidenceDigestMetadata]) {
		return "", fmt.Errorf("invalid compact-only quality evidence binding")
	}
	raw, _ := json.Marshal(rawLineage)
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&lineage) != nil {
		return "", fmt.Errorf("unknown configuration runtime lineage field")
	}
	for key, value := range LineageMetadata(lineage) {
		if a.Metadata[key] != value {
			return "", fmt.Errorf("configuration runtime lineage conflicts with metadata")
		}
	}
	prefix := ""
	switch a.ArtifactKind {
	case model.PlatformArtifactKindReleaseSet:
		prefix = "release-"
	case model.PlatformArtifactKindEdgeRouteBundle:
		prefix = "route-"
	case model.PlatformArtifactKindDNSAnswerBundle:
		prefix = "dns-"
	case model.PlatformArtifactKindCaddyRouteConfig:
		prefix = "tls-"
	default:
		return "", fmt.Errorf("runtime input contract requires a compiled artifact")
	}
	digest, err := Digest(lineage)
	if err != nil || a.Generation != prefix+strings.TrimPrefix(digest, "sha256:") {
		return "", fmt.Errorf("configuration runtime generation mismatch")
	}
	if prefix != "release-" {
		var policy PolicySnapshot
		raw, err := json.Marshal(a.Content["policy"])
		if err != nil || json.Unmarshal(raw, &policy) != nil ||
			(version == ConfigurationInputCompilerVersion && policy.RuntimeInputMode != RuntimeInputConfigurationV1) ||
			(policy.RuntimeInputMode != "" && policy.RuntimeInputMode != RuntimeInputConfigurationV1) ||
			policy.Generation != lineage.PolicyGeneration {
			return "", fmt.Errorf("configuration runtime policy missing")
		}
		policyDigest, err := Digest(policy)
		if err != nil || policyDigest != lineage.PolicyDigest {
			return "", fmt.Errorf("configuration runtime policy digest mismatch")
		}
	}
	return runtime, nil
}

// ArtifactRetainsOriginalInput distinguishes pre-v38 full-history contracts
// from current operational-only inputs. Invalid lineage is never interpreted
// as permission to omit an expected dependency.
func ArtifactRetainsOriginalInput(a model.PlatformArtifact) (bool, error) {
	if _, err := ArtifactRuntimeInputDigest(a); err != nil {
		return true, err
	}
	version := a.Metadata["compiler_version"]
	if version == "" {
		var lineage Lineage
		if raw, exists := a.Content["lineage"]; exists {
			encoded, err := json.Marshal(raw)
			if err != nil || json.Unmarshal(encoded, &lineage) != nil {
				return true, fmt.Errorf("invalid runtime input lineage")
			}
		}
		version = lineage.CompilerVersion
	}
	return version != CompactOnlyInputCompilerVersion, nil
}

// QualityEvidenceInput is a separate optional object, not a compiler replay
// input. Its digest is covered by the artifact's metadata signature. It contains
// no arbitrary Facts, route/runtime inventory or other full snapshot fields.
func QualityEvidenceInput(in RuntimeSnapshot) RuntimeSnapshot {
	out := RuntimeSnapshot{IntentGeneration: in.IntentGeneration, PolicyGeneration: in.PolicyGeneration, CapturedAt: in.CapturedAt}
	for _, fact := range in.DNSSelections {
		if len(fact.PhysicalEvidence) != 0 || len(fact.RetainedPhysicalEvidence) != 0 {
			out.DNSSelections = append(out.DNSSelections, fact)
		}
	}
	return out
}
