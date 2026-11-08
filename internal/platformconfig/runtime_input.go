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
	}
	return out
}

// ArtifactRuntimeInputDigest resolves the operational dependency declared by a
// compiled artifact. Callers must separately verify the artifact signature and
// full dependency closure. An invalid declaration never falls back to history.
// Original input provenance remains unchanged and may have separate retention
// roots; choosing an operational dependency grants no disposal permission.
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
	declared := runtimeDeclared ||
		a.Metadata["compiler_version"] == ConfigurationInputCompilerVersion || lineage.CompilerVersion == ConfigurationInputCompilerVersion
	if !declared {
		return input, nil
	}
	if !present || runtime == "" || !runtimeInputDigestPattern.MatchString(runtime) ||
		!runtimeInputDigestPattern.MatchString(input) || lineage.RuntimeInputDigest != runtime ||
		lineage.CompilerVersion != ConfigurationInputCompilerVersion || lineage.IntentGeneration == "" || lineage.PolicyGeneration == "" ||
		!runtimeInputDigestPattern.MatchString(lineage.IntentDigest) || !runtimeInputDigestPattern.MatchString(lineage.PolicyDigest) {
		return "", fmt.Errorf("invalid configuration runtime input contract")
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
		if err != nil || json.Unmarshal(raw, &policy) != nil || policy.RuntimeInputMode != RuntimeInputConfigurationV1 ||
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
