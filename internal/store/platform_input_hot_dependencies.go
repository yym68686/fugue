package store

import (
	"encoding/json"
	"fmt"
	"sort"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// compilerInputArtifactHotNode follows declared identities, including retained
// DNS publications and producer policies. Embedded copies supply expected
// hashes, not replacement ledger rows: every edge is resolved independently.
// This parses metadata only; it never confers signature or body verification.
func compilerInputArtifactHotNode(artifact model.PlatformArtifact) (inputarchive.HotNode, error) {
	return compilerInputArtifactDependencyNode(artifact, nil, true, platformconfig.DecodeDNSRouteDependencyEvidenceObserved)
}

func compilerInputProjectedArtifactHotNodeObserved(artifact model.PlatformArtifact, cost *CompilerInputHotDecodeCost) (inputarchive.HotNode, error) {
	return compilerInputArtifactDependencyNode(artifact, cost, true, platformconfig.DecodeProjectedDNSRouteDependencyEvidenceObserved)
}

// Ordinary serving/activation follows the new signed operational dependency.
// Retention inventory and explicit full-history reads retain BOTH dependencies:
// changing a runtime reader never releases the original provenance obligation.
func compilerInputArtifactRuntimeNode(artifact model.PlatformArtifact) (inputarchive.HotNode, error) {
	return compilerInputArtifactDependencyNode(artifact, nil, false, platformconfig.DecodeDNSRouteDependencyEvidenceObserved)
}

type compilerInputDNSDependencyDecoder func(model.PlatformArtifact, *platformconfig.DNSDependencyDecodeCost) (platformconfig.DNSRouteDependencyEvidence, error)

func compilerInputArtifactDependencyNode(artifact model.PlatformArtifact, cost *CompilerInputHotDecodeCost, retainOriginal bool, decodeDNS compilerInputDNSDependencyDecoder) (inputarchive.HotNode, error) {
	node := inputarchive.HotNode{Identity: inputarchive.HotReference{Kind: "artifact", ID: artifact.ID}, ContentHash: artifact.ContentHash, References: []inputarchive.HotReference{}}
	if artifact.ID == "" || !inputDigestPattern.MatchString(artifact.ContentHash) {
		return node, fmt.Errorf("hot artifact identity invalid: %w", ErrConflict)
	}
	add := func(kind, id, expected string) error {
		if id == "" || len(id) > 512 || expected != "" && !inputDigestPattern.MatchString(expected) {
			return fmt.Errorf("hot artifact dependency identity invalid: %w", ErrConflict)
		}
		node.References = append(node.References, inputarchive.HotReference{Kind: kind, ID: id, ExpectedHash: expected})
		return nil
	}
	input, err := platformconfig.ArtifactRuntimeInputDigest(artifact)
	if err != nil {
		return node, fmt.Errorf("hot artifact runtime input contract invalid: %w", ErrConflict)
	}
	if input != "" {
		if !inputDigestPattern.MatchString(input) {
			return node, fmt.Errorf("hot artifact input digest invalid: %w", ErrConflict)
		}
		if err := add("input", input, input); err != nil {
			return node, err
		}
	}
	if original := artifact.Metadata["input_snapshot_digest"]; retainOriginal && artifact.Metadata["runtime_input_digest"] != "" && original != input {
		if err := add("input", original, original); err != nil {
			return node, err
		}
	}
	retainedLineage := false
	if retainOriginal {
		refs, declared, err := compilerInputRetainedLineageSources(artifact)
		if err != nil {
			return node, err
		}
		node.References = append(node.References, refs...)
		retainedLineage = declared
	}
	if id := artifact.Metadata[platformconfig.ProducerPolicyReleaseMetadata]; id != "" {
		if err := add("release", id, ""); err != nil {
			return node, err
		}
	}
	if artifact.ArtifactKind == model.PlatformArtifactKindReleaseSet {
		// LKG promotion independently loads this policy by exact kind/scope/
		// generation. Its inputs are dependencies too, not merely the direct
		// artifact_ids children or a trusted metadata label.
		if generation := artifact.Metadata["policy_generation"]; generation != "" && !retainedLineage {
			if len(generation) > 512 || artifact.ScopeKey == "" {
				return node, ErrConflict
			}
			node.References = append(node.References, inputarchive.HotReference{Kind: "generation", ID: generation, ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: artifact.ScopeKey})
		}
		raw, err := json.Marshal(artifact.Content["artifact_ids"])
		var ids []string
		if err != nil || len(raw) > 64<<10 || json.Unmarshal(raw, &ids) != nil || len(ids) == 0 || len(ids) > 64 {
			return node, fmt.Errorf("hot release set members invalid: %w", ErrConflict)
		}
		seen := map[string]bool{}
		for _, id := range ids {
			if id == artifact.ID || seen[id] {
				return node, fmt.Errorf("hot release set has a repeated or self member: %w", ErrConflict)
			}
			seen[id] = true
			if err := add("artifact", id, ""); err != nil {
				return node, err
			}
		}
	}
	if artifact.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
		var dnsCost *platformconfig.DNSDependencyDecodeCost
		if cost != nil {
			if cost.DNS == nil {
				cost.DNS = &platformconfig.DNSDependencyDecodeCost{}
			}
			dnsCost = cost.DNS
		}
		evidence, err := decodeDNS(artifact, dnsCost)
		if err != nil {
			return node, fmt.Errorf("hot DNS publication dependencies invalid: %w", ErrConflict)
		}
		for _, dependency := range evidence.Dependencies {
			if err := add("release", dependency.ReleaseID, ""); err != nil {
				return node, err
			}
			for _, member := range dependency.Artifacts {
				if err := add("artifact", member.ID, member.ContentHash); err != nil {
					return node, err
				}
			}
		}
		sources, err := platformconfig.DNSRouteSourceAuthorizations(artifact)
		if err != nil {
			return node, fmt.Errorf("hot DNS producer authorization invalid: %w", ErrConflict)
		}
		for _, source := range sources {
			if err := add("artifact", source.PolicyArtifactID, source.PolicyDigest); err != nil {
				return node, err
			}
		}
		for _, cell := range evidence.Publications {
			if cell.ProducerPolicy != nil {
				if err := add("release", cell.ProducerPolicy.ReleaseID, ""); err != nil {
					return node, err
				}
				if err := add("artifact", cell.ProducerPolicy.ArtifactID, cell.ProducerPolicy.ContentHash); err != nil {
					return node, err
				}
			}
		}
		previous := evidence.Previous
		if previous != nil && previous.ProducerPolicy != nil {
			if err := add("release", previous.ProducerPolicy.ReleaseID, ""); err != nil {
				return node, err
			}
			if err := add("artifact", previous.ProducerPolicy.ArtifactID, previous.ProducerPolicy.ContentHash); err != nil {
				return node, err
			}
		}
	}
	if artifact.ArtifactKind == model.PlatformArtifactKindPolicySnapshot && artifact.Content["schema_version"] == platformproducer.Schema {
		policy, err := platformproducer.Decode(artifact)
		if err != nil {
			return node, err
		}
		if policy.DNSSource != nil {
			// The transitional DNS producer reads a verified global source,
			// not the business-static-intent fields (which must be empty).
			// Retain its exact policy and activation; that policy recursively
			// introduces the immutable static/projection dependencies.
			source := policy.DNSSource.ProducerPolicy
			if err := add("artifact", source.ArtifactID, source.ContentHash); err != nil {
				return node, err
			}
			if err := add("release", source.ReleaseID, ""); err != nil {
				return node, err
			}
		} else {
			if err := add("artifact", policy.StaticIntentArtifactID, policy.StaticIntentDigest); err != nil {
				return node, err
			}
			// A valid shadow static producer need not consume projection
			// policy. Decode already rejects a partial ID/digest pair.
			if policy.DNSPolicyArtifactID != "" {
				if err := add("artifact", policy.DNSPolicyArtifactID, policy.DNSPolicyDigest); err != nil {
					return node, err
				}
			}
		}
	}
	// Keep different expected hashes as different edges. Deduplicating by ID
	// alone would hide a conflicting embedded publication.
	byBinding := map[string]inputarchive.HotReference{}
	for _, ref := range node.References {
		raw, _ := json.Marshal(ref)
		byBinding[string(raw)] = ref
	}
	bindings := make([]string, 0, len(byBinding))
	for key := range byBinding {
		bindings = append(bindings, key)
	}
	sort.Strings(bindings)
	node.References = node.References[:0]
	for _, key := range bindings {
		node.References = append(node.References, byBinding[key])
	}
	return node, nil
}

// Full retention includes the immutable compiler sources, independently of
// the operational snapshot. A compact runtime input cannot release the intent
// and policy used to produce its original signed generation. Resolve both by
// exact kind/scope/generation; the same-snapshot ledger resolver then binds the
// source artifact's content hash. Lineage digests use typed encoding and must
// not be confused with the map-encoded artifact content hash. This metadata
// traversal does not independently authenticate either original source.
func compilerInputRetainedLineageSources(a model.PlatformArtifact) ([]inputarchive.HotReference, bool, error) {
	var lineage platformconfig.Lineage
	if raw, exists := a.Content["lineage"]; exists {
		encoded, err := json.Marshal(raw)
		if err != nil || len(encoded) > 16<<10 || json.Unmarshal(encoded, &lineage) != nil {
			return nil, true, ErrConflict
		}
	}
	declared := a.Metadata["compiler_version"] != "" || lineage.CompilerVersion != "" ||
		lineage.IntentGeneration != "" || lineage.PolicyGeneration != "" || lineage.IntentDigest != "" || lineage.PolicyDigest != ""
	if !declared {
		return nil, false, nil
	}
	if a.ScopeKey == "" || lineage.CompilerVersion == "" {
		return nil, true, ErrConflict
	}
	for key, value := range platformconfig.LineageMetadata(lineage) {
		if a.Metadata[key] != value {
			return nil, true, ErrConflict
		}
	}
	refs := make([]inputarchive.HotReference, 0, 2)
	for _, source := range []struct{ kind, generation, digest string }{
		{model.PlatformArtifactKindPlatformIntent, lineage.IntentGeneration, lineage.IntentDigest},
		{model.PlatformArtifactKindPolicySnapshot, lineage.PolicyGeneration, lineage.PolicyDigest},
	} {
		if source.generation == "" || len(source.generation) > 512 || !inputDigestPattern.MatchString(source.digest) {
			return nil, true, ErrConflict
		}
		refs = append(refs, inputarchive.HotReference{Kind: "generation", ID: source.generation,
			ArtifactKind: source.kind, ScopeKey: a.ScopeKey})
	}
	return refs, true, nil
}
