package api

import (
	"encoding/json"
	"math"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func firstPlatformArtifactSecretPath(artifact model.PlatformArtifact) string {
	if artifact.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || !platformproducer.IsPolicyScope(artifact.ScopeKey) || artifact.Content["input_source"] != platformproducer.DNSProducerInputSource {
		return firstSecretLikeContentPath(artifact.Content, "")
	}
	policy, err := platformproducer.Decode(artifact)
	if err != nil || policy.InputSource != platformproducer.DNSProducerInputSource || policy.DNSSource == nil {
		return firstSecretLikeContentPath(artifact.Content, "")
	}
	// A typed positive publication sequence is not a credential. Detach the
	// content and remove only that exact field after complete policy decoding;
	// recursively scan everything else, including topology labels and values.
	raw, err := json.Marshal(artifact.Content)
	var content map[string]any
	if err != nil || json.Unmarshal(raw, &content) != nil {
		return "dns_source.producer_policy.fencing_token"
	}
	source, sourceOK := content["dns_source"].(map[string]any)
	ref, refOK := source["producer_policy"].(map[string]any)
	sequence, sequenceOK := ref["fencing_token"].(float64)
	if !sourceOK || !refOK || !sequenceOK || sequence < 1 || sequence > 9007199254740991 || math.Trunc(sequence) != sequence {
		return "dns_source.producer_policy.fencing_token"
	}
	delete(ref, "fencing_token")
	return firstSecretLikeContentPath(content, "")
}
