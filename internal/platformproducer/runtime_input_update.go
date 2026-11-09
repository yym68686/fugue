package platformproducer

import (
	"fmt"
	"reflect"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// ValidateRuntimeInputUpdate authorizes a new configuration representation,
// without changing executable intent, observation expiry or an old generation.
// Artifact signatures and publication/LKG CAS belong to the Store transaction.
func ValidateRuntimeInputUpdate(previous, next Policy, static, oldPolicy, newPolicy model.PlatformArtifact) error {
	if previous.Mode != "serving" || next.Mode != "serving" || previous.Serving == nil || next.Serving == nil || previous.Serving.SinglePublication || next.Serving.SinglePublication ||
		previous.Generation == next.Generation || previous.DNSPolicyArtifactID == next.DNSPolicyArtifactID || previous.DNSPolicyArtifactID == "" ||
		static.ID != previous.StaticIntentArtifactID || static.ContentHash != previous.StaticIntentDigest || static.ScopeKey != previous.TargetScope ||
		oldPolicy.ID != previous.DNSPolicyArtifactID || oldPolicy.ContentHash != previous.DNSPolicyDigest || oldPolicy.ScopeKey != previous.TargetScope ||
		newPolicy.ID != next.DNSPolicyArtifactID || newPolicy.ContentHash != next.DNSPolicyDigest || newPolicy.ScopeKey != next.TargetScope || newPolicy.GenerationSequence <= oldPolicy.GenerationSequence {
		return fmt.Errorf("runtime input transition requires exact continuous signed sources")
	}
	before, after := previous, next
	before.Generation, after.Generation = "", ""
	before.DNSPolicyArtifactID, after.DNSPolicyArtifactID = "", ""
	before.DNSPolicyDigest, after.DNSPolicyDigest = "", ""
	if !reflect.DeepEqual(before, after) {
		return fmt.Errorf("runtime input transition changed producer intent or controls")
	}
	intent, err := DecodeStaticIntent(static)
	if err != nil {
		return err
	}
	old, err := DecodeProjectionPolicy(oldPolicy, intent.Consumers, previous.HostedZoneTemplates)
	if err != nil {
		return err
	}
	updated, err := DecodeProjectionPolicy(newPolicy, intent.Consumers, next.HostedZoneTemplates)
	if err != nil {
		return err
	}
	if err := ValidatePinnedSources(previous, intent, &old); err != nil {
		return err
	}
	if err := ValidatePinnedSources(next, intent, &updated); err != nil {
		return err
	}
	if old.RuntimeInputMode != "" || updated.RuntimeInputMode != platformconfig.RuntimeInputConfigurationV1 || old.Generation == updated.Generation {
		return fmt.Errorf("runtime input transition requires a new explicit configuration-v1 generation")
	}
	old.Generation, updated.Generation = "", ""
	old.RuntimeInputMode, updated.RuntimeInputMode = "", ""
	if !reflect.DeepEqual(old, updated) {
		return fmt.Errorf("runtime input transition changed operational configuration")
	}
	return nil
}
