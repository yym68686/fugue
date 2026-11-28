package buildadmission

import (
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func TestBuildFingerprintIgnoresOutputAndCredentialMetadata(t *testing.T) {
	source := model.AppSource{CommitSHA: strings.Repeat("a", 40)}
	policy := sourceimport.EffectiveBuilderPolicy(sourceimport.BuilderPodPolicy{}, 6<<30)
	spec := model.AppSpec{Env: map[string]string{"TOKEN": "test-secret"}}
	first := InputFingerprint(source, spec, policy)
	source.RepoAuthToken, source.ResolvedImageRef, source.DetectedStack = "new-credential", "output-image", "detected"
	spec.Image, spec.RestartToken = "runtime-image", "restart-only"
	if got := InputFingerprint(source, spec, policy); got != first || strings.Contains(got, "test-secret") {
		t.Fatal("volatile metadata changed or leaked into fingerprint", got)
	}
	spec.Env = map[string]string{"TOKEN": "changed-secret"}
	if InputFingerprint(source, spec, policy) == first {
		t.Fatal("changed configuration did not invalidate guard")
	}
}
