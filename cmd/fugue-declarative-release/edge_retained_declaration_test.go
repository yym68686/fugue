package main

import (
	"testing"

	"fugue/internal/declarativerelease"
)

func TestRetainedWorkerDeclarationRequiresExactPublishedRuntime(t *testing.T) {
	release, old, next, _ := retirementFixture(t)
	live := retirementLive(t, release, next)
	target := declarativerelease.TargetIdentity{Present: true, ConfigSHA: "source", ManifestSHA: "manifest", OCIRevision: "revision", ImageRef: "image"}
	if !retainedWorkerDeclarationMatches(target, target, next, live) {
		t.Fatal("exact immutable retained declaration rejected")
	}
	if retainedWorkerDeclarationMatches(target, target, old, live) {
		t.Fatal("obsolete LKG environment was substituted for the committed Worker")
	}
	for _, mutate := range []func(*declarativerelease.TargetIdentity){
		func(value *declarativerelease.TargetIdentity) { value.ConfigSHA = "other" },
		func(value *declarativerelease.TargetIdentity) { value.ManifestSHA = "other" },
		func(value *declarativerelease.TargetIdentity) { value.OCIRevision = "other" },
		func(value *declarativerelease.TargetIdentity) { value.ImageRef = "other" },
		func(value *declarativerelease.TargetIdentity) { value.Present = false },
	} {
		changed := target
		mutate(&changed)
		if retainedWorkerDeclarationMatches(changed, target, next, live) {
			t.Fatal("unbound retained identity accepted")
		}
	}
	container, _, _ := retirementContainer(live, "containers", "dns")
	container["image"] = "foreign"
	if retainedWorkerDeclarationMatches(target, target, next, live) {
		t.Fatal("unreviewed runtime drift accepted")
	}
}
