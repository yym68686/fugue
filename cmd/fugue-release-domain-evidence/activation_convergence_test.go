package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/releasedomain"
)

func TestImageActivationConvergenceCommandReportsPendingActivation(t *testing.T) {
	const base = "1111111111111111111111111111111111111111"
	const target = "2222222222222222222222222222222222222222"
	digest := func(value byte) string { return "sha256:" + string(bytes.Repeat([]byte{value}, 64)) }
	build, err := releasedomain.NewBuildArtifactPlan(base, target, digest('1'), []releasedomain.BuildArtifact{
		{Name: "image_cache", SourceBaseCommit: base, ArtifactDigest: digest('2'), ProvenanceDigest: digest('3')},
	})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := releasedomain.NewImageActivationPlan(base, target, build.Digest, digest('4'), nil)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := releasedomain.NewImageActivationEvidence(releasedomain.ImageActivationEvidence{
		BaseCommit: base, TargetCommit: target, BuildArtifactPlanDigest: build.Digest,
		ResolvedImageActivationPlanDigest: activation.Digest, BuiltOnlyArtifacts: []string{"image_cache"},
	})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	buildPath := filepath.Join(directory, "build.json")
	activationPath := filepath.Join(directory, "activation.json")
	evidencePath := filepath.Join(directory, "evidence.json")
	for path, value := range map[string][]byte{
		buildPath:      mustMarshalBuildPlan(t, build),
		activationPath: mustMarshalActivationPlan(t, activation),
		evidencePath:   mustMarshalActivationEvidence(t, evidence),
	} {
		if err := os.WriteFile(path, value, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	status := runImageActivationConvergence([]string{
		"--build-artifact-plan", buildPath,
		"--image-activation-plan", activationPath,
		"--image-activation-evidence", evidencePath,
	}, &stdout, &stderr)
	if status != 0 || stdout.String() != "pending\timage_cache\n" || stderr.Len() != 0 {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout.String(), stderr.String())
	}
}

func TestImageActivationConvergenceCommandRejectsMutatedEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	status := runImageActivationConvergence(nil, &stdout, &stderr)
	if status != 1 || stdout.Len() != 0 || stderr.String() != activationConvergenceArgumentsError+"\n" {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout.String(), stderr.String())
	}
}

func mustMarshalBuildPlan(t *testing.T, value releasedomain.BuildArtifactPlan) []byte {
	t.Helper()
	encoded, err := releasedomain.MarshalBuildArtifactPlan(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustMarshalActivationPlan(t *testing.T, value releasedomain.ImageActivationPlan) []byte {
	t.Helper()
	encoded, err := releasedomain.MarshalImageActivationPlan(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustMarshalActivationEvidence(t *testing.T, value releasedomain.ImageActivationEvidence) []byte {
	t.Helper()
	encoded, err := releasedomain.MarshalImageActivationEvidence(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
