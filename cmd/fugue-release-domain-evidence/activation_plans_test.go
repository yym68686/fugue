package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/releasedomain"
)

const (
	activationTestBase   = "1111111111111111111111111111111111111111"
	activationTestTarget = "2222222222222222222222222222222222222222"
)

func TestRunImageActivationPlansWritesStrictFiveFileReportWithoutChangingResolverInput(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	ownership := []byte(`apiVersion: release-domain.fugue.dev/v1
kind: ReleaseDomainOwnership
domains:
  - node-local
  - authoritative-dns
  - control-plane
  - image-cache
  - backup
requiredBindings:
  - releaseNamespace
fileRules: []
valueRules: []
objectRules:
  - id: api
    domain: control-plane
    apiGroup: apps
    version: v1
    kind: Deployment
    scope: Namespaced
    namespace: ${releaseNamespace}
    name: fugue-api
`)
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}
	base, err := releasedomain.CanonicalizeRenderedManifest([]byte(activationTestDeployment("registry.test/api@"+activationTestDigest("a"))), spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	targetDigest := activationTestDigest("c")
	target, err := releasedomain.CanonicalizeRenderedManifest([]byte(activationTestDeployment("registry.test/api:"+activationTestTarget)), spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	document, err := newEvidenceDocument(evidenceResult{
		baseCommit: activationTestBase, targetCommit: activationTestTarget,
		changes: []releasedomain.ChangedFile{{Status: releasedomain.ChangeModified, Path: "internal/releasedomain/types.go"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	changed = append(changed, '\n')
	context, err := releasedomain.NewClassificationContextEvidence(
		"fugue-system", map[string]string{"releaseNamespace": "fugue-system"}, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := releasedomain.BuildPlan(releasedomain.PlanInput{
		Files: releasedomain.FileClassification{Domains: []releasedomain.Domain{}, Evidence: []releasedomain.Evidence{}},
		Rendered: releasedomain.ClassifyRendered(base, target, spec, releasedomain.RenderedOptions{
			DefaultNamespace: "fugue-system", Bindings: context.BindingMap(),
		}),
		Digests: releasedomain.DigestEvidence{
			Base: activationTestDigest("1"), Target: activationTestDigest("2"), Live: activationTestDigest("1"),
			BaseManifest: activationTestBytesDigest(base), TargetManifest: activationTestBytesDigest(target),
			RepeatedTargetManifest: activationTestBytesDigest(target), Ownership: activationTestBytesDigest(ownership),
			ChangedFiles: document.Digest, ClassificationContext: context,
		},
	})
	planBytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planBytes = append(planBytes, '\n')

	paths := map[string][]byte{
		"changed.json": changed, "ownership.yaml": ownership,
		"plan.json": planBytes, "base.yaml": base, "target.yaml": target,
	}
	for name, data := range paths {
		if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(root, "report")
	var stderr bytes.Buffer
	exit := runImageActivationPlans([]string{
		"--changed-evidence", filepath.Join(root, "changed.json"),
		"--ownership", filepath.Join(root, "ownership.yaml"),
		"--plan", filepath.Join(root, "plan.json"),
		"--plan-digest", plan.PlanDigest,
		"--base-manifest", filepath.Join(root, "base.yaml"),
		"--target-manifest", filepath.Join(root, "target.yaml"),
		"--trusted-base", activationTestBase,
		"--trusted-target", activationTestTarget,
		"--provenance-digest", activationTestDigest("f"),
		"--artifact", "api=" + activationTestBase + "=" + targetDigest + "=registry.test/api@" + targetDigest,
		"--artifact", "edge=" + activationTestBase + "=" + activationTestDigest("e") + "=registry.test/edge@" + activationTestDigest("e"),
		"--output-dir", output,
	}, ioDiscard{}, &stderr)
	if exit != 0 {
		t.Fatalf("exit=%d stderr=%q", exit, stderr.String())
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 || entries[0].Name() != "build-artifact-plan.json" ||
		entries[1].Name() != "composite-decomposition-evidence.json" ||
		entries[2].Name() != "image-activation-evidence.json" || entries[3].Name() != "image-activation-plan.json" ||
		entries[4].Name() != "immutable-target-manifest.yaml" {
		t.Fatalf("report inventory = %#v", entries)
	}
	buildBytes, err := os.ReadFile(filepath.Join(output, "build-artifact-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var buildIdentity struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(buildBytes, &buildIdentity); err != nil {
		t.Fatal(err)
	}
	build, err := releasedomain.DecodeAndVerifyBuildArtifactPlan(bytes.NewReader(buildBytes), buildIdentity.Digest)
	if err != nil || len(build.Artifacts) != 2 {
		t.Fatalf("build plan = %#v err=%v", build, err)
	}
	if build.Artifacts[0].PublishedImageRef != "registry.test/api@"+targetDigest ||
		build.Artifacts[1].PublishedImageRef != "registry.test/edge@"+activationTestDigest("e") {
		t.Fatalf("build plan did not seal published image refs: %#v", build.Artifacts)
	}
	activationBytes, err := os.ReadFile(filepath.Join(output, "image-activation-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var activationIdentity struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(activationBytes, &activationIdentity); err != nil {
		t.Fatal(err)
	}
	activation, err := releasedomain.DecodeAndVerifyImageActivationPlan(bytes.NewReader(activationBytes), activationIdentity.Digest)
	if err != nil || len(activation.Activations) != 0 {
		t.Fatalf("activation plan = %#v err=%v", activation, err)
	}
	evidenceBytes, err := os.ReadFile(filepath.Join(output, "image-activation-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidenceIdentity struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(evidenceBytes, &evidenceIdentity); err != nil {
		t.Fatal(err)
	}
	evidence, err := releasedomain.DecodeAndVerifyImageActivationEvidence(bytes.NewReader(evidenceBytes), evidenceIdentity.Digest)
	if err != nil || evidence.Complete || len(evidence.Unresolved) != 1 ||
		evidence.Unresolved[0].Reason != "target-image-not-immutable" ||
		len(evidence.BuiltOnlyArtifacts) != 2 ||
		evidence.ResolvedImageActivationPlanDigest != activation.Digest {
		t.Fatalf("activation evidence = %#v err=%v", evidence, err)
	}
	decompositionBytes, err := os.ReadFile(filepath.Join(output, "composite-decomposition-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var decompositionIdentity struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(decompositionBytes, &decompositionIdentity); err != nil {
		t.Fatal(err)
	}
	decomposition, err := releasedomain.DecodeAndVerifyCompositeDecompositionEvidence(bytes.NewReader(decompositionBytes), decompositionIdentity.Digest)
	if err != nil || decomposition.Complete || len(decomposition.Steps) != 0 ||
		decomposition.ImageActivationPlanDigest != activation.Digest ||
		decomposition.ImageActivationEvidenceDigest != evidence.Digest {
		t.Fatalf("composite decomposition = %#v err=%v", decomposition, err)
	}
	immutableTarget, err := os.ReadFile(filepath.Join(output, "immutable-target-manifest.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(immutableTarget, []byte("registry.test/api@"+targetDigest)) ||
		bytes.Contains(immutableTarget, []byte("registry.test/api:"+activationTestTarget)) {
		t.Fatalf("immutable target manifest = %s", immutableTarget)
	}
	if !bytes.Contains(target, []byte("registry.test/api:"+activationTestTarget)) {
		t.Fatal("original resolver target input was unexpectedly changed")
	}
}

func TestBuildArtifactFlagsRequireExactPublishedImageRef(t *testing.T) {
	var values buildArtifactFlags
	legacy := "api=" + activationTestBase + "=" + activationTestDigest("a")
	if err := values.Set(legacy); err == nil {
		t.Fatal("legacy producer input without a published image ref was accepted")
	}
	sealed := legacy + "=registry.test/api@" + activationTestDigest("a")
	if err := values.Set(sealed); err != nil {
		t.Fatalf("sealed producer input was rejected: %v", err)
	}
	if len(values) != 1 || values[0].PublishedImageRef != "registry.test/api@"+activationTestDigest("a") {
		t.Fatalf("published image ref was not parsed exactly: %#v", values)
	}
}

func TestParseImageActivationPlanFlagsRejectsDuplicateOrRelativeOutput(t *testing.T) {
	base := []string{
		"--changed-evidence", "changed.json", "--ownership", "ownership.yaml",
		"--plan", "plan.json", "--plan-digest", activationTestDigest("a"),
		"--base-manifest", "base.yaml", "--target-manifest", "target.yaml",
		"--trusted-base", activationTestBase, "--trusted-target", activationTestTarget,
		"--provenance-digest", activationTestDigest("f"), "--output-dir", "relative",
	}
	if _, err := parseImageActivationPlanFlags(base); err == nil {
		t.Fatal("relative output directory was accepted")
	}
	duplicate := append(append([]string(nil), base...), "--trusted-base", activationTestBase)
	if _, err := parseImageActivationPlanFlags(duplicate); err == nil {
		t.Fatal("duplicate scalar flag was accepted")
	}
}

func TestWriteActivationPlanBuildErrorIncludesOnlyFixedReasonCode(t *testing.T) {
	var stderr bytes.Buffer
	writeActivationPlanBuildError(&stderr, nil)
	if got := stderr.String(); got != activationPlanBuildError+": unspecified-internal-invariant\n" {
		t.Fatalf("nil build error output = %q", got)
	}

	stderr.Reset()
	privateIdentity := "apps/v1 Deployment private-namespace/private-name"
	writeActivationPlanBuildError(&stderr, &activationPlanTestError{message: "workload containers are missing for " + privateIdentity})
	if got := stderr.String(); got != activationPlanBuildError+": workload-containers-missing\n" {
		t.Fatalf("classified build error output = %q", got)
	}
	if strings.Contains(stderr.String(), privateIdentity) {
		t.Fatalf("classified build error leaked manifest identity: %q", stderr.String())
	}

	stderr.Reset()
	writeActivationPlanBuildError(&stderr, &activationPlanTestError{message: "build artifact and release plan binding mismatch"})
	if got := stderr.String(); got != activationPlanBuildError+": plan-binding-mismatch\n" {
		t.Fatalf("plan binding build error output = %q", got)
	}

	stderr.Reset()
	writeActivationPlanBuildError(&stderr, &activationPlanTestError{message: "image activation plan and evidence binding mismatch"})
	if got := stderr.String(); got != activationPlanBuildError+": composite-decomposition-binding-mismatch\n" {
		t.Fatalf("decomposition binding build error output = %q", got)
	}
}

type activationPlanTestError struct{ message string }

func (err *activationPlanTestError) Error() string { return err.message }

func activationTestDeployment(image string) string {
	return "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: fugue-api\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: fugue-api\n  template:\n    metadata:\n      labels:\n        app: fugue-api\n    spec:\n      containers:\n        - name: api\n          image: " + image + "\n"
}

func activationTestDigest(digit string) string { return "sha256:" + strings.Repeat(digit, 64) }

func activationTestBytesDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
