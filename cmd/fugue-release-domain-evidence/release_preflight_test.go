package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/releasedomain"
)

func TestReleasePreflightAcceptsFreshReadOnlySnapshot(t *testing.T) {
	fixture := writeReleasePreflightFixture(t, activationTestTarget)
	receipt, err := verifyReleasePreflightSnapshot(releasePreflightOptions{
		snapshotDir: fixture,
		tmpDir:      privateReleasePreflightTemp(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := marshalReleasePreflightReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, append(canonical, '\n')) {
		t.Fatalf("receipt is not canonical: %s", encoded)
	}
	if decoded["status"] != "pass" || decoded["kind"] != "ReleasePreflightReceipt" {
		t.Fatalf("unexpected receipt: %s", encoded)
	}
}

func TestReleasePreflightCatchesPushBlockingProductionRegressions(t *testing.T) {
	t.Run("DownwardAPI volume rejects spec.nodeName", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "helm-release.json"), func(document map[string]any) {
			manifest := document["manifest"].(string)
			document["manifest"] = strings.Replace(manifest, "metadata.labels['topology.kubernetes.io/hostname']", "spec.nodeName", 1)
		})
		assertReleasePreflightRejected(t, fixture, "fieldPath")
	})

	t.Run("DownwardAPI selector retains quotes", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "helm-release.json"), func(document map[string]any) {
			manifest := document["manifest"].(string)
			document["manifest"] = strings.Replace(
				manifest,
				"metadata.labels['topology.kubernetes.io/hostname']",
				"metadata.labels[topology.kubernetes.io/hostname]",
				1,
			)
		})
		assertReleasePreflightRejected(t, fixture, "fieldPath")
	})

	t.Run("wrong telemetry resource name", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "live-workloads.json"), func(document map[string]any) {
			items := document["items"].([]any)
			metadata := items[1].(map[string]any)["metadata"].(map[string]any)
			metadata["name"] = "fugue-telemetry"
		})
		assertReleasePreflightRejected(t, fixture, "resource")
	})

	t.Run("wide Deployment selector cannot claim API cohort", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "live-workloads.json"), func(document map[string]any) {
			for _, raw := range document["items"].([]any) {
				spec := raw.(map[string]any)["spec"].(map[string]any)
				spec["selector"] = map[string]any{"matchLabels": map[string]any{"app": "fugue"}}
				template := spec["template"].(map[string]any)
				template["metadata"].(map[string]any)["labels"] = map[string]any{"app": "fugue"}
			}
		})
		assertReleasePreflightRejected(t, fixture, "cohort selector")
	})

	t.Run("wide Deployment selector derives exact component cohort", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "live-workloads.json"), func(document map[string]any) {
			for index, raw := range document["items"].([]any) {
				spec := raw.(map[string]any)["spec"].(map[string]any)
				spec["selector"] = map[string]any{"matchLabels": map[string]any{"app": "fugue"}}
				template := spec["template"].(map[string]any)
				component := []string{"api", "telemetry-agent"}[index]
				template["metadata"].(map[string]any)["labels"] = map[string]any{
					"app": "fugue", releasePreflightComponentLabel: component,
				}
			}
		})
		if _, err := verifyReleasePreflightSnapshot(releasePreflightOptions{
			snapshotDir: fixture,
			tmpDir:      privateReleasePreflightTemp(t),
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("candidate bundle window rejects terminal empty state", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "candidate-bundle-readback.json"), func(document map[string]any) {
			observations := document["observations"].([]any)
			observations[len(observations)-1] = releasePreflightCandidateBundleObservation(10, "pending")
		})
		assertReleasePreflightRejected(t, fixture, "candidate bundle")
	})

	t.Run("candidate bundle window rejects terminal unstable health", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "candidate-bundle-readback.json"), func(document map[string]any) {
			observations := document["observations"].([]any)
			terminal := observations[len(observations)-1].(map[string]any)
			terminal["consecutiveHealthy"] = float64(1)
			terminal["durableHealthy"] = false
		})
		assertReleasePreflightRejected(t, fixture, "candidate bundle")
	})

	t.Run("candidate bundle window advances once", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "candidate-bundle-readback.json"), func(document map[string]any) {
			document["advanceCount"] = float64(2)
		})
		assertReleasePreflightRejected(t, fixture, "candidate bundle")
	})

	t.Run("candidate bundle window rejects post-readback drift", func(t *testing.T) {
		for _, test := range []struct {
			name, field, value string
		}{
			{name: "UID", field: "podUID", value: "pod-other-uid"},
			{name: "release epoch", field: "releaseEpoch", value: "pdp-20260802t213001z-abcd"},
			{name: "bundle", field: "bundleVersion", value: "bundle-other"},
			{name: "route generation", field: "routeGeneration", value: "routegen_other"},
		} {
			t.Run(test.name, func(t *testing.T) {
				fixture := writeReleasePreflightFixture(t, activationTestTarget)
				mutateReleasePreflightJSON(t, filepath.Join(fixture, "candidate-bundle-readback.json"), func(document map[string]any) {
					document["postReadback"].(map[string]any)[test.field] = test.value
				})
				assertReleasePreflightRejected(t, fixture, "candidate bundle")
			})
		}
	})

	t.Run("cross-boundary identity rejects invalid EqualFold pair", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "candidate-bundle-readback.json"), func(document map[string]any) {
			for _, raw := range document["observations"].([]any) {
				raw.(map[string]any)["releaseEpoch"] = "PDP-%"
			}
			document["postReadback"].(map[string]any)["releaseEpoch"] = "pdp-%"
		})
		assertReleasePreflightRejected(t, fixture, "candidate bundle")
	})

	t.Run("TMPDIR not writable", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		tmp := t.TempDir()
		if err := os.Chmod(tmp, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(tmp, 0o700) })
		_, err := verifyReleasePreflightSnapshot(releasePreflightOptions{snapshotDir: fixture, tmpDir: tmp})
		if err == nil || !strings.Contains(err.Error(), "TMPDIR") {
			t.Fatalf("unwritable TMPDIR error = %v", err)
		}
	})

	t.Run("live-only annotation drift", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "live-workloads.json"), func(document map[string]any) {
			items := document["items"].([]any)
			metadata := items[0].(map[string]any)["metadata"].(map[string]any)
			metadata["annotations"] = map[string]any{"example.invalid/live-only": "drift"}
		})
		assertReleasePreflightRejected(t, fixture, "annotation")
	})

	t.Run("old activation evidence mixed in", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		old := writeReleasePreflightFixture(t, strings.Repeat("3", 40))
		oldEvidence, err := os.ReadFile(filepath.Join(old, "release-evidence", "image-activation-evidence.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fixture, "release-evidence", "image-activation-evidence.json"), oldEvidence, 0o600); err != nil {
			t.Fatal(err)
		}
		assertReleasePreflightRejected(t, fixture, "activation evidence")
	})

	t.Run("API incorrectly marked built-only", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		path := filepath.Join(fixture, "release-evidence", "image-activation-evidence.json")
		mutateReleasePreflightJSON(t, path, func(document map[string]any) {
			document["builtOnlyArtifacts"] = []any{"api", "edge"}
		})
		assertReleasePreflightRejected(t, fixture, "built-only")
	})

	t.Run("receipt array type round-trip", func(t *testing.T) {
		fixture := writeReleasePreflightFixture(t, activationTestTarget)
		receiptPath := filepath.Join(fixture, "build-receipt.json")
		receiptBytes, err := os.ReadFile(receiptPath)
		if err != nil {
			t.Fatal(err)
		}
		var images []any
		if err := json.Unmarshal(receiptBytes, &images); err != nil {
			t.Fatal(err)
		}
		wrapped := mustCanonicalReleasePreflightJSON(t, map[string]any{"images": images})
		writeReleasePreflightFile(t, receiptPath, wrapped)
		mutateReleasePreflightJSON(t, filepath.Join(fixture, "snapshot.json"), func(document map[string]any) {
			document["buildReceiptDigest"] = releasePreflightDigest(wrapped)
		})
		assertReleasePreflightRejected(t, fixture, "schema/type round-trip")
	})
}

func TestReleasePreflightRejectsUnsafeRollbackTargetAndInputBudget(t *testing.T) {
	fixture := writeReleasePreflightFixture(t, activationTestTarget)
	mutateReleasePreflightJSON(t, filepath.Join(fixture, "snapshot.json"), func(document map[string]any) {
		document["rollbackTargetRevision"] = float64(16)
	})
	assertReleasePreflightRejected(t, fixture, "rollback")

	fixture = writeReleasePreflightFixture(t, activationTestTarget)
	if err := os.Truncate(filepath.Join(fixture, "live-workloads.json"), releasePreflightFileLimit+1); err != nil {
		t.Fatal(err)
	}
	assertReleasePreflightRejected(t, fixture, "limit")
}

func TestReleasePreflightKeepsSnapshotReadOnly(t *testing.T) {
	fixture := writeReleasePreflightFixture(t, activationTestTarget)
	output := filepath.Join(fixture, "receipt.json")
	var stdout, stderr bytes.Buffer
	status := runReleasePreflight([]string{"--snapshot", fixture, "--output", output}, &stdout, &stderr)
	if status != 1 || stdout.Len() != 0 || stderr.String() != releasePreflightOutputError+"\n" {
		t.Fatalf("status=%d stdout=%q stderr=%q", status, stdout.String(), stderr.String())
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatalf("preflight wrote into its read-only snapshot: %v", err)
	}
}

func TestMainExposesReleasePreflightSubcommand(t *testing.T) {
	mainSource, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(mainSource, []byte(`args[0] == "release-preflight"`)) {
		t.Fatal("main command does not expose release-preflight")
	}
}

func TestCommittedReleasePreflightFixture(t *testing.T) {
	fixture, err := filepath.Abs(filepath.Join("testdata", "release-preflight"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifyReleasePreflightSnapshot(releasePreflightOptions{
		snapshotDir: fixture,
		tmpDir:      privateReleasePreflightTemp(t),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseToolingUsesTheTwoMakeEntrypoints(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"prepush:\n\t@python3 ./scripts/prepush.py",
		"release-preflight:\n",
		"go run ./cmd/fugue-release-domain-evidence release-preflight",
	} {
		if !bytes.Contains(makefile, []byte(required)) {
			t.Fatalf("Makefile is missing authoritative entrypoint %q", required)
		}
	}
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"run: make prepush",
		"run: make release-preflight SNAPSHOT=",
		"run: make test",
		"if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'",
	} {
		if bytes.Count(workflow, []byte(required)) != 1 {
			t.Fatalf("CI must contain exactly one %q entry", required)
		}
	}
}

func TestReleasePreflightValidatesCurrentChartDownwardAPI(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	chart, err := filepath.Abs(filepath.Join("..", "..", "deploy", "helm", "fugue"))
	if err != nil {
		t.Fatal(err)
	}
	variants := []struct {
		name string
		args []string
	}{
		{name: "default"},
		{name: "production-ha", args: []string{"--values", filepath.Join(chart, "values-production-ha.yaml")}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			arguments := []string{"template", "fugue", chart, "--namespace", "fugue-system"}
			arguments = append(arguments, variant.args...)
			command := exec.Command("helm", arguments...)
			manifest, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("Helm render failed: %v\n%s", err, manifest)
			}
			if err := verifyReleasePreflightDownwardAPI(manifest, "current Chart render"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertReleasePreflightRejected(t *testing.T, fixture, fragment string) {
	t.Helper()
	_, err := verifyReleasePreflightSnapshot(releasePreflightOptions{
		snapshotDir: fixture,
		tmpDir:      privateReleasePreflightTemp(t),
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(fragment)) {
		t.Fatalf("preflight error = %v, want fragment %q", err, fragment)
	}
}

func privateReleasePreflightTemp(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeReleasePreflightFixture(t *testing.T, targetCommit string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	evidenceDir := filepath.Join(root, "release-evidence")
	if err := os.Mkdir(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	baseCommit := activationTestBase
	namespace := "fugue-system"
	ownership := []byte(`apiVersion: release-domain.fugue.dev/v1
kind: ReleaseDomainOwnership
domains: [node-local, authoritative-dns, control-plane, image-cache, backup]
requiredBindings: [releaseNamespace]
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
  - id: telemetry
    domain: control-plane
    apiGroup: apps
    version: v1
    kind: Deployment
    scope: Namespaced
    namespace: ${releaseNamespace}
    name: fugue-telemetry-agent
`)
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}
	baseRaw := releasePreflightDeployment("fugue-api", "api", "registry.test/api@"+activationTestDigest("a")) + "\n---\n" +
		releasePreflightDeployment("fugue-telemetry-agent", "telemetry-agent", "registry.test/telemetry@"+activationTestDigest("b"))
	targetRaw := releasePreflightDeployment("fugue-api", "api", "registry.test/api:"+targetCommit) + "\n---\n" +
		releasePreflightDeployment("fugue-telemetry-agent", "telemetry-agent", "registry.test/telemetry:"+targetCommit)
	baseManifest, err := releasedomain.CanonicalizeRenderedManifest([]byte(baseRaw), spec, namespace)
	if err != nil {
		t.Fatal(err)
	}
	targetManifest, err := releasedomain.CanonicalizeRenderedManifest([]byte(targetRaw), spec, namespace)
	if err != nil {
		t.Fatal(err)
	}

	liveList := map[string]any{
		"apiVersion": "v1", "kind": "List", "metadata": map[string]any{},
		"items": []any{
			releasePreflightLiveWorkload("fugue-api", "api", "registry.test/api@"+activationTestDigest("a")),
			releasePreflightLiveWorkload("fugue-telemetry-agent", "telemetry-agent", "registry.test/telemetry@"+activationTestDigest("b")),
		},
	}
	liveBytes := mustCanonicalReleasePreflightJSON(t, liveList)
	expanded, err := expandObservedKubernetesList(liveBytes, namespace)
	if err != nil {
		t.Fatal(err)
	}
	observedManifest, err := releasedomain.MaterializeObservedLiveImageManifest(baseManifest, expanded, ownership, namespace)
	if err != nil {
		t.Fatal(err)
	}

	changedDocument, err := newEvidenceDocument(evidenceResult{
		baseCommit: baseCommit, targetCommit: targetCommit,
		changes: []releasedomain.ChangedFile{{
			Status: releasedomain.ChangeModified, Path: "internal/controller/controller.go",
			ConsumerDomains: []releasedomain.Domain{releasedomain.DomainControlPlane},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	changedBytes := mustCanonicalReleasePreflightJSON(t, changedDocument)
	changed, err := releasedomain.DecodeAndVerifyChangedFileEvidence(bytes.NewReader(changedBytes), baseCommit, targetCommit)
	if err != nil {
		t.Fatal(err)
	}
	context, err := releasedomain.NewClassificationContextEvidence(namespace, map[string]string{"releaseNamespace": namespace}, false)
	if err != nil {
		t.Fatal(err)
	}
	plan := releasedomain.BuildPlan(releasedomain.PlanInput{
		Files: releasedomain.FileClassification{
			Domains: []releasedomain.Domain{releasedomain.DomainControlPlane},
			Evidence: []releasedomain.Evidence{{
				Source: "changed-file", Subject: "internal/controller/controller.go",
				Domains: []releasedomain.Domain{releasedomain.DomainControlPlane},
			}},
		},
		Rendered: releasedomain.ClassifyRendered(baseManifest, targetManifest, spec, releasedomain.RenderedOptions{
			DefaultNamespace: namespace, Bindings: context.BindingMap(),
		}),
		Digests: releasedomain.DigestEvidence{
			Base: activationTestDigest("1"), Target: activationTestDigest("2"), Live: activationTestDigest("1"),
			BaseManifest: activationTestBytesDigest(baseManifest), TargetManifest: activationTestBytesDigest(targetManifest),
			RepeatedTargetManifest: activationTestBytesDigest(targetManifest), Ownership: activationTestBytesDigest(ownership),
			ChangedFiles: changedDocument.Digest, ClassificationContext: context,
		},
	})
	if plan.Result != releasedomain.OutcomeSingle || plan.SelectedDomain != releasedomain.DomainControlPlane {
		t.Fatalf("fixture conservative plan = %#v", plan)
	}

	buildReceipt := []map[string]any{
		releasePreflightBuildReceiptItem("api", "registry.test/api", activationTestDigest("c"), targetCommit),
		releasePreflightBuildReceiptItem("edge", "registry.test/edge", activationTestDigest("e"), targetCommit),
		releasePreflightBuildReceiptItem("telemetry_agent", "registry.test/telemetry", activationTestDigest("d"), targetCommit),
	}
	buildReceiptBytes := mustCanonicalReleasePreflightJSON(t, buildReceipt)
	provenanceDigest := releasePreflightDigest(buildReceiptBytes)
	buildPlan, err := releasedomain.NewBuildArtifactPlan(baseCommit, targetCommit, changed.Digest(), []releasedomain.BuildArtifact{
		{Name: "api", SourceBaseCommit: baseCommit, ArtifactDigest: activationTestDigest("c"), ProvenanceDigest: provenanceDigest, PublishedImageRef: "registry.test/api@" + activationTestDigest("c")},
		{Name: "edge", SourceBaseCommit: baseCommit, ArtifactDigest: activationTestDigest("e"), ProvenanceDigest: provenanceDigest, PublishedImageRef: "registry.test/edge@" + activationTestDigest("e")},
		{Name: "telemetry_agent", SourceBaseCommit: baseCommit, ArtifactDigest: activationTestDigest("d"), ProvenanceDigest: provenanceDigest, PublishedImageRef: "registry.test/telemetry@" + activationTestDigest("d")},
	})
	if err != nil {
		t.Fatal(err)
	}
	immutableTarget, err := releasedomain.MaterializeObservedLiveRelativeTargetPublishedImageRefs(
		baseManifest, observedManifest, targetManifest, ownership, namespace, targetCommit, buildPlan, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	activationPlan, activationEvidence, err := releasedomain.BuildImageActivationReportFromManifests(releasedomain.ImageActivationPlanInput{
		BuildPlan: buildPlan, ReleasePlan: plan, Ownership: ownership,
		BaseManifest: baseManifest, ObservedLiveManifest: observedManifest,
		TargetManifest: targetManifest, ImmutableTargetManifest: immutableTarget,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(activationPlan.Activations) != 2 || !activationEvidence.Complete ||
		!equalStringSlices(activationEvidence.BuiltOnlyArtifacts, []string{"edge"}) {
		t.Fatalf("fixture activation partition plan=%#v evidence=%#v", activationPlan, activationEvidence)
	}
	decomposition, err := releasedomain.BuildCompositeDecompositionEvidence(activationPlan, activationEvidence)
	if err != nil {
		t.Fatal(err)
	}
	activationRendered := releasedomain.ClassifyRendered(baseManifest, immutableTarget, spec, releasedomain.RenderedOptions{
		DefaultNamespace: namespace, Bindings: context.BindingMap(),
	})
	report, err := releasedomain.BuildOperationalDomainEvidenceFromObservedLiveActivation(
		changed, buildPlan, activationPlan, activationEvidence, activationRendered,
		activationTestBytesDigest(baseManifest), activationTestBytesDigest(observedManifest),
		activationTestBytesDigest(targetManifest), activationTestBytesDigest(immutableTarget),
		activationTestBytesDigest(ownership), plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	helmRelease := map[string]any{
		"name": "fugue", "namespace": namespace, "version": 17,
		"manifest": baseRaw, "hooks": []any{},
		"config": map[string]any{"api": map[string]any{"image": map[string]any{"tag": baseCommit}}},
	}
	metadata := map[string]any{
		"apiVersion": "release-domain.fugue.dev/v1", "kind": "ReleasePreflightSnapshot",
		"releaseName": "fugue", "namespace": namespace,
		"helmRevision": 17, "targetRevision": 18, "rollbackTargetRevision": 17,
		"trustedBaseCommit": baseCommit, "trustedTargetCommit": targetCommit,
		"buildReceiptDigest": provenanceDigest, "operationalReportDigest": report.Digest,
		"planDigest": plan.PlanDigest,
	}
	postReadback := releasePreflightCandidateBundleObservation(11, "pass")
	postReadback["releaseEpoch"] = "pdp-20260802t213000z-abcd"
	candidateBundleReadback := map[string]any{
		"apiVersion": "release-domain.fugue.dev/v1", "kind": "CandidateBundleReadbackWindow",
		"windowSeconds": 90, "intervalSeconds": 5, "advanceCount": 1,
		"observations": []any{
			releasePreflightCandidateBundleObservation(0, "pending"),
			releasePreflightCandidateBundleObservation(5, "warming"),
			releasePreflightCandidateBundleObservation(10, "pass"),
		},
		"postReadback": postReadback,
	}

	writeReleasePreflightFile(t, filepath.Join(root, "snapshot.json"), mustCanonicalReleasePreflightJSON(t, metadata))
	writeReleasePreflightFile(t, filepath.Join(root, "helm-release.json"), mustCanonicalReleasePreflightJSON(t, helmRelease))
	writeReleasePreflightFile(t, filepath.Join(root, "live-workloads.json"), liveBytes)
	writeReleasePreflightFile(t, filepath.Join(root, "build-receipt.json"), buildReceiptBytes)
	writeReleasePreflightFile(t, filepath.Join(root, "candidate-bundle-readback.json"), mustCanonicalReleasePreflightJSON(t, candidateBundleReadback))
	writeReleasePreflightFile(t, filepath.Join(root, "ownership-v1.yaml"), ownership)
	writeReleasePreflightFile(t, filepath.Join(root, "target-manifest.yaml"), targetManifest)
	writeReleasePreflightFile(t, filepath.Join(root, "operational-domain-evidence.json"), mustMarshalOperationalReport(t, report))
	writeReleasePreflightFile(t, filepath.Join(evidenceDir, "build-artifact-plan.json"), mustMarshalBuildPlan(t, buildPlan))
	writeReleasePreflightFile(t, filepath.Join(evidenceDir, "image-activation-plan.json"), mustMarshalActivationPlan(t, activationPlan))
	writeReleasePreflightFile(t, filepath.Join(evidenceDir, "image-activation-evidence.json"), mustMarshalActivationEvidence(t, activationEvidence))
	writeReleasePreflightFile(t, filepath.Join(evidenceDir, "composite-decomposition-evidence.json"), mustMarshalDecomposition(t, decomposition))
	writeReleasePreflightFile(t, filepath.Join(evidenceDir, "immutable-target-manifest.yaml"), immutableTarget)
	writeReleasePreflightFile(t, filepath.Join(evidenceDir, "observed-live-manifest.yaml"), observedManifest)
	return root
}

func releasePreflightCandidateBundleObservation(elapsedSeconds int, state string) map[string]any {
	observation := map[string]any{
		"elapsedSeconds": elapsedSeconds, "state": state, "podUID": "pod-candidate-uid",
		"releaseEpoch":  "pdp-20260802T213000Z-AbCd",
		"bundleVersion": "", "bundleDigest": "", "routeGeneration": "",
		"bundleSelection": false, "localAsk": false, "directTLS": false,
		"consecutiveHealthy": 0, "durableHealthy": false,
	}
	if state == "warming" || state == "pass" {
		observation["bundleVersion"] = "bundle-2"
		observation["bundleDigest"] = activationTestDigest("9")
		observation["routeGeneration"] = "routegen_2"
		observation["bundleSelection"] = true
		observation["localAsk"] = true
		observation["directTLS"] = true
	}
	if state == "warming" {
		observation["consecutiveHealthy"] = 1
	}
	if state == "pass" {
		observation["consecutiveHealthy"] = 2
		observation["durableHealthy"] = true
	}
	return observation
}

func releasePreflightDeployment(name, container, image string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: fugue-system
spec:
  selector:
    matchLabels: {app: %s}
  template:
    metadata:
      labels: {app: %s}
    spec:
      containers:
        - name: %s
          image: %s
      volumes:
        - name: node-identity
          downwardAPI:
            items:
              - path: node_name
                fieldRef:
                  fieldPath: metadata.labels['topology.kubernetes.io/hostname']
`, name, name, name, container, image)
}

func releasePreflightLiveWorkload(name, container, image string) map[string]any {
	return map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": name, "namespace": "fugue-system",
			"uid": "uid-" + name, "resourceVersion": "101", "generation": 2,
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"},
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": name}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": container, "image": image}}},
			},
		},
		"status": map[string]any{"readyReplicas": 1},
	}
}

func releasePreflightBuildReceiptItem(component, repository, digest, revision string) map[string]any {
	return map[string]any{
		"component": component, "repository": repository, "immutable_ref": repository + "@" + digest,
		"source_tag": revision, "oci_revision": revision, "top_digest": digest,
		"platform_manifest_digest": digest, "config_digest": activationTestDigest("f"),
		"verification": "registry_manifest_config_and_layer_get",
	}
}

func mutateReleasePreflightJSON(t *testing.T, path string, mutate func(map[string]any)) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	mutate(document)
	writeReleasePreflightFile(t, path, mustCanonicalReleasePreflightJSON(t, document))
}

func mustCanonicalReleasePreflightJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func writeReleasePreflightFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func releasePreflightDigest(data []byte) string {
	sum := sha256.Sum256(bytes.TrimSuffix(data, []byte("\n")))
	return fmt.Sprintf("sha256:%x", sum[:])
}

func mustMarshalDecomposition(t *testing.T, value releasedomain.CompositeDecompositionEvidence) []byte {
	t.Helper()
	data, err := releasedomain.MarshalCompositeDecompositionEvidence(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustMarshalOperationalReport(t *testing.T, value releasedomain.OperationalDomainEvidence) []byte {
	t.Helper()
	data, err := releasedomain.MarshalOperationalDomainEvidence(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
