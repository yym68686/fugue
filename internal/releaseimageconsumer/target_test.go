package releaseimageconsumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const testHeadSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testDigest(character string) string {
	return "sha256:" + strings.Repeat(character, 64)
}

func testArtifact(component, repository, top, platform string) Artifact {
	return Artifact{
		Component:              component,
		Repository:             repository,
		SourceTag:              testHeadSHA,
		TopDigest:              top,
		PlatformManifestDigest: platform,
		ConfigDigest:           testDigest("f"),
		OCIRevision:            testHeadSHA,
		ImmutableRef:           repository + "@" + top,
		Verification:           registryVerifyMode,
	}
}

func testBuiltActivation(artifact Artifact, helmPath, kind, name, container string) Activation {
	return Activation{
		Component:             artifact.Component,
		SourceMode:            "built",
		SourceTemplateRef:     artifact.ImmutableRef,
		SelectedRef:           artifact.ImmutableRef,
		Repository:            artifact.Repository,
		SourceTag:             testHeadSHA,
		Digest:                artifact.TopDigest,
		RuntimeManifestDigest: artifact.PlatformManifestDigest,
		PinState:              "pinned",
		MigrationAllowed:      false,
		HelmPath:              helmPath,
		Workload:              Workload{Kind: kind, Name: name, Container: container},
	}
}

func testLock() Lock {
	api := testArtifact("api", "ghcr.io/acme/fugue-api", testDigest("1"), testDigest("4"))
	controller := testArtifact("controller", "ghcr.io/acme/fugue-controller", testDigest("2"), testDigest("5"))
	drain := testArtifact("drain_agent", "ghcr.io/acme/fugue-drain-agent", testDigest("3"), testDigest("6"))
	return Lock{
		SchemaVersion: 1,
		Producer:      lockProducer,
		Release: LockRelease{
			Repository: "acme/fugue",
			Workflow:   releaseWorkflow,
			RunID:      "123",
			RunAttempt: "1",
			HeadSHA:    testHeadSHA,
			Platform:   releasePlatform,
		},
		Artifacts: []Artifact{api, controller, drain},
		Activations: []Activation{
			testBuiltActivation(api, "api.image", "Deployment", "fugue-fugue-api", "api"),
			testBuiltActivation(controller, "controller.image", "Deployment", "fugue-fugue-controller", "controller"),
			testBuiltActivation(drain, "runtime.strictDrain.agent.image", "Configuration", "fugue-fugue-controller", "controller"),
		},
	}
}

func canonicalLockFixture(t *testing.T, lock Lock) []byte {
	t.Helper()
	artifacts := make([]Artifact, len(lock.Artifacts))
	copy(artifacts, lock.Artifacts)
	lock.Artifacts = artifacts
	activations := make([]Activation, len(lock.Activations))
	copy(activations, lock.Activations)
	lock.Activations = activations
	sort.Slice(lock.Artifacts, func(i, j int) bool { return lock.Artifacts[i].Component < lock.Artifacts[j].Component })
	sort.Slice(lock.Activations, func(i, j int) bool {
		left, right := lock.Activations[i], lock.Activations[j]
		leftKey := strings.Join([]string{left.Component, left.HelmPath, left.Workload.Kind, left.Workload.Name, left.Workload.Container, left.SourceMode, left.SelectedRef}, "\x00")
		rightKey := strings.Join([]string{right.Component, right.HelmPath, right.Workload.Kind, right.Workload.Name, right.Workload.Container, right.SourceMode, right.SelectedRef}, "\x00")
		return leftKey < rightKey
	})
	lock.LockDigest = ""
	encoded, err := canonicalJSON(lock)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	delete(document, "lock_digest")
	withoutDigest, err := canonicalJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	document["lock_digest"] = digestBytes(withoutDigest)
	result, err := canonicalJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	return append(result, '\n')
}

func lockDocumentFixture(t *testing.T, lock Lock) map[string]any {
	t.Helper()
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(canonicalLockFixture(t, lock)))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	return document
}

func rehashedLockDocument(t *testing.T, document map[string]any) []byte {
	t.Helper()
	delete(document, "lock_digest")
	withoutDigest, err := canonicalJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	document["lock_digest"] = digestBytes(withoutDigest)
	encoded, err := canonicalJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}

func baseManifest(lock Lock) string {
	api := lock.Activations[0]
	controller := lock.Activations[1]
	drain := lock.Activations[2]
	return fmt.Sprintf(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-api
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: api
          image: %s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-controller
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: controller
          image: %s
          env:
            - name: FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY
              value: %s
            - name: FUGUE_DRAIN_AGENT_IMAGE_TAG
              value: %s
            - name: FUGUE_DRAIN_AGENT_IMAGE_DIGEST
              value: %s
            - name: FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY
              value: IfNotPresent
`, api.SelectedRef, controller.SelectedRef, drain.Repository, drain.SourceTag, drain.Digest)
}

func prePullLock() Lock {
	lock := testLock()
	controller := lock.Artifacts[1]
	lock.Activations = append(lock.Activations, testBuiltActivation(controller, "imagePrePull.image", "DaemonSet", "fugue-fugue-image-prepull", "image-prepull"))
	return lock
}

func prePullManifest(lock Lock, images []string) string {
	var selected string
	for _, activation := range lock.Activations {
		if activation.HelmPath == "imagePrePull.image" {
			selected = activation.SelectedRef
			break
		}
	}
	return baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-image-prepull
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: image-prepull
          image: %s
          command:
            - /bin/bash
            - -lc
            - |
              set -euo pipefail
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: fugue-fugue-image-prepull
  namespace: fugue-system
data:
  images: |-
    %s
`, selected, strings.ReplaceAll(strings.Join(images, "\n"), "\n", "\n    "))
}

func enablePrePull(helm map[string]any, images []string) {
	values := make([]any, len(images))
	for index, image := range images {
		values[index] = image
	}
	helm["config"] = map[string]any{"imagePrePull": map[string]any{"enabled": true, "images": values}}
}

func helmFixture(manifest string) map[string]any {
	return map[string]any{
		"name":      "fugue",
		"namespace": "fugue-system",
		"version":   8,
		"info": map[string]any{
			"status":         "pending-upgrade",
			"description":    "Dry run complete",
			"first_deployed": "2026-01-02T03:04:05Z",
			"last_deployed":  "2026-01-02T03:04:06Z",
		},
		"chart": map[string]any{
			"metadata": map[string]any{"name": "fugue"},
			"values": map[string]any{
				"nameOverride":     "",
				"fullnameOverride": "",
				"registry":         map[string]any{"enabled": false},
				"registryGC":       map[string]any{"enabled": false},
				"registryJanitor":  map[string]any{"enabled": false},
				"nodeJanitor":      map[string]any{"enabled": false},
				"topologyLabeler":  map[string]any{"enabled": false},
				"imagePrePull":     map[string]any{"enabled": false, "images": []any{}},
				"observability":    map[string]any{"enabled": false, "agent": map[string]any{"enabled": false}},
				"imageCache":       map[string]any{"enabled": false},
				"edge": map[string]any{
					"enabled": false, "groups": []any{}, "blueGreen": map[string]any{"enabled": false, "migration": map[string]any{"keepLegacyDirect": false}},
					"dynamic": map[string]any{"enabled": false, "blueGreen": map[string]any{"enabled": true}}, "sshFront": map[string]any{"enabled": true},
				},
				"dns":          map[string]any{"enabled": false, "groups": []any{}},
				"meshRecovery": map[string]any{"enabled": false},
			},
		},
		"config":   map[string]any{},
		"manifest": manifest,
		"hooks":    []any{},
	}
}

func writeFixture(t *testing.T, name string, data []byte) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func invokeTarget(t *testing.T, lock Lock, first, repeated map[string]any) (TargetEvidence, error) {
	t.Helper()
	first = cloneJSONMap(t, first)
	repeated = cloneJSONMap(t, repeated)
	first["info"].(map[string]any)["last_deployed"] = "2026-01-02T03:04:06.000001Z"
	repeated["info"].(map[string]any)["last_deployed"] = "2026-01-02T03:04:06.000002Z"
	return invokeTargetExact(t, lock, first, repeated)
}

func cloneJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func invokeTargetExact(t *testing.T, lock Lock, first, repeated map[string]any) (TargetEvidence, error) {
	t.Helper()
	lockBytes := canonicalLockFixture(t, lock)
	decodedLock, err := decodeAndValidateLock(lockBytes)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := writeFixture(t, "release-image-lock.json", lockBytes)
	firstBytes, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	repeatedBytes, err := json.Marshal(repeated)
	if err != nil {
		t.Fatal(err)
	}
	firstPath := writeFixture(t, "target.json", firstBytes)
	repeatedPath := writeFixture(t, "target-repeat.json", repeatedBytes)
	return VerifyTargetRender(lockPath, firstPath, repeatedPath, TargetExpectations{
		LockDigest: decodedLock.LockDigest, HeadSHA: decodedLock.Release.HeadSHA,
		Repository: decodedLock.Release.Repository, RunID: decodedLock.Release.RunID, RunAttempt: decodedLock.Release.RunAttempt,
		ReleaseFullname: "fugue-fugue", ReleaseName: "fugue", Namespace: "fugue-system", LiveRevision: 7,
	})
}

func TestVerifyTargetRenderCanonicalProjection(t *testing.T) {
	lock := testLock()
	helm := helmFixture(baseManifest(lock))
	evidence, err := invokeTarget(t, lock, helm, helm)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Bindings) != 3 || evidence.LockDigest == "" || evidence.EvidenceDigest == "" {
		t.Fatalf("unexpected evidence: %#v", evidence)
	}
	if err := VerifyTargetEvidenceDigest(evidence); err != nil {
		t.Fatal(err)
	}
	first, err := EncodeTargetEvidence(evidence)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeTargetEvidence(evidence)
	if err != nil || !bytes.Equal(first, second) || first[len(first)-1] != '\n' {
		t.Fatalf("evidence is not deterministic: err=%v", err)
	}
}

func TestVerifyTargetRenderRequiresIndependentFiles(t *testing.T) {
	lock := testLock()
	helmBytes, _ := json.Marshal(helmFixture(baseManifest(lock)))
	lockPath := writeFixture(t, "lock.json", canonicalLockFixture(t, lock))
	renderPath := writeFixture(t, "render.json", helmBytes)
	expectedLock, err := decodeAndValidateLock(canonicalLockFixture(t, lock))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyTargetRender(lockPath, renderPath, renderPath, TargetExpectations{
		LockDigest: expectedLock.LockDigest, HeadSHA: testHeadSHA, Repository: "acme/fugue", RunID: "123", RunAttempt: "1",
		ReleaseFullname: "fugue-fugue", ReleaseName: "fugue", Namespace: "fugue-system", LiveRevision: 7,
	}); err == nil {
		t.Fatal("same dry-run file was accepted twice")
	}
}

func TestVerifyTargetRenderRejectsMissingPreserveActivationEvenForUnknownRepository(t *testing.T) {
	lock := testLock()
	manifest := baseManifest(lock) + `---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-edge
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge
          image: registry.example.com/previous-edge:stable
`
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"edge": map[string]any{"enabled": true, "sshFront": map[string]any{"enabled": false}}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "consumer count") {
		t.Fatalf("missing preserve activation was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderCanonicalizesLegalPinnedTagDigestSelection(t *testing.T) {
	lock := testLock()
	repository := "registry.example.com/previous-edge"
	digest := testDigest("7")
	lock.Activations = append(lock.Activations, Activation{
		Component: "edge", SourceMode: "preserve",
		SourceTemplateRef: repository + ":stable@" + digest,
		SelectedRef:       repository + ":stable@" + digest,
		Repository:        repository, SourceTag: "stable", Digest: digest, RuntimeManifestDigest: testDigest("8"),
		PinState: "pinned", HelmPath: "edge.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-edge", Container: "edge"},
	})
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-edge
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge
          image: %s@%s
`, repository, digest)
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"edge": map[string]any{"enabled": true, "sshFront": map[string]any{"enabled": false}}}
	evidence, err := invokeTarget(t, lock, helm, helm)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, binding := range evidence.Bindings {
		if binding.HelmPath == "edge.image" {
			found = true
			if binding.SelectedRef != repository+"@"+digest {
				t.Fatalf("evidence did not record canonical rendered ref: %#v", binding)
			}
		}
	}
	if !found {
		t.Fatal("edge binding missing from evidence")
	}
}

func TestVerifyTargetRenderAcceptsExplicitLegacyUnpinnedPreserveSelection(t *testing.T) {
	lock := testLock()
	repository := "registry.example.com/legacy-edge"
	lock.Activations = append(lock.Activations, Activation{
		Component: "edge", SourceMode: "preserve",
		SourceTemplateRef: repository + ":stable", SelectedRef: repository + ":stable",
		Repository: repository, SourceTag: "stable", PinState: "legacy_unpinned", MigrationAllowed: true,
		HelmPath: "edge.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-edge", Container: "edge"},
	})
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-edge
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge
          image: %s:stable
`, repository)
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"edge": map[string]any{"enabled": true, "sshFront": map[string]any{"enabled": false}}}
	evidence, err := invokeTarget(t, lock, helm, helm)
	if err != nil {
		t.Fatal(err)
	}
	for _, binding := range evidence.Bindings {
		if binding.HelmPath == "edge.image" && binding.SelectedRef != repository+":stable" {
			t.Fatalf("legacy rendered ref drifted: %#v", binding)
		}
	}
}

func TestVerifyTargetRenderRejectsManagedRepositoryInInitContainer(t *testing.T) {
	lock := testLock()
	manifest := strings.Replace(baseManifest(lock), "      containers:\n        - name: api", fmt.Sprintf("      initContainers:\n        - name: hidden\n          image: %s\n      containers:\n        - name: api", lock.Activations[0].SelectedRef), 1)
	helm := helmFixture(manifest)
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "unclassified initContainer") {
		t.Fatalf("managed initContainer was not rejected: %v", err)
	}
}

func TestCanonicalRepositoryCollapsesEquivalentRegistryAuthorities(t *testing.T) {
	for _, pair := range [][2]string{
		{"ghcr.io/acme/control-api", "ghcr.io:443/acme/control-api"},
		{"docker.io/library/busybox", "busybox"},
		{"docker.io/acme/tool", "acme/tool"},
		{"docker.io/library/busybox", "registry-1.docker.io/busybox"},
		{"docker.io/acme/tool", "registry.hub.docker.com:443/acme/tool"},
		{"docker.io/library/example.com", "example.com"},
		{"docker.io/library/localhost", "localhost"},
	} {
		if left, right := canonicalRepository(pair[0]), canonicalRepository(pair[1]); left != right {
			t.Fatalf("equivalent repository authorities did not canonicalize together: %q != %q", left, right)
		}
	}
}

func TestRepositoryWithinManagedBoundaryUsesSlashDelimitedAncestors(t *testing.T) {
	namespaces := make(map[string]struct{}, 4098)
	for index := 0; index < 4096; index++ {
		namespaces[fmt.Sprintf("registry.example.com/tenant-%04d", index)] = struct{}{}
	}
	namespaces["ghcr.io/acme"] = struct{}{}
	namespaces["docker.io/library"] = struct{}{}
	managed := map[string]struct{}{canonicalRepository("registry.example.com/exact"): {}}
	for name, test := range map[string]struct {
		repository string
		want       bool
	}{
		"exact":            {repository: "registry.example.com/exact", want: true},
		"nested-namespace": {repository: "ghcr.io/acme/team/control-api", want: true},
		"docker-library":   {repository: "busybox", want: true},
		"prefix-collision": {repository: "ghcr.io/acme-other/control-api", want: false},
		"unmanaged":        {repository: "registry.example.com/other/control-api", want: false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := repositoryWithinManagedBoundary(test.repository, managed, namespaces); got != test.want {
				t.Fatalf("repositoryWithinManagedBoundary(%q)=%t, want %t", test.repository, got, test.want)
			}
		})
	}
}

func BenchmarkRepositoryWithinManagedBoundaryManyNamespaces(b *testing.B) {
	namespaces := make(map[string]struct{}, 4097)
	for index := 0; index < 4096; index++ {
		namespaces[fmt.Sprintf("registry.example.com/tenant-%04d", index)] = struct{}{}
	}
	namespaces["ghcr.io/acme"] = struct{}{}
	managed := map[string]struct{}{}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !repositoryWithinManagedBoundary("ghcr.io/acme/team/control-api", managed, namespaces) {
			b.Fatal("managed namespace was not found")
		}
	}
}

func TestVerifyTargetRenderRejectsManagedAliasesNamespacesAndDigests(t *testing.T) {
	lock := testLock()
	api := testArtifact("api", "ghcr.io/acme/control-api", testDigest("1"), testDigest("4"))
	lock.Artifacts[0] = api
	lock.Activations[0] = testBuiltActivation(api, "api.image", "Deployment", "fugue-fugue-api", "api")

	for name, image := range map[string]string{
		"default-https-authority": "ghcr.io:443/acme/control-api@" + testDigest("1"),
		"same-managed-namespace":  "ghcr.io/acme/db-migrate@" + testDigest("7"),
		"managed-digest-mirror":   "mirror.example.com/cache/control-api@" + testDigest("1"),
		"fugue-repository-name":   "registry.example.com/other/fugue-db-migrate@" + testDigest("7"),
	} {
		t.Run(name, func(t *testing.T) {
			manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: batch/v1
kind: Job
metadata:
  name: migrate
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: migrate
          image: %s
`, image)
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "unclassified container") {
				t.Fatalf("unclassified managed image identity was not rejected: %v", err)
			}
		})
	}
}

func TestVerifyTargetRenderRejectsDockerHubOwnerNamespaceAlias(t *testing.T) {
	lock := testLock()
	api := testArtifact("api", "acme/control-api", testDigest("1"), testDigest("4"))
	lock.Artifacts[0] = api
	lock.Activations[0] = testBuiltActivation(api, "api.image", "Deployment", "fugue-fugue-api", "api")
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: batch/v1
kind: Job
metadata:
  name: migrate
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: migrate
          image: acme/db-migrate@%s
`, testDigest("7"))
	helm := helmFixture(manifest)
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "unclassified container") {
		t.Fatalf("Docker Hub owner namespace alias was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsSiblingOwnerRepositoryWithPreserveOnlyLock(t *testing.T) {
	lock := testLock()
	lock.Artifacts = []Artifact{}
	for index := range lock.Activations {
		lock.Activations[index].SourceMode = "preserve"
		lock.Activations[index].MigrationAllowed = false
	}
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: batch/v1
kind: Job
metadata:
  name: preserve-sibling
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: migrate
          image: ghcr.io/acme/db-migrate@%s
`, testDigest("7"))
	helm := helmFixture(manifest)
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "unclassified container") {
		t.Fatalf("preserve-only owner namespace sibling was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsSingleComponentRegistryLookingAlias(t *testing.T) {
	for managed, alias := range map[string]string{
		"example.com": "docker.io/library/example.com:other",
		"localhost":   "docker.io/library/localhost:other",
	} {
		t.Run(managed, func(t *testing.T) {
			lock := testLock()
			api := testArtifact("api", managed, testDigest("1"), testDigest("4"))
			lock.Artifacts[0] = api
			lock.Activations[0] = testBuiltActivation(api, "api.image", "Deployment", "fugue-fugue-api", "api")
			manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: batch/v1
kind: Job
metadata:
  name: alias
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: alias
          image: %s
`, alias)
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "unclassified container") {
				t.Fatalf("single-component managed repository alias was not rejected: %v", err)
			}
		})
	}
}

func TestVerifyTargetRenderAcceptsExactUnqualifiedManagedRepository(t *testing.T) {
	lock := testLock()
	drain := testArtifact("drain_agent", "fugue-drain-agent", testDigest("3"), testDigest("6"))
	lock.Artifacts[2] = drain
	lock.Activations[2] = testBuiltActivation(drain, "runtime.strictDrain.agent.image", "Configuration", "fugue-fugue-controller", "controller")
	helm := helmFixture(baseManifest(lock))
	if _, err := invokeTarget(t, lock, helm, helm); err != nil {
		t.Fatalf("exact unqualified managed drain repository was rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsUnqualifiedManagedRepositoryInSplitPullArgs(t *testing.T) {
	lock := testLock()
	api := testArtifact("api", "api", testDigest("1"), testDigest("4"))
	lock.Artifacts[0] = api
	lock.Activations[0] = testBuiltActivation(api, "api.image", "Deployment", "fugue-fugue-api", "api")
	manifest := baseManifest(lock) + `---
apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: puller
  namespace: fugue-system
spec:
  command:
    - crictl
    - pull
  args:
    - api
`
	helm := helmFixture(manifest)
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "authoritative manifest locations") {
		t.Fatalf("unqualified managed split pull was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsDockerHubOwnerAlias(t *testing.T) {
	lock := testLock()
	api := testArtifact("api", "docker.io/acme/control-api", testDigest("1"), testDigest("4"))
	lock.Artifacts[0] = api
	lock.Activations[0] = testBuiltActivation(api, "api.image", "Deployment", "fugue-fugue-api", "api")
	manifest := baseManifest(lock) + `---
apiVersion: batch/v1
kind: Job
metadata:
  name: owner-alias
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: migrate
          image: registry.hub.docker.com/acme/db-migrate:other
`
	helm := helmFixture(manifest)
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "unclassified container") {
		t.Fatalf("Docker Hub managed-owner alias was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderScopesBareRepositoryOccurrencesToImageFields(t *testing.T) {
	lock := testLock()
	telemetry := testArtifact("telemetry_agent", "fugue-telemetry-agent", testDigest("7"), testDigest("8"))
	lock.Artifacts = append(lock.Artifacts, telemetry)
	lock.Activations = append(lock.Activations, testBuiltActivation(telemetry, "observability.agent.image", "Deployment", "fugue-fugue-telemetry-agent", "telemetry-agent"))
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-telemetry-agent
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: telemetry-agent
          image: %s
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: telemetry-metrics
  namespace: fugue-system
data:
  job_name: fugue-telemetry-agent
`, telemetry.ImmutableRef)
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"observability": map[string]any{"agent": map[string]any{"enabled": true}}}
	if _, err := invokeTarget(t, lock, helm, helm); err != nil {
		t.Fatalf("bare managed repository in a non-image field caused a false positive: %v", err)
	}

	unsafeManifest := strings.Replace(manifest, "job_name: fugue-telemetry-agent", "repository: fugue-telemetry-agent", 1)
	unsafeHelm := helmFixture(unsafeManifest)
	unsafeHelm["config"] = map[string]any{"observability": map[string]any{"agent": map[string]any{"enabled": true}}}
	if _, err := invokeTarget(t, lock, unsafeHelm, unsafeHelm); err == nil || !strings.Contains(err.Error(), "authoritative manifest locations") {
		t.Fatalf("bare managed repository in an image identity field was not rejected: %v", err)
	}
	scriptManifest := manifest + `---
apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: telemetry-puller
  namespace: fugue-system
spec:
  script: k3s crictl pull fugue-telemetry-agent
`
	scriptHelm := helmFixture(scriptManifest)
	scriptHelm["config"] = map[string]any{"observability": map[string]any{"agent": map[string]any{"enabled": true}}}
	if _, err := invokeTarget(t, lock, scriptHelm, scriptHelm); err == nil || !strings.Contains(err.Error(), "authoritative manifest locations") {
		t.Fatalf("bare managed repository in an executable field was not rejected: %v", err)
	}
	for name, spec := range map[string]string{
		"managed-explicit-tag":  "  warm: fugue-telemetry-agent:stable",
		"managed-non-port-tag":  "  warm: fugue-telemetry-agent:99999",
		"novel-explicit-digest": "  warm: fugue-db-migrate@" + testDigest("9"),
	} {
		t.Run(name, func(t *testing.T) {
			candidateManifest := manifest + `---
apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: warm-image
  namespace: fugue-system
spec:
` + spec + "\n"
			candidate := helmFixture(candidateManifest)
			candidate["config"] = map[string]any{"observability": map[string]any{"agent": map[string]any{"enabled": true}}}
			if _, err := invokeTarget(t, lock, candidate, candidate); err == nil || !strings.Contains(err.Error(), "authoritative manifest locations") {
				t.Fatalf("explicit image identity outside an image field was not rejected: %v", err)
			}
		})
	}
}

func TestVerifyTargetRenderScansManagedIdentitiesAcrossWholeObjects(t *testing.T) {
	lock := testLock()
	managed := lock.Activations[0].SelectedRef
	for name, spec := range map[string]string{
		"custom-image-field":       "  image: " + managed,
		"tekton-step":              "  steps:\n    - name: migrate\n      image: " + managed,
		"image-volume":             "  volumes:\n    - name: payload\n      image:\n        reference: " + managed,
		"split-identity":           "  repository: ghcr.io:443/acme/fugue-api\n  digest: " + testDigest("1"),
		"renamed-owner-repo":       "  image: ghcr.io/acme/db-migrate@" + testDigest("7"),
		"script-scalar":            "  script: |\n    k3s crictl pull " + managed + ";",
		"script-bare-pull":         "  script: |\n    k3s crictl pull fugue-db-migrate",
		"script-composed-owner":    "  script: 'crictl pull ghcr.io/acme\"/\"control-api:stable'",
		"script-composed-digest":   "  script: 'crictl pull mirror.example.com/cache/control-api@sha256:\"" + strings.TrimPrefix(testDigest("1"), "sha256:") + "\"'",
		"argv-bare-pull":           "  args:\n    - pull\n    - fugue-db-migrate",
		"split-pull-args":          "  command:\n    - crictl\n    - pull\n  args:\n    - fugue-db-migrate",
		"split-env-args":           "  command:\n    - env\n  args:\n    - fugue-app-ssh",
		"split-shell-args":         "  command:\n    - /bin/sh\n    - -c\n  args:\n    - fugue-novel",
		"split-shell-option-args":  "  command:\n    - /bin/sh\n  args:\n    - -c\n    - fugue-novel",
		"split-env-shell-args":     "  command:\n    - /usr/bin/env\n  args:\n    - MODE=test\n    - /bin/sh\n    - -lc\n    - fugue-novel",
		"split-timeout-args":       "  command:\n    - timeout\n  args:\n    - '5'\n    - fugue-novel",
		"split-nice-args":          "  command:\n    - nice\n  args:\n    - fugue-novel",
		"split-chroot-args":        "  command:\n    - chroot\n  args:\n    - /tmp\n    - fugue-novel",
		"command-timeout":          "  command:\n    - timeout\n    - '5'\n    - fugue-novel",
		"command-env-chdir":        "  command:\n    - /usr/bin/env\n    - -C\n    - /tmp\n    - fugue-novel",
		"entrypoint-timeout":       "  entrypoint:\n    - timeout\n    - '5'\n    - fugue-novel",
		"entrypoint-pull-args":     "  entrypoint:\n    - crictl\n    - pull\n  args:\n    - fugue-db-migrate",
		"split-env-string":         "  command:\n    - env\n  args:\n    - -S\n    - fugue-novel",
		"split-env-string-equals":  "  command:\n    - env\n  args:\n    - --split-string=fugue-novel",
		"split-exec-alias":         "  command:\n    - exec\n  args:\n    - -a\n    - alias\n    - fugue-novel",
		"split-tini-exit-code":     "  command:\n    - tini\n  args:\n    - -e\n    - '143'\n    - fugue-novel",
		"split-dumb-init-rewrite":  "  command:\n    - dumb-init\n  args:\n    - --rewrite\n    - '15:3'\n    - fugue-novel",
		"split-busybox-shell":      "  command:\n    - busybox\n  args:\n    - sh\n    - -c\n    - fugue-novel",
		"command-busybox-shell":    "  command:\n    - busybox\n    - sh\n    - -c\n    - fugue-novel",
		"entrypoint-toybox-shell":  "  entrypoint:\n    - toybox\n    - sh\n    - -c\n  args:\n    - fugue-novel",
		"script-escaped-bare":      "  script: fugue\\-novel",
		"script-escaped-absolute":  "  script: /usr/local/bin/fugue\\-novel",
		"script-quoted-bare":       "  script: 'fugue\"-\"novel'",
		"script-quoted-absolute":   "  script: '/usr/local/bin/fugue\"-\"novel'",
		"script-line-continuation": "  script: |\n    fugue\\\n    -novel",
		"script-timeout-wrapper":   "  script: timeout 5 fugue-app-ssh",
		"script-env-wrapper":       "  script: /usr/bin/env fugue-app-ssh",
		"script-assignment-bare":   "  script: 'CMD=fugue-app-ssh; \"$CMD\"'",
		"script-assignment-path":   "  script: 'CMD=/usr/local/bin/fugue-app-ssh; \"$CMD\"'",
		"tekton-fugue-command":     "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - /usr/local/bin/fugue-novel",
		"tekton-app-ssh-command":   "  steps:\n    - name: ssh\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - /usr/local/bin/fugue-app-ssh",
		"tekton-relative-command":  "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - fugue-novel",
		"tekton-relative-app-ssh":  "  steps:\n    - name: ssh\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - fugue-app-ssh",
		"tekton-dynamic-command":   "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - '/usr/local/bin/fugue-${ROLE}'",
		"tekton-invalid-command":   "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - /usr/local/bin/fugue--novel",
		"tekton-relative-dynamic":  "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - 'fugue-${ROLE}'",
		"tekton-relative-invalid":  "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - fugue--novel",
		"tekton-env-relative":      "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - env\n        - fugue-novel",
		"tekton-double-slash":      "  steps:\n    - name: ssh\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - /usr/local/bin//fugue-app-ssh",
		"tekton-dot-segment":       "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - /usr/local/bin/./fugue-novel",
		"tekton-parent-segment":    "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - /usr/local/lib/../bin/fugue-novel",
		"tekton-working-directory": "  steps:\n    - name: ssh\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - ./fugue-app-ssh",
		"tekton-other-directory":   "  steps:\n    - name: migrate\n      image: registry.example.com/vendor/tool:stable\n      command:\n        - /opt/bin/fugue-novel",
		"json-scalar":              "  config: '{\"image\":\"" + managed + "\"}'",
		"json-escaped-slashes":     "  config: '{\"image\":\"ghcr.io\\/acme\\/control-api:stable\"}'",
		"json-unicode-slashes":     "  config: '{\"image\":\"ghcr.io\\u002facme\\u002fcontrol-api:stable\"}'",
		"json-unicode-digest":      "  config: '{\"image\":\"mirror.example.com/cache/control-api@sha256:\\u0031" + strings.Repeat("1", 63) + "\"}'",
		"split-bare-repo":          "  repository: fugue-db-migrate\n  tag: stable",
		"image-name-bare":          "  imageName: fugue-db-migrate:stable",
		"camel-image-repository":   "  containerImageRepository: fugue-db-migrate",
		"managed-map-key":          "  images:\n    \"" + managed + "\": {}",
		"managed-digest-key":       "  digests:\n    \"" + testDigest("1") + "\": true",
	} {
		t.Run(name, func(t *testing.T) {
			manifest := baseManifest(lock) + `---
apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: novel-worker
  namespace: fugue-system
spec:
` + spec + "\n"
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "authoritative manifest locations") {
				t.Fatalf("managed identity outside a pod container was not rejected: %v", err)
			}
		})
	}
	for name, spec := range map[string]string{
		"leading-zero-port-image": "  image: ghcr.io:0443/acme/fugue-api:stable",
		"uppercase-port-script":   "  script: |\n    k3s crictl pull GHCR.IO:0443/acme/fugue-api:stable;",
		"managed-digest-suffix":   "  config: malformed@@" + testDigest("1"),
		"managed-sha512-prefix":   "  script: k3s crictl pull ghcr.io/acme/fugue-api@sha512:" + strings.Repeat("a", 128),
		"managed-unknown-digest":  "  script: k3s crictl pull ghcr.io/acme/fugue-api@sha999:abcdef",
	} {
		t.Run(name, func(t *testing.T) {
			manifest := baseManifest(lock) + `---
apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: loose-image-worker
  namespace: fugue-system
spec:
` + spec + "\n"
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil {
				t.Fatal("loose managed image identity was accepted")
			}
		})
	}

	external := baseManifest(lock) + `---
apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: external-worker
  namespace: fugue-system
spec:
  image: registry.example.com/vendor/tool:stable
  endpoint: http://fugue-fugue-api:8080
  imageCacheRegistryBase: fugue-fugue-registry.fugue-system.svc.cluster.local:5000
`
	helm := helmFixture(external)
	if _, err := invokeTarget(t, lock, helm, helm); err != nil {
		t.Fatalf("unrelated external custom image was rejected: %v", err)
	}
}

func TestVerifyTargetRenderValidatesImagePrePullConsumers(t *testing.T) {
	lock := prePullLock()
	externalImages := []string{"busybox:1.36", "registry.example.com/vendor/tool@" + testDigest("7")}
	helm := helmFixture(prePullManifest(lock, externalImages))
	enablePrePull(helm, externalImages)
	if _, err := invokeTarget(t, lock, helm, helm); err != nil {
		t.Fatalf("valid external image pre-pull projection was rejected: %v", err)
	}

	for name, image := range map[string]string{
		"managed-authority-alias": "ghcr.io:443/acme/fugue-api@" + testDigest("1"),
		"managed-digest-mirror":   "mirror.example.com/cache/api@" + testDigest("1"),
		"same-owner-namespace":    "ghcr.io/acme/db-migrate@" + testDigest("7"),
		"fugue-repository":        "registry.example.com/other/fugue-helper@" + testDigest("7"),
	} {
		t.Run(name, func(t *testing.T) {
			images := []string{image}
			candidate := helmFixture(prePullManifest(lock, images))
			enablePrePull(candidate, images)
			if _, err := invokeTarget(t, lock, candidate, candidate); err == nil || !strings.Contains(err.Error(), "authoritative manifest locations") {
				t.Fatalf("managed image pre-pull consumer was not rejected: %v", err)
			}
		})
	}

	drifted := helmFixture(prePullManifest(lock, []string{"busybox:1.35"}))
	enablePrePull(drifted, []string{"busybox:1.36"})
	if _, err := invokeTarget(t, lock, drifted, drifted); err == nil || !strings.Contains(err.Error(), "does not exactly match") {
		t.Fatalf("drifted image pre-pull ConfigMap was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsEphemeralAndUnknownFugueConsumers(t *testing.T) {
	lock := testLock()
	managed := lock.Activations[0].SelectedRef
	for name, extra := range map[string]string{
		"pod-ephemeral": fmt.Sprintf(`---
apiVersion: v1
kind: Pod
metadata:
  name: debug-pod
  namespace: fugue-system
spec:
  containers:
    - name: base
      image: busybox:1.36
  ephemeralContainers:
    - name: hidden
      image: %s
`, managed),
		"template-ephemeral": fmt.Sprintf(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: unrelated
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: base
          image: busybox:1.36
      ephemeralContainers:
        - name: hidden
          image: %s
`, managed),
		"custom-workload": `---
apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: novel-worker
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: novel
          image: registry.example.com/novel:stable
          command:
            - /usr/local/bin/fugue-novel
`,
	} {
		t.Run(name, func(t *testing.T) {
			helm := helmFixture(baseManifest(lock) + extra)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil {
				t.Fatal("unclassified pod consumer was accepted")
			}
		})
	}
}

func TestVerifyTargetRenderRejectsAppSSHConsumerWithOrWithoutArtifact(t *testing.T) {
	for _, withArtifact := range []bool{false, true} {
		t.Run(fmt.Sprintf("artifact-%t", withArtifact), func(t *testing.T) {
			lock := testLock()
			repository := "registry.example.com/fugue-app-ssh"
			if withArtifact {
				lock.Artifacts = append(lock.Artifacts, testArtifact("app_ssh", repository, testDigest("7"), testDigest("8")))
			}
			manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: batch/v1
kind: Job
metadata:
  name: fugue-fugue-app-ssh
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: ssh-helper
          image: %s:stable
`, repository)
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "artifact-only") {
				t.Fatalf("app_ssh runtime consumer was not rejected: %v", err)
			}
		})
	}
}

func TestVerifyTargetRenderRejectsDrainCarrierDuplicateAndValueFrom(t *testing.T) {
	lock := testLock()
	for name, mutation := range map[string]func(string) string{
		"duplicate": func(manifest string) string {
			return strings.Replace(manifest, "            - name: FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY", "            - name: FUGUE_DRAIN_AGENT_IMAGE_TAG\n              value: duplicate\n            - name: FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY", 1)
		},
		"valueFrom": func(manifest string) string {
			return strings.Replace(manifest, "            - name: FUGUE_DRAIN_AGENT_IMAGE_TAG\n              value: "+testHeadSHA, "            - name: FUGUE_DRAIN_AGENT_IMAGE_TAG\n              valueFrom:\n                fieldRef:\n                  fieldPath: metadata.name", 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			manifest := mutation(baseManifest(lock))
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil {
				t.Fatal("unsafe drain carrier was accepted")
			}
		})
	}
}

func TestVerifyTargetRenderRejectsUnsafeYAMLAndSecrets(t *testing.T) {
	lock := testLock()
	mutations := map[string]string{
		"duplicate-key": strings.Replace(baseManifest(lock), "kind: Deployment", "kind: Deployment\nkind: Deployment", 1),
		"anchor":        strings.Replace(baseManifest(lock), "name: fugue-fugue-api", "name: &api fugue-fugue-api", 1),
		"secret": `apiVersion: v1
kind: Secret
metadata:
  name: hidden
data:
  token: c2VjcmV0
`,
		"secret-list": `apiVersion: v1
kind: SecretList
metadata:
  name: hidden-list
items:
  - apiVersion: v1
    kind: Secret
    metadata:
      name: hidden
    data:
      token: c2VjcmV0
`,
		"exact-list-secret": `apiVersion: v1
kind: List
items:
  - apiVersion: v1
    kind: Secret
    metadata:
      name: hidden
    data:
      token: c2VjcmV0
`,
		"non-list-items": `apiVersion: example.test/v1
kind: CustomWorker
metadata:
  name: hidden-list
items: []
`,
	}
	for name, manifest := range mutations {
		t.Run(name, func(t *testing.T) {
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil {
				t.Fatal("unsafe YAML was accepted")
			}
		})
	}
	hookHelm := helmFixture(baseManifest(lock))
	hookHelm["hooks"] = []any{map[string]any{"manifest": mutations["secret-list"]}}
	if _, err := invokeTarget(t, lock, hookHelm, hookHelm); err == nil {
		t.Fatal("SecretList in a Helm hook was accepted")
	}
}

func TestUntrustedParserErrorsDoNotEchoInputMarkers(t *testing.T) {
	marker := strings.Repeat("SensitiveMarker", 128)
	checks := map[string]error{}
	_, checks["yaml-alias"] = decodeManifest([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: leak\ndata:\n  value: *"+marker+"\n"), "fugue-system")

	helm := helmFixture(baseManifest(testLock()))
	helm[marker] = true
	helmBytes, err := json.Marshal(helm)
	if err != nil {
		t.Fatal(err)
	}
	_, checks["helm-root-field"] = decodeHelmRelease(helmBytes)

	lockDocument := lockDocumentFixture(t, testLock())
	lockDocument[marker] = true
	lockBytes, err := canonicalJSON(lockDocument)
	if err != nil {
		t.Fatal(err)
	}
	_, checks["lock-field"] = decodeAndValidateLock(append(lockBytes, '\n'))

	_, checks["long-kind"] = decodeManifest([]byte("apiVersion: v1\nkind: "+marker+"\nmetadata:\n  name: leak\n"), "fugue-system")
	_, checks["long-api-version"] = decodeManifest([]byte("apiVersion: "+marker+"\nkind: ConfigMap\nmetadata:\n  name: leak\n"), "fugue-system")
	_, checks["nested-unknown-key"] = decodeManifest([]byte("apiVersion: example.test/v1\nkind: CustomWorker\nmetadata:\n  name: leak\nspec:\n  "+marker+":\n    spec:\n      containers:\n        - name: broken\n"), "fugue-system")

	for name, checkErr := range checks {
		t.Run(name, func(t *testing.T) {
			if checkErr == nil {
				t.Fatal("unsafe input unexpectedly succeeded")
			}
			if strings.Contains(checkErr.Error(), marker) || len(checkErr.Error()) > 512 {
				t.Fatalf("error disclosed or amplified untrusted input: %q", checkErr)
			}
		})
	}
	imageMarker := strings.Repeat("sensitive", 24)
	lock := testLock()
	manifest := strings.Replace(baseManifest(lock), lock.Activations[0].SelectedRef, "registry.example.com/"+imageMarker+":stable", 1)
	helm = helmFixture(manifest)
	_, mismatchErr := invokeTarget(t, lock, helm, helm)
	if mismatchErr == nil || strings.Contains(mismatchErr.Error(), imageMarker) || len(mismatchErr.Error()) > 512 {
		t.Fatalf("image mismatch error disclosed or amplified an untrusted reference: %q", mismatchErr)
	}
}

func TestStrictManifestRejectsNumericMemoryAmplification(t *testing.T) {
	for name, node := range map[string]*yaml.Node{
		"huge-exponent": {Kind: yaml.ScalarNode, Tag: "!!float", Value: "1e1000000"},
		"huge-integer":  {Kind: yaml.ScalarNode, Tag: "!!int", Value: strings.Repeat("9", 1025)},
	} {
		t.Run(name, func(t *testing.T) {
			count := 0
			if _, err := strictYAMLValue(node, "$", 0, &count); err == nil {
				t.Fatal("numeric amplification scalar was accepted")
			}
		})
	}
}

func TestImageCandidateScannerDropsOversizedTokens(t *testing.T) {
	oversized := "a" + strings.Repeat(":a", 1<<19)
	if _, err := boundedImageCandidates(oversized); err == nil || !strings.Contains(err.Error(), "oversized") {
		t.Fatalf("oversized continuous candidate was not rejected: %v", err)
	}
	if _, err := boundedImageCandidates(strings.Repeat("x,", maxImageCandidatesPerScalar+1)); err == nil || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("excessive candidate count was not rejected: %v", err)
	}
	managed := "ghcr.io/acme/fugue-api@" + testDigest("1")
	candidates, err := boundedImageCandidates("pull " + managed + ";")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 || candidates[1] != managed {
		t.Fatalf("bounded scanner lost a valid delimited image candidate: %#v", candidates)
	}
	candidateCount := maxImageCandidates - 1
	if err := collectManagedIdentityOccurrences("alpha beta", "", true, map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}, map[string]int{}, map[string]int{}, &candidateCount); err == nil || !strings.Contains(err.Error(), "candidate limit") {
		t.Fatalf("shared image scan candidate budget was not enforced: %v", err)
	}
	candidateCount = 0
	hugeEnvName := map[string]any{"name": strings.Repeat("💣", 4096) + "IMAGE", "value": strings.Repeat("a,", 1024)}
	if err := collectManagedIdentityOccurrences(hugeEnvName, "", true, map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}, map[string]int{}, map[string]int{}, &candidateCount); err != nil {
		t.Fatalf("bounded field classification amplified an unrelated env name: %v", err)
	}
	bomb := strings.Repeat("fugue-novel ", maxImageCandidatesPerScalar+1)
	if _, err := siblingCommandArgumentExecutables(map[string]any{
		"command": []any{"sh"},
		"args":    []any{"-c", bomb},
	}); err == nil || !strings.Contains(err.Error(), "too many candidate") {
		t.Fatalf("sibling shell argv candidate bomb was not rejected: %v", err)
	}
	if _, err := siblingCommandArgumentImagePulls(map[string]any{
		"command": []any{"crictl", "pull"},
		"args":    []any{strings.Repeat("candidate ", maxImageCandidatesPerScalar+1)},
	}, map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}); err == nil || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("sibling pull argv candidate bomb was not rejected: %v", err)
	}
	oversizedEntrypoint := make([]any, maxImageCandidatesPerScalar+1)
	for index := range oversizedEntrypoint {
		oversizedEntrypoint[index] = "argument"
	}
	candidateCount = 0
	if err := collectManagedIdentityOccurrences(map[string]any{"entrypoint": oversizedEntrypoint}, "", true,
		map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}, map[string]int{}, map[string]int{}, &candidateCount); err == nil || !strings.Contains(err.Error(), "argv exceeds item limit") {
		t.Fatalf("oversized entrypoint argv was not rejected: %v", err)
	}
}

func TestExactShellLineBindingIgnoresQuotedAndHeredocData(t *testing.T) {
	expected := `gc_lease="fugue-fugue-registry-gc"`
	for name, script := range map[string]string{
		"multiline-quote":          "ignored='\n" + expected + "\n'",
		"multiple-heredocs":        "cat <<ONE <<TWO\nfirst\nONE\n" + expected + "\nTWO",
		"indented-fake-terminator": "cat <<EOF\n  EOF\n" + expected + "\nEOF",
		"tab-strip-space-fake":     "cat <<-EOF\n  EOF\n" + expected + "\n\tEOF",
		"continued-argument":       "printf '%s' \\\n" + expected,
	} {
		t.Run(name, func(t *testing.T) {
			container := manifestContainer{Arguments: []string{script}}
			if count := countExactShellLine(container, expected); count != 0 {
				t.Fatalf("data-only shell line counted as executable binding: %d", count)
			}
		})
	}
	valid := manifestContainer{Arguments: []string{"cat <<-EOF\nbody\n\tEOF\n" + expected}}
	if count := countExactShellLine(valid, expected); count != 1 {
		t.Fatalf("valid code after tab-stripped heredoc counted %d times", count)
	}
	newlineBomb := manifestContainer{Arguments: []string{strings.Repeat("\n", maxShellLines+1)}}
	if count := countExactShellLine(newlineBomb, expected); count != -1 {
		t.Fatalf("shell newline bomb returned count %d", count)
	}
	heredocBomb := manifestContainer{Arguments: []string{strings.Repeat("<<A ", maxShellHeredocs+1)}}
	if count := countExactShellLine(heredocBomb, expected); count != -1 {
		t.Fatalf("shell heredoc delimiter bomb returned count %d", count)
	}
}

func TestManifestExpansionAndContainerBudgets(t *testing.T) {
	leaf := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "bounded"},
	}
	exhaustedObjects := &yamlBudget{ExpandedObjects: maxExpandedObjects}
	if _, err := expandManifestObject(leaf, "fugue-system", exhaustedObjects, 0); err == nil || !strings.Contains(err.Error(), "object limit") {
		t.Fatalf("expanded object budget was not enforced: %v", err)
	}

	var nested any = leaf
	for index := 0; index < maxManifestListDepth+1; index++ {
		nested = map[string]any{"apiVersion": "v1", "kind": "List", "items": []any{nested}}
	}
	if _, err := expandManifestObject(nested.(map[string]any), "fugue-system", &yamlBudget{}, 0); err == nil || !strings.Contains(err.Error(), "depth limit") {
		t.Fatalf("nested List depth budget was not enforced: %v", err)
	}

	exhaustedContainers := &yamlBudget{ParsedContainers: maxManifestContainers}
	rawContainers := []any{map[string]any{"name": "extra", "image": "busybox:1.36"}}
	if _, err := parseManifestContainerArray(rawContainers, "containers", false, false, true, exhaustedContainers); err == nil || !strings.Contains(err.Error(), "container limit") {
		t.Fatalf("shared container budget was not enforced: %v", err)
	}

	deep := any(map[string]any{"spec": map[string]any{"containers": rawContainers}})
	for index := 0; index < maxYAMLDepth/2; index++ {
		deep = map[string]any{"nested": deep}
	}
	containers, err := recursiveManifestContainers(deep, "$", 0, &yamlBudget{})
	if err != nil || len(containers) != 1 {
		t.Fatalf("deep container accumulator was not linear and complete: count=%d err=%v", len(containers), err)
	}
}

func TestVerifyTargetRenderRejectsWrapperFenceAndRepeatDrift(t *testing.T) {
	lock := testLock()
	for name, mutate := range map[string]func(map[string]any){
		"wrong-status":  func(value map[string]any) { value["info"].(map[string]any)["status"] = "pending-install" },
		"wrong-version": func(value map[string]any) { value["version"] = 9 },
		"wrong-name":    func(value map[string]any) { value["name"] = "other" },
		"unknown-root":  func(value map[string]any) { value["unexpected"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			first := helmFixture(baseManifest(lock))
			mutate(first)
			repeated := helmFixture(baseManifest(lock))
			if _, err := invokeTarget(t, lock, first, repeated); err == nil {
				t.Fatal("invalid Helm wrapper was accepted")
			}
		})
	}
	first := helmFixture(baseManifest(lock))
	repeated := helmFixture(baseManifest(lock) + "\n# drift\n")
	if _, err := invokeTarget(t, lock, first, repeated); err == nil || !strings.Contains(err.Error(), "different manifests") {
		t.Fatalf("repeat drift was not rejected: %v", err)
	}
	sameTimestampFirst := helmFixture(baseManifest(lock))
	sameTimestampRepeated := cloneJSONMap(t, sameTimestampFirst)
	if _, err := invokeTargetExact(t, lock, sameTimestampFirst, sameTimestampRepeated); err == nil || !strings.Contains(err.Error(), "distinct deployment timestamps") {
		t.Fatalf("copied dry-run timestamp was not rejected: %v", err)
	}
	applyFirst := helmFixture(baseManifest(lock))
	applyRepeated := helmFixture(baseManifest(lock))
	applyFirst["apply_method"] = "csa"
	applyRepeated["apply_method"] = "ssa"
	if _, err := invokeTarget(t, lock, applyFirst, applyRepeated); err == nil || !strings.Contains(err.Error(), "apply semantics") {
		t.Fatalf("apply-method drift was not rejected: %v", err)
	}
}

func TestDecodeHelmReleaseSupportsExactV3V4ApplyMethods(t *testing.T) {
	for name, method := range map[string]any{"v3-absent": "", "v4-csa": "csa", "v4-ssa": "ssa"} {
		t.Run(name, func(t *testing.T) {
			fixture := helmFixture(baseManifest(testLock()))
			if method != "" {
				fixture["apply_method"] = method
			}
			encoded, err := json.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeHelmRelease(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.ApplyMethod != method {
				t.Fatalf("unexpected apply method: %q", decoded.ApplyMethod)
			}
		})
	}
	for name, method := range map[string]any{"null": nil, "unknown": "future", "number": 1} {
		t.Run(name, func(t *testing.T) {
			fixture := helmFixture(baseManifest(testLock()))
			fixture["apply_method"] = method
			encoded, err := json.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeHelmRelease(encoded); err == nil {
				t.Fatal("invalid apply_method was accepted")
			}
		})
	}
}

func TestTargetEvidenceBindsHelmApplyMethod(t *testing.T) {
	lock := testLock()
	base := helmFixture(baseManifest(lock))
	evidenceByMethod := make(map[string]TargetEvidence)
	for _, method := range []string{"csa", "ssa"} {
		first := cloneJSONMap(t, base)
		repeated := cloneJSONMap(t, base)
		first["apply_method"] = method
		repeated["apply_method"] = method
		evidence, err := invokeTarget(t, lock, first, repeated)
		if err != nil {
			t.Fatalf("verify %s render: %v", method, err)
		}
		if evidence.ApplyMethod != method {
			t.Fatalf("evidence apply method=%q, want %q", evidence.ApplyMethod, method)
		}
		evidenceByMethod[method] = evidence
	}
	if evidenceByMethod["csa"].EvidenceDigest == evidenceByMethod["ssa"].EvidenceDigest {
		t.Fatal("CSA and SSA evidence digests are interchangeable")
	}
	invalid := evidenceByMethod["csa"]
	invalid.ApplyMethod = "unsafe"
	if err := VerifyTargetEvidenceDigest(invalid); err == nil {
		t.Fatal("invalid evidence apply method was accepted")
	}
}

func TestVerifyTargetRenderRejectsManagedHookConsumer(t *testing.T) {
	lock := testLock()
	helm := helmFixture(baseManifest(lock))
	helm["hooks"] = []any{map[string]any{
		"name": "managed-hook",
		"manifest": fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: managed-hook
spec:
  containers:
    - name: api
      image: %s
`, lock.Activations[0].SelectedRef),
	}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "Helm hook") {
		t.Fatalf("managed hook was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsNovelFugueHookWithoutManagedRepository(t *testing.T) {
	lock := testLock()
	helm := helmFixture(baseManifest(lock))
	helm["hooks"] = []any{map[string]any{
		"name": "novel-hook",
		"manifest": `apiVersion: batch/v1
kind: Job
metadata:
  name: novel-hook
spec:
  template:
    spec:
      containers:
        - name: novel
          image: registry.example.com/novel:stable
          command:
            - /usr/local/bin/fugue-novel
`,
	}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "Helm hook") {
		t.Fatalf("novel Fugue hook was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsCommentOnlyHook(t *testing.T) {
	lock := testLock()
	helm := helmFixture(baseManifest(lock))
	helm["hooks"] = []any{map[string]any{"name": "empty-hook", "manifest": "# hidden or empty\n"}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "contains no Kubernetes object") {
		t.Fatalf("comment-only hook was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderMapsNormalizedGroupIndexAndDNSContainer(t *testing.T) {
	lock := testLock()
	edgeRepository := "registry.example.com/fugue-edge"
	edgeDigest := testDigest("7")
	for _, activation := range []Activation{
		{
			Component: "edge", SourceMode: "preserve", SourceTemplateRef: edgeRepository + "@" + edgeDigest,
			SelectedRef: edgeRepository + "@" + edgeDigest, Repository: edgeRepository, SourceTag: "stable",
			Digest: edgeDigest, RuntimeManifestDigest: testDigest("8"), PinState: "pinned",
			HelmPath: "edge.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-edge", Container: "edge"},
		},
		{
			Component: "edge", SourceMode: "preserve", SourceTemplateRef: edgeRepository + "@" + edgeDigest,
			SelectedRef: edgeRepository + "@" + edgeDigest, Repository: edgeRepository, SourceTag: "stable",
			Digest: edgeDigest, RuntimeManifestDigest: testDigest("8"), PinState: "pinned",
			HelmPath: "edge.groups[0].image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-edge-country-de", Container: "edge"},
		},
		{
			Component: "edge", SourceMode: "preserve", SourceTemplateRef: edgeRepository + "@" + edgeDigest,
			SelectedRef: edgeRepository + "@" + edgeDigest, Repository: edgeRepository, SourceTag: "stable",
			Digest: edgeDigest, RuntimeManifestDigest: testDigest("8"), PinState: "pinned",
			HelmPath: "dns.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-dns", Container: "custom-dns"},
		},
		{
			Component: "edge", SourceMode: "preserve", SourceTemplateRef: edgeRepository + "@" + edgeDigest,
			SelectedRef: edgeRepository + "@" + edgeDigest, Repository: edgeRepository, SourceTag: "stable",
			Digest: edgeDigest, RuntimeManifestDigest: testDigest("8"), PinState: "pinned",
			HelmPath: "dns.groups[0].image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-dns-country-de", Container: "custom-dns"},
		},
	} {
		lock.Activations = append(lock.Activations, activation)
	}
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-edge
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge
          image: %s
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-edge-country-de
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge
          image: %s
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-dns
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: custom-dns
          image: %s
          command:
            - /usr/local/bin/fugue-dns
          env:
            - name: FUGUE_DNS_ZONE
              value: example.com
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-dns-country-de
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: custom-dns
          image: %s
          command:
            - /usr/local/bin/fugue-dns
          env:
            - name: FUGUE_DNS_ZONE
              value: example.com
`, edgeRepository+"@"+edgeDigest, edgeRepository+"@"+edgeDigest, edgeRepository+"@"+edgeDigest, edgeRepository+"@"+edgeDigest)
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{
		"edge": map[string]any{"enabled": true, "sshFront": map[string]any{"enabled": false}, "groups": []any{map[string]any{"name": "Country_DE"}}},
		"dns":  map[string]any{"enabled": true, "groups": []any{map[string]any{"name": "Country_DE"}}},
	}
	if _, err := invokeTarget(t, lock, helm, helm); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyTargetRenderRejectsNormalizedGroupCollision(t *testing.T) {
	lock := testLock()
	helm := helmFixture(baseManifest(lock))
	helm["config"] = map[string]any{"dns": map[string]any{"groups": []any{
		map[string]any{"name": "country_de"},
		map[string]any{"name": "country-de"},
	}}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "normalize to the same") {
		t.Fatalf("normalized group collision was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderEnforcesEnabledTopologyAndCompleteBlueGreenSet(t *testing.T) {
	lock := testLock()
	frontRef := "registry.example.com/fugue-edge@" + testDigest("7")
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-edge-front
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge-front
          image: %s
`, frontRef)
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"edge": map[string]any{
		"enabled":   true,
		"blueGreen": map[string]any{"enabled": true},
		"sshFront":  map[string]any{"enabled": false},
	}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "edge-worker-a") {
		t.Fatalf("partial blue/green topology was not rejected: %v", err)
	}

	for name, config := range map[string]map[string]any{
		"image-cache":      {"imageCache": map[string]any{"enabled": true}},
		"telemetry":        {"observability": map[string]any{"enabled": true, "agent": map[string]any{"enabled": true}}},
		"node-janitor":     {"nodeJanitor": map[string]any{"enabled": true}},
		"topology-labeler": {"topologyLabeler": map[string]any{"enabled": true}},
		"image-prepull":    {"imagePrePull": map[string]any{"enabled": true, "images": []any{"external.example/image:one"}}},
		"registry-gc":      {"registry": map[string]any{"enabled": true}, "registryGC": map[string]any{"enabled": true}},
		"registry-janitor": {"registry": map[string]any{"enabled": true}, "registryJanitor": map[string]any{"enabled": true}},
		"dns":              {"dns": map[string]any{"enabled": true}},
		"mesh":             {"meshRecovery": map[string]any{"enabled": true}},
	} {
		t.Run(name, func(t *testing.T) {
			wrapper := helmFixture(baseManifest(lock))
			wrapper["config"] = config
			if _, err := invokeTarget(t, lock, wrapper, wrapper); err == nil || !strings.Contains(err.Error(), "missing") {
				t.Fatalf("enabled-but-missing consumer was not rejected: %v", err)
			}
		})
	}
}

func TestTelemetryTopologyDependsOnlyOnAgentEnabled(t *testing.T) {
	lock := testLock()
	artifact := testArtifact("telemetry_agent", "registry.example.com/fugue-telemetry-agent", testDigest("7"), testDigest("8"))
	lock.Artifacts = append(lock.Artifacts, artifact)
	lock.Activations = append(lock.Activations, testBuiltActivation(artifact, "observability.agent.image", "Deployment", "fugue-fugue-telemetry-agent", "telemetry-agent"))
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-telemetry-agent
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: telemetry-agent
          image: %s
`, artifact.ImmutableRef)
	enabled := helmFixture(manifest)
	enabled["config"] = map[string]any{"observability": map[string]any{"enabled": false, "agent": map[string]any{"enabled": true}}}
	if _, err := invokeTarget(t, lock, enabled, enabled); err != nil {
		t.Fatalf("observability.enabled=false, agent.enabled=true was rejected: %v", err)
	}
	disabled := helmFixture(manifest)
	disabled["config"] = map[string]any{"observability": map[string]any{"enabled": true, "agent": map[string]any{"enabled": false}}}
	if _, err := invokeTarget(t, lock, disabled, disabled); err == nil || !strings.Contains(err.Error(), "disabled consumer") {
		t.Fatalf("agent.enabled=false rendered telemetry was not rejected: %v", err)
	}
}

func TestTelemetryTopologyBooleanMatrix(t *testing.T) {
	defaults := helmFixture("")["chart"].(map[string]any)["values"].(map[string]any)
	for _, observabilityEnabled := range []bool{false, true} {
		for _, agentEnabled := range []bool{false, true} {
			name := fmt.Sprintf("observability-%t-agent-%t", observabilityEnabled, agentEnabled)
			t.Run(name, func(t *testing.T) {
				config := map[string]any{"observability": map[string]any{"enabled": observabilityEnabled, "agent": map[string]any{"enabled": agentEnabled}}}
				topology, err := effectiveReleaseTopology(defaults, config)
				if err != nil {
					t.Fatal(err)
				}
				if topology.TelemetryAgent != agentEnabled {
					t.Fatalf("TelemetryAgent=%t, want %t", topology.TelemetryAgent, agentEnabled)
				}
			})
		}
	}
}

func TestVerifyTargetRenderRejectsDefaultEntrypointOverrides(t *testing.T) {
	lock := testLock()
	apiImage := lock.Activations[0].SelectedRef
	for name, override := range map[string]string{
		"command": "          command:\n            - /bin/false\n",
		"args":    "          args:\n            - unsafe\n",
	} {
		t.Run("api-"+name, func(t *testing.T) {
			manifest := strings.Replace(baseManifest(lock), "          image: "+apiImage+"\n", "          image: "+apiImage+"\n"+override, 1)
			helm := helmFixture(manifest)
			if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "authoritative image entrypoint") {
				t.Fatalf("API default entrypoint override was not rejected: %v", err)
			}
		})
	}

	edge := testArtifact("edge", "registry.example.com/fugue-edge", testDigest("7"), testDigest("8"))
	lock.Artifacts = append(lock.Artifacts, edge)
	lock.Activations = append(lock.Activations, testBuiltActivation(edge, "edge.image", "DaemonSet", "fugue-fugue-edge", "edge"))
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-edge
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge
          image: %s
          args:
            - unsafe
`, edge.ImmutableRef)
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"edge": map[string]any{"enabled": true, "sshFront": map[string]any{"enabled": false}}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "authoritative image entrypoint") {
		t.Fatalf("edge worker default entrypoint override was not rejected: %v", err)
	}
}

func TestExactFugueExecutableConsumersRejectArguments(t *testing.T) {
	for name, test := range map[string]struct {
		command   string
		helmPath  string
		container string
		workload  string
	}{
		"dns":           {command: "/usr/local/bin/fugue-dns", helmPath: "dns.image", container: "dns", workload: "fugue-fugue-dns"},
		"edge-front":    {command: "/usr/local/bin/fugue-edge-front", helmPath: "edge.blueGreen.front.image", container: "edge-front", workload: "fugue-fugue-edge-front"},
		"ssh-front":     {command: "/usr/local/bin/fugue-ssh-front", helmPath: "edge.sshFront.image", container: "ssh-front", workload: "fugue-fugue-edge-ssh-front"},
		"mesh-recovery": {command: "/usr/local/bin/fugue-mesh-recovery", helmPath: "meshRecovery.image", container: "mesh-recovery", workload: "fugue-fugue-mesh-recovery"},
	} {
		t.Run(name, func(t *testing.T) {
			container := manifestContainer{Name: test.container, Command: []string{test.command}, Arguments: []string{"--help"}}
			activation := Activation{HelmPath: test.helmPath, Workload: Workload{Kind: "DaemonSet", Name: test.workload, Container: test.container}}
			if _, err := expectedFugueExecutables(container, activation); err == nil || !strings.Contains(err.Error(), "authoritative executable") {
				t.Fatalf("exact Fugue executable arguments were accepted: %v", err)
			}
		})
	}
}

func TestScriptConsumersRequireAuthoritativeShellCarrier(t *testing.T) {
	for name, activation := range map[string]Activation{
		"registry-gc":      {HelmPath: "controller.image", Workload: Workload{Kind: "CronJob", Name: "fugue-fugue-registry-gc", Container: "registry-gc"}},
		"registry-janitor": {HelmPath: "controller.image", Workload: Workload{Kind: "CronJob", Name: "fugue-fugue-registry-janitor", Container: "registry-janitor"}},
		"node-janitor":     {HelmPath: "nodeJanitor.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-node-janitor", Container: "node-janitor"}},
		"topology-labeler": {HelmPath: "topologyLabeler.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-topology-labeler", Container: "topology-labeler"}},
		"image-prepull":    {HelmPath: "imagePrePull.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-image-prepull", Container: "image-prepull"}},
	} {
		t.Run(name, func(t *testing.T) {
			container := manifestContainer{Name: activation.Workload.Container, Command: []string{"/bin/true"}}
			if _, err := expectedFugueExecutables(container, activation); err == nil || !strings.Contains(err.Error(), "authoritative shell carrier") {
				t.Fatalf("non-authoritative script carrier was accepted: %v", err)
			}
		})
	}
}

func TestVerifyTargetRenderAllowsDNSContainerNamedEdge(t *testing.T) {
	lock := testLock()
	edge := testArtifact("edge", "registry.example.com/fugue-edge", testDigest("7"), testDigest("8"))
	lock.Artifacts = append(lock.Artifacts, edge)
	lock.Activations = append(lock.Activations, testBuiltActivation(edge, "dns.image", "DaemonSet", "fugue-fugue-dns", "edge"))
	manifest := baseManifest(lock) + fmt.Sprintf(`---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-dns
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: edge
          image: %s
          command:
            - /usr/local/bin/fugue-dns
          env:
            - name: FUGUE_DNS_ZONE
              value: example.com
`, edge.ImmutableRef)
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"dns": map[string]any{"enabled": true, "containerName": "edge", "zone": "example.com"}}
	if _, err := invokeTarget(t, lock, helm, helm); err != nil {
		t.Fatalf("valid DNS semantic container name edge was rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsDNSGroupRenderedWhileDNSDisabled(t *testing.T) {
	lock := testLock()
	manifest := baseManifest(lock) + `---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fugue-fugue-dns-country-de
  namespace: fugue-system
spec:
  template:
    spec:
      containers:
        - name: custom-dns
          image: registry.example.com/legacy-dns:stable
          command:
            - /usr/local/bin/fugue-dns
          env:
            - name: FUGUE_DNS_ZONE
              value: example.com
`
	helm := helmFixture(manifest)
	helm["config"] = map[string]any{"dns": map[string]any{
		"enabled": false,
		"groups":  []any{map[string]any{"name": "country_de"}},
	}}
	if _, err := invokeTarget(t, lock, helm, helm); err == nil || !strings.Contains(err.Error(), "disabled DNS consumer") {
		t.Fatalf("known dns.enabled=false group rendering bug was not rejected: %v", err)
	}
}

func TestVerifyTargetRenderRejectsTamperedOrNonCanonicalLock(t *testing.T) {
	lock := testLock()
	helm := helmFixture(baseManifest(lock))
	firstBytes, _ := json.Marshal(helm)
	firstPath := writeFixture(t, "first.json", firstBytes)
	repeatPath := writeFixture(t, "repeat.json", firstBytes)
	for name, lockBytes := range map[string][]byte{
		"pretty":    bytes.ReplaceAll(canonicalLockFixture(t, lock), []byte(","), []byte(", ")),
		"duplicate": []byte(`{"schema_version":1,"schema_version":1}`),
	} {
		t.Run(name, func(t *testing.T) {
			lockPath := writeFixture(t, "lock.json", lockBytes)
			if _, err := VerifyTargetRender(lockPath, firstPath, repeatPath, TargetExpectations{
				LockDigest: testDigest("f"), HeadSHA: testHeadSHA, Repository: "acme/fugue", RunID: "123", RunAttempt: "1",
				ReleaseFullname: "fugue-fugue", ReleaseName: "fugue", Namespace: "fugue-system", LiveRevision: 7,
			}); err == nil {
				t.Fatal("invalid lock was accepted")
			}
		})
	}
}

func TestStrictJSONRejectsDuplicateTrailingAndUnpairedSurrogates(t *testing.T) {
	for name, input := range map[string]string{
		"duplicate":      `{"key":1,"key":2}`,
		"trailing":       `{} {}`,
		"high-surrogate": `{"key":"\ud800"}`,
		"low-surrogate":  `{"key":"\udc00"}`,
		"bad-escape":     `{"key":"\x20"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := scanStrictJSON([]byte(input)); err == nil {
				t.Fatal("unsafe JSON was accepted")
			}
		})
	}
	if err := scanStrictJSON([]byte(`{"key":"\ud83d\ude00"}`)); err != nil {
		t.Fatalf("valid surrogate pair was rejected: %v", err)
	}
}

func TestLockRevalidationRejectsMissingNullUnsortedAndConflictingFields(t *testing.T) {
	base := testLock()
	mutations := map[string]func(map[string]any){
		"null-artifacts": func(document map[string]any) { document["artifacts"] = nil },
		"missing-migration-bool": func(document map[string]any) {
			activation := document["activations"].([]any)[0].(map[string]any)
			delete(activation, "migration_allowed")
		},
		"null-digest": func(document map[string]any) {
			activation := document["activations"].([]any)[0].(map[string]any)
			activation["digest"] = nil
		},
		"unsorted-artifacts": func(document map[string]any) {
			artifacts := document["artifacts"].([]any)
			artifacts[0], artifacts[1] = artifacts[1], artifacts[0]
		},
		"unsorted-activations": func(document map[string]any) {
			activations := document["activations"].([]any)
			activations[0], activations[1] = activations[1], activations[0]
		},
		"unsafe-helm-path": func(document map[string]any) {
			activation := document["activations"].([]any)[0].(map[string]any)
			activation["helm_path"] = "api.image;touch"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			document := lockDocumentFixture(t, base)
			mutate(document)
			if _, err := decodeAndValidateLock(rehashedLockDocument(t, document)); err == nil {
				t.Fatal("malformed but self-consistent lock was accepted")
			}
		})
	}

	conflict := testLock()
	controller := conflict.Artifacts[1]
	cron := testBuiltActivation(controller, "controller.image", "CronJob", "fugue-fugue-registry-gc", "registry-gc")
	cron.SourceMode = "preserve"
	conflict.Activations = append(conflict.Activations, cron)
	if _, err := decodeAndValidateLock(canonicalLockFixture(t, conflict)); err == nil || !strings.Contains(err.Error(), "one Helm image path") {
		t.Fatalf("same Helm path conflicting identity was not rejected: %v", err)
	}

	for _, repository := range []string{"registry.example.com:00001/repo", "registry.example.com:+1/repo"} {
		if err := validateRepository(repository); err == nil {
			t.Fatalf("builder-invalid registry port was accepted: %s", repository)
		}
	}
}

func TestTargetRenderRequiresExternallyFencedLockIdentity(t *testing.T) {
	lock := testLock()
	lockBytes := canonicalLockFixture(t, lock)
	decoded, err := decodeAndValidateLock(lockBytes)
	if err != nil {
		t.Fatal(err)
	}
	helm := helmFixture(baseManifest(lock))
	helmBytes, _ := json.Marshal(helm)
	lockPath := writeFixture(t, "lock.json", lockBytes)
	firstPath := writeFixture(t, "first.json", helmBytes)
	repeatedPath := writeFixture(t, "repeat.json", helmBytes)
	expected := TargetExpectations{
		LockDigest: decoded.LockDigest, HeadSHA: testHeadSHA, Repository: "acme/fugue", RunID: "123", RunAttempt: "1",
		ReleaseFullname: "fugue-fugue", ReleaseName: "fugue", Namespace: "fugue-system", LiveRevision: 7,
	}
	for name, mutate := range map[string]func(*TargetExpectations){
		"digest":     func(value *TargetExpectations) { value.LockDigest = testDigest("9") },
		"head":       func(value *TargetExpectations) { value.HeadSHA = strings.Repeat("b", 40) },
		"repository": func(value *TargetExpectations) { value.Repository = "other/fugue" },
		"run":        func(value *TargetExpectations) { value.RunID = "999" },
		"attempt":    func(value *TargetExpectations) { value.RunAttempt = "2" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := expected
			mutate(&candidate)
			if _, err := VerifyTargetRender(lockPath, firstPath, repeatedPath, candidate); err == nil || !strings.Contains(err.Error(), "externally fenced") {
				t.Fatalf("mismatched external fence was accepted: %v", err)
			}
		})
	}
}

func TestSecureInputsRejectLooseModeAndSymlink(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "input.json")
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readLimitedFile(path, 1024, "input"); err == nil {
		t.Fatal("loosely permissioned input was accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(directory, "link.json")
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := readLimitedFile(symlink, 1024, "input"); err == nil {
		t.Fatal("symlink input was accepted")
	}
	hardlink := filepath.Join(directory, "hardlink.json")
	if err := os.Link(path, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := readLimitedFile(path, 1024, "input"); err == nil {
		t.Fatal("multiply linked input was accepted")
	}
	fifo := filepath.Join(directory, "input.fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readLimitedFile(fifo, 1024, "input"); err == nil {
		t.Fatal("FIFO input was accepted")
	}
}

func preserveLockForRenderedConsumers(t *testing.T, expected map[string]expectedConsumer) Lock {
	t.Helper()
	lock := Lock{
		SchemaVersion: 1,
		Producer:      lockProducer,
		Artifacts:     []Artifact{},
		Activations:   []Activation{},
		Release: LockRelease{
			Repository: "acme/fugue", Workflow: releaseWorkflow, RunID: "123", RunAttempt: "1", HeadSHA: testHeadSHA, Platform: releasePlatform,
		},
	}
	for _, consumer := range expected {
		parsed, err := parseImageRef(consumer.SelectedRef)
		if err != nil {
			t.Fatalf("parse current chart consumer %s: %v", consumer.HelmPath, err)
		}
		sourceTag := parsed.Tag
		if sourceTag == "" {
			sourceTag = "rendered"
		}
		activation := Activation{
			Component: consumer.Component, SourceMode: "preserve", SourceTemplateRef: consumer.SelectedRef,
			SelectedRef: consumer.SelectedRef, Repository: parsed.Repository, SourceTag: sourceTag,
			HelmPath: consumer.HelmPath, Workload: consumer.Workload,
		}
		if parsed.Digest == "" {
			activation.PinState = "legacy_unpinned"
			activation.MigrationAllowed = true
		} else {
			activation.Digest = parsed.Digest
			activation.RuntimeManifestDigest = parsed.Digest
			activation.PinState = "pinned"
		}
		lock.Activations = append(lock.Activations, activation)
	}
	return lock
}

func targetWrapperForHelmOutput(t *testing.T, output []byte) map[string]any {
	t.Helper()
	var wrapper map[string]any
	if err := json.Unmarshal(output, &wrapper); err != nil {
		t.Fatal(err)
	}
	wrapper["version"] = 8
	info := wrapper["info"].(map[string]any)
	info["status"] = "pending-upgrade"
	info["first_deployed"] = "2026-01-02T03:04:05Z"
	info["last_deployed"] = "2026-01-02T03:04:06Z"
	return wrapper
}

func TestCurrentChartDryRunIsStrictlyDecodable(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "fugue")
	command := exec.Command(helm, "install", "fugue", chart, "--namespace", "fugue-system", "--dry-run=client", "--hide-secret", "--output", "json")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render current chart: %v\n%s", err, output)
	}
	if len(output) > maxHelmReleaseBytes {
		t.Fatalf("current Helm output exceeds verifier limit: %d", len(output))
	}
	release, err := decodeHelmRelease(output)
	if err != nil {
		t.Fatalf("decode current chart dry run: %v", err)
	}
	objects, err := decodeManifest([]byte(release.Manifest), release.Namespace)
	if err != nil {
		t.Fatalf("decode current chart manifest: %v", err)
	}
	if len(objects) < 20 {
		t.Fatalf("current chart unexpectedly rendered only %d objects", len(objects))
	}
	byKey := make(map[objectKey]manifestObject, len(objects))
	for _, object := range objects {
		key := objectKey{Kind: object.Kind, Namespace: object.Namespace, Name: object.Name}
		if _, duplicate := byKey[key]; duplicate {
			t.Fatalf("current chart has duplicate object %#v", key)
		}
		byKey[key] = object
	}
	expected, err := buildExpectedConsumers(byKey, release.ComputedFullname, release.Namespace, release.Groups, release.Topology)
	if err != nil {
		t.Fatalf("inventory current chart consumers: %v", err)
	}
	for _, projection := range []string{
		consumerProjectionKey("api", "api.image", Workload{Kind: "Deployment", Name: "fugue-fugue-api", Container: "api"}),
		consumerProjectionKey("controller", "controller.image", Workload{Kind: "Deployment", Name: "fugue-fugue-controller", Container: "controller"}),
		consumerProjectionKey("drain_agent", "runtime.strictDrain.agent.image", Workload{Kind: "Configuration", Name: "fugue-fugue-controller", Container: "controller"}),
	} {
		if _, exists := expected[projection]; !exists {
			t.Fatalf("current chart is missing required consumer projection %q", projection)
		}
	}
	for index, hook := range release.HookManifests {
		if _, err := decodeManifest([]byte(hook), release.Namespace); err != nil {
			t.Fatalf("decode current chart hook %d: %v", index, err)
		}
	}

	lock := Lock{
		SchemaVersion: 1,
		Producer:      lockProducer,
		Artifacts:     []Artifact{},
		Activations:   []Activation{},
		Release: LockRelease{
			Repository: "acme/fugue", Workflow: releaseWorkflow, RunID: "123", RunAttempt: "1", HeadSHA: testHeadSHA, Platform: releasePlatform,
		},
	}
	for _, consumer := range expected {
		parsed, err := parseImageRef(consumer.SelectedRef)
		if err != nil {
			t.Fatalf("parse current chart consumer %s: %v", consumer.HelmPath, err)
		}
		sourceTag := parsed.Tag
		if sourceTag == "" {
			sourceTag = "rendered"
		}
		activation := Activation{
			Component: consumer.Component, SourceMode: "preserve", SourceTemplateRef: consumer.SelectedRef,
			SelectedRef: consumer.SelectedRef, Repository: parsed.Repository, SourceTag: sourceTag,
			HelmPath: consumer.HelmPath, Workload: consumer.Workload,
		}
		if parsed.Digest == "" {
			activation.PinState = "legacy_unpinned"
			activation.MigrationAllowed = true
		} else {
			activation.Digest = parsed.Digest
			activation.RuntimeManifestDigest = parsed.Digest
			activation.PinState = "pinned"
		}
		lock.Activations = append(lock.Activations, activation)
	}
	var wrapper map[string]any
	if err := json.Unmarshal(output, &wrapper); err != nil {
		t.Fatal(err)
	}
	wrapper["version"] = 8
	info := wrapper["info"].(map[string]any)
	info["status"] = "pending-upgrade"
	info["first_deployed"] = "2026-01-02T03:04:05Z"
	info["last_deployed"] = "2026-01-02T03:04:06Z"
	evidence, err := invokeTarget(t, lock, wrapper, wrapper)
	if err != nil {
		t.Fatalf("full current-chart consumer verification: %v", err)
	}
	if len(evidence.Bindings) != len(expected) {
		t.Fatalf("current-chart binding count=%d, expected=%d", len(evidence.Bindings), len(expected))
	}
	extraMaintenance := cloneJSONMap(t, wrapper)
	renderedManifest := extraMaintenance["manifest"].(string)
	renderedManifest = strings.Replace(renderedManifest,
		"fugue-registry-maintenance active-imports --format count",
		"fugue-registry-maintenance active-imports --format count\n                  fugue-registry-maintenance active-imports --format count", 1)
	extraMaintenance["manifest"] = renderedManifest
	if _, err := invokeTarget(t, lock, extraMaintenance, extraMaintenance); err == nil || !strings.Contains(err.Error(), "executable context") {
		t.Fatalf("registry GC executable multiset growth was not rejected: %v", err)
	}
	relocatedLease := cloneJSONMap(t, wrapper)
	relocatedManifest := relocatedLease["manifest"].(string)
	relocatedManifest = strings.Replace(relocatedManifest,
		`gc_lease="fugue-fugue-registry-gc"`,
		"gc_lease=\"\"\n                  fugue-fugue-registry-gc", 1)
	if relocatedManifest == relocatedLease["manifest"].(string) {
		t.Fatal("current chart registry lease assignment fixture was not found")
	}
	relocatedLease["manifest"] = relocatedManifest
	if _, err := invokeTarget(t, lock, relocatedLease, relocatedLease); err == nil || !strings.Contains(err.Error(), "executable context") {
		t.Fatalf("registry GC benign token relocation was not rejected: %v", err)
	}
	quotedLease := cloneJSONMap(t, wrapper)
	quotedManifest := quotedLease["manifest"].(string)
	quotedManifest = strings.Replace(quotedManifest,
		`gc_lease="fugue-fugue-registry-gc"`,
		"gc_lease=\"\"\n                  ignored='\n                  gc_lease=\"fugue-fugue-registry-gc\"\n                  '", 1)
	if quotedManifest == quotedLease["manifest"].(string) {
		t.Fatal("current chart quoted registry lease assignment fixture was not found")
	}
	quotedLease["manifest"] = quotedManifest
	if _, err := invokeTarget(t, lock, quotedLease, quotedLease); err == nil || !strings.Contains(err.Error(), "executable context") {
		t.Fatalf("registry GC token relocated into a multiline quote was not rejected: %v", err)
	}
	deadBranchLease := cloneJSONMap(t, wrapper)
	deadBranchManifest := deadBranchLease["manifest"].(string)
	deadBranchManifest = strings.Replace(deadBranchManifest,
		`gc_lease="fugue-fugue-registry-gc"`,
		"if false; then\n                  gc_lease=\"fugue-fugue-registry-gc\"\n                  fi", 1)
	if deadBranchManifest == deadBranchLease["manifest"].(string) {
		t.Fatal("current chart dead-branch registry lease fixture was not found")
	}
	deadBranchLease["manifest"] = deadBranchManifest
	if _, err := invokeTarget(t, lock, deadBranchLease, deadBranchLease); err == nil || !strings.Contains(err.Error(), "executable context") {
		t.Fatalf("registry GC authoritative prefix moved into a dead branch was not rejected: %v", err)
	}
}

func TestComplexCurrentChartDryRunPassesFullConsumerVerification(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "fugue")
	command := exec.Command(helm, "install", "fugue", chart,
		"--namespace", "fugue-system", "--dry-run=client", "--hide-secret", "--output", "json",
		"--set", "edge.blueGreen.enabled=true",
		"--set", "edge.dynamic.enabled=true",
		"--set", "edge.caddy.enabled=true",
		"--set", "edge.edgeGroupID=default",
		"--set", "observability.agent.enabled=true",
		"--set", "dns.enabled=true",
		"--set", "dns.zone=example.test",
		"--set-json", `dns.answerIPs=["192.0.2.10"]`,
		"--set-json", `edge.groups=[{"name":"country-de","edgeGroupID":"country-de","nodeSelector":{"kubernetes.io/hostname":"edge-de"}}]`,
		"--set-json", `dns.groups=[{"name":"country-de","edgeGroupID":"country-de","nodeSelector":{"kubernetes.io/hostname":"dns-de"},"answerIPs":["192.0.2.11"]}]`,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render complex current chart: %v\n%s", err, output)
	}
	release, err := decodeHelmRelease(output)
	if err != nil {
		t.Fatalf("decode complex current chart: %v", err)
	}
	objects, err := decodeManifest([]byte(release.Manifest), release.Namespace)
	if err != nil {
		t.Fatalf("decode complex current chart manifest: %v", err)
	}
	byKey := make(map[objectKey]manifestObject, len(objects))
	for _, object := range objects {
		key := objectKey{Kind: object.Kind, Namespace: object.Namespace, Name: object.Name}
		if _, duplicate := byKey[key]; duplicate {
			t.Fatalf("complex current chart has duplicate object %#v", key)
		}
		byKey[key] = object
	}
	expected, err := buildExpectedConsumers(byKey, release.ComputedFullname, release.Namespace, release.Groups, release.Topology)
	if err != nil {
		t.Fatalf("inventory complex current chart consumers: %v", err)
	}
	if len(expected) < 20 {
		t.Fatalf("complex current chart unexpectedly has only %d consumers", len(expected))
	}
	lock := preserveLockForRenderedConsumers(t, expected)
	wrapper := targetWrapperForHelmOutput(t, output)
	evidence, err := invokeTarget(t, lock, wrapper, wrapper)
	if err != nil {
		t.Fatalf("full complex current-chart consumer verification: %v", err)
	}
	if len(evidence.Bindings) != len(expected) {
		t.Fatalf("complex current-chart binding count=%d, expected=%d", len(evidence.Bindings), len(expected))
	}
}

func TestVerifierAcceptsCanonicalPythonLockBuilderOutput(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	base := testLock()
	preserve := testLock()
	preserve.Activations = append(preserve.Activations, Activation{
		Component: "edge", SourceMode: "preserve",
		SourceTemplateRef: "registry.example.com/fugue-edge:stable@" + testDigest("7"),
		SelectedRef:       "registry.example.com/fugue-edge:stable@" + testDigest("7"),
		Repository:        "registry.example.com/fugue-edge", SourceTag: "stable", Digest: testDigest("7"), RuntimeManifestDigest: testDigest("8"),
		PinState: "pinned", HelmPath: "edge.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-edge", Container: "edge"},
	})
	migration := testLock()
	migration.Activations = append(migration.Activations, Activation{
		Component: "edge", SourceMode: "migration",
		SourceTemplateRef: "registry.example.com/fugue-edge:legacy",
		SelectedRef:       "registry.example.com/fugue-edge:legacy@" + testDigest("9"),
		Repository:        "registry.example.com/fugue-edge", SourceTag: "legacy", Digest: testDigest("9"), RuntimeManifestDigest: testDigest("a"),
		PinState: "pinned", MigrationAllowed: true, HelmPath: "edge.image", Workload: Workload{Kind: "DaemonSet", Name: "fugue-fugue-edge", Container: "edge"},
	})
	for name, lock := range map[string]Lock{"built": base, "preserve-tag-digest": preserve, "migration-tag-digest": migration} {
		t.Run(name, func(t *testing.T) {
			input := map[string]any{
				"schema_version": lock.SchemaVersion,
				"release":        lock.Release,
				"artifacts":      lock.Artifacts,
				"activations":    lock.Activations,
			}
			inputBytes, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(directory, "input.json")
			outputPath := filepath.Join(directory, "release-image-lock.json")
			if err := os.WriteFile(inputPath, inputBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			builder := filepath.Join("..", "..", "scripts", "build_release_image_lock.py")
			command := exec.Command(python, builder, "--input", inputPath, "--output", outputPath)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("build canonical Python lock: %v\n%s", err, output)
			}
			output, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeAndValidateLock(output)
			if err != nil {
				t.Fatalf("Go verifier rejected Python builder lock: %v", err)
			}
			if decoded.LockDigest == "" || decoded.Release.HeadSHA != testHeadSHA {
				t.Fatalf("unexpected decoded lock: %#v", decoded)
			}
		})
	}
}
