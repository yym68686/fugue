package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"syscall"

	"fugue/internal/releasedomain"
	"gopkg.in/yaml.v3"
)

const (
	releasePreflightFileLimit  int64 = 8 << 20
	releasePreflightTotalLimit int64 = 16 << 20

	releasePreflightArgumentsError = "fugue-release-domain-evidence release-preflight: invalid arguments"
	releasePreflightInputError     = "fugue-release-domain-evidence release-preflight: snapshot verification failed"
	releasePreflightOutputError    = "fugue-release-domain-evidence release-preflight: receipt output failed"
)

var (
	releasePreflightCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	releasePreflightDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	releasePreflightSelectorField = regexp.MustCompile(`^metadata\.(labels|annotations)\['[A-Za-z0-9]([A-Za-z0-9._/-]{0,251}[A-Za-z0-9])?'\]$`)
	releasePreflightBundleVersion = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	releasePreflightRouteGen      = regexp.MustCompile(`^routegen_[0-9a-z]+$`)
	releasePreflightPodUID        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

type releasePreflightOptions struct {
	snapshotDir string
	outputPath  string
	tmpDir      string
}

type releasePreflightSnapshot struct {
	APIVersion              string `json:"apiVersion"`
	Kind                    string `json:"kind"`
	BuildReceiptDigest      string `json:"buildReceiptDigest"`
	HelmRevision            uint64 `json:"helmRevision"`
	Namespace               string `json:"namespace"`
	OperationalReportDigest string `json:"operationalReportDigest"`
	PlanDigest              string `json:"planDigest"`
	ReleaseName             string `json:"releaseName"`
	RollbackTargetRevision  uint64 `json:"rollbackTargetRevision"`
	TargetRevision          uint64 `json:"targetRevision"`
	TrustedBaseCommit       string `json:"trustedBaseCommit"`
	TrustedTargetCommit     string `json:"trustedTargetCommit"`
}

type releasePreflightReceipt struct {
	APIVersion              string   `json:"apiVersion"`
	BuildReceiptDigest      string   `json:"buildReceiptDigest"`
	CandidateReadbackDigest string   `json:"candidateReadbackDigest"`
	Checks                  []string `json:"checks"`
	HelmManifestDigest      string   `json:"helmManifestDigest"`
	HelmValuesDigest        string   `json:"helmValuesDigest"`
	Kind                    string   `json:"kind"`
	LiveSnapshotDigest      string   `json:"liveSnapshotDigest"`
	Namespace               string   `json:"namespace"`
	OperationalReportDigest string   `json:"operationalReportDigest"`
	ReleaseName             string   `json:"releaseName"`
	RollbackTargetRevision  uint64   `json:"rollbackTargetRevision"`
	SnapshotDigest          string   `json:"snapshotDigest"`
	Status                  string   `json:"status"`
	TargetCommit            string   `json:"targetCommit"`
	TargetRevision          uint64   `json:"targetRevision"`
}

type releasePreflightCandidateBundleWindow struct {
	AdvanceCount    uint64                               `json:"advanceCount"`
	APIVersion      string                               `json:"apiVersion"`
	IntervalSeconds uint64                               `json:"intervalSeconds"`
	Kind            string                               `json:"kind"`
	Observations    []releasePreflightCandidateBundleAck `json:"observations"`
	PostReadback    releasePreflightCandidateBundleAck   `json:"postReadback"`
	WindowSeconds   uint64                               `json:"windowSeconds"`
}

type releasePreflightCandidateBundleAck struct {
	BundleDigest       string `json:"bundleDigest"`
	BundleSelection    bool   `json:"bundleSelection"`
	BundleVersion      string `json:"bundleVersion"`
	ConsecutiveHealthy uint64 `json:"consecutiveHealthy"`
	DirectTLS          bool   `json:"directTLS"`
	DurableHealthy     bool   `json:"durableHealthy"`
	ElapsedSeconds     uint64 `json:"elapsedSeconds"`
	LocalAsk           bool   `json:"localAsk"`
	PodUID             string `json:"podUID"`
	ReleaseEpoch       string `json:"releaseEpoch"`
	RouteGeneration    string `json:"routeGeneration"`
	State              string `json:"state"`
}

type releasePreflightBuildReceipt struct {
	Component              string `json:"component"`
	ConfigDigest           string `json:"config_digest"`
	ImmutableRef           string `json:"immutable_ref"`
	OCIRevision            string `json:"oci_revision"`
	PlatformManifestDigest string `json:"platform_manifest_digest"`
	Repository             string `json:"repository"`
	SourceTag              string `json:"source_tag"`
	TopDigest              string `json:"top_digest"`
	Verification           string `json:"verification"`
}

type releasePreflightEvidence struct {
	buildPlan         releasedomain.BuildArtifactPlan
	activationPlan    releasedomain.ImageActivationPlan
	activation        releasedomain.ImageActivationEvidence
	decomposition     releasedomain.CompositeDecompositionEvidence
	immutableTarget   []byte
	observedLive      []byte
	operationalReport releasedomain.OperationalDomainEvidence
}

func runReleasePreflight(args []string, stdout, stderr io.Writer) int {
	options, err := parseReleasePreflightFlags(args)
	if err != nil {
		fmt.Fprintln(stderr, releasePreflightArgumentsError)
		return 1
	}
	receipt, err := verifyReleasePreflightSnapshot(options)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %s\n", releasePreflightInputError, err)
		return 1
	}
	encoded, err := marshalReleasePreflightReceipt(receipt)
	if err != nil {
		fmt.Fprintln(stderr, releasePreflightOutputError)
		return 1
	}
	if options.outputPath == "-" {
		if _, err := stdout.Write(encoded); err != nil {
			fmt.Fprintln(stderr, releasePreflightOutputError)
			return 1
		}
		return 0
	}
	protected, err := releasePreflightProtectedPaths(options.snapshotDir, options.outputPath)
	if err != nil {
		fmt.Fprintln(stderr, releasePreflightOutputError)
		return 1
	}
	if err := writePrivateAtomicFile(options.outputPath, encoded, protected...); err != nil {
		fmt.Fprintln(stderr, releasePreflightOutputError)
		return 1
	}
	return 0
}

func releasePreflightProtectedPaths(snapshotDir, outputPath string) ([]string, error) {
	root, err := filepath.Abs(snapshotDir)
	if err != nil {
		return nil, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	output, err := filepath.Abs(outputPath)
	if err != nil {
		return nil, err
	}
	resolvedOutputParent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return nil, err
	}
	resolvedOutput := filepath.Join(resolvedOutputParent, filepath.Base(output))
	relative, err := filepath.Rel(root, resolvedOutput)
	if err != nil || relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("receipt output must be outside the read-only snapshot")
	}
	paths := []string{
		"snapshot.json", "helm-release.json", "live-workloads.json", "build-receipt.json", "candidate-bundle-readback.json",
		"ownership-v1.yaml", "target-manifest.yaml", "operational-domain-evidence.json",
		filepath.Join("release-evidence", "build-artifact-plan.json"),
		filepath.Join("release-evidence", "composite-decomposition-evidence.json"),
		filepath.Join("release-evidence", "image-activation-evidence.json"),
		filepath.Join("release-evidence", "image-activation-plan.json"),
		filepath.Join("release-evidence", "immutable-target-manifest.yaml"),
		filepath.Join("release-evidence", "observed-live-manifest.yaml"),
	}
	for index := range paths {
		paths[index] = filepath.Join(root, paths[index])
	}
	return paths, nil
}

func parseReleasePreflightFlags(args []string) (releasePreflightOptions, error) {
	options := releasePreflightOptions{outputPath: "-", tmpDir: os.TempDir()}
	flags := flag.NewFlagSet("release-preflight", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.snapshotDir, "snapshot", "", "read-only release preflight snapshot directory")
	flags.StringVar(&options.outputPath, "output", options.outputPath, "canonical receipt path or -")
	seen := map[string]struct{}{}
	for _, argument := range args {
		if !strings.HasPrefix(argument, "--") {
			continue
		}
		name := strings.TrimPrefix(argument, "--")
		if before, _, ok := strings.Cut(name, "="); ok {
			name = before
		}
		if name != "snapshot" && name != "output" {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return releasePreflightOptions{}, fmt.Errorf("duplicate --%s", name)
		}
		seen[name] = struct{}{}
	}
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return releasePreflightOptions{}, fmt.Errorf("invalid flags")
	}
	if strings.TrimSpace(options.snapshotDir) == "" || strings.TrimSpace(options.snapshotDir) != options.snapshotDir {
		return releasePreflightOptions{}, fmt.Errorf("--snapshot is required without surrounding whitespace")
	}
	if strings.TrimSpace(options.outputPath) == "" || strings.TrimSpace(options.outputPath) != options.outputPath {
		return releasePreflightOptions{}, fmt.Errorf("--output is invalid")
	}
	return options, nil
}

func verifyReleasePreflightSnapshot(options releasePreflightOptions) (releasePreflightReceipt, error) {
	root, err := validateReleasePreflightDirectory(options.snapshotDir, false)
	if err != nil {
		return releasePreflightReceipt{}, fmt.Errorf("snapshot directory: %w", err)
	}
	wantedRoot := []string{
		"build-receipt.json", "candidate-bundle-readback.json", "helm-release.json", "live-workloads.json",
		"operational-domain-evidence.json", "ownership-v1.yaml", "release-evidence",
		"snapshot.json", "target-manifest.yaml",
	}
	if err := verifyReleasePreflightInventory(root, wantedRoot); err != nil {
		return releasePreflightReceipt{}, err
	}
	evidenceDir, err := validateReleasePreflightDirectory(filepath.Join(root, "release-evidence"), false)
	if err != nil {
		return releasePreflightReceipt{}, fmt.Errorf("release evidence directory: %w", err)
	}
	wantedEvidence := []string{
		"build-artifact-plan.json", "composite-decomposition-evidence.json",
		"image-activation-evidence.json", "image-activation-plan.json",
		"immutable-target-manifest.yaml", "observed-live-manifest.yaml",
	}
	if err := verifyReleasePreflightInventory(evidenceDir, wantedEvidence); err != nil {
		return releasePreflightReceipt{}, err
	}

	paths := []string{
		"snapshot.json", "helm-release.json", "live-workloads.json", "build-receipt.json", "candidate-bundle-readback.json",
		"ownership-v1.yaml", "target-manifest.yaml", "operational-domain-evidence.json",
	}
	for _, name := range wantedEvidence {
		paths = append(paths, filepath.Join("release-evidence", name))
	}
	inputs := make(map[string][]byte, len(paths))
	var total int64
	for _, name := range paths {
		data, err := readReleasePreflightFile(filepath.Join(root, name))
		if err != nil {
			return releasePreflightReceipt{}, fmt.Errorf("input %s: %w", name, err)
		}
		total += int64(len(data))
		if total > releasePreflightTotalLimit {
			return releasePreflightReceipt{}, fmt.Errorf("snapshot input total exceeds limit %d", releasePreflightTotalLimit)
		}
		inputs[name] = data
	}

	var snapshot releasePreflightSnapshot
	if err := decodeCanonicalReleasePreflightJSON(inputs["snapshot.json"], &snapshot); err != nil {
		return releasePreflightReceipt{}, fmt.Errorf("snapshot metadata: %w", err)
	}
	if err := verifyReleasePreflightMetadata(snapshot); err != nil {
		return releasePreflightReceipt{}, err
	}

	workdir, err := createReleasePreflightWorkdir(options.tmpDir)
	if err != nil {
		return releasePreflightReceipt{}, err
	}
	defer os.RemoveAll(workdir)
	candidateReadback := inputs["candidate-bundle-readback.json"]
	if err := verifyReleasePreflightCandidateBundleWindow(candidateReadback); err != nil {
		return releasePreflightReceipt{}, fmt.Errorf("candidate bundle readback: %w", err)
	}

	ownership := inputs["ownership-v1.yaml"]
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return releasePreflightReceipt{}, fmt.Errorf("ownership verification: %w", err)
	}
	helmManifest, helmValues, err := verifyReleasePreflightHelmRelease(inputs["helm-release.json"], snapshot, spec)
	if err != nil {
		return releasePreflightReceipt{}, err
	}
	targetManifest := inputs["target-manifest.yaml"]
	canonicalTarget, err := releasedomain.CanonicalizeRenderedManifest(targetManifest, spec, snapshot.Namespace)
	if err != nil || !bytes.Equal(canonicalTarget, targetManifest) {
		return releasePreflightReceipt{}, fmt.Errorf("target manifest is not canonical ownership evidence")
	}
	if err := verifyReleasePreflightDownwardAPI(targetManifest, "target manifest"); err != nil {
		return releasePreflightReceipt{}, err
	}

	evidence, err := decodeReleasePreflightEvidence(inputs, snapshot)
	if err != nil {
		return releasePreflightReceipt{}, err
	}
	for _, manifest := range []struct {
		label string
		data  []byte
	}{
		{"immutable target manifest", evidence.immutableTarget},
		{"observed live manifest", evidence.observedLive},
	} {
		if err := verifyReleasePreflightDownwardAPI(manifest.data, manifest.label); err != nil {
			return releasePreflightReceipt{}, err
		}
	}
	witness := evidence.operationalReport.ActivationWitness[0]
	for _, item := range []struct{ label, got, want string }{
		{"base manifest", releasePreflightDigestBytes(helmManifest), witness.BaseManifestDigest},
		{"target manifest", releasePreflightDigestBytes(targetManifest), witness.TargetManifestDigest},
		{"immutable target manifest", releasePreflightDigestBytes(evidence.immutableTarget), witness.ImmutableTargetManifestDigest},
		{"ownership", releasePreflightDigestBytes(ownership), witness.OwnershipDigest},
	} {
		if item.got != item.want {
			return releasePreflightReceipt{}, fmt.Errorf("%s digest differs from prepared authorization", item.label)
		}
	}

	live := inputs["live-workloads.json"]
	if err := verifyReleasePreflightLiveAnnotations(helmManifest, live, snapshot.Namespace); err != nil {
		return releasePreflightReceipt{}, err
	}
	if err := verifyReleasePreflightWorkloadCohortSelectors(helmManifest, live, spec, snapshot.Namespace); err != nil {
		return releasePreflightReceipt{}, err
	}
	expanded, err := expandObservedKubernetesList(live, snapshot.Namespace)
	if err != nil {
		return releasePreflightReceipt{}, fmt.Errorf("live workload snapshot: %w", err)
	}
	observed, err := releasedomain.MaterializeObservedLiveImageManifest(helmManifest, expanded, ownership, snapshot.Namespace)
	if err != nil {
		return releasePreflightReceipt{}, fmt.Errorf("live manifest projection: %w", err)
	}
	if !bytes.Equal(observed, evidence.observedLive) || releasePreflightDigestBytes(observed) != witness.ObservedLiveManifestDigest {
		return releasePreflightReceipt{}, fmt.Errorf("fresh live snapshot differs from prepared activation evidence")
	}
	if err := verifyReleasePreflightActivationResources(live, evidence.activationPlan, snapshot.Namespace); err != nil {
		return releasePreflightReceipt{}, err
	}

	buildReceipt, err := decodeReleasePreflightBuildReceipt(inputs["build-receipt.json"], snapshot)
	if err != nil {
		return releasePreflightReceipt{}, err
	}
	if err := verifyReleasePreflightBuildPartition(buildReceipt, evidence.buildPlan, evidence.activationPlan, evidence.activation); err != nil {
		return releasePreflightReceipt{}, err
	}

	checks := []string{
		"activation-builtOnly-partition", "build-receipt-offline", "candidate-bundle-warmup-readback", "composite-authorization",
		"fresh-live-resources", "helm-revision-manifest-values", "input-bounds-permissions",
		"ownership-bindings", "rendered-downwardapi-fieldpaths", "rollback-target", "tmpdir-private-workdir",
		"workload-cohort-selectors",
	}
	return releasePreflightReceipt{
		APIVersion: "release-domain.fugue.dev/v1", Kind: "ReleasePreflightReceipt", Status: "pass",
		BuildReceiptDigest: snapshot.BuildReceiptDigest, CandidateReadbackDigest: releasePreflightDigestBytes(candidateReadback), Checks: checks,
		HelmManifestDigest: releasePreflightDigestBytes(helmManifest), HelmValuesDigest: releasePreflightDigestBytes(helmValues),
		LiveSnapshotDigest: releasePreflightDigestBytes(live), Namespace: snapshot.Namespace,
		OperationalReportDigest: snapshot.OperationalReportDigest, ReleaseName: snapshot.ReleaseName,
		RollbackTargetRevision: snapshot.RollbackTargetRevision,
		SnapshotDigest:         releasePreflightDigestBytes(inputs["snapshot.json"]),
		TargetCommit:           snapshot.TrustedTargetCommit, TargetRevision: snapshot.TargetRevision,
	}, nil
}

func verifyReleasePreflightCandidateBundleWindow(data []byte) error {
	var window releasePreflightCandidateBundleWindow
	if err := decodeCanonicalReleasePreflightJSON(data, &window); err != nil {
		return fmt.Errorf("schema/type round-trip: %w", err)
	}
	if window.APIVersion != "release-domain.fugue.dev/v1" || window.Kind != "CandidateBundleReadbackWindow" || window.AdvanceCount != 1 ||
		window.WindowSeconds != 90 || window.IntervalSeconds != 5 || len(window.Observations) == 0 || len(window.Observations) > 18 {
		return fmt.Errorf("fixed 18x5s warm-up window is invalid")
	}
	for index, observation := range window.Observations {
		if observation.ElapsedSeconds != uint64(index)*window.IntervalSeconds {
			return fmt.Errorf("observation cadence exceeds the fixed 90s window")
		}
		if releasePreflightCanonicalIdentity(observation.PodUID, 128) == "" || releasePreflightCanonicalIdentity(observation.ReleaseEpoch, 256) == "" {
			return fmt.Errorf("candidate Pod/release identity is invalid")
		}
		last := index == len(window.Observations)-1
		switch observation.State {
		case "pending":
			if last || observation.BundleVersion != "" || observation.BundleDigest != "" || observation.RouteGeneration != "" ||
				observation.BundleSelection || observation.LocalAsk || observation.DirectTLS || observation.ConsecutiveHealthy != 0 || observation.DurableHealthy {
				return fmt.Errorf("empty bundle is allowed only before the terminal observation")
			}
		case "warming":
			if last || !releasePreflightCandidateBundleAckComplete(observation) || observation.ConsecutiveHealthy >= 2 || observation.DurableHealthy {
				return fmt.Errorf("candidate smoke may warm only before durable consecutive health")
			}
		case "pass":
			if !last || !releasePreflightCandidateBundleAckComplete(observation) || observation.ConsecutiveHealthy < 2 || !observation.DurableHealthy {
				return fmt.Errorf("successful bundle selection/local ask/direct TLS and durable health observation is invalid")
			}
		default:
			return fmt.Errorf("candidate bundle observation state is invalid")
		}
	}
	terminal := window.Observations[len(window.Observations)-1]
	for _, observation := range window.Observations {
		if !releasePreflightCanonicalIdentityEqual(observation.PodUID, terminal.PodUID, 128) ||
			!releasePreflightCanonicalIdentityEqual(observation.ReleaseEpoch, terminal.ReleaseEpoch, 256) {
			return fmt.Errorf("candidate Pod/release identity drifted during the warm-up window")
		}
	}
	post := window.PostReadback
	if post.State != "pass" || post.ElapsedSeconds < terminal.ElapsedSeconds || post.ElapsedSeconds >= window.WindowSeconds ||
		!releasePreflightCandidateBundleAckComplete(post) || post.ConsecutiveHealthy < 2 || !post.DurableHealthy ||
		!releasePreflightCanonicalIdentityEqual(post.PodUID, terminal.PodUID, 128) ||
		!releasePreflightCanonicalIdentityEqual(post.ReleaseEpoch, terminal.ReleaseEpoch, 256) ||
		!releasePreflightCanonicalIdentityEqual(post.BundleVersion, terminal.BundleVersion, 128) ||
		post.BundleDigest != terminal.BundleDigest ||
		!releasePreflightCanonicalIdentityEqual(post.RouteGeneration, terminal.RouteGeneration, 128) {
		return fmt.Errorf("post-readback UID/bundle/routeGeneration differs from the successful observation")
	}
	return nil
}

func releasePreflightCandidateBundleAckComplete(observation releasePreflightCandidateBundleAck) bool {
	podUID := releasePreflightCanonicalIdentity(observation.PodUID, 128)
	releaseEpoch := releasePreflightCanonicalIdentity(observation.ReleaseEpoch, 256)
	bundleVersion := releasePreflightCanonicalIdentity(observation.BundleVersion, 128)
	routeGeneration := releasePreflightCanonicalIdentity(observation.RouteGeneration, 128)
	return releasePreflightPodUID.MatchString(podUID) && releaseEpoch != "" &&
		releasePreflightBundleVersion.MatchString(bundleVersion) &&
		releasePreflightDigestPattern.MatchString(observation.BundleDigest) &&
		releasePreflightRouteGen.MatchString(routeGeneration) &&
		observation.BundleSelection && observation.LocalAsk && observation.DirectTLS
}

func releasePreflightCanonicalIdentityEqual(left, right string, maxLength int) bool {
	left = releasePreflightCanonicalIdentity(left, maxLength)
	right = releasePreflightCanonicalIdentity(right, maxLength)
	return left != "" && left == right
}

// releasePreflightCanonicalIdentity mirrors the durable edge identity boundary:
// trim, lowercase, validate its bounded ASCII alphabet, then compare exactly.
func releasePreflightCanonicalIdentity(value string, maxLength int) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" || len(value) > maxLength {
		return ""
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case strings.ContainsRune("-._:+/@", character):
		default:
			return ""
		}
	}
	return value
}

func verifyReleasePreflightMetadata(snapshot releasePreflightSnapshot) error {
	if snapshot.APIVersion != "release-domain.fugue.dev/v1" || snapshot.Kind != "ReleasePreflightSnapshot" {
		return fmt.Errorf("snapshot identity is unsupported")
	}
	if !validReleasePreflightName(snapshot.ReleaseName) || !validReleasePreflightName(snapshot.Namespace) {
		return fmt.Errorf("release identity is invalid")
	}
	if !releasePreflightCommitPattern.MatchString(snapshot.TrustedBaseCommit) ||
		!releasePreflightCommitPattern.MatchString(snapshot.TrustedTargetCommit) ||
		snapshot.TrustedBaseCommit == snapshot.TrustedTargetCommit {
		return fmt.Errorf("trusted release commits are invalid")
	}
	for _, digest := range []string{snapshot.BuildReceiptDigest, snapshot.OperationalReportDigest, snapshot.PlanDigest} {
		if !releasePreflightDigestPattern.MatchString(digest) {
			return fmt.Errorf("trusted release digest is invalid")
		}
	}
	if snapshot.HelmRevision == 0 || snapshot.TargetRevision != snapshot.HelmRevision+1 ||
		snapshot.RollbackTargetRevision != snapshot.HelmRevision {
		return fmt.Errorf("rollback target must be the exact current Helm revision")
	}
	return nil
}

func verifyReleasePreflightHelmRelease(data []byte, snapshot releasePreflightSnapshot, spec *releasedomain.OwnershipSpec) ([]byte, []byte, error) {
	manifest, err := extractCanonicalHelmReleaseManifest(
		data, snapshot.ReleaseName, snapshot.Namespace, snapshot.HelmRevision, true,
	)
	if err != nil || len(bytes.TrimSpace(manifest)) == 0 {
		return nil, nil, fmt.Errorf("Helm revision manifest/values input is invalid")
	}
	var envelope struct {
		Config json.RawMessage `json:"config"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || len(envelope.Config) == 0 || bytes.Equal(envelope.Config, []byte("null")) {
		return nil, nil, fmt.Errorf("Helm revision values are missing")
	}
	var values map[string]any
	if err := json.Unmarshal(envelope.Config, &values); err != nil || values == nil {
		return nil, nil, fmt.Errorf("Helm revision values are invalid")
	}
	helmValues, err := json.Marshal(values)
	if err != nil {
		return nil, nil, fmt.Errorf("Helm revision values are invalid")
	}
	canonical, err := releasedomain.CanonicalizeRenderedManifest(manifest, spec, snapshot.Namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("Helm revision manifest ownership: %w", err)
	}
	if err := verifyReleasePreflightDownwardAPI(canonical, "Helm revision manifest"); err != nil {
		return nil, nil, err
	}
	return canonical, helmValues, nil
}

func verifyReleasePreflightDownwardAPI(data []byte, label string) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for document := 0; ; document++ {
		var object map[string]any
		err := decoder.Decode(&object)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s DownwardAPI template %d is invalid: %w", label, document, err)
		}
		if len(object) == 0 {
			continue
		}
		kind, _ := object["kind"].(string)
		spec, _ := object["spec"].(map[string]any)
		switch kind {
		case "Deployment", "DaemonSet", "StatefulSet", "Job":
			template, _ := spec["template"].(map[string]any)
			podSpec, _ := template["spec"].(map[string]any)
			if err := verifyReleasePreflightPodFieldPaths(podSpec); err != nil {
				return fmt.Errorf("%s DownwardAPI fieldPath: %w", label, err)
			}
		case "CronJob":
			jobTemplate, _ := spec["jobTemplate"].(map[string]any)
			jobSpec, _ := jobTemplate["spec"].(map[string]any)
			template, _ := jobSpec["template"].(map[string]any)
			podSpec, _ := template["spec"].(map[string]any)
			if err := verifyReleasePreflightPodFieldPaths(podSpec); err != nil {
				return fmt.Errorf("%s DownwardAPI fieldPath: %w", label, err)
			}
		}
	}
}

func verifyReleasePreflightPodFieldPaths(podSpec map[string]any) error {
	for _, listName := range []string{"containers", "initContainers", "ephemeralContainers"} {
		containers, _ := podSpec[listName].([]any)
		for _, value := range containers {
			container, _ := value.(map[string]any)
			environment, _ := container["env"].([]any)
			for _, envValue := range environment {
				env, _ := envValue.(map[string]any)
				valueFrom, _ := env["valueFrom"].(map[string]any)
				fieldRef, exists := valueFrom["fieldRef"].(map[string]any)
				if !exists {
					continue
				}
				fieldPath, ok := fieldRef["fieldPath"].(string)
				if !ok || !releasePreflightAllowedFieldPath(fieldPath, true) {
					return fmt.Errorf("environment fieldRef %q is unsupported or malformed", fieldPath)
				}
				if apiVersion, exists := fieldRef["apiVersion"]; exists && apiVersion != "v1" {
					return fmt.Errorf("environment fieldRef apiVersion is unsupported")
				}
			}
		}
	}
	volumes, _ := podSpec["volumes"].([]any)
	for _, value := range volumes {
		volume, _ := value.(map[string]any)
		downward, exists := volume["downwardAPI"].(map[string]any)
		if !exists {
			continue
		}
		items, ok := downward["items"].([]any)
		if !ok || len(items) == 0 {
			return fmt.Errorf("volume items must be a non-empty rendered list")
		}
		paths := make(map[string]struct{}, len(items))
		for _, itemValue := range items {
			item, ok := itemValue.(map[string]any)
			if !ok {
				return fmt.Errorf("volume item is not an object")
			}
			path, ok := item["path"].(string)
			if !ok || path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || path == "." || strings.HasPrefix(path, "..") {
				return fmt.Errorf("volume item path is incomplete or unsafe")
			}
			if _, duplicate := paths[path]; duplicate {
				return fmt.Errorf("volume item path %q is duplicated", path)
			}
			paths[path] = struct{}{}
			fieldRef, hasFieldRef := item["fieldRef"].(map[string]any)
			resourceFieldRef, hasResourceFieldRef := item["resourceFieldRef"].(map[string]any)
			if hasFieldRef == hasResourceFieldRef {
				return fmt.Errorf("volume item %q must contain exactly one fieldRef or resourceFieldRef", path)
			}
			if hasResourceFieldRef {
				resource, ok := resourceFieldRef["resource"].(string)
				if !ok || resource == "" || strings.TrimSpace(resource) != resource {
					return fmt.Errorf("volume item %q resourceFieldRef is incomplete", path)
				}
				continue
			}
			fieldPath, ok := fieldRef["fieldPath"].(string)
			if !ok || !releasePreflightAllowedFieldPath(fieldPath, false) {
				return fmt.Errorf("volume fieldRef %q is unsupported or malformed", fieldPath)
			}
			if apiVersion, exists := fieldRef["apiVersion"]; exists && apiVersion != "v1" {
				return fmt.Errorf("volume fieldRef apiVersion is unsupported")
			}
		}
	}
	return nil
}

func releasePreflightAllowedFieldPath(fieldPath string, environment bool) bool {
	if releasePreflightSelectorField.MatchString(fieldPath) {
		return true
	}
	common := map[string]struct{}{
		"metadata.name": {}, "metadata.namespace": {}, "metadata.uid": {},
	}
	if _, ok := common[fieldPath]; ok {
		return true
	}
	if !environment {
		return fieldPath == "metadata.annotations" || fieldPath == "metadata.labels"
	}
	environmentOnly := map[string]struct{}{
		"spec.nodeName": {}, "spec.serviceAccountName": {}, "status.hostIP": {},
		"status.hostIPs": {}, "status.podIP": {}, "status.podIPs": {},
	}
	_, ok := environmentOnly[fieldPath]
	return ok
}

func decodeReleasePreflightEvidence(inputs map[string][]byte, snapshot releasePreflightSnapshot) (releasePreflightEvidence, error) {
	readDigest := func(name string) (string, error) {
		var identity struct {
			Digest string `json:"digest"`
		}
		if err := json.Unmarshal(inputs[name], &identity); err != nil || !releasePreflightDigestPattern.MatchString(identity.Digest) {
			return "", fmt.Errorf("%s digest is invalid", name)
		}
		return identity.Digest, nil
	}
	buildName := filepath.Join("release-evidence", "build-artifact-plan.json")
	activationPlanName := filepath.Join("release-evidence", "image-activation-plan.json")
	activationName := filepath.Join("release-evidence", "image-activation-evidence.json")
	decompositionName := filepath.Join("release-evidence", "composite-decomposition-evidence.json")
	buildDigest, err := readDigest(buildName)
	if err != nil {
		return releasePreflightEvidence{}, err
	}
	buildPlan, err := releasedomain.DecodeAndVerifyBuildArtifactPlan(bytes.NewReader(inputs[buildName]), buildDigest)
	if err != nil {
		return releasePreflightEvidence{}, fmt.Errorf("build plan is invalid: %w", err)
	}
	planDigest, err := readDigest(activationPlanName)
	if err != nil {
		return releasePreflightEvidence{}, err
	}
	activationPlan, err := releasedomain.DecodeAndVerifyImageActivationPlan(bytes.NewReader(inputs[activationPlanName]), planDigest)
	if err != nil {
		return releasePreflightEvidence{}, fmt.Errorf("activation plan is invalid: %w", err)
	}
	activationDigest, err := readDigest(activationName)
	if err != nil {
		return releasePreflightEvidence{}, fmt.Errorf("built-only activation evidence is invalid: %w", err)
	}
	activation, err := releasedomain.DecodeAndVerifyImageActivationEvidence(bytes.NewReader(inputs[activationName]), activationDigest)
	if err != nil {
		return releasePreflightEvidence{}, fmt.Errorf("built-only activation evidence is invalid: %w", err)
	}
	decompositionDigest, err := readDigest(decompositionName)
	if err != nil {
		return releasePreflightEvidence{}, err
	}
	decomposition, err := releasedomain.DecodeAndVerifyCompositeDecompositionEvidence(bytes.NewReader(inputs[decompositionName]), decompositionDigest)
	if err != nil {
		return releasePreflightEvidence{}, fmt.Errorf("composite authorization evidence is invalid: %w", err)
	}
	report, err := releasedomain.DecodeAndVerifyOperationalDomainEvidence(
		bytes.NewReader(inputs["operational-domain-evidence.json"]), snapshot.OperationalReportDigest,
	)
	if err != nil {
		return releasePreflightEvidence{}, fmt.Errorf("operational authorization report is invalid: %w", err)
	}
	if report.Policy != releasedomain.OperationalObservedLiveActivationPolicy || !releasePreflightAuthorizationReady(report) ||
		report.BaseCommit != snapshot.TrustedBaseCommit || report.TargetCommit != snapshot.TrustedTargetCommit ||
		report.PlanDigest != snapshot.PlanDigest || len(report.ActivationWitness) != 1 {
		return releasePreflightEvidence{}, fmt.Errorf("operational authorization report identity or eligibility mismatch")
	}
	witness := report.ActivationWitness[0]
	if !reflect.DeepEqual(buildPlan, witness.BuildPlan) || !reflect.DeepEqual(activationPlan, witness.Plan) {
		return releasePreflightEvidence{}, fmt.Errorf("prepared build or activation plan differs from operational authorization")
	}
	if !reflect.DeepEqual(activation, witness.Evidence) {
		return releasePreflightEvidence{}, fmt.Errorf("prepared activation evidence differs from operational authorization")
	}
	expectedDecomposition, err := releasedomain.BuildCompositeDecompositionEvidence(activationPlan, activation)
	if err != nil || !reflect.DeepEqual(decomposition, expectedDecomposition) {
		return releasePreflightEvidence{}, fmt.Errorf("composite authorization does not match activation evidence")
	}
	if !activation.Complete || len(activation.Unresolved) != 0 {
		return releasePreflightEvidence{}, fmt.Errorf("activation evidence is incomplete")
	}
	return releasePreflightEvidence{
		buildPlan: buildPlan, activationPlan: activationPlan, activation: activation,
		decomposition:     decomposition,
		immutableTarget:   inputs[filepath.Join("release-evidence", "immutable-target-manifest.yaml")],
		observedLive:      inputs[filepath.Join("release-evidence", "observed-live-manifest.yaml")],
		operationalReport: report,
	}, nil
}

func releasePreflightAuthorizationReady(report releasedomain.OperationalDomainEvidence) bool {
	if report.AuthorizationEligible {
		return true
	}
	if len(report.Issues) != 0 || !report.ClassificationAgrees {
		return false
	}
	switch report.ConservativeOutcome {
	case releasedomain.OutcomeZero:
		return report.Observation == releasedomain.OutcomeZero && report.CandidateDomain == ""
	case releasedomain.OutcomeSingle:
		return report.Observation == releasedomain.OutcomeSingle &&
			report.CandidateDomain == report.ConservativeDomain
	default:
		return false
	}
}

func decodeReleasePreflightBuildReceipt(data []byte, snapshot releasePreflightSnapshot) ([]releasePreflightBuildReceipt, error) {
	if releasePreflightDigestCanonicalJSON(data) != snapshot.BuildReceiptDigest {
		return nil, fmt.Errorf("build receipt digest mismatch")
	}
	var receipt []releasePreflightBuildReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil || receipt == nil {
		return nil, fmt.Errorf("build receipt canonical schema/type round-trip is invalid")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !releasePreflightCanonicalJSONBytes(data, canonical) {
		return nil, fmt.Errorf("build receipt bytes are not canonical")
	}
	previous := ""
	for _, artifact := range receipt {
		if !validReleasePreflightName(artifact.Component) || (previous != "" && artifact.Component <= previous) {
			return nil, fmt.Errorf("build receipt components are not uniquely canonical")
		}
		previous = artifact.Component
		if artifact.OCIRevision != snapshot.TrustedTargetCommit || artifact.SourceTag != snapshot.TrustedTargetCommit ||
			artifact.Verification != "registry_manifest_config_and_layer_get" ||
			!releasePreflightDigestPattern.MatchString(artifact.TopDigest) ||
			!releasePreflightDigestPattern.MatchString(artifact.ConfigDigest) ||
			!releasePreflightDigestPattern.MatchString(artifact.PlatformManifestDigest) ||
			artifact.ImmutableRef != artifact.Repository+"@"+artifact.TopDigest ||
			artifact.Repository == "" || strings.ContainsAny(artifact.Repository, "@ \t\r\n") {
			return nil, fmt.Errorf("build receipt artifact %s is invalid", artifact.Component)
		}
	}
	return receipt, nil
}

func verifyReleasePreflightBuildPartition(
	receipt []releasePreflightBuildReceipt,
	buildPlan releasedomain.BuildArtifactPlan,
	activationPlan releasedomain.ImageActivationPlan,
	activation releasedomain.ImageActivationEvidence,
) error {
	if len(receipt) != len(buildPlan.Artifacts) {
		return fmt.Errorf("build receipt and build plan artifact counts differ")
	}
	receiptByName := make(map[string]releasePreflightBuildReceipt, len(receipt))
	for _, item := range receipt {
		receiptByName[item.Component] = item
	}
	partition := make(map[string]string, len(buildPlan.Artifacts))
	for _, item := range activationPlan.Activations {
		if _, duplicate := partition[item.ArtifactName]; duplicate {
			return fmt.Errorf("activation/built-only partition duplicates %s", item.ArtifactName)
		}
		partition[item.ArtifactName] = "activation"
	}
	for _, name := range activation.BuiltOnlyArtifacts {
		if _, duplicate := partition[name]; duplicate {
			return fmt.Errorf("activation/built-only partition overlaps %s", name)
		}
		partition[name] = "built-only"
	}
	for _, artifact := range buildPlan.Artifacts {
		item, ok := receiptByName[artifact.Name]
		if !ok || item.TopDigest != artifact.ArtifactDigest || item.ImmutableRef != artifact.PublishedImageRef ||
			artifact.ProvenanceDigest == "" || artifact.ProvenanceDigest != releasePreflightDigestCanonicalReceipt(receipt) {
			return fmt.Errorf("build receipt does not bind artifact %s", artifact.Name)
		}
		if partition[artifact.Name] == "" {
			return fmt.Errorf("artifact %s is absent from activation/built-only partition", artifact.Name)
		}
	}
	return nil
}

func verifyReleasePreflightActivationResources(data []byte, plan releasedomain.ImageActivationPlan, namespace string) error {
	var list struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Items      []struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(data, &list); err != nil || list.APIVersion != "v1" || list.Kind != "List" {
		return fmt.Errorf("live resource snapshot is invalid")
	}
	resources := make(map[string]struct{}, len(list.Items))
	for _, item := range list.Items {
		resources[strings.Join([]string{item.APIVersion, item.Kind, item.Metadata.Namespace, item.Metadata.Name}, "\x00")] = struct{}{}
	}
	for _, activation := range plan.Activations {
		workload := activation.Workload
		if workload.Namespace != namespace {
			return fmt.Errorf("activation resource namespace differs from snapshot")
		}
		key := strings.Join([]string{workload.APIVersion, workload.Kind, workload.Namespace, workload.Name}, "\x00")
		if _, exists := resources[key]; !exists {
			return fmt.Errorf("activation resource %s/%s is absent from fresh live snapshot", workload.Kind, workload.Name)
		}
	}
	return nil
}

func verifyReleasePreflightLiveAnnotations(baseManifest, liveJSON []byte, namespace string) error {
	base, err := releasePreflightManifestWorkloads(baseManifest, namespace)
	if err != nil {
		return fmt.Errorf("base annotation inventory: %w", err)
	}
	var list struct {
		APIVersion string            `json:"apiVersion"`
		Kind       string            `json:"kind"`
		Items      []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(liveJSON, &list); err != nil || list.APIVersion != "v1" || list.Kind != "List" {
		return fmt.Errorf("live annotation inventory is invalid")
	}
	live := make(map[string]releasePreflightWorkload, len(list.Items))
	for _, raw := range list.Items {
		var object map[string]any
		if err := json.Unmarshal(raw, &object); err != nil {
			return fmt.Errorf("live annotation object is invalid")
		}
		workload, ok := releasePreflightWorkloadFromMap(object, namespace)
		if !ok {
			continue
		}
		if _, duplicate := live[workload.key]; duplicate {
			return fmt.Errorf("live resource identity is duplicated")
		}
		live[workload.key] = workload
	}
	for key, expected := range base {
		observed, exists := live[key]
		if !exists {
			return fmt.Errorf("Helm workload resource %s is absent from live snapshot", expected.name)
		}
		if err := compareReleasePreflightAnnotations(expected.annotations, observed.annotations, releasePreflightAllowedObjectAnnotations); err != nil {
			return fmt.Errorf("live-only annotation drift for resource %s: %w", expected.name, err)
		}
		if err := compareReleasePreflightAnnotations(expected.podAnnotations, observed.podAnnotations, releasePreflightAllowedPodAnnotations); err != nil {
			return fmt.Errorf("live-only pod annotation drift for resource %s: %w", expected.name, err)
		}
	}
	return nil
}

type releasePreflightWorkload struct {
	apiVersion     string
	kind           string
	key            string
	name           string
	labels         map[string]string
	annotations    map[string]string
	podLabels      map[string]string
	podAnnotations map[string]string
	selector       map[string]string
	selectorValid  bool
	containers     []string
}

var releasePreflightAllowedObjectAnnotations = map[string]struct{}{
	"deployment.kubernetes.io/revision":        {},
	"deprecated.daemonset.template.generation": {},
	"meta.helm.sh/release-name":                {},
	"meta.helm.sh/release-namespace":           {},
}

var releasePreflightAllowedPodAnnotations = map[string]struct{}{
	"fugue.io/public-data-plane-release-id":   {},
	"fugue.io/public-data-plane-release-mode": {},
}

func releasePreflightManifestWorkloads(data []byte, namespace string) (map[string]releasePreflightWorkload, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	result := map[string]releasePreflightWorkload{}
	for {
		var object map[string]any
		err := decoder.Decode(&object)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		workload, ok := releasePreflightWorkloadFromMap(object, namespace)
		if !ok {
			continue
		}
		if _, duplicate := result[workload.key]; duplicate {
			return nil, fmt.Errorf("workload identity is duplicated")
		}
		result[workload.key] = workload
	}
	return result, nil
}

func releasePreflightWorkloadFromMap(object map[string]any, defaultNamespace string) (releasePreflightWorkload, bool) {
	apiVersion, _ := object["apiVersion"].(string)
	kind, _ := object["kind"].(string)
	if (apiVersion != "apps/v1" || (kind != "Deployment" && kind != "DaemonSet" && kind != "StatefulSet")) &&
		(apiVersion != "batch/v1" || kind != "CronJob") {
		return releasePreflightWorkload{}, false
	}
	metadata, _ := object["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, _ := metadata["namespace"].(string)
	if namespace == "" {
		namespace = defaultNamespace
	}
	if name == "" || namespace == "" {
		return releasePreflightWorkload{}, false
	}
	spec, _ := object["spec"].(map[string]any)
	if kind == "CronJob" {
		job, _ := spec["jobTemplate"].(map[string]any)
		spec, _ = job["spec"].(map[string]any)
	}
	selectorObject, selectorObjectOK := spec["selector"].(map[string]any)
	selector, selectorOK := releasePreflightStrictStringMap(selectorObject["matchLabels"])
	if expressions, exists := selectorObject["matchExpressions"]; exists {
		items, ok := expressions.([]any)
		selectorOK = selectorOK && ok && len(items) == 0
	}
	template, _ := spec["template"].(map[string]any)
	templateMetadata, _ := template["metadata"].(map[string]any)
	templateSpec, _ := template["spec"].(map[string]any)
	containers := releasePreflightContainerNames(templateSpec["containers"])
	return releasePreflightWorkload{
		apiVersion: apiVersion, kind: kind,
		key: strings.Join([]string{apiVersion, kind, namespace, name}, "\x00"), name: name,
		labels:         releasePreflightStringMap(metadata["labels"]),
		annotations:    releasePreflightStringMap(metadata["annotations"]),
		podLabels:      releasePreflightStringMap(templateMetadata["labels"]),
		podAnnotations: releasePreflightStringMap(templateMetadata["annotations"]),
		selector:       selector,
		selectorValid:  selectorObjectOK && selectorOK && len(selector) != 0,
		containers:     containers,
	}, true
}

func verifyReleasePreflightWorkloadCohortSelectors(baseManifest, liveJSON []byte, spec *releasedomain.OwnershipSpec, namespace string) error {
	base, err := releasePreflightManifestWorkloads(baseManifest, namespace)
	if err != nil {
		return fmt.Errorf("base workload cohort inventory: %w", err)
	}
	var list struct {
		APIVersion string            `json:"apiVersion"`
		Kind       string            `json:"kind"`
		Items      []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(liveJSON, &list); err != nil || list.APIVersion != "v1" || list.Kind != "List" {
		return fmt.Errorf("workload cohort selector inventory is invalid")
	}
	live := make(map[string]releasePreflightWorkload, len(list.Items))
	all := make([]releasePreflightWorkload, 0, len(list.Items))
	for _, raw := range list.Items {
		var object map[string]any
		if err := json.Unmarshal(raw, &object); err != nil {
			return fmt.Errorf("workload cohort selector object is invalid")
		}
		workload, ok := releasePreflightWorkloadFromMap(object, namespace)
		if !ok {
			continue
		}
		if _, duplicate := live[workload.key]; duplicate {
			return fmt.Errorf("workload cohort selector identity is duplicated")
		}
		live[workload.key] = workload
		all = append(all, workload)
	}
	for key, expected := range base {
		if expected.apiVersion != "apps/v1" {
			continue
		}
		observed, exists := live[key]
		if !exists {
			return fmt.Errorf("workload cohort selector resource %s is absent", expected.name)
		}
		if err := verifyReleasePreflightExactCohortSelector(observed, all, spec, namespace); err != nil {
			return fmt.Errorf("workload cohort selector for %s: %w", expected.name, err)
		}
	}
	return nil
}

const releasePreflightComponentLabel = "app.kubernetes.io/component"

func verifyReleasePreflightExactCohortSelector(workload releasePreflightWorkload, all []releasePreflightWorkload, spec *releasedomain.OwnershipSpec, namespace string) error {
	if !workload.selectorValid || !releasePreflightSelectorMatches(workload.selector, workload.podLabels) {
		return fmt.Errorf("selector is invalid or does not match its Pod template")
	}
	exact := make(map[string]string, len(workload.selector)+1)
	for key, value := range workload.selector {
		exact[key] = value
	}
	if component := exact[releasePreflightComponentLabel]; component != "" {
		if workload.podLabels[releasePreflightComponentLabel] != component {
			return fmt.Errorf("component constraint differs from its Pod template")
		}
	} else if component := workload.podLabels[releasePreflightComponentLabel]; component != "" && releasePreflightComponentIsDerived(workload, spec, namespace, component) {
		exact[releasePreflightComponentLabel] = component
	} else if releasePreflightSelectorMatchCount(workload.selector, all) != 1 ||
		!releasePreflightHasExactOwnership(workload, spec, namespace) {
		return fmt.Errorf("wide selector has no exact component or ownership derivation")
	}
	if releasePreflightSelectorMatchCount(exact, all) != 1 {
		return fmt.Errorf("derived selector does not identify exactly one workload cohort")
	}
	return nil
}

func releasePreflightComponentIsDerived(workload releasePreflightWorkload, spec *releasedomain.OwnershipSpec, namespace, component string) bool {
	containerMatch := false
	for _, name := range workload.containers {
		if name == component {
			containerMatch = true
			break
		}
	}
	if containerMatch {
		return true
	}
	matches := 0
	for _, rule := range spec.ObjectRules {
		if releasePreflightOwnershipRuleMatchesWorkload(rule, workload, namespace) &&
			rule.RequiredLabels[releasePreflightComponentLabel] == component {
			matches++
		}
	}
	return matches == 1
}

func releasePreflightHasExactOwnership(workload releasePreflightWorkload, spec *releasedomain.OwnershipSpec, namespace string) bool {
	matches := 0
	for _, rule := range spec.ObjectRules {
		if strings.Contains(rule.Name, "${") || rule.NamePrefix != "" || rule.Name != workload.name {
			continue
		}
		if releasePreflightOwnershipRuleMatchesWorkload(rule, workload, namespace) {
			matches++
		}
	}
	return matches == 1
}

func releasePreflightOwnershipRuleMatchesWorkload(rule releasedomain.ObjectRule, workload releasePreflightWorkload, namespace string) bool {
	apiGroup, version := releasePreflightSplitAPIVersion(workload.apiVersion)
	if rule.APIGroup != apiGroup || rule.Version != version || rule.Kind != workload.kind || rule.Scope != releasedomain.ScopeNamespaced {
		return false
	}
	if rule.Namespace != "" && rule.Namespace != "${releaseNamespace}" && rule.Namespace != namespace {
		return false
	}
	if rule.Name != "" && !strings.Contains(rule.Name, "${") && rule.Name != workload.name {
		return false
	}
	for key, value := range rule.RequiredLabels {
		if strings.Contains(value, "${") {
			continue
		}
		if workload.labels[key] != value {
			return false
		}
	}
	return true
}

func releasePreflightSplitAPIVersion(apiVersion string) (string, string) {
	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) == 1 {
		return "", parts[0]
	}
	return parts[0], parts[1]
}

func releasePreflightSelectorMatchCount(selector map[string]string, workloads []releasePreflightWorkload) int {
	count := 0
	for _, workload := range workloads {
		if workload.apiVersion == "apps/v1" && releasePreflightSelectorMatches(selector, workload.podLabels) {
			count++
		}
	}
	return count
}

func releasePreflightSelectorMatches(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func releasePreflightStrictStringMap(value any) (map[string]string, bool) {
	input, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	result := make(map[string]string, len(input))
	for key, raw := range input {
		text, ok := raw.(string)
		if !ok || key == "" || text == "" {
			return nil, false
		}
		result[key] = text
	}
	return result, true
}

func releasePreflightContainerNames(value any) []string {
	items, _ := value.([]any)
	result := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, raw := range items {
		container, _ := raw.(map[string]any)
		name, _ := container["name"].(string)
		if name == "" {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	return result
}

func releasePreflightStringMap(value any) map[string]string {
	input, _ := value.(map[string]any)
	result := make(map[string]string, len(input))
	for key, raw := range input {
		if text, ok := raw.(string); ok {
			result[key] = text
		}
	}
	return result
}

func compareReleasePreflightAnnotations(expected, observed map[string]string, allowed map[string]struct{}) error {
	for key, expectedValue := range expected {
		if observed[key] != expectedValue {
			return fmt.Errorf("annotation %q differs from Helm revision", key)
		}
	}
	for key := range observed {
		if _, inBase := expected[key]; inBase {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("annotation %q is not an allowed live-only diff", key)
		}
	}
	return nil
}

func validateReleasePreflightDirectory(path string, requirePrivate bool) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil || filepath.Clean(absolute) != absolute {
		return "", fmt.Errorf("path is invalid")
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("path must be a non-symlink directory")
	}
	if info.Mode().Perm()&0o022 != 0 || (requirePrivate && info.Mode().Perm()&0o077 != 0) {
		return "", fmt.Errorf("directory permissions are unsafe")
	}
	if err := verifyReleasePreflightOwner(info); err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || !filepath.IsAbs(resolved) {
		return "", fmt.Errorf("directory path cannot be resolved")
	}
	return resolved, nil
}

func verifyReleasePreflightInventory(directory string, wanted []string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	sort.Strings(got)
	expected := append([]string(nil), wanted...)
	sort.Strings(expected)
	if !reflect.DeepEqual(got, expected) {
		return fmt.Errorf("snapshot inventory differs: got %v want %v", got, expected)
	}
	return nil
}

func readReleasePreflightFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("path must be a regular non-symlink file")
	}
	if info.Size() < 1 || info.Size() > releasePreflightFileLimit {
		return nil, fmt.Errorf("file size exceeds limit %d", releasePreflightFileLimit)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("file permissions are unsafe")
	}
	if err := verifyReleasePreflightOwner(info); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("file identity changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, releasePreflightFileLimit+1))
	if err != nil || int64(len(data)) > releasePreflightFileLimit {
		return nil, fmt.Errorf("file read exceeds limit %d", releasePreflightFileLimit)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, after) || after.Size() != int64(len(data)) {
		return nil, fmt.Errorf("file identity changed while reading")
	}
	return data, nil
}

func verifyReleasePreflightOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 && info.Mode().IsRegular() {
		return fmt.Errorf("path ownership or link count is unsafe")
	}
	return nil
}

func createReleasePreflightWorkdir(tmpDir string) (string, error) {
	if strings.TrimSpace(tmpDir) == "" || !filepath.IsAbs(tmpDir) {
		return "", fmt.Errorf("TMPDIR must be an absolute writable directory")
	}
	info, err := os.Lstat(tmpDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o300 != 0o300 {
		return "", fmt.Errorf("TMPDIR must be an owned writable directory")
	}
	if err := verifyReleasePreflightOwner(info); err != nil {
		sharedSticky := info.Mode()&os.ModeSticky != 0 && info.Mode().Perm()&0o003 == 0o003
		if !sharedSticky {
			return "", fmt.Errorf("TMPDIR: %w", err)
		}
	}
	workdir, err := os.MkdirTemp(tmpDir, "fugue-release-preflight.")
	if err != nil {
		return "", fmt.Errorf("TMPDIR is not writable")
	}
	if err := os.Chmod(workdir, 0o700); err != nil {
		_ = os.RemoveAll(workdir)
		return "", fmt.Errorf("TMPDIR private workdir permissions: %w", err)
	}
	return workdir, nil
}

func decodeCanonicalReleasePreflightJSON(data []byte, output any) error {
	var generic any
	genericDecoder := json.NewDecoder(bytes.NewReader(data))
	genericDecoder.UseNumber()
	if err := genericDecoder.Decode(&generic); err != nil || genericDecoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("JSON document is invalid")
	}
	canonical, err := json.Marshal(generic)
	if err != nil || !releasePreflightCanonicalJSONBytes(data, canonical) {
		return fmt.Errorf("JSON bytes are not canonical")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	return nil
}

func marshalReleasePreflightReceipt(receipt releasePreflightReceipt) ([]byte, error) {
	value := map[string]any{
		"apiVersion": receipt.APIVersion, "buildReceiptDigest": receipt.BuildReceiptDigest,
		"candidateReadbackDigest": receipt.CandidateReadbackDigest,
		"checks":                  receipt.Checks, "helmManifestDigest": receipt.HelmManifestDigest,
		"helmValuesDigest": receipt.HelmValuesDigest, "kind": receipt.Kind,
		"liveSnapshotDigest": receipt.LiveSnapshotDigest, "namespace": receipt.Namespace,
		"operationalReportDigest": receipt.OperationalReportDigest, "releaseName": receipt.ReleaseName,
		"rollbackTargetRevision": receipt.RollbackTargetRevision, "snapshotDigest": receipt.SnapshotDigest,
		"status": receipt.Status, "targetCommit": receipt.TargetCommit, "targetRevision": receipt.TargetRevision,
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	encoded = append(encoded, '\n')
	var decoded releasePreflightReceipt
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil || decoder.Decode(&struct{}{}) != io.EOF || !reflect.DeepEqual(decoded, receipt) {
		return nil, fmt.Errorf("receipt schema/type round-trip mismatch")
	}
	var generic any
	genericDecoder := json.NewDecoder(bytes.NewReader(encoded))
	genericDecoder.UseNumber()
	if err := genericDecoder.Decode(&generic); err != nil || genericDecoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("receipt canonical JSON round-trip failed")
	}
	repeated, err := json.Marshal(generic)
	if err != nil || !bytes.Equal(repeated, bytes.TrimSpace(encoded)) {
		return nil, fmt.Errorf("receipt canonical JSON round-trip mismatch")
	}
	return encoded, nil
}

func releasePreflightDigestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", digest[:])
}

func releasePreflightDigestCanonicalJSON(data []byte) string {
	return releasePreflightDigestBytes(bytes.TrimSpace(data))
}

func releasePreflightDigestCanonicalReceipt(receipt []releasePreflightBuildReceipt) string {
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return ""
	}
	return releasePreflightDigestBytes(encoded)
}

func releasePreflightCanonicalJSONBytes(data, canonical []byte) bool {
	return bytes.Equal(data, canonical) ||
		(len(data) == len(canonical)+1 && data[len(data)-1] == '\n' && bytes.Equal(data[:len(data)-1], canonical))
}

func validReleasePreflightName(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 253 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}
