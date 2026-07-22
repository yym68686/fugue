package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"fugue/internal/releasedomain"
)

const (
	activationPlanInputLimit = 32 << 20

	activationPlanArgumentsError = "fugue-release-domain-evidence image-activation-plans: invalid arguments"
	activationPlanInputError     = "fugue-release-domain-evidence image-activation-plans: input evidence is invalid"
	activationPlanBuildError     = "fugue-release-domain-evidence image-activation-plans: plan construction failed"
	activationPlanOutputError    = "fugue-release-domain-evidence image-activation-plans: output failed"
)

type buildArtifactFlags []releasedomain.BuildArtifact

func (values *buildArtifactFlags) String() string { return "" }

func (values *buildArtifactFlags) Set(value string) error {
	parts := strings.Split(value, "=")
	if len(parts) != 4 {
		return fmt.Errorf("artifact must be name=source-base-commit=artifact-digest=published-image-ref")
	}
	for _, part := range parts {
		if strings.TrimSpace(part) != part || part == "" {
			return fmt.Errorf("artifact fields must be non-empty without surrounding whitespace")
		}
	}
	*values = append(*values, releasedomain.BuildArtifact{
		Name: parts[0], SourceBaseCommit: parts[1], ArtifactDigest: parts[2], PublishedImageRef: parts[3],
	})
	return nil
}

type activationPlanOptions struct {
	changedEvidencePath string
	ownershipPath       string
	planPath            string
	planDigest          string
	baseManifestPath    string
	targetManifestPath  string
	trustedBase         string
	trustedTarget       string
	provenanceDigest    string
	outputDirectory     string
	artifacts           buildArtifactFlags
}

func runImageActivationPlans(args []string, _ io.Writer, stderr io.Writer) int {
	options, err := parseImageActivationPlanFlags(args)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanArgumentsError)
		return 1
	}
	changedBytes, _, err := readBoundedRegularFile(options.changedEvidencePath, activationPlanInputLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanInputError)
		return 1
	}
	changed, err := releasedomain.DecodeAndVerifyChangedFileEvidence(bytes.NewReader(changedBytes), options.trustedBase, options.trustedTarget)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanInputError)
		return 1
	}
	ownership, _, err := readBoundedRegularFile(options.ownershipPath, canonicalOwnershipLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanInputError)
		return 1
	}
	planBytes, _, err := readBoundedRegularFile(options.planPath, activationPlanInputLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanInputError)
		return 1
	}
	plan, err := releasedomain.DecodeAndVerifyPlan(bytes.NewReader(planBytes), options.planDigest)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanInputError)
		return 1
	}
	baseManifest, _, err := readBoundedRegularFile(options.baseManifestPath, canonicalManifestInputLimit, true)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanInputError)
		return 1
	}
	targetManifest, _, err := readBoundedRegularFile(options.targetManifestPath, canonicalManifestInputLimit, true)
	if err != nil {
		fmt.Fprintln(stderr, activationPlanInputError)
		return 1
	}

	artifacts := append([]releasedomain.BuildArtifact(nil), options.artifacts...)
	for index := range artifacts {
		artifacts[index].ProvenanceDigest = options.provenanceDigest
	}
	buildPlan, err := releasedomain.NewBuildArtifactPlan(
		options.trustedBase,
		options.trustedTarget,
		changed.Digest(),
		artifacts,
	)
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	immutableTargetManifest, err := releasedomain.MaterializeTargetPublishedImageRefs(
		targetManifest, ownership, plan.Digests.ClassificationContext.DefaultNamespace,
		options.trustedTarget, buildPlan,
	)
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	activationPlan, activationEvidence, err := releasedomain.BuildImageActivationReportFromManifests(releasedomain.ImageActivationPlanInput{
		BuildPlan: buildPlan, ReleasePlan: plan, Ownership: ownership,
		BaseManifest: baseManifest, TargetManifest: targetManifest,
		ImmutableTargetManifest: immutableTargetManifest,
	})
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	decompositionEvidence, err := releasedomain.BuildCompositeDecompositionEvidence(activationPlan, activationEvidence)
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	buildBytes, err := releasedomain.MarshalBuildArtifactPlan(buildPlan)
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	activationBytes, err := releasedomain.MarshalImageActivationPlan(activationPlan)
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	evidenceBytes, err := releasedomain.MarshalImageActivationEvidence(activationEvidence)
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	decompositionBytes, err := releasedomain.MarshalCompositeDecompositionEvidence(decompositionEvidence)
	if err != nil {
		writeActivationPlanBuildError(stderr, err)
		return 1
	}
	if err := writeActivationPlanDirectory(
		options.outputDirectory, buildBytes, activationBytes, evidenceBytes,
		decompositionBytes, immutableTargetManifest,
	); err != nil {
		fmt.Fprintln(stderr, activationPlanOutputError)
		return 1
	}
	return 0
}

func writeActivationPlanBuildError(stderr io.Writer, err error) {
	fmt.Fprintf(stderr, "%s: %s\n", activationPlanBuildError, activationPlanBuildErrorCode(err))
}

func activationPlanBuildErrorCode(err error) string {
	if err == nil || strings.TrimSpace(err.Error()) == "" {
		return "unspecified-internal-invariant"
	}
	message := err.Error()
	known := []struct {
		fragment string
		code     string
	}{
		{"image activation plan and evidence binding mismatch", "composite-decomposition-binding-mismatch"},
		{"verify build artifact plan:", "build-artifact-plan-invalid"},
		{"verify release plan:", "release-plan-invalid"},
		{"build artifact and release plan binding mismatch", "plan-binding-mismatch"},
		{"rendered manifest or ownership digest mismatch", "rendered-input-digest-mismatch"},
		{"verify classification context:", "classification-context-invalid"},
		{"load ownership:", "ownership-invalid"},
		{"immutable target manifest", "immutable-target-manifest-invalid"},
		{"target-commit workload image", "immutable-target-image-unresolved"},
		{"published image repository is ambiguous", "immutable-target-image-ambiguous"},
		{"validate ownership bindings:", "ownership-bindings-invalid"},
		{"rendered manifests contain incomplete object evidence", "rendered-object-evidence-incomplete"},
		{"rendered manifests contain duplicate object identities", "rendered-object-identity-duplicated"},
		{"workload pod spec is missing", "workload-pod-spec-missing"},
		{"workload containers are missing", "workload-containers-missing"},
		{"workload containers are invalid", "workload-containers-invalid"},
		{"workload initContainers are invalid", "workload-init-containers-invalid"},
		{"workload container is invalid", "workload-container-invalid"},
		{"workload container identity is invalid", "workload-container-identity-invalid"},
		{"workload container name is duplicated", "workload-container-name-duplicated"},
		{"workload kind changed", "workload-kind-changed"},
		{"base ownership match failed:", "base-ownership-match-failed"},
		{"target ownership match failed:", "target-ownership-match-failed"},
		{"image activation ownership rules overlap", "activation-ownership-overlap"},
		{"marshal rendered workload:", "rendered-workload-marshal-failed"},
		{"image activation", "image-activation-contract-invalid"},
		{"activation gap", "activation-gap-contract-invalid"},
		{"build artifact", "build-artifact-contract-invalid"},
		{"composite decomposition", "composite-decomposition-contract-invalid"},
	}
	for _, item := range known {
		if strings.Contains(message, item.fragment) {
			return item.code
		}
	}
	return "internal-invariant-unclassified"
}

func parseImageActivationPlanFlags(args []string) (activationPlanOptions, error) {
	allowed := map[string]bool{
		"changed-evidence": false, "ownership": false, "plan": false,
		"plan-digest": false, "base-manifest": false, "target-manifest": false,
		"trusted-base": false, "trusted-target": false, "provenance-digest": false,
		"output-dir": false, "artifact": true,
	}
	seen := map[string]struct{}{}
	for _, argument := range args {
		if strings.HasPrefix(argument, "-") && !strings.HasPrefix(argument, "--") {
			return activationPlanOptions{}, fmt.Errorf("only canonical --long flags are accepted")
		}
		if !strings.HasPrefix(argument, "--") {
			continue
		}
		name := strings.TrimPrefix(argument, "--")
		if before, _, found := strings.Cut(name, "="); found {
			name = before
		}
		repeatable, ok := allowed[name]
		if !ok {
			continue
		}
		if !repeatable {
			if _, duplicate := seen[name]; duplicate {
				return activationPlanOptions{}, fmt.Errorf("duplicate flag --%s", name)
			}
			seen[name] = struct{}{}
		}
	}

	var options activationPlanOptions
	flags := flag.NewFlagSet("image-activation-plans", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.changedEvidencePath, "changed-evidence", "", "revision-bound changed-file evidence")
	flags.StringVar(&options.ownershipPath, "ownership", "", "release-domain ownership YAML")
	flags.StringVar(&options.planPath, "plan", "", "verified conservative release-domain plan")
	flags.StringVar(&options.planDigest, "plan-digest", "", "independently trusted release-domain plan digest")
	flags.StringVar(&options.baseManifestPath, "base-manifest", "", "private canonical live manifest")
	flags.StringVar(&options.targetManifestPath, "target-manifest", "", "private canonical target manifest")
	flags.StringVar(&options.trustedBase, "trusted-base", "", "trusted exact base commit")
	flags.StringVar(&options.trustedTarget, "trusted-target", "", "trusted exact target commit")
	flags.StringVar(&options.provenanceDigest, "provenance-digest", "", "verified build provenance digest")
	flags.StringVar(&options.outputDirectory, "output-dir", "", "new private report output directory")
	flags.Var(&options.artifacts, "artifact", "built name=source-base-commit=artifact-digest=published-image-ref (repeatable)")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return activationPlanOptions{}, fmt.Errorf("invalid flags")
	}
	for name, value := range map[string]string{
		"--changed-evidence":  options.changedEvidencePath,
		"--ownership":         options.ownershipPath,
		"--plan":              options.planPath,
		"--plan-digest":       options.planDigest,
		"--base-manifest":     options.baseManifestPath,
		"--target-manifest":   options.targetManifestPath,
		"--trusted-base":      options.trustedBase,
		"--trusted-target":    options.trustedTarget,
		"--provenance-digest": options.provenanceDigest,
		"--output-dir":        options.outputDirectory,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return activationPlanOptions{}, fmt.Errorf("%s is required without surrounding whitespace", name)
		}
	}
	if !filepath.IsAbs(options.outputDirectory) || filepath.Clean(options.outputDirectory) != options.outputDirectory {
		return activationPlanOptions{}, fmt.Errorf("--output-dir must be an absolute normalized path")
	}
	return options, nil
}

func writeActivationPlanDirectory(
	output string,
	buildPlan, activationPlan, activationEvidence, decompositionEvidence, immutableTargetManifest []byte,
) (resultErr error) {
	parentPath := filepath.Dir(output)
	parentInfo, err := os.Lstat(parentPath)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 || parentInfo.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("output parent must be a private non-symlink directory")
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		return fmt.Errorf("output directory must not exist")
	}
	temporary, err := os.MkdirTemp(parentPath, ".image-activation-plans-")
	if err != nil {
		return err
	}
	defer func() {
		if temporary != "" {
			if err := os.RemoveAll(temporary); resultErr == nil && err != nil {
				resultErr = err
			}
		}
	}()
	if err := os.Chmod(temporary, 0o700); err != nil {
		return err
	}
	if err := writePrivateAtomicFile(filepath.Join(temporary, "build-artifact-plan.json"), buildPlan); err != nil {
		return err
	}
	if err := writePrivateAtomicFile(filepath.Join(temporary, "image-activation-plan.json"), activationPlan); err != nil {
		return err
	}
	if err := writePrivateAtomicFile(filepath.Join(temporary, "image-activation-evidence.json"), activationEvidence); err != nil {
		return err
	}
	if err := writePrivateAtomicFile(filepath.Join(temporary, "composite-decomposition-evidence.json"), decompositionEvidence); err != nil {
		return err
	}
	if err := writePrivateAtomicFile(filepath.Join(temporary, "immutable-target-manifest.yaml"), immutableTargetManifest); err != nil {
		return err
	}
	directory, err := os.Open(temporary)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	if err := directory.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, output); err != nil {
		return err
	}
	temporary = ""
	parent, err := os.Open(parentPath)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}
