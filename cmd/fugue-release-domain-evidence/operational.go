package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"reflect"
	"strings"

	"fugue/internal/releasedomain"
)

const (
	operationalInputLimit = 8 << 20

	operationalArgumentsError = "fugue-release-domain-evidence operational-report: invalid arguments"
	operationalInputError     = "fugue-release-domain-evidence operational-report: input evidence is invalid"
	operationalBuildError     = "fugue-release-domain-evidence operational-report: report construction failed"
	operationalOutputError    = "fugue-release-domain-evidence operational-report: output failed"

	operationalImagePlanArgumentsError = "fugue-release-domain-evidence operational-image-plan: invalid arguments"
	operationalImagePlanInputError     = "fugue-release-domain-evidence operational-image-plan: input evidence is invalid"
	operationalImagePlanBuildError     = "fugue-release-domain-evidence operational-image-plan: plan construction failed"
	operationalImagePlanOutputError    = "fugue-release-domain-evidence operational-image-plan: output failed"
)

type operationalImageTargetFlags []releasedomain.OperationalImageRolloutTarget

func (values *operationalImageTargetFlags) String() string { return "" }

func (values *operationalImageTargetFlags) Set(value string) error {
	parts := strings.Split(value, "=")
	if len(parts) != 3 || strings.TrimSpace(parts[0]) != parts[0] ||
		strings.TrimSpace(parts[1]) != parts[1] || strings.TrimSpace(parts[2]) != parts[2] ||
		parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return fmt.Errorf("target must be name=source-base-commit=artifact-digest")
	}
	*values = append(*values, releasedomain.OperationalImageRolloutTarget{
		Name:             parts[0],
		SourceBaseCommit: parts[1],
		ArtifactDigest:   parts[2],
	})
	return nil
}

type operationalImagePlanOptions struct {
	changedEvidencePath string
	trustedBase         string
	trustedTarget       string
	outputPath          string
	targets             operationalImageTargetFlags
}

func runOperationalImagePlan(args []string, _ io.Writer, stderr io.Writer) int {
	options, err := parseOperationalImagePlanFlags(args)
	if err != nil {
		fmt.Fprintln(stderr, operationalImagePlanArgumentsError)
		return 1
	}
	changedBytes, changedResolved, err := readBoundedRegularFile(options.changedEvidencePath, operationalInputLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, operationalImagePlanInputError)
		return 1
	}
	changed, err := releasedomain.DecodeAndVerifyChangedFileEvidence(bytes.NewReader(changedBytes), options.trustedBase, options.trustedTarget)
	if err != nil {
		fmt.Fprintln(stderr, operationalImagePlanInputError)
		return 1
	}
	plan, err := releasedomain.NewOperationalImageRolloutPlan(
		options.trustedBase,
		options.trustedTarget,
		changed.Digest(),
		[]releasedomain.OperationalImageRolloutTarget(options.targets),
	)
	if err != nil {
		fmt.Fprintln(stderr, operationalImagePlanBuildError)
		return 1
	}
	encoded, err := releasedomain.MarshalOperationalImageRolloutPlan(plan)
	if err != nil {
		fmt.Fprintln(stderr, operationalImagePlanBuildError)
		return 1
	}
	if err := writePrivateAtomicFile(options.outputPath, encoded, changedResolved); err != nil {
		fmt.Fprintln(stderr, operationalImagePlanOutputError)
		return 1
	}
	return 0
}

func parseOperationalImagePlanFlags(args []string) (operationalImagePlanOptions, error) {
	allowed := map[string]bool{
		"changed-evidence": false,
		"trusted-base":     false,
		"trusted-target":   false,
		"output":           false,
		"target":           true,
	}
	seen := map[string]struct{}{}
	for _, argument := range args {
		if strings.HasPrefix(argument, "-") && !strings.HasPrefix(argument, "--") {
			return operationalImagePlanOptions{}, fmt.Errorf("only canonical --long flags are accepted")
		}
		if !strings.HasPrefix(argument, "--") {
			continue
		}
		name := strings.TrimPrefix(argument, "--")
		if before, _, found := strings.Cut(name, "="); found {
			name = before
		}
		repeatable, ok := allowed[name]
		if !ok || repeatable {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return operationalImagePlanOptions{}, fmt.Errorf("duplicate flag --%s", name)
		}
		seen[name] = struct{}{}
	}

	var options operationalImagePlanOptions
	flags := flag.NewFlagSet("operational-image-plan", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.changedEvidencePath, "changed-evidence", "", "revision-bound changed-file evidence")
	flags.StringVar(&options.trustedBase, "trusted-base", "", "trusted exact base commit")
	flags.StringVar(&options.trustedTarget, "trusted-target", "", "trusted exact target commit")
	flags.StringVar(&options.outputPath, "output", "", "private operational image plan path")
	flags.Var(&options.targets, "target", "selected name=source-base-commit=artifact-digest (repeatable)")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return operationalImagePlanOptions{}, fmt.Errorf("invalid flags")
	}
	for name, value := range map[string]string{
		"--changed-evidence": options.changedEvidencePath,
		"--trusted-base":     options.trustedBase,
		"--trusted-target":   options.trustedTarget,
		"--output":           options.outputPath,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return operationalImagePlanOptions{}, fmt.Errorf("%s is required without surrounding whitespace", name)
		}
	}
	if options.outputPath == "-" {
		return operationalImagePlanOptions{}, fmt.Errorf("--output must be a file path")
	}
	return options, nil
}

type operationalReportOptions struct {
	changedEvidencePath         string
	imagePlanPath               string
	buildPlanPath               string
	activationPlanPath          string
	activationEvidencePath      string
	ownershipPath               string
	baseManifestPath            string
	targetManifestPath          string
	immutableTargetManifestPath string
	planPath                    string
	planDigest                  string
	trustedBase                 string
	trustedTarget               string
	outputPath                  string
}

func runOperationalReport(args []string, _ io.Writer, stderr io.Writer) int {
	options, err := parseOperationalReportFlags(args)
	if err != nil {
		fmt.Fprintln(stderr, operationalArgumentsError)
		return 1
	}
	changedBytes, changedResolved, err := readBoundedRegularFile(options.changedEvidencePath, operationalInputLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, operationalInputError)
		return 1
	}
	changed, err := releasedomain.DecodeAndVerifyChangedFileEvidence(bytes.NewReader(changedBytes), options.trustedBase, options.trustedTarget)
	if err != nil {
		fmt.Fprintln(stderr, operationalInputError)
		return 1
	}
	planBytes, planResolved, err := readBoundedRegularFile(options.planPath, operationalInputLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, operationalInputError)
		return 1
	}
	plan, err := releasedomain.DecodeAndVerifyPlan(bytes.NewReader(planBytes), options.planDigest)
	if err != nil {
		fmt.Fprintln(stderr, operationalInputError)
		return 1
	}
	resolvedInputs := []string{changedResolved, planResolved}
	var report releasedomain.OperationalDomainEvidence
	if options.imagePlanPath != "" {
		imagePlanBytes, imagePlanResolved, readErr := readBoundedRegularFile(options.imagePlanPath, operationalInputLimit, false)
		if readErr != nil {
			fmt.Fprintln(stderr, operationalInputError)
			return 1
		}
		imagePlan, decodeErr := releasedomain.DecodeAndVerifyOperationalImageRolloutPlan(
			bytes.NewReader(imagePlanBytes), options.trustedBase, options.trustedTarget, changed.Digest(),
		)
		if decodeErr != nil {
			fmt.Fprintln(stderr, operationalInputError)
			return 1
		}
		report, err = releasedomain.BuildOperationalDomainEvidence(changed, imagePlan, plan)
		resolvedInputs = append(resolvedInputs, imagePlanResolved)
	} else {
		report, resolvedInputs, err = buildActivationOperationalReport(options, changed, plan, resolvedInputs)
	}
	if err != nil {
		fmt.Fprintln(stderr, operationalBuildError)
		return 1
	}
	encoded, err := releasedomain.MarshalOperationalDomainEvidence(report)
	if err != nil {
		fmt.Fprintln(stderr, operationalBuildError)
		return 1
	}
	if err := writePrivateAtomicFile(
		options.outputPath,
		encoded,
		resolvedInputs...,
	); err != nil {
		fmt.Fprintln(stderr, operationalOutputError)
		return 1
	}
	return 0
}

func buildActivationOperationalReport(
	options operationalReportOptions,
	changed releasedomain.ChangedFileEvidence,
	plan releasedomain.Plan,
	resolved []string,
) (releasedomain.OperationalDomainEvidence, []string, error) {
	read := func(path string) ([]byte, string, error) {
		return readBoundedRegularFile(path, operationalInputLimit, false)
	}
	buildBytes, buildResolved, err := read(options.buildPlanPath)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	activationBytes, activationResolved, err := read(options.activationPlanPath)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	evidenceBytes, evidenceResolved, err := read(options.activationEvidencePath)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	ownership, ownershipResolved, err := read(options.ownershipPath)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	baseManifest, baseResolved, err := read(options.baseManifestPath)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	targetManifest, targetResolved, err := read(options.targetManifestPath)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	immutableTarget, immutableResolved, err := read(options.immutableTargetManifestPath)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	buildDigest, err := operationalContractDigest(buildBytes)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	activationDigest, err := operationalContractDigest(activationBytes)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	evidenceDigest, err := operationalContractDigest(evidenceBytes)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	buildPlan, err := releasedomain.DecodeAndVerifyBuildArtifactPlan(bytes.NewReader(buildBytes), buildDigest)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	activationPlan, err := releasedomain.DecodeAndVerifyImageActivationPlan(bytes.NewReader(activationBytes), activationDigest)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	activationEvidence, err := releasedomain.DecodeAndVerifyImageActivationEvidence(bytes.NewReader(evidenceBytes), evidenceDigest)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	rebuiltPlan, rebuiltEvidence, err := releasedomain.BuildImageActivationReportFromManifests(releasedomain.ImageActivationPlanInput{
		BuildPlan: buildPlan, ReleasePlan: plan, Ownership: ownership,
		BaseManifest: baseManifest, TargetManifest: targetManifest, ImmutableTargetManifest: immutableTarget,
	})
	if err != nil || !reflect.DeepEqual(rebuiltPlan, activationPlan) || !reflect.DeepEqual(rebuiltEvidence, activationEvidence) {
		return releasedomain.OperationalDomainEvidence{}, nil, fmt.Errorf("image activation artifact rederivation mismatch")
	}
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	context := plan.Digests.ClassificationContext
	activationRendered := releasedomain.ClassifyRendered(baseManifest, immutableTarget, spec, releasedomain.RenderedOptions{
		DefaultNamespace: context.DefaultNamespace, Bindings: context.BindingMap(), IgnoreHelmTestHooks: false,
	})
	report, err := releasedomain.BuildOperationalDomainEvidenceFromActivation(
		changed, buildPlan, activationPlan, activationEvidence, activationRendered,
		digestOperationalInput(baseManifest), digestOperationalInput(targetManifest),
		digestOperationalInput(immutableTarget), digestOperationalInput(ownership), plan,
	)
	if err != nil {
		return releasedomain.OperationalDomainEvidence{}, nil, err
	}
	resolved = append(resolved, buildResolved, activationResolved, evidenceResolved, ownershipResolved, baseResolved, targetResolved, immutableResolved)
	return report, resolved, nil
}

func operationalContractDigest(data []byte) (string, error) {
	var envelope struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || strings.TrimSpace(envelope.Digest) != envelope.Digest || envelope.Digest == "" {
		return "", fmt.Errorf("contract digest is invalid")
	}
	return envelope.Digest, nil
}

func digestOperationalInput(data []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

func parseOperationalReportFlags(args []string) (operationalReportOptions, error) {
	allowed := map[string]struct{}{
		"changed-evidence":          {},
		"image-plan":                {},
		"build-artifact-plan":       {},
		"image-activation-plan":     {},
		"image-activation-evidence": {},
		"ownership":                 {},
		"base-manifest":             {},
		"target-manifest":           {},
		"immutable-target-manifest": {},
		"plan":                      {},
		"plan-digest":               {},
		"trusted-base":              {},
		"trusted-target":            {},
		"output":                    {},
	}
	seen := map[string]struct{}{}
	for _, argument := range args {
		if strings.HasPrefix(argument, "-") && !strings.HasPrefix(argument, "--") {
			return operationalReportOptions{}, fmt.Errorf("only canonical --long flags are accepted")
		}
		if !strings.HasPrefix(argument, "--") {
			continue
		}
		name := strings.TrimPrefix(argument, "--")
		if before, _, found := strings.Cut(name, "="); found {
			name = before
		}
		if _, ok := allowed[name]; !ok {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return operationalReportOptions{}, fmt.Errorf("duplicate flag --%s", name)
		}
		seen[name] = struct{}{}
	}

	var options operationalReportOptions
	flags := flag.NewFlagSet("operational-report", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.changedEvidencePath, "changed-evidence", "", "revision-bound changed-file evidence")
	flags.StringVar(&options.imagePlanPath, "image-plan", "", "exact image rollout-plan evidence")
	flags.StringVar(&options.buildPlanPath, "build-artifact-plan", "", "exact build artifact plan")
	flags.StringVar(&options.activationPlanPath, "image-activation-plan", "", "exact live-relative image activation plan")
	flags.StringVar(&options.activationEvidencePath, "image-activation-evidence", "", "exact image activation completeness evidence")
	flags.StringVar(&options.ownershipPath, "ownership", "", "exact release ownership policy")
	flags.StringVar(&options.baseManifestPath, "base-manifest", "", "exact live base manifest")
	flags.StringVar(&options.targetManifestPath, "target-manifest", "", "exact pre-materialization target manifest")
	flags.StringVar(&options.immutableTargetManifestPath, "immutable-target-manifest", "", "exact immutable activation target manifest")
	flags.StringVar(&options.planPath, "plan", "", "existing release-domain plan")
	flags.StringVar(&options.planDigest, "plan-digest", "", "independently trusted release-domain plan digest")
	flags.StringVar(&options.trustedBase, "trusted-base", "", "trusted exact base commit")
	flags.StringVar(&options.trustedTarget, "trusted-target", "", "trusted exact target commit")
	flags.StringVar(&options.outputPath, "output", "", "private operational report path")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return operationalReportOptions{}, fmt.Errorf("invalid flags")
	}
	for name, value := range map[string]string{
		"--changed-evidence": options.changedEvidencePath,
		"--plan":             options.planPath,
		"--plan-digest":      options.planDigest,
		"--trusted-base":     options.trustedBase,
		"--trusted-target":   options.trustedTarget,
		"--output":           options.outputPath,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return operationalReportOptions{}, fmt.Errorf("%s is required without surrounding whitespace", name)
		}
	}
	activationValues := map[string]string{
		"--build-artifact-plan":       options.buildPlanPath,
		"--image-activation-plan":     options.activationPlanPath,
		"--image-activation-evidence": options.activationEvidencePath,
		"--ownership":                 options.ownershipPath,
		"--base-manifest":             options.baseManifestPath,
		"--target-manifest":           options.targetManifestPath,
		"--immutable-target-manifest": options.immutableTargetManifestPath,
	}
	activationMode := false
	for _, value := range activationValues {
		if value != "" {
			activationMode = true
		}
	}
	if activationMode == (options.imagePlanPath != "") {
		return operationalReportOptions{}, fmt.Errorf("exactly one operational witness mode is required")
	}
	if activationMode {
		for name, value := range activationValues {
			if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
				return operationalReportOptions{}, fmt.Errorf("%s is required without surrounding whitespace", name)
			}
		}
	} else if strings.TrimSpace(options.imagePlanPath) == "" || strings.TrimSpace(options.imagePlanPath) != options.imagePlanPath {
		return operationalReportOptions{}, fmt.Errorf("--image-plan is required without surrounding whitespace")
	}
	if options.outputPath == "-" {
		return operationalReportOptions{}, fmt.Errorf("--output must be a file path")
	}
	return options, nil
}
