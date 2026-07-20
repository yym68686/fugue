package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
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
	changedEvidencePath string
	imagePlanPath       string
	planPath            string
	planDigest          string
	trustedBase         string
	trustedTarget       string
	outputPath          string
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
	imagePlanBytes, imagePlanResolved, err := readBoundedRegularFile(options.imagePlanPath, operationalInputLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, operationalInputError)
		return 1
	}
	imagePlan, err := releasedomain.DecodeAndVerifyOperationalImageRolloutPlan(
		bytes.NewReader(imagePlanBytes),
		options.trustedBase,
		options.trustedTarget,
		changed.Digest(),
	)
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
	report, err := releasedomain.BuildOperationalDomainEvidence(changed, imagePlan, plan)
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
		changedResolved,
		imagePlanResolved,
		planResolved,
	); err != nil {
		fmt.Fprintln(stderr, operationalOutputError)
		return 1
	}
	return 0
}

func parseOperationalReportFlags(args []string) (operationalReportOptions, error) {
	allowed := map[string]struct{}{
		"changed-evidence": {},
		"image-plan":       {},
		"plan":             {},
		"plan-digest":      {},
		"trusted-base":     {},
		"trusted-target":   {},
		"output":           {},
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
		"--image-plan":       options.imagePlanPath,
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
	if options.outputPath == "-" {
		return operationalReportOptions{}, fmt.Errorf("--output must be a file path")
	}
	return options, nil
}
