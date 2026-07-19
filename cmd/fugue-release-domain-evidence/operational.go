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
)

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
