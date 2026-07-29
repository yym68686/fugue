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
	activationConvergenceArgumentsError = "fugue-release-domain-evidence image-activation-convergence: invalid arguments"
	activationConvergenceInputError     = "fugue-release-domain-evidence image-activation-convergence: input evidence is invalid"
)

type activationConvergenceOptions struct {
	buildPlanPath          string
	activationPlanPath     string
	activationEvidencePath string
}

func runImageActivationConvergence(args []string, stdout, stderr io.Writer) int {
	options, err := parseImageActivationConvergenceFlags(args)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceArgumentsError)
		return 1
	}
	read := func(path string) ([]byte, error) {
		value, _, readErr := readBoundedRegularFile(path, activationPlanInputLimit, false)
		return value, readErr
	}
	buildBytes, err := read(options.buildPlanPath)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	activationBytes, err := read(options.activationPlanPath)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	evidenceBytes, err := read(options.activationEvidencePath)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	buildDigest, err := operationalContractDigest(buildBytes)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	activationDigest, err := operationalContractDigest(activationBytes)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	evidenceDigest, err := operationalContractDigest(evidenceBytes)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	buildPlan, err := releasedomain.DecodeAndVerifyBuildArtifactPlan(bytes.NewReader(buildBytes), buildDigest)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	activationPlan, err := releasedomain.DecodeAndVerifyImageActivationPlan(bytes.NewReader(activationBytes), activationDigest)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	activationEvidence, err := releasedomain.DecodeAndVerifyImageActivationEvidence(bytes.NewReader(evidenceBytes), evidenceDigest)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	convergence, err := releasedomain.EvaluateImageActivationConvergence(buildPlan, activationPlan, activationEvidence)
	if err != nil {
		fmt.Fprintln(stderr, activationConvergenceInputError)
		return 1
	}
	if convergence.Complete {
		fmt.Fprintln(stdout, "complete\t-")
		return 0
	}
	fmt.Fprintf(stdout, "pending\t%s\n", strings.Join(convergence.PendingArtifacts, ","))
	return 0
}

func parseImageActivationConvergenceFlags(args []string) (activationConvergenceOptions, error) {
	var options activationConvergenceOptions
	flags := flag.NewFlagSet("fugue-release-domain-evidence image-activation-convergence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.buildPlanPath, "build-artifact-plan", "", "verified build artifact plan")
	flags.StringVar(&options.activationPlanPath, "image-activation-plan", "", "verified image activation plan")
	flags.StringVar(&options.activationEvidencePath, "image-activation-evidence", "", "verified image activation evidence")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return activationConvergenceOptions{}, fmt.Errorf("invalid arguments")
	}
	for _, value := range []string{options.buildPlanPath, options.activationPlanPath, options.activationEvidencePath} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return activationConvergenceOptions{}, fmt.Errorf("required path is missing or non-canonical")
		}
	}
	return options, nil
}
