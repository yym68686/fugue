package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"

	"fugue/internal/releasedomain"
)

// classify-files is deliberately only a read-only render-policy hint. Its
// output is never an execution authorization and is not accepted by an
// adapter; authorize still requires the independent rendered evidence,
// transaction envelope, argv binding, and reverse ownership proof.
func runClassifyFilesCommand(args []string, stdout, stderr io.Writer) int {
	if err := rejectDuplicateFlags(args, map[string]bool{
		"ownership": false, "changed-evidence": false,
		"trusted-base-commit": false, "trusted-target-commit": false,
	}); err != nil {
		return classifyFilesFailure(stderr)
	}
	flags := flag.NewFlagSet("fugue-release-domain-dispatch classify-files", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var ownershipPath, evidencePath, baseCommit, targetCommit string
	flags.StringVar(&ownershipPath, "ownership", "", "release-domain ownership document")
	flags.StringVar(&evidencePath, "changed-evidence", "", "revision-bound changed-file evidence")
	flags.StringVar(&baseCommit, "trusted-base-commit", "", "trusted exact base Git commit")
	flags.StringVar(&targetCommit, "trusted-target-commit", "", "trusted exact target Git commit")
	if err := flags.Parse(args); err != nil {
		return classifyFilesFailure(stderr)
	}
	if flags.NArg() != 0 {
		return classifyFilesFailure(stderr)
	}
	for name, value := range map[string]string{
		"--ownership":             ownershipPath,
		"--changed-evidence":      evidencePath,
		"--trusted-base-commit":   baseCommit,
		"--trusted-target-commit": targetCommit,
	} {
		if err := validateRequiredString(name, value); err != nil {
			return classifyFilesFailure(stderr)
		}
	}
	ownership, err := readSecureSource(ownershipPath, maxOwnershipBytes, false)
	if err != nil {
		return classifyFilesFailure(stderr)
	}
	evidenceData, err := readSecureSource(evidencePath, maxChangedEvidenceBytes, false)
	if err != nil {
		return classifyFilesFailure(stderr)
	}
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return classifyFilesFailure(stderr)
	}
	evidence, err := releasedomain.DecodeAndVerifyChangedFileEvidence(bytes.NewReader(evidenceData), baseCommit, targetCommit)
	if err != nil {
		return classifyFilesFailure(stderr)
	}
	classification := releasedomain.ClassifyFiles(evidence.Changes(), spec)
	if len(classification.Unknown) != 0 {
		return writeClassifyResult(stdout, stderr, "unknown\n", 2)
	}
	switch len(classification.Domains) {
	case 0:
		if !classification.AllNonRuntime {
			return writeClassifyResult(stdout, stderr, "unknown\n", 2)
		}
		return writeClassifyResult(stdout, stderr, "zero\n", 0)
	case 1:
		domain, err := releasedomain.ParseDomain(string(classification.Domains[0]))
		if err != nil {
			return writeClassifyResult(stdout, stderr, "unknown\n", 2)
		}
		return writeClassifyResult(stdout, stderr, fmt.Sprintf("single\t%s\n", domain), 0)
	default:
		return writeClassifyResult(stdout, stderr, "multiple\n", 2)
	}
}

func writeClassifyResult(stdout, stderr io.Writer, result string, exitCode int) int {
	if _, err := io.WriteString(stdout, result); err != nil {
		return classifyFilesFailure(stderr)
	}
	return exitCode
}

func classifyFilesFailure(stderr io.Writer) int {
	_, _ = io.WriteString(stderr, "fugue-release-domain-dispatch: classify-files failed\n")
	return 1
}
