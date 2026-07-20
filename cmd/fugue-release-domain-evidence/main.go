package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/releasedomain"
)

const (
	evidenceAPIVersion     = "release-domain.fugue.dev/v1"
	evidenceKind           = "ChangedFileEvidence"
	evidencePolicy         = "refs-only-offline-v1"
	defaultEvidenceTimeout = 5 * time.Minute
	maxEvidenceStringBytes = 8 << 20
	maxEvidenceOutputBytes = 32 << 20

	evidenceArgumentsError  = "fugue-release-domain-evidence: invalid arguments"
	evidenceProductionError = "fugue-release-domain-evidence: evidence production failed"
	evidenceIncompleteError = "fugue-release-domain-evidence: evidence is incomplete; refusing to publish output"
	evidenceDeadlineError   = "fugue-release-domain-evidence: evidence deadline exceeded"
	evidenceBindingError    = "fugue-release-domain-evidence: evidence binding failed"
	evidenceEncodingError   = "fugue-release-domain-evidence: evidence encoding failed"
	evidenceOutputError     = "fugue-release-domain-evidence: evidence output failed"
)

type cliOptions struct {
	repository     string
	baseRevision   string
	targetRevision string
	outputPath     string
	goBinary       string
	timeout        time.Duration
}

type evidencePayload struct {
	APIVersion   string                      `json:"apiVersion"`
	Kind         string                      `json:"kind"`
	Policy       string                      `json:"policy"`
	BaseCommit   string                      `json:"baseCommit"`
	TargetCommit string                      `json:"targetCommit"`
	Changes      []releasedomain.ChangedFile `json:"changes"`
}

type evidenceDocument struct {
	APIVersion   string                      `json:"apiVersion"`
	Kind         string                      `json:"kind"`
	Policy       string                      `json:"policy"`
	BaseCommit   string                      `json:"baseCommit"`
	TargetCommit string                      `json:"targetCommit"`
	Changes      []releasedomain.ChangedFile `json:"changes"`
	Digest       string                      `json:"digest"`
}

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "canonicalize-manifest" {
		os.Exit(runCanonicalizeManifest(args[1:], os.Stdout, os.Stderr))
	}
	if len(args) > 0 && args[0] == "operational-report" {
		os.Exit(runOperationalReport(args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(run(context.Background(), args, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	options, err := parseFlags(args, stderr)
	if err != nil {
		fmt.Fprintln(stderr, evidenceArgumentsError)
		return 1
	}

	ctx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()

	result, warnings, err := produceEvidence(ctx, options, execCommandRunner{})
	if err != nil {
		fmt.Fprintln(stderr, evidenceProductionError)
		return 1
	}
	if len(warnings) != 0 {
		fmt.Fprintln(stderr, evidenceIncompleteError)
		return 1
	}
	if ctx.Err() != nil {
		fmt.Fprintln(stderr, evidenceDeadlineError)
		return 1
	}

	document, err := newEvidenceDocument(result)
	if err != nil {
		fmt.Fprintln(stderr, evidenceBindingError)
		return 1
	}

	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		fmt.Fprintln(stderr, evidenceEncodingError)
		return 1
	}
	if len(encoded)+1 > maxEvidenceOutputBytes {
		fmt.Fprintln(stderr, evidenceEncodingError)
		return 1
	}
	encoded = append(encoded, '\n')
	if options.outputPath == "-" {
		if _, err := stdout.Write(encoded); err != nil {
			fmt.Fprintln(stderr, evidenceOutputError)
			return 1
		}
		return 0
	}
	if err := writePrivateAtomicFile(options.outputPath, encoded); err != nil {
		fmt.Fprintln(stderr, evidenceOutputError)
		return 1
	}
	return 0
}

func parseFlags(args []string, _ io.Writer) (cliOptions, error) {
	options := cliOptions{
		repository: ".",
		outputPath: "-",
		goBinary:   "go",
		timeout:    defaultEvidenceTimeout,
	}
	flags := flag.NewFlagSet("fugue-release-domain-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.repository, "repo", options.repository, "repository containing the compared revisions")
	flags.StringVar(&options.baseRevision, "base", "", "base Git commit or revision")
	flags.StringVar(&options.targetRevision, "target", "", "target Git commit or revision")
	flags.StringVar(&options.outputPath, "output", options.outputPath, "output revision-bound evidence JSON path or -")
	flags.StringVar(&options.goBinary, "go", options.goBinary, "Go command used for package consumer evidence")
	flags.DurationVar(&options.timeout, "timeout", options.timeout, "overall evidence production deadline")
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if flags.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	for name, value := range map[string]string{
		"--repo":   options.repository,
		"--base":   options.baseRevision,
		"--target": options.targetRevision,
		"--output": options.outputPath,
		"--go":     options.goBinary,
	} {
		if strings.TrimSpace(value) == "" {
			return cliOptions{}, fmt.Errorf("%s is required", name)
		}
	}
	if options.timeout <= 0 {
		return cliOptions{}, fmt.Errorf("--timeout must be greater than zero")
	}
	return options, nil
}

func newEvidenceDocument(result evidenceResult) (evidenceDocument, error) {
	if err := validateEvidenceStringBudget(result); err != nil {
		return evidenceDocument{}, err
	}
	payload := evidencePayload{
		APIVersion:   evidenceAPIVersion,
		Kind:         evidenceKind,
		Policy:       evidencePolicy,
		BaseCommit:   result.baseCommit,
		TargetCommit: result.targetCommit,
		Changes:      result.changes,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return evidenceDocument{}, err
	}
	if len(encoded) > maxEvidenceOutputBytes {
		return evidenceDocument{}, fmt.Errorf("encoded evidence exceeds limit %d", maxEvidenceOutputBytes)
	}
	digest := sha256.Sum256(encoded)
	return evidenceDocument{
		APIVersion:   payload.APIVersion,
		Kind:         payload.Kind,
		Policy:       payload.Policy,
		BaseCommit:   payload.BaseCommit,
		TargetCommit: payload.TargetCommit,
		Changes:      payload.Changes,
		Digest:       fmt.Sprintf("sha256:%x", digest[:]),
	}, nil
}

func validateEvidenceStringBudget(result evidenceResult) error {
	remaining := maxEvidenceStringBytes
	consume := func(value string) error {
		if len(value) > remaining {
			return fmt.Errorf("evidence string bytes exceed limit %d", maxEvidenceStringBytes)
		}
		remaining -= len(value)
		return nil
	}
	for _, value := range []string{result.baseCommit, result.targetCommit} {
		if err := consume(value); err != nil {
			return err
		}
	}
	for _, change := range result.changes {
		if err := consume(change.Path); err != nil {
			return err
		}
		for _, value := range change.ValuePointers {
			if err := consume(value); err != nil {
				return err
			}
		}
		for _, domain := range change.ConsumerDomains {
			if err := consume(string(domain)); err != nil {
				return err
			}
		}
		for _, domain := range change.SemanticDomains {
			if err := consume(string(domain)); err != nil {
				return err
			}
		}
		for _, value := range change.OutsideConsumers {
			if err := consume(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func writePrivateAtomicFile(filename string, data []byte, protectedPaths ...string) (resultErr error) {
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return fmt.Errorf("resolve output path: %w", err)
	}
	resolvedDirectory, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return fmt.Errorf("resolve output directory: %w", err)
	}
	resolvedOutput := filepath.Join(resolvedDirectory, filepath.Base(absolute))
	for _, protectedPath := range protectedPaths {
		protectedAbsolute, err := filepath.Abs(protectedPath)
		if err != nil {
			return fmt.Errorf("resolve protected path: %w", err)
		}
		protectedResolved, err := filepath.EvalSymlinks(protectedAbsolute)
		if err != nil {
			return fmt.Errorf("resolve protected path: %w", err)
		}
		if filepath.Clean(protectedResolved) == filepath.Clean(resolvedOutput) {
			return fmt.Errorf("output path aliases a protected input")
		}
		protectedInfo, err := os.Stat(protectedResolved)
		if err != nil {
			return fmt.Errorf("inspect protected path: %w", err)
		}
		if outputInfo, err := os.Stat(resolvedOutput); err == nil {
			if os.SameFile(protectedInfo, outputInfo) {
				return fmt.Errorf("output file aliases a protected input")
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect output path: %w", err)
		}
	}
	if info, err := os.Lstat(resolvedOutput); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("output path must not be a symbolic link")
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("output path must be a regular file")
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect output path: %w", err)
	}

	temporary, err := os.CreateTemp(resolvedDirectory, "."+filepath.Base(resolvedOutput)+".tmp-")
	if err != nil {
		return fmt.Errorf("create private output: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		if temporary != nil {
			if err := temporary.Close(); resultErr == nil && err != nil {
				resultErr = fmt.Errorf("close private output: %w", err)
			}
		}
		if temporaryName != "" {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set private output mode: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write private output: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync private output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		temporary = nil
		return fmt.Errorf("close private output: %w", err)
	}
	temporary = nil
	if err := os.Rename(temporaryName, resolvedOutput); err != nil {
		return fmt.Errorf("publish private output: %w", err)
	}
	temporaryName = ""
	directory, err := os.Open(resolvedDirectory)
	if err != nil {
		return fmt.Errorf("open output directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync output directory: %w", err)
	}
	return nil
}
