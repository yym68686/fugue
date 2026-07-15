package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"fugue/internal/releaseimageconsumer"
)

type targetOptions struct {
	lockPath                string
	helmReleaseJSONPath     string
	repeatedReleaseJSONPath string
	expectedLockDigest      string
	expectedHeadSHA         string
	expectedRepository      string
	expectedRunID           string
	expectedRunAttempt      string
	releaseFullname         string
	expectedReleaseName     string
	expectedNamespace       string
	expectedLiveRevision    int
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "fugue-release-image-consumer-verify: a subcommand is required")
		return 1
	}
	if args[0] != "target-render" {
		fmt.Fprintf(stderr, "fugue-release-image-consumer-verify: unsupported subcommand %q\n", args[0])
		return 1
	}
	options, err := parseTargetFlags(args[1:], stderr)
	if err != nil {
		fmt.Fprintf(stderr, "fugue-release-image-consumer-verify: %v\n", err)
		return 1
	}
	evidence, err := releaseimageconsumer.VerifyTargetRender(
		options.lockPath,
		options.helmReleaseJSONPath,
		options.repeatedReleaseJSONPath,
		releaseimageconsumer.TargetExpectations{
			LockDigest:      options.expectedLockDigest,
			HeadSHA:         options.expectedHeadSHA,
			Repository:      options.expectedRepository,
			RunID:           options.expectedRunID,
			RunAttempt:      options.expectedRunAttempt,
			ReleaseFullname: options.releaseFullname,
			ReleaseName:     options.expectedReleaseName,
			Namespace:       options.expectedNamespace,
			LiveRevision:    options.expectedLiveRevision,
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "fugue-release-image-consumer-verify: target render verification failed: %v\n", err)
		return 1
	}
	encoded, err := releaseimageconsumer.EncodeTargetEvidence(evidence)
	if err != nil {
		fmt.Fprintf(stderr, "fugue-release-image-consumer-verify: encode target evidence: %v\n", err)
		return 1
	}
	if _, err := stdout.Write(encoded); err != nil {
		fmt.Fprintf(stderr, "fugue-release-image-consumer-verify: write target evidence: %v\n", err)
		return 1
	}
	return 0
}

func parseTargetFlags(args []string, stderr io.Writer) (targetOptions, error) {
	var options targetOptions
	seenFlags := make(map[string]struct{})
	for _, argument := range args {
		if !strings.HasPrefix(argument, "-") || argument == "-" || argument == "--" {
			continue
		}
		name := strings.TrimLeft(strings.SplitN(argument, "=", 2)[0], "-")
		if _, duplicate := seenFlags[name]; duplicate {
			return targetOptions{}, fmt.Errorf("--%s was supplied more than once", name)
		}
		seenFlags[name] = struct{}{}
	}
	flags := flag.NewFlagSet("target-render", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.lockPath, "lock", "", "canonical release-image-lock.json")
	flags.StringVar(&options.helmReleaseJSONPath, "helm-release-json", "", "first Helm upgrade dry-run JSON")
	flags.StringVar(&options.repeatedReleaseJSONPath, "repeated-helm-release-json", "", "separate repeated Helm upgrade dry-run JSON")
	flags.StringVar(&options.expectedLockDigest, "expected-lock-digest", "", "fenced release image lock digest")
	flags.StringVar(&options.expectedHeadSHA, "expected-head-sha", "", "fenced workflow head commit")
	flags.StringVar(&options.expectedRepository, "expected-repository", "", "fenced owner/repository identity")
	flags.StringVar(&options.expectedRunID, "expected-run-id", "", "fenced workflow run id")
	flags.StringVar(&options.expectedRunAttempt, "expected-run-attempt", "", "fenced workflow run attempt")
	flags.StringVar(&options.releaseFullname, "release-fullname", "", "expected rendered chart fullname")
	flags.StringVar(&options.expectedReleaseName, "expected-release-name", "", "fenced Helm release name")
	flags.StringVar(&options.expectedNamespace, "expected-namespace", "", "fenced Helm release namespace")
	flags.Func("expected-live-revision", "positive fenced live Helm revision", func(value string) error {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			return fmt.Errorf("expected-live-revision must be a positive integer")
		}
		options.expectedLiveRevision = parsed
		return nil
	})
	if err := flags.Parse(args); err != nil {
		return targetOptions{}, err
	}
	if flags.NArg() != 0 {
		return targetOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	for name, value := range map[string]string{
		"--lock":                       options.lockPath,
		"--helm-release-json":          options.helmReleaseJSONPath,
		"--repeated-helm-release-json": options.repeatedReleaseJSONPath,
		"--expected-lock-digest":       options.expectedLockDigest,
		"--expected-head-sha":          options.expectedHeadSHA,
		"--expected-repository":        options.expectedRepository,
		"--expected-run-id":            options.expectedRunID,
		"--expected-run-attempt":       options.expectedRunAttempt,
		"--release-fullname":           options.releaseFullname,
		"--expected-release-name":      options.expectedReleaseName,
		"--expected-namespace":         options.expectedNamespace,
	} {
		if strings.TrimSpace(value) == "" {
			return targetOptions{}, fmt.Errorf("%s is required", name)
		}
	}
	if options.expectedLiveRevision < 1 {
		return targetOptions{}, fmt.Errorf("--expected-live-revision is required")
	}
	return options, nil
}
