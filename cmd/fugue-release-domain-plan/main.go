package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"fugue/internal/releasedomain"
)

type bindingFlags map[string]string

func (values *bindingFlags) String() string {
	if values == nil || len(*values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*values))
	for key := range *values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+(*values)[key])
	}
	return strings.Join(parts, ",")
}

func (values *bindingFlags) Set(value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("binding must be valid UTF-8")
	}
	key, resolved, found := strings.Cut(value, "=")
	if !found || strings.TrimSpace(key) == "" || strings.TrimSpace(resolved) == "" {
		return fmt.Errorf("binding must be key=value with non-empty fields")
	}
	if *values == nil {
		*values = bindingFlags{}
	}
	if _, exists := (*values)[key]; exists {
		return fmt.Errorf("binding %q was supplied more than once", key)
	}
	(*values)[key] = resolved
	return nil
}

type cliOptions struct {
	ownershipPath       string
	changedFilesPath    string
	changedFilesZPath   string
	baseManifestPath    string
	targetManifestPath  string
	repeatedTargetPath  string
	outputPath          string
	defaultNamespace    string
	ssotBaseDigest      string
	ssotTargetDigest    string
	ssotLiveDigest      string
	ignoreHelmTestHooks bool
	bindings            bindingFlags
}

const (
	invalidInvocationMessage = "fugue-release-domain-plan: planner flags are invalid\n"
	planConstructionMessage  = "fugue-release-domain-plan: plan construction failed\n"
	planEncodingMessage      = "fugue-release-domain-plan: plan encoding failed\n"
	planPublicationMessage   = "fugue-release-domain-plan: plan publication failed\n"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	options, err := parseFlags(args)
	if err != nil {
		_, _ = io.WriteString(stderr, invalidInvocationMessage)
		return 1
	}
	plan, err := buildPlan(options)
	if err != nil {
		_, _ = io.WriteString(stderr, planConstructionMessage)
		return 1
	}
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		_, _ = io.WriteString(stderr, planEncodingMessage)
		return 1
	}
	encoded = append(encoded, '\n')
	if options.outputPath == "" || options.outputPath == "-" {
		if _, err := stdout.Write(encoded); err != nil {
			_, _ = io.WriteString(stderr, planPublicationMessage)
			return 1
		}
	} else if err := writePrivateAtomicFile(options.outputPath, encoded, plannerInputPaths(options)...); err != nil {
		_, _ = io.WriteString(stderr, planPublicationMessage)
		return 1
	}
	if plan.Result == releasedomain.OutcomeMultiple || plan.Result == releasedomain.OutcomeUnknown {
		return 2
	}
	return 0
}

func plannerInputPaths(options cliOptions) []string {
	changedPath := options.changedFilesPath
	if changedPath == "" {
		changedPath = options.changedFilesZPath
	}
	return []string{
		options.ownershipPath,
		changedPath,
		options.baseManifestPath,
		options.targetManifestPath,
		options.repeatedTargetPath,
	}
}

func parseFlags(args []string) (cliOptions, error) {
	var options cliOptions
	flags := flag.NewFlagSet("fugue-release-domain-plan", flag.ContinueOnError)
	// flag.FlagSet otherwise echoes raw flag names and values on parse errors.
	// Planner inputs are private release evidence, so only run's fixed failure
	// class is allowed to cross the CLI boundary.
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.ownershipPath, "ownership", "deploy/release-domains/ownership-v1.yaml", "ownership YAML")
	flags.StringVar(&options.changedFilesPath, "changed-files", "", "enriched changed-files JSON")
	flags.StringVar(&options.changedFilesZPath, "changed-files-z", "", "NUL-delimited git --name-status input")
	flags.StringVar(&options.baseManifestPath, "base-manifest", "", "base rendered manifest")
	flags.StringVar(&options.targetManifestPath, "target-manifest", "", "target rendered manifest")
	flags.StringVar(&options.repeatedTargetPath, "repeated-target-manifest", "", "independently repeated target render")
	flags.StringVar(&options.outputPath, "output", "-", "output release-domain-plan.json path or -")
	flags.StringVar(&options.defaultNamespace, "namespace", "", "effective release namespace for manifests that omit metadata.namespace")
	flags.StringVar(&options.ssotBaseDigest, "base-digest", "", "opaque SSoT base digest")
	flags.StringVar(&options.ssotTargetDigest, "target-digest", "", "opaque SSoT target digest")
	flags.StringVar(&options.ssotLiveDigest, "live-digest", "", "opaque SSoT live digest")
	flags.BoolVar(&options.ignoreHelmTestHooks, "ignore-helm-test-hooks", false, "record and ignore Helm test hooks that the real upgrade does not execute")
	flags.Var(&options.bindings, "binding", "resolved ownership binding key=value (repeatable)")
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if flags.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if (options.changedFilesPath == "") == (options.changedFilesZPath == "") {
		return cliOptions{}, fmt.Errorf("exactly one of --changed-files or --changed-files-z is required")
	}
	for name, value := range map[string]string{
		"--ownership":                options.ownershipPath,
		"--base-manifest":            options.baseManifestPath,
		"--target-manifest":          options.targetManifestPath,
		"--repeated-target-manifest": options.repeatedTargetPath,
		"--base-digest":              options.ssotBaseDigest,
		"--target-digest":            options.ssotTargetDigest,
		"--live-digest":              options.ssotLiveDigest,
	} {
		if strings.TrimSpace(value) == "" {
			return cliOptions{}, fmt.Errorf("%s is required", name)
		}
		if !utf8.ValidString(value) {
			return cliOptions{}, fmt.Errorf("%s must be valid UTF-8", name)
		}
	}
	for name, value := range map[string]string{
		"--namespace":     options.defaultNamespace,
		"--base-digest":   options.ssotBaseDigest,
		"--target-digest": options.ssotTargetDigest,
		"--live-digest":   options.ssotLiveDigest,
	} {
		if !utf8.ValidString(value) {
			return cliOptions{}, fmt.Errorf("%s must be valid UTF-8", name)
		}
	}
	if options.defaultNamespace != "" {
		if current := options.bindings["releaseNamespace"]; current != "" && current != options.defaultNamespace {
			return cliOptions{}, fmt.Errorf("--namespace and releaseNamespace binding differ")
		}
		if options.bindings == nil {
			options.bindings = bindingFlags{}
		}
		options.bindings["releaseNamespace"] = options.defaultNamespace
	} else {
		options.defaultNamespace = options.bindings["releaseNamespace"]
	}
	return options, nil
}

func buildPlan(options cliOptions) (releasedomain.Plan, error) {
	ownershipData, err := os.ReadFile(options.ownershipPath)
	if err != nil {
		return releasedomain.Plan{}, fmt.Errorf("read ownership: %w", err)
	}
	spec, err := releasedomain.LoadOwnership(strings.NewReader(string(ownershipData)))
	if err != nil {
		return releasedomain.Plan{}, err
	}

	changedPath := options.changedFilesPath
	if changedPath == "" {
		changedPath = options.changedFilesZPath
	}
	changedData, err := os.ReadFile(changedPath)
	if err != nil {
		return releasedomain.Plan{}, fmt.Errorf("read changed files: %w", err)
	}
	var changes []releasedomain.ChangedFile
	if options.changedFilesPath != "" {
		changes, err = releasedomain.DecodeChangedFilesJSON(strings.NewReader(string(changedData)))
	} else {
		changes, err = releasedomain.ParseNameStatusZ(strings.NewReader(string(changedData)))
	}
	if err != nil {
		return releasedomain.Plan{}, err
	}

	baseManifest, err := os.ReadFile(options.baseManifestPath)
	if err != nil {
		return releasedomain.Plan{}, fmt.Errorf("read base manifest: %w", err)
	}
	targetManifest, err := os.ReadFile(options.targetManifestPath)
	if err != nil {
		return releasedomain.Plan{}, fmt.Errorf("read target manifest: %w", err)
	}
	repeatedTarget, err := os.ReadFile(options.repeatedTargetPath)
	if err != nil {
		return releasedomain.Plan{}, fmt.Errorf("read repeated target manifest: %w", err)
	}
	classificationContext, err := releasedomain.NewClassificationContextEvidence(
		options.defaultNamespace,
		map[string]string(options.bindings),
		options.ignoreHelmTestHooks,
	)
	if err != nil {
		return releasedomain.Plan{}, fmt.Errorf("classification context: %w", err)
	}

	files := releasedomain.ClassifyFiles(changes, spec)
	// ClassifyRendered accepts one options value for the base/target pair. Build
	// it solely from the persisted context so neither side can silently use a
	// different namespace, binding set, or hook policy.
	rendered := releasedomain.ClassifyRendered(baseManifest, targetManifest, spec, releasedomain.RenderedOptions{
		DefaultNamespace:    classificationContext.DefaultNamespace,
		Bindings:            classificationContext.BindingMap(),
		IgnoreHelmTestHooks: classificationContext.IgnoreHelmTestHooks,
	})
	return releasedomain.BuildPlan(releasedomain.PlanInput{
		Files:    files,
		Rendered: rendered,
		Digests: releasedomain.DigestEvidence{
			Base:                   options.ssotBaseDigest,
			Target:                 options.ssotTargetDigest,
			Live:                   options.ssotLiveDigest,
			BaseManifest:           digestBytes(baseManifest),
			TargetManifest:         digestBytes(targetManifest),
			RepeatedTargetManifest: digestBytes(repeatedTarget),
			Ownership:              digestBytes(ownershipData),
			ChangedFiles:           digestBytes(changedData),
			ClassificationContext:  classificationContext,
		},
	}), nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
