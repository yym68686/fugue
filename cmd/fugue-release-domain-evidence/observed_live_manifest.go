package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"fugue/internal/releasedomain"
)

const (
	observedLiveArgumentsError = "fugue-release-domain-evidence observed-live-manifest: invalid arguments"
	observedLiveInputError     = "fugue-release-domain-evidence observed-live-manifest: input evidence is invalid"
	observedLiveBuildError     = "fugue-release-domain-evidence observed-live-manifest: construction failed"
	observedLiveOutputError    = "fugue-release-domain-evidence observed-live-manifest: output failed"
	observedLiveInputLimit     = 32 << 20
)

type observedLiveOptions struct {
	baseManifestPath  string
	liveWorkloadsPath string
	ownershipPath     string
	defaultNamespace  string
	outputPath        string
}

func runObservedLiveManifest(args []string, _ io.Writer, stderr io.Writer) int {
	options, err := parseObservedLiveFlags(args)
	if err != nil {
		fmt.Fprintln(stderr, observedLiveArgumentsError)
		return 1
	}
	base, baseResolved, err := readBoundedRegularFile(options.baseManifestPath, canonicalManifestInputLimit, true)
	if err != nil {
		fmt.Fprintln(stderr, observedLiveInputError)
		return 1
	}
	liveList, liveResolved, err := readBoundedRegularFile(options.liveWorkloadsPath, observedLiveInputLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, observedLiveInputError)
		return 1
	}
	ownership, ownershipResolved, err := readBoundedRegularFile(options.ownershipPath, canonicalOwnershipLimit, false)
	if err != nil {
		fmt.Fprintln(stderr, observedLiveInputError)
		return 1
	}
	liveWorkloads, err := expandObservedKubernetesList(liveList)
	if err != nil {
		fmt.Fprintln(stderr, observedLiveInputError)
		return 1
	}
	observed, err := releasedomain.MaterializeObservedLiveImageManifest(
		base, liveWorkloads, ownership, options.defaultNamespace,
	)
	if err != nil {
		fmt.Fprintln(stderr, observedLiveBuildError)
		return 1
	}
	if err := writePrivateAtomicFile(
		options.outputPath, observed, baseResolved, liveResolved, ownershipResolved,
	); err != nil {
		fmt.Fprintln(stderr, observedLiveOutputError)
		return 1
	}
	return 0
}

func parseObservedLiveFlags(args []string) (observedLiveOptions, error) {
	allowed := map[string]bool{
		"base-manifest":  false,
		"live-workloads": false,
		"ownership":      false,
		"namespace":      false,
		"output":         false,
	}
	seen := map[string]struct{}{}
	for _, argument := range args {
		if strings.HasPrefix(argument, "-") && !strings.HasPrefix(argument, "--") {
			return observedLiveOptions{}, fmt.Errorf("only canonical --long flags are accepted")
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
			return observedLiveOptions{}, fmt.Errorf("duplicate flag --%s", name)
		}
		seen[name] = struct{}{}
	}

	var options observedLiveOptions
	flags := flag.NewFlagSet("observed-live-manifest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.baseManifestPath, "base-manifest", "", "private canonical Helm base manifest")
	flags.StringVar(&options.liveWorkloadsPath, "live-workloads", "", "private kubectl workload List JSON")
	flags.StringVar(&options.ownershipPath, "ownership", "", "release-domain ownership document")
	flags.StringVar(&options.defaultNamespace, "namespace", "", "trusted release namespace")
	flags.StringVar(&options.outputPath, "output", "", "private canonical observed-live manifest")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return observedLiveOptions{}, fmt.Errorf("invalid flags")
	}
	for name, value := range map[string]string{
		"--base-manifest":  options.baseManifestPath,
		"--live-workloads": options.liveWorkloadsPath,
		"--ownership":      options.ownershipPath,
		"--namespace":      options.defaultNamespace,
		"--output":         options.outputPath,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return observedLiveOptions{}, fmt.Errorf("%s is required without surrounding whitespace", name)
		}
	}
	if options.outputPath == "-" {
		return observedLiveOptions{}, fmt.Errorf("--output must be a file path")
	}
	return options, nil
}

func expandObservedKubernetesList(data []byte) ([]byte, error) {
	var document struct {
		APIVersion string                     `json:"apiVersion"`
		Kind       string                     `json:"kind"`
		Metadata   map[string]json.RawMessage `json:"metadata"`
		Items      []json.RawMessage          `json:"items"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode Kubernetes workload list: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("Kubernetes workload list contains trailing data")
	}
	if document.APIVersion != "v1" || document.Kind != "List" || document.Items == nil {
		return nil, fmt.Errorf("Kubernetes workload list identity is invalid")
	}
	var output bytes.Buffer
	for index, item := range document.Items {
		item = bytes.TrimSpace(item)
		if len(item) < 2 || item[0] != '{' || item[len(item)-1] != '}' || !json.Valid(item) {
			return nil, fmt.Errorf("Kubernetes workload list item %d is invalid", index)
		}
		if index != 0 {
			output.WriteString("\n---\n")
		}
		output.Write(item)
		output.WriteByte('\n')
		if output.Len() > observedLiveInputLimit {
			return nil, fmt.Errorf("expanded Kubernetes workload list exceeds limit")
		}
	}
	return output.Bytes(), nil
}
