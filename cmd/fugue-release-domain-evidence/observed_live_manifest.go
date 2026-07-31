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
		minimal, err := minimizeObservedKubernetesWorkload(item)
		if err != nil {
			return nil, fmt.Errorf("Kubernetes workload list item %d: %w", index, err)
		}
		if index != 0 {
			output.WriteString("\n---\n")
		}
		output.Write(minimal)
		output.WriteByte('\n')
		if output.Len() > observedLiveInputLimit {
			return nil, fmt.Errorf("expanded Kubernetes workload list exceeds limit")
		}
	}
	return output.Bytes(), nil
}

// minimizeObservedKubernetesWorkload deliberately discards API-server
// defaults, status, managedFields, annotations, resources, and every other
// non-image value. Those live fields are neither Helm source of truth nor
// release authorization. Keeping only identity plus named container images
// also bounds the observation before the canonical manifest parser sees it.
func minimizeObservedKubernetesWorkload(data []byte) ([]byte, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("decode workload: %w", err)
	}
	var apiVersion, kind string
	if err := json.Unmarshal(object["apiVersion"], &apiVersion); err != nil || apiVersion == "" {
		return nil, fmt.Errorf("workload apiVersion is invalid")
	}
	if err := json.Unmarshal(object["kind"], &kind); err != nil || kind == "" {
		return nil, fmt.Errorf("workload kind is invalid")
	}
	expectedVersion := map[string]string{
		"Deployment": "apps/v1", "DaemonSet": "apps/v1", "StatefulSet": "apps/v1",
		"CronJob": "batch/v1",
	}[kind]
	if expectedVersion == "" || apiVersion != expectedVersion {
		return nil, fmt.Errorf("workload identity is unsupported")
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(object["metadata"], &metadata); err != nil {
		return nil, fmt.Errorf("workload metadata is invalid")
	}
	var name, namespace string
	if err := json.Unmarshal(metadata["name"], &name); err != nil || name == "" {
		return nil, fmt.Errorf("workload name is invalid")
	}
	if err := json.Unmarshal(metadata["namespace"], &namespace); err != nil || namespace == "" {
		return nil, fmt.Errorf("workload namespace is invalid")
	}

	var spec map[string]json.RawMessage
	if err := json.Unmarshal(object["spec"], &spec); err != nil {
		return nil, fmt.Errorf("workload spec is invalid")
	}
	var podSpec map[string]json.RawMessage
	if kind == "CronJob" {
		jobTemplate, err := observedNestedJSONMap(spec, "jobTemplate")
		if err != nil {
			return nil, err
		}
		jobSpec, err := observedNestedJSONMap(jobTemplate, "spec")
		if err != nil {
			return nil, err
		}
		template, err := observedNestedJSONMap(jobSpec, "template")
		if err != nil {
			return nil, err
		}
		podSpec, err = observedNestedJSONMap(template, "spec")
		if err != nil {
			return nil, err
		}
	} else {
		template, err := observedNestedJSONMap(spec, "template")
		if err != nil {
			return nil, err
		}
		podSpec, err = observedNestedJSONMap(template, "spec")
		if err != nil {
			return nil, err
		}
	}
	minimalPodSpec, err := minimizeObservedPodSpec(podSpec)
	if err != nil {
		return nil, err
	}
	minimal := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": namespace},
	}
	if kind == "CronJob" {
		minimal["spec"] = map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{
			"template": map[string]any{"spec": minimalPodSpec},
		}}}
	} else {
		minimal["spec"] = map[string]any{"template": map[string]any{"spec": minimalPodSpec}}
	}
	encoded, err := json.Marshal(minimal)
	if err != nil {
		return nil, fmt.Errorf("encode minimized workload: %w", err)
	}
	return encoded, nil
}

func observedNestedJSONMap(parent map[string]json.RawMessage, field string) (map[string]json.RawMessage, error) {
	var child map[string]json.RawMessage
	if err := json.Unmarshal(parent[field], &child); err != nil {
		return nil, fmt.Errorf("workload %s is invalid", field)
	}
	return child, nil
}

func minimizeObservedPodSpec(podSpec map[string]json.RawMessage) (map[string]any, error) {
	result := map[string]any{}
	for _, field := range []string{"containers", "initContainers"} {
		raw, exists := podSpec[field]
		if !exists {
			if field == "containers" {
				return nil, fmt.Errorf("workload containers are missing")
			}
			continue
		}
		var containers []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &containers); err != nil || len(containers) == 0 {
			return nil, fmt.Errorf("workload %s are invalid", field)
		}
		minimalContainers := make([]any, 0, len(containers))
		seen := map[string]struct{}{}
		for _, container := range containers {
			var name, image string
			if err := json.Unmarshal(container["name"], &name); err != nil || name == "" {
				return nil, fmt.Errorf("workload %s name is invalid", field)
			}
			if err := json.Unmarshal(container["image"], &image); err != nil || image == "" {
				return nil, fmt.Errorf("workload %s image is invalid", field)
			}
			if _, duplicate := seen[name]; duplicate {
				return nil, fmt.Errorf("workload container name is duplicated")
			}
			seen[name] = struct{}{}
			minimalContainers = append(minimalContainers, map[string]any{"name": name, "image": image})
		}
		result[field] = minimalContainers
	}
	return result, nil
}
