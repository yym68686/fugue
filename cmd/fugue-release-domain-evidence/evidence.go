package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"fugue/internal/releasedomain"
	"gopkg.in/yaml.v3"
)

const valuesFilePattern = "deploy/helm/fugue/values*.yaml"

const (
	maxRevisionOutput    int64 = 4 << 10
	maxNameStatusOutput  int64 = 32 << 20
	maxYAMLBlobOutput    int64 = 16 << 20
	maxTreeListOutput    int64 = 32 << 20
	maxSnapshotOutput    int64 = 272 << 20
	maxGoListOutput      int64 = 64 << 20
	maxGoModJSONOutput   int64 = 4 << 20
	maxCommandStderr     int64 = 1 << 20
	maxCommandInput      int64 = 8 << 20
	maxChangedFiles            = 100_000
	maxSnapshotEntries         = 100_000
	maxSnapshotFileSize  int64 = 64 << 20
	maxExtractedBytes    int64 = 256 << 20
	maxYAMLDepth               = 128
	maxYAMLNodes               = 200_000
	maxGoListPackages          = 100_000
	maxValuePointers           = 50_000
	maxValuePointerBytes       = 4 << 20
)

var errCommandOutputLimit = errors.New("command output exceeds evidence limit")

var commandDomains = map[string]releasedomain.Domain{
	"cmd/fugue-api":         releasedomain.DomainControlPlane,
	"cmd/fugue-controller":  releasedomain.DomainControlPlane,
	"cmd/fugue-dns":         releasedomain.DomainAuthoritativeDNS,
	"cmd/fugue-image-cache": releasedomain.DomainImageCache,
}

type commandRunner interface {
	Run(ctx context.Context, directory string, environment []string, input []byte, outputLimit int64, name string, args ...string) ([]byte, error)
}

type execCommandRunner struct{}

func (execCommandRunner) Run(
	ctx context.Context,
	directory string,
	environment []string,
	input []byte,
	outputLimit int64,
	name string,
	args ...string,
) ([]byte, error) {
	if outputLimit <= 0 {
		return nil, fmt.Errorf("%s: invalid output limit %d", name, outputLimit)
	}
	if int64(len(input)) > maxCommandInput {
		return nil, fmt.Errorf("%s: command input size %d exceeds evidence limit %d", name, len(input), maxCommandInput)
	}
	commandContext, cancelCommand := context.WithCancel(ctx)
	defer cancelCommand()
	commandGroup, err := startEvidenceCommandGroup()
	if err != nil {
		return nil, fmt.Errorf("%s: create private command process group: %w", name, err)
	}
	command := exec.CommandContext(commandContext, name, args...)
	command.Dir = directory
	if environment != nil {
		command.Env = environment
	}
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	configureEvidenceCommand(command, commandGroup)
	commandOutput := &limitedBuffer{limit: outputLimit, onLimit: cancelCommand}
	commandStderr := &limitedBuffer{limit: maxCommandStderr, onLimit: cancelCommand}
	command.Stdout = commandOutput
	command.Stderr = commandStderr
	if err := command.Start(); err != nil {
		cleanupErr := cleanupEvidenceCommandGroup(commandGroup)
		if cleanupErr != nil {
			return nil, fmt.Errorf("%s: %w (process-group cleanup: %v)", name, err, cleanupErr)
		}
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	err = command.Wait()
	cleanupErr := cleanupEvidenceCommandGroup(commandGroup)
	if commandOutput.Exceeded() || commandStderr.Exceeded() {
		return nil, fmt.Errorf("%s: %w", name, errCommandOutputLimit)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("%s: %w", name, ctxErr)
	}
	if cleanupErr != nil {
		return nil, fmt.Errorf("%s: clean command process group: %w", name, cleanupErr)
	}
	if err == nil {
		return append([]byte(nil), commandOutput.Bytes()...), nil
	}
	detail := strings.TrimSpace(commandStderr.String())
	if detail == "" {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return nil, fmt.Errorf("%s: %w: %s", name, err, detail)
}

type limitedBuffer struct {
	buffer   bytes.Buffer
	limit    int64
	onLimit  func()
	exceeded atomic.Bool
}

func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	remaining := buffer.limit - int64(buffer.buffer.Len())
	if remaining <= 0 {
		buffer.markExceeded()
		return 0, errCommandOutputLimit
	}
	if int64(len(data)) > remaining {
		written, _ := buffer.buffer.Write(data[:remaining])
		buffer.markExceeded()
		return written, errCommandOutputLimit
	}
	return buffer.buffer.Write(data)
}

func (buffer *limitedBuffer) Bytes() []byte { return buffer.buffer.Bytes() }

func (buffer *limitedBuffer) String() string { return buffer.buffer.String() }

func (buffer *limitedBuffer) Exceeded() bool { return buffer.exceeded.Load() }

func (buffer *limitedBuffer) markExceeded() {
	if buffer.exceeded.CompareAndSwap(false, true) && buffer.onLimit != nil {
		buffer.onLimit()
	}
}

type evidenceResult struct {
	baseCommit   string
	targetCommit string
	changes      []releasedomain.ChangedFile
}

func produceEvidence(ctx context.Context, options cliOptions, runner commandRunner) (evidenceResult, []string, error) {
	repository, err := filepath.Abs(options.repository)
	if err != nil {
		return evidenceResult{}, nil, fmt.Errorf("resolve repository: %w", err)
	}
	info, err := os.Stat(repository)
	if err != nil {
		return evidenceResult{}, nil, fmt.Errorf("stat repository: %w", err)
	}
	if !info.IsDir() {
		return evidenceResult{}, nil, fmt.Errorf("repository %s is not a directory", repository)
	}

	baseObject, err := resolveCommit(ctx, runner, repository, options.baseRevision)
	if err != nil {
		return evidenceResult{}, nil, fmt.Errorf("resolve base revision: %w", err)
	}
	targetObject, err := resolveCommit(ctx, runner, repository, options.targetRevision)
	if err != nil {
		return evidenceResult{}, nil, fmt.Errorf("resolve target revision: %w", err)
	}
	nameStatus, err := runner.Run(
		ctx,
		repository,
		gitEvidenceEnvironment(),
		nil,
		maxNameStatusOutput,
		"git",
		"diff",
		"--no-ext-diff",
		"--no-renames",
		"--ignore-submodules=none",
		"--name-status",
		"-z",
		baseObject,
		targetObject,
		"--",
	)
	if err != nil {
		return evidenceResult{}, nil, fmt.Errorf("read changed paths: %w", err)
	}
	changes, err := releasedomain.ParseNameStatusZ(bytes.NewReader(nameStatus))
	if err != nil {
		return evidenceResult{}, nil, err
	}
	if len(changes) > maxChangedFiles {
		return evidenceResult{}, nil, fmt.Errorf("changed path count %d exceeds limit %d", len(changes), maxChangedFiles)
	}
	if changes == nil {
		changes = make([]releasedomain.ChangedFile, 0)
	}
	sort.Slice(changes, func(left, right int) bool {
		if changes[left].Path != changes[right].Path {
			return changes[left].Path < changes[right].Path
		}
		return changes[left].Status < changes[right].Status
	})

	warnings := make([]string, 0)
	enrichValuesEvidence(ctx, runner, repository, baseObject, targetObject, changes, &warnings)
	enrichGoEvidence(ctx, runner, repository, baseObject, targetObject, options.goBinary, changes, &warnings)
	sort.Strings(warnings)
	warnings = compactStrings(warnings)
	return evidenceResult{baseCommit: baseObject, targetCommit: targetObject, changes: changes}, warnings, nil
}

func resolveCommit(ctx context.Context, runner commandRunner, repository, revision string) (string, error) {
	output, err := runner.Run(
		ctx,
		repository,
		gitEvidenceEnvironment(),
		nil,
		maxRevisionOutput,
		"git",
		"rev-parse",
		"--verify",
		"--end-of-options",
		revision+"^{commit}",
	)
	if err != nil {
		return "", err
	}
	object := strings.TrimSpace(string(output))
	if !validGitObjectID(object) {
		return "", fmt.Errorf("git returned an invalid commit object %q", object)
	}
	return object, nil
}

func validGitObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

type yamlDocument struct {
	exists bool
	value  any
}

func enrichValuesEvidence(
	ctx context.Context,
	runner commandRunner,
	repository string,
	baseObject string,
	targetObject string,
	changes []releasedomain.ChangedFile,
	warnings *[]string,
) {
	for index := range changes {
		change := &changes[index]
		matched, err := path.Match(valuesFilePattern, change.Path)
		if err != nil || !matched {
			continue
		}
		base, target, err := loadChangedYAMLDocuments(ctx, runner, repository, baseObject, targetObject, *change)
		if err != nil {
			*warnings = append(*warnings, fmt.Sprintf("%s: values leaf evidence unavailable: %v", change.Path, err))
			continue
		}
		pointers, err := changedLeafPointers(base, target)
		if err != nil {
			*warnings = append(*warnings, fmt.Sprintf("%s: values leaf evidence unavailable: %v", change.Path, err))
			continue
		}
		change.ValuePointers = pointers
	}
}

func loadChangedYAMLDocuments(
	ctx context.Context,
	runner commandRunner,
	repository string,
	baseObject string,
	targetObject string,
	change releasedomain.ChangedFile,
) (yamlDocument, yamlDocument, error) {
	var base yamlDocument
	var target yamlDocument
	var err error
	switch change.Status {
	case releasedomain.ChangeAdded:
		target, err = loadYAMLDocument(ctx, runner, repository, targetObject, change.Path)
	case releasedomain.ChangeDeleted:
		base, err = loadYAMLDocument(ctx, runner, repository, baseObject, change.Path)
	case releasedomain.ChangeModified:
		base, err = loadYAMLDocument(ctx, runner, repository, baseObject, change.Path)
		if err == nil {
			target, err = loadYAMLDocument(ctx, runner, repository, targetObject, change.Path)
		}
	default:
		err = fmt.Errorf("unsupported change status %q", change.Status)
	}
	if err != nil {
		return yamlDocument{}, yamlDocument{}, err
	}
	return base, target, nil
}

func loadYAMLDocument(
	ctx context.Context,
	runner commandRunner,
	repository string,
	object string,
	repositoryPath string,
) (yamlDocument, error) {
	data, err := runner.Run(
		ctx,
		repository,
		gitEvidenceEnvironment(),
		nil,
		maxYAMLBlobOutput,
		"git",
		"cat-file",
		"blob",
		object+":"+repositoryPath,
	)
	if err != nil {
		return yamlDocument{}, err
	}
	value, err := decodeYAMLDocument(data)
	if err != nil {
		return yamlDocument{}, err
	}
	return yamlDocument{exists: true, value: value}, nil
}

func decodeYAMLDocument(data []byte) (any, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var value any
	if err := decoder.Decode(&value); err != nil && err != io.EOF {
		return nil, fmt.Errorf("decode YAML: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("decode trailing YAML: %w", err)
		}
		return nil, fmt.Errorf("values file must contain exactly one YAML document")
	}
	return normalizeYAMLValue(value)
}

func normalizeYAMLValue(value any) (any, error) {
	nodes := 0
	return normalizeYAMLValueAtDepth(value, 0, &nodes)
}

func normalizeYAMLValueAtDepth(value any, depth int, nodes *int) (any, error) {
	if depth > maxYAMLDepth {
		return nil, fmt.Errorf("YAML nesting exceeds limit %d", maxYAMLDepth)
	}
	*nodes++
	if *nodes > maxYAMLNodes {
		return nil, fmt.Errorf("YAML node count exceeds limit %d", maxYAMLNodes)
	}
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			normalized, err := normalizeYAMLValueAtDepth(child, depth+1, nodes)
			if err != nil {
				return nil, err
			}
			result[key] = normalized
		}
		return result, nil
	case map[any]any:
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			stringKey, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("YAML mapping key %v is not a string", key)
			}
			normalized, err := normalizeYAMLValueAtDepth(child, depth+1, nodes)
			if err != nil {
				return nil, err
			}
			result[stringKey] = normalized
		}
		return result, nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			normalized, err := normalizeYAMLValueAtDepth(child, depth+1, nodes)
			if err != nil {
				return nil, err
			}
			result[index] = normalized
		}
		return result, nil
	default:
		return value, nil
	}
}

type pointerCollector struct {
	values map[string]struct{}
	bytes  int
	err    error
}

func newPointerCollector() *pointerCollector {
	return &pointerCollector{values: map[string]struct{}{}}
}

func (collector *pointerCollector) add(pointer string) {
	if collector.err != nil {
		return
	}
	if _, exists := collector.values[pointer]; exists {
		return
	}
	if len(collector.values) >= maxValuePointers {
		collector.err = fmt.Errorf("changed values pointer count exceeds limit %d", maxValuePointers)
		return
	}
	if len(pointer) > maxValuePointerBytes-collector.bytes {
		collector.err = fmt.Errorf("changed values pointer bytes exceed limit %d", maxValuePointerBytes)
		return
	}
	collector.values[pointer] = struct{}{}
	collector.bytes += len(pointer)
}

func changedLeafPointers(base, target yamlDocument) ([]string, error) {
	pointers := newPointerCollector()
	switch {
	case !base.exists && !target.exists:
		return nil, nil
	case !base.exists:
		collectLeafPointers(target.value, "", pointers)
	case !target.exists:
		collectLeafPointers(base.value, "", pointers)
	default:
		diffYAMLValues(base.value, target.value, "", pointers)
	}
	if pointers.err != nil {
		return nil, pointers.err
	}
	result := make([]string, 0, len(pointers.values))
	for pointer := range pointers.values {
		result = append(result, pointer)
	}
	sort.Strings(result)
	return result, nil
}

func diffYAMLValues(base, target any, pointer string, pointers *pointerCollector) {
	if pointers.err != nil {
		return
	}
	if reflect.DeepEqual(base, target) {
		return
	}
	baseMap, baseIsMap := base.(map[string]any)
	targetMap, targetIsMap := target.(map[string]any)
	if baseIsMap && targetIsMap {
		keys := make([]string, 0, len(baseMap)+len(targetMap))
		seen := map[string]struct{}{}
		for key := range baseMap {
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		for key := range targetMap {
			if _, ok := seen[key]; !ok {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			baseChild, baseOK := baseMap[key]
			targetChild, targetOK := targetMap[key]
			childPointer := appendJSONPointer(pointer, key)
			switch {
			case !baseOK:
				collectLeafPointers(targetChild, childPointer, pointers)
			case !targetOK:
				collectLeafPointers(baseChild, childPointer, pointers)
			default:
				diffYAMLValues(baseChild, targetChild, childPointer, pointers)
			}
		}
		return
	}
	baseList, baseIsList := base.([]any)
	targetList, targetIsList := target.([]any)
	if baseIsList && targetIsList {
		limit := len(baseList)
		if len(targetList) > limit {
			limit = len(targetList)
		}
		for index := 0; index < limit; index++ {
			childPointer := appendJSONPointer(pointer, fmt.Sprintf("%d", index))
			switch {
			case index >= len(baseList):
				collectLeafPointers(targetList[index], childPointer, pointers)
			case index >= len(targetList):
				collectLeafPointers(baseList[index], childPointer, pointers)
			default:
				diffYAMLValues(baseList[index], targetList[index], childPointer, pointers)
			}
		}
		return
	}
	collectLeafPointers(base, pointer, pointers)
	collectLeafPointers(target, pointer, pointers)
}

func collectLeafPointers(value any, pointer string, pointers *pointerCollector) {
	if pointers.err != nil {
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			pointers.add(pointer)
			return
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			collectLeafPointers(typed[key], appendJSONPointer(pointer, key), pointers)
		}
	case []any:
		if len(typed) == 0 {
			pointers.add(pointer)
			return
		}
		for index, child := range typed {
			collectLeafPointers(child, appendJSONPointer(pointer, fmt.Sprintf("%d", index)), pointers)
		}
	default:
		pointers.add(pointer)
	}
}

func appendJSONPointer(pointer, token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	token = strings.ReplaceAll(token, "/", "~1")
	return pointer + "/" + token
}

type commandConsumer struct {
	path   string
	domain releasedomain.Domain
}

type consumerGraph struct {
	modulePath string
	consumers  map[string][]commandConsumer
}

type goListPackage struct {
	Dir        string   `json:"Dir"`
	ImportPath string   `json:"ImportPath"`
	Name       string   `json:"Name"`
	Deps       []string `json:"Deps"`
}

func enrichGoEvidence(
	ctx context.Context,
	runner commandRunner,
	repository string,
	baseObject string,
	targetObject string,
	goBinary string,
	changes []releasedomain.ChangedFile,
	warnings *[]string,
) {
	needBase := false
	needTarget := false
	for _, change := range changes {
		if !goChangeNeedsConsumerEvidence(change.Path) {
			continue
		}
		switch change.Status {
		case releasedomain.ChangeAdded:
			needTarget = true
		case releasedomain.ChangeDeleted:
			needBase = true
		case releasedomain.ChangeModified:
			needBase = true
			needTarget = true
		}
	}
	if !needBase && !needTarget {
		return
	}

	var baseGraph *consumerGraph
	var targetGraph *consumerGraph
	if needBase {
		graph, err := buildConsumerGraph(ctx, runner, repository, baseObject, goBinary)
		if err != nil {
			*warnings = append(*warnings, "base Go consumer graph unavailable: "+err.Error())
		} else {
			baseGraph = graph
		}
	}
	if needTarget {
		graph, err := buildConsumerGraph(ctx, runner, repository, targetObject, goBinary)
		if err != nil {
			*warnings = append(*warnings, "target Go consumer graph unavailable: "+err.Error())
		} else {
			targetGraph = graph
		}
	}

	for index := range changes {
		change := &changes[index]
		if !goChangeNeedsConsumerEvidence(change.Path) {
			continue
		}
		graphs := make([]*consumerGraph, 0, 2)
		complete := true
		switch change.Status {
		case releasedomain.ChangeAdded:
			complete = targetGraph != nil
			graphs = append(graphs, targetGraph)
		case releasedomain.ChangeDeleted:
			complete = baseGraph != nil
			graphs = append(graphs, baseGraph)
		case releasedomain.ChangeModified:
			complete = baseGraph != nil && targetGraph != nil
			graphs = append(graphs, baseGraph, targetGraph)
		default:
			complete = false
		}
		if !complete {
			continue
		}
		change.ConsumerDomains, change.OutsideConsumers = consumerEvidence(change.Path, graphs...)
	}
}

func goChangeNeedsConsumerEvidence(repositoryPath string) bool {
	return strings.HasSuffix(repositoryPath, ".go") && !strings.HasSuffix(repositoryPath, "_test.go")
}

func buildConsumerGraph(
	ctx context.Context,
	runner commandRunner,
	repository string,
	object string,
	goBinary string,
) (*consumerGraph, error) {
	snapshot, err := os.MkdirTemp("", "fugue-release-domain-evidence-")
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}
	cleanupPath := snapshot
	defer os.RemoveAll(cleanupPath)
	snapshot, err = filepath.EvalSymlinks(snapshot)
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot: %w", err)
	}
	if err := extractRepositoryTree(ctx, runner, repository, object, snapshot); err != nil {
		return nil, fmt.Errorf("extract revision: %w", err)
	}
	offlineEnvironment, cleanupOfflineEnvironment, err := buildOfflineGoEnvironment(
		ctx,
		runner,
		snapshot,
		goBinary,
	)
	if err != nil {
		return nil, err
	}
	defer cleanupOfflineEnvironment()
	if err := rejectLocalModuleReplacements(ctx, runner, snapshot, offlineEnvironment, goBinary); err != nil {
		return nil, err
	}

	var combined *consumerGraph
	for _, platform := range []struct {
		goos   string
		goarch string
	}{
		{goos: "linux", goarch: "amd64"},
		{goos: "linux", goarch: "arm64"},
	} {
		environment := withEnvironment(offlineEnvironment, map[string]string{
			"CGO_ENABLED": "0",
			"GOARCH":      platform.goarch,
			"GOOS":        platform.goos,
		})
		output, err := runner.Run(
			ctx,
			snapshot,
			environment,
			nil,
			maxGoListOutput,
			goBinary,
			"list",
			"-mod=readonly",
			"-buildvcs=false",
			"-json",
			"./cmd/...",
		)
		if err != nil {
			return nil, fmt.Errorf("list %s/%s command packages: %w", platform.goos, platform.goarch, err)
		}
		graph, err := decodeConsumerGraph(snapshot, output)
		if err != nil {
			return nil, fmt.Errorf("decode %s/%s command packages: %w", platform.goos, platform.goarch, err)
		}
		combined, err = mergeConsumerGraphs(combined, graph)
		if err != nil {
			return nil, err
		}
	}
	return combined, nil
}

type goModEditJSON struct {
	Replace []struct {
		Old struct {
			Path    string `json:"Path"`
			Version string `json:"Version"`
		} `json:"Old"`
		New struct {
			Path    string `json:"Path"`
			Version string `json:"Version"`
		} `json:"New"`
	} `json:"Replace"`
}

func rejectLocalModuleReplacements(
	ctx context.Context,
	runner commandRunner,
	directory string,
	environment []string,
	goBinary string,
) error {
	output, err := runner.Run(
		ctx,
		directory,
		environment,
		nil,
		maxGoModJSONOutput,
		goBinary,
		"mod",
		"edit",
		"-json",
	)
	if err != nil {
		return fmt.Errorf("inspect revision go.mod: %w", err)
	}
	var edited goModEditJSON
	if err := json.Unmarshal(output, &edited); err != nil {
		return fmt.Errorf("decode revision go.mod: %w", err)
	}
	for _, replacement := range edited.Replace {
		if replacement.New.Version == "" {
			return fmt.Errorf(
				"revision go.mod contains unsupported local replacement %s => %s",
				replacement.Old.Path,
				replacement.New.Path,
			)
		}
	}
	return nil
}

func buildOfflineGoEnvironment(
	ctx context.Context,
	runner commandRunner,
	directory string,
	goBinary string,
) ([]string, func(), error) {
	discoveryEnvironment := withEnvironment(withoutEnvironmentPrefix(os.Environ(), "GO"), map[string]string{
		"GO111MODULE": "on",
		"GOAMD64":     "v1",
		"GOARM64":     "v8.0",
		"GOENV":       "off",
		"GOFLAGS":     "",
		"GONOPROXY":   "none",
		"GONOSUMDB":   "*",
		"GOPRIVATE":   "",
		"GOPROXY":     "off",
		"GOSUMDB":     "off",
		"GOTELEMETRY": "off",
		"GOTOOLCHAIN": "local",
		"GOVCS":       "*:off",
		"GOWORK":      "off",
	})
	moduleCacheOutput, err := runner.Run(
		ctx,
		directory,
		discoveryEnvironment,
		nil,
		maxRevisionOutput,
		goBinary,
		"env",
		"GOMODCACHE",
	)
	if err != nil {
		return nil, func() {}, fmt.Errorf("locate read-only module cache: %w", err)
	}
	moduleCache := strings.TrimSpace(string(moduleCacheOutput))
	if moduleCache == "" || !filepath.IsAbs(moduleCache) || strings.ContainsAny(moduleCache, "\r\n") {
		return nil, func() {}, fmt.Errorf("go returned invalid module cache %q", moduleCache)
	}
	moduleDownloadCache := filepath.Join(moduleCache, "cache", "download")
	proxy := "off"
	if info, statErr := os.Stat(moduleDownloadCache); statErr == nil && info.IsDir() {
		proxyURL := &url.URL{Scheme: "file", Path: filepath.ToSlash(moduleDownloadCache)}
		proxy = proxyURL.String() + ",off"
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return nil, func() {}, fmt.Errorf("inspect read-only module download cache: %w", statErr)
	}

	privateRoot, err := os.MkdirTemp("", "fugue-release-domain-go-cache-")
	if err != nil {
		return nil, func() {}, fmt.Errorf("create private Go cache root: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(privateRoot) }
	privatePaths := map[string]string{
		"GOCACHE":    filepath.Join(privateRoot, "build"),
		"GOMODCACHE": filepath.Join(privateRoot, "modules"),
		"GOPATH":     filepath.Join(privateRoot, "gopath"),
		"GOTMPDIR":   filepath.Join(privateRoot, "tmp"),
		"HOME":       filepath.Join(privateRoot, "home"),
		"TMPDIR":     filepath.Join(privateRoot, "tmp"),
	}
	for _, privatePath := range privatePaths {
		if err := os.MkdirAll(privatePath, 0o700); err != nil {
			cleanup()
			return nil, func() {}, fmt.Errorf("create private Go cache directory: %w", err)
		}
	}
	replacements := map[string]string{
		"GO111MODULE":  "on",
		"GOAMD64":      "v1",
		"GOARM64":      "v8.0",
		"GOENV":        "off",
		"GOEXPERIMENT": "",
		"GOFLAGS":      "-modcacherw",
		"GONOPROXY":    "none",
		"GONOSUMDB":    "*",
		"GOPRIVATE":    "",
		"GOPROXY":      proxy,
		"GOSUMDB":      "off",
		"GOTELEMETRY":  "off",
		"GOTOOLCHAIN":  "local",
		"GOVCS":        "*:off",
		"GOWORK":       "off",
	}
	for name, value := range privatePaths {
		replacements[name] = value
	}
	return withEnvironment(withoutEnvironmentPrefix(os.Environ(), "GO"), replacements), cleanup, nil
}

func decodeConsumerGraph(snapshot string, data []byte) (*consumerGraph, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	packages := make([]goListPackage, 0)
	for {
		var listed goListPackage
		if err := decoder.Decode(&listed); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if listed.Name == "main" {
			packages = append(packages, listed)
			if len(packages) > maxGoListPackages {
				return nil, fmt.Errorf("go list package count exceeds limit %d", maxGoListPackages)
			}
		}
	}
	if len(packages) == 0 {
		return nil, fmt.Errorf("go list returned no command packages")
	}

	graph := &consumerGraph{consumers: map[string][]commandConsumer{}}
	for _, listed := range packages {
		relativeDirectory, err := filepath.Rel(snapshot, listed.Dir)
		if err != nil {
			return nil, fmt.Errorf("resolve command directory %s: %w", listed.Dir, err)
		}
		relativeDirectory = filepath.ToSlash(relativeDirectory)
		if relativeDirectory == "." || relativeDirectory == ".." || strings.HasPrefix(relativeDirectory, "../") {
			return nil, fmt.Errorf("command directory %s is outside the revision snapshot", listed.Dir)
		}
		suffix := "/" + relativeDirectory
		if !strings.HasSuffix(listed.ImportPath, suffix) {
			return nil, fmt.Errorf("command import path %s does not end in %s", listed.ImportPath, suffix)
		}
		modulePath := strings.TrimSuffix(listed.ImportPath, suffix)
		if modulePath == "" {
			return nil, fmt.Errorf("command import path %s has no module prefix", listed.ImportPath)
		}
		if graph.modulePath == "" {
			graph.modulePath = modulePath
		} else if graph.modulePath != modulePath {
			return nil, fmt.Errorf("commands span modules %s and %s", graph.modulePath, modulePath)
		}

		consumer := commandConsumer{path: relativeDirectory, domain: commandDomains[relativeDirectory]}
		dependencies := append(append([]string(nil), listed.Deps...), listed.ImportPath)
		for _, dependency := range dependencies {
			if dependency != graph.modulePath && !strings.HasPrefix(dependency, graph.modulePath+"/") {
				continue
			}
			graph.consumers[dependency] = appendUniqueConsumer(graph.consumers[dependency], consumer)
		}
	}
	return graph, nil
}

func mergeConsumerGraphs(left, right *consumerGraph) (*consumerGraph, error) {
	if left == nil {
		return right, nil
	}
	if right == nil {
		return left, nil
	}
	if left.modulePath != right.modulePath {
		return nil, fmt.Errorf("consumer graph module mismatch: %s != %s", left.modulePath, right.modulePath)
	}
	for packagePath, consumers := range right.consumers {
		for _, consumer := range consumers {
			left.consumers[packagePath] = appendUniqueConsumer(left.consumers[packagePath], consumer)
		}
	}
	return left, nil
}

func appendUniqueConsumer(consumers []commandConsumer, candidate commandConsumer) []commandConsumer {
	for _, consumer := range consumers {
		if consumer.path == candidate.path && consumer.domain == candidate.domain {
			return consumers
		}
	}
	return append(consumers, candidate)
}

func consumerEvidence(repositoryPath string, graphs ...*consumerGraph) ([]releasedomain.Domain, []string) {
	domainSet := map[releasedomain.Domain]struct{}{}
	outsideSet := map[string]struct{}{}
	packageDirectory := path.Dir(repositoryPath)
	for _, graph := range graphs {
		if graph == nil {
			continue
		}
		packagePath := graph.modulePath
		if packageDirectory != "." {
			packagePath += "/" + packageDirectory
		}
		for _, consumer := range graph.consumers[packagePath] {
			if consumer.domain == "" {
				outsideSet[consumer.path] = struct{}{}
				continue
			}
			domainSet[consumer.domain] = struct{}{}
		}
	}
	domains := make([]releasedomain.Domain, 0, len(domainSet))
	for _, domain := range releasedomain.KnownDomains() {
		if _, ok := domainSet[domain]; ok {
			domains = append(domains, domain)
		}
	}
	outside := make([]string, 0, len(outsideSet))
	for consumer := range outsideSet {
		outside = append(outside, consumer)
	}
	sort.Strings(outside)
	return domains, outside
}

type repositoryTreeEntry struct {
	mode   os.FileMode
	object string
	path   string
}

func extractRepositoryTree(
	ctx context.Context,
	runner commandRunner,
	repository string,
	object string,
	destination string,
) error {
	listing, err := runner.Run(
		ctx,
		repository,
		gitEvidenceEnvironment(),
		nil,
		maxTreeListOutput,
		"git",
		"ls-tree",
		"-r",
		"-z",
		"--full-tree",
		object,
		"--",
	)
	if err != nil {
		return fmt.Errorf("list revision tree: %w", err)
	}
	entries, err := parseRepositoryTree(listing)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	var batchInput bytes.Buffer
	for _, entry := range entries {
		batchInput.WriteString(entry.object)
		batchInput.WriteByte('\n')
	}
	batch, err := runner.Run(
		ctx,
		repository,
		gitEvidenceEnvironment(),
		batchInput.Bytes(),
		maxSnapshotOutput,
		"git",
		"cat-file",
		"--batch",
	)
	if err != nil {
		return fmt.Errorf("read revision blobs: %w", err)
	}
	return materializeRepositoryTree(ctx, destination, entries, batch)
}

func parseRepositoryTree(data []byte) ([]repositoryTreeEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if data[len(data)-1] != 0 {
		return nil, fmt.Errorf("tree listing must be NUL terminated")
	}
	records := bytes.Split(data[:len(data)-1], []byte{0})
	if len(records) > maxSnapshotEntries {
		return nil, fmt.Errorf("revision tree entry count %d exceeds limit %d", len(records), maxSnapshotEntries)
	}
	entries := make([]repositoryTreeEntry, 0, len(records))
	seenPaths := make(map[string]struct{}, len(records))
	for _, record := range records {
		metadata, pathBytes, found := bytes.Cut(record, []byte{'\t'})
		if !found || !utf8.Valid(pathBytes) {
			return nil, fmt.Errorf("revision tree contains an invalid entry")
		}
		fields := strings.Fields(string(metadata))
		if len(fields) != 3 {
			return nil, fmt.Errorf("revision tree entry has invalid metadata %q", metadata)
		}
		repositoryPath := string(pathBytes)
		cleanedPath := path.Clean(repositoryPath)
		if repositoryPath == "" || strings.Contains(repositoryPath, "\\") || path.IsAbs(repositoryPath) ||
			cleanedPath == "." || cleanedPath == ".." || strings.HasPrefix(cleanedPath, "../") || cleanedPath != repositoryPath {
			return nil, fmt.Errorf("revision tree contains invalid path %q", repositoryPath)
		}
		if _, duplicate := seenPaths[repositoryPath]; duplicate {
			return nil, fmt.Errorf("revision tree contains duplicate path %q", repositoryPath)
		}
		seenPaths[repositoryPath] = struct{}{}
		if fields[1] != "blob" || !validGitObjectID(fields[2]) {
			return nil, fmt.Errorf("revision tree path %q has unsupported object %s %s", repositoryPath, fields[1], fields[2])
		}
		var mode os.FileMode
		switch fields[0] {
		case "100644":
			mode = 0o644
		case "100755":
			mode = 0o755
		default:
			return nil, fmt.Errorf("revision tree path %q has unsupported mode %s", repositoryPath, fields[0])
		}
		entries = append(entries, repositoryTreeEntry{mode: mode, object: fields[2], path: repositoryPath})
	}
	return entries, nil
}

func materializeRepositoryTree(ctx context.Context, destination string, entries []repositoryTreeEntry, data []byte) error {
	reader := bufio.NewReader(bytes.NewReader(data))
	var extractedBytes int64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read blob header for %q: %w", entry.path, err)
		}
		if len(header) > 256 {
			return fmt.Errorf("blob header for %q exceeds limit", entry.path)
		}
		fields := strings.Fields(strings.TrimSuffix(header, "\n"))
		if len(fields) != 3 || fields[0] != entry.object || fields[1] != "blob" {
			return fmt.Errorf("blob header for %q does not match the revision tree", entry.path)
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 || size > maxSnapshotFileSize {
			return fmt.Errorf("blob size for %q is invalid or exceeds limit %d", entry.path, maxSnapshotFileSize)
		}
		if extractedBytes > maxExtractedBytes-size {
			return fmt.Errorf("revision extracted bytes exceed limit %d", maxExtractedBytes)
		}
		extractedBytes += size
		target := filepath.Join(destination, filepath.FromSlash(entry.path))
		relative, err := filepath.Rel(destination, target)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("revision path %q escapes the snapshot", entry.path)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, entry.mode)
		if err != nil {
			return err
		}
		written, copyErr := io.CopyN(file, reader, size)
		closeErr := file.Close()
		if copyErr != nil || written != size {
			if copyErr != nil {
				return copyErr
			}
			return fmt.Errorf("blob for %q wrote %d bytes, expected %d", entry.path, written, size)
		}
		if closeErr != nil {
			return closeErr
		}
		separator, err := reader.ReadByte()
		if err != nil || separator != '\n' {
			return fmt.Errorf("blob for %q is not newline terminated", entry.path)
		}
	}
	if trailing, err := reader.ReadByte(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("read trailing blob data: %w", err)
		}
		return fmt.Errorf("blob batch contains trailing byte %q", trailing)
	}
	return nil
}

func gitEvidenceEnvironment() []string {
	return withEnvironment(withoutEnvironmentPrefix(os.Environ(), "GIT_"), map[string]string{
		"GIT_CONFIG_COUNT":       "0",
		"GIT_CONFIG_GLOBAL":      os.DevNull,
		"GIT_CONFIG_NOSYSTEM":    "1",
		"GIT_CONFIG_SYSTEM":      os.DevNull,
		"GIT_NO_REPLACE_OBJECTS": "1",
		"GIT_NO_LAZY_FETCH":      "1",
		"GIT_OPTIONAL_LOCKS":     "0",
		"GIT_TERMINAL_PROMPT":    "0",
		"LANG":                   "C",
		"LC_ALL":                 "C",
	})
}

func withoutEnvironmentPrefix(current []string, prefix string) []string {
	result := make([]string, 0, len(current))
	for _, entry := range current {
		key, _, found := strings.Cut(entry, "=")
		if found && strings.HasPrefix(key, prefix) {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func withEnvironment(current []string, replacements map[string]string) []string {
	result := make([]string, 0, len(current)+len(replacements))
	for _, entry := range current {
		key, _, found := strings.Cut(entry, "=")
		if _, replace := replacements[key]; found && replace {
			continue
		}
		result = append(result, entry)
	}
	keys := make([]string, 0, len(replacements))
	for key := range replacements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result = append(result, key+"="+replacements[key])
	}
	return result
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}
