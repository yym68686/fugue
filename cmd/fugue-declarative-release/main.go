package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes"
)

const maxChangedPathsBytes = 1 << 20

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runContext(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "declarative-release:", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	return runContext(context.Background(), args, output)
}

func runContext(ctx context.Context, args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: fugue-declarative-release <plan|emit-github-output|emit-delivery|build|receipt|prepare|execute|guardian-submit|repair-monitor|restore-monitor|reconcile|install-monitor-record> ...")
	}
	switch args[0] {
	case "plan":
		return runPlan(args, output)
	case "emit-github-output":
		return runEmitGitHubOutput(args)
	case "emit-delivery":
		return runEmitDelivery(args, output)
	case "build":
		return runBuild(args, output)
	case "receipt":
		return runReceipt(args, output)
	case "prepare":
		return runPrepare(args, output)
	case "execute":
		return runExecuteContext(ctx, args, output)
	case "guardian-submit":
		return runGuardianSubmit(args, output)
	case "repair-monitor":
		return runRepairMonitorContext(ctx, args, output)
	case "restore-monitor":
		return runRestoreMonitorContext(ctx, args, output)
	case "reconcile":
		return runReconcile(args, output)
	case "install-monitor-record":
		return runInstallMonitorRecord(args, output)
	default:
		return errors.New("usage: fugue-declarative-release <plan|emit-github-output|emit-delivery|build|receipt|prepare|execute|guardian-submit|repair-monitor|restore-monitor|reconcile|install-monitor-record> ...")
	}
}

func runEmitDelivery(args []string, output io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release emit-delivery PLAN COMPONENT")
	}
	plan, err := readPlan(args[1])
	if err != nil {
		return err
	}
	release, err := selectedRelease(plan, args[2])
	if err != nil {
		return err
	}
	value := "direct\n"
	if release.Delivery != nil {
		if release.Delivery.Writer != "guardian" {
			return errors.New("production delivery writer is unsupported")
		}
		value = "guardian\n"
	}
	_, err = io.WriteString(output, value)
	return err
}

func runGuardianSubmit(args []string, output io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: fugue-declarative-release guardian-submit PLAN_DIR")
	}
	files, err := readPlanDirectory(args[1])
	if err != nil {
		return err
	}
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
	if err != nil || len(plan.Releases) != 1 {
		return errors.New("Guardian submission requires one exact component plan")
	}
	release := plan.Releases[0]
	if release.Delivery == nil || release.Delivery.Writer != "guardian" {
		return errors.New("component is not enrolled in Guardian delivery")
	}
	key := releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group}
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Guardian submission client: %w", err)
	}
	store, err := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{{
		Key: key, Namespace: release.Workload.Namespace, MonitorComponent: release.ComponentID,
		DependencyService: release.Delivery.DependencyService,
	}})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	_, desired, err := store.PublishDesired(ctx, key, files)
	if err != nil {
		return err
	}
	status, waitErr := store.WaitForTerminal(ctx, key, desired, 3*time.Second)
	if status.StatusDigest != "" {
		raw, encodeErr := declarativerelease.CanonicalJSON(status)
		if encodeErr != nil {
			return encodeErr
		}
		if _, encodeErr = output.Write(append(raw, '\n')); encodeErr != nil {
			return encodeErr
		}
	}
	return waitErr
}

func runPlan(args []string, output io.Writer) error {
	if len(args) != 5 {
		return errors.New("usage: fugue-declarative-release plan REGISTRY BASE_SHA HEAD_SHA CHANGED_PATHS_FILE")
	}
	registryPath, baseSHA, headSHA, changedPath := args[1], args[2], args[3], args[4]
	for _, revision := range []string{baseSHA, headSHA} {
		if err := exec.Command("git", "cat-file", "-e", revision+"^{commit}").Run(); err != nil {
			return fmt.Errorf("Git commit %s is unavailable: %w", revision, err)
		}
	}
	registry, err := loadProductionRegistry(registryPath)
	if err != nil {
		return err
	}
	registry, err = expandComponentDependencyRoots(registry)
	if err != nil {
		return err
	}
	changed, err := readChangedPaths(changedPath)
	if err != nil {
		return err
	}
	changed, err = addProductionRuntimeChanges(registry, headSHA, changed)
	if err != nil {
		return err
	}
	plan, err := declarativerelease.BuildPlan(registry, baseSHA, headSHA, changed)
	if err != nil {
		return err
	}
	if registry.EdgeGroupRegistryPath != "" && containsPath(changed, registry.EdgeGroupRegistryPath) {
		currentEdge, edgeErr := loadEdgeGroupRegistryFile(registry.EdgeGroupRegistryPath)
		if edgeErr != nil {
			return edgeErr
		}
		previousEdge, edgeErr := loadGitEdgeGroupRegistry(baseSHA, registryPath)
		if edgeErr != nil {
			return edgeErr
		}
		if edgeErr := declarativerelease.ValidateEdgeGroupRegistryUpdate(previousEdge, currentEdge, plan, changed); edgeErr != nil {
			return edgeErr
		}
	}
	if len(plan.Releases) > 0 {
		parentRaw, parentErr := exec.Command("git", "rev-parse", headSHA+"^").Output()
		if parentErr != nil || strings.TrimSpace(string(parentRaw)) != baseSHA {
			return errors.New("runtime production atom must be one direct-child commit")
		}
	}
	current := make(map[string]declarativerelease.Intent, len(plan.Releases))
	previous := make(map[string]declarativerelease.Intent, len(plan.Releases))
	superseded := make(map[string]declarativerelease.Intent, len(plan.Releases))
	previousConfigSHA := make(map[string]string, len(plan.Releases))
	for _, release := range plan.Releases {
		intent, intentErr := loadIntent(release.IntentPath)
		if intentErr != nil {
			return fmt.Errorf("load component %q intent: %w", release.ComponentID, intentErr)
		}
		current[release.ComponentID] = intent
		prior, priorSHA, found, priorErr := loadGitIntent(baseSHA, release.IntentPath)
		if priorErr != nil {
			return fmt.Errorf("load component %q previous intent: %w", release.ComponentID, priorErr)
		}
		if found {
			previous[release.ComponentID] = prior
			previousConfigSHA[release.ComponentID] = resolvePreviousConfigSHA(baseSHA, release.IntentPath, intent, prior, priorSHA)
		}
		if intent.SupersedesFailedConfigSHA != "" {
			if !found {
				return fmt.Errorf("component %q superseded failed atom has no prior component intent", release.ComponentID)
			}
			if err := exec.Command("git", "merge-base", "--is-ancestor", intent.SupersedesFailedConfigSHA, baseSHA).Run(); err != nil {
				return fmt.Errorf("component %q superseded failed atom is not in the trusted base ancestry", release.ComponentID)
			}
			failedIntent, failedErr := loadGitIntentAt(intent.SupersedesFailedConfigSHA, release.IntentPath)
			if failedErr != nil {
				return fmt.Errorf("load component %q superseded failed atom: %w", release.ComponentID, failedErr)
			}
			failedRaw, failedErr := exec.Command("git", "rev-list", "-1", intent.SupersedesFailedConfigSHA, "--", release.IntentPath).CombinedOutput()
			if failedErr != nil || strings.TrimSpace(string(failedRaw)) != intent.SupersedesFailedConfigSHA {
				return fmt.Errorf("component %q superseded failed atom is not an exact production intent atom", release.ComponentID)
			}
			superseded[intent.SupersedesFailedConfigSHA] = failedIntent
			if err := exec.Command("git", "merge-base", "--is-ancestor", intent.ExpectedPreviousConfigSHA, baseSHA).Run(); err != nil {
				return fmt.Errorf("component %q recovered predecessor is not in the trusted base ancestry", release.ComponentID)
			}
			recoveredRaw, recoveredErr := exec.Command("git", "rev-list", "-1", intent.ExpectedPreviousConfigSHA, "--", release.IntentPath).CombinedOutput()
			if recoveredErr != nil || strings.TrimSpace(string(recoveredRaw)) != intent.ExpectedPreviousConfigSHA {
				return fmt.Errorf("component %q recovered predecessor is not an exact production intent atom", release.ComponentID)
			}
		}
	}
	bound, err := declarativerelease.BindIntents(registry, plan, current, previous, previousConfigSHA, superseded)
	if err != nil {
		return err
	}
	encoded, err := declarativerelease.CanonicalJSON(bound)
	if err != nil {
		return err
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}

func resolvePreviousConfigSHA(baseSHA, intentPath string, current, previous declarativerelease.Intent, previousIntentSHA string) string {
	declared := current.ExpectedPreviousConfigSHA
	if !current.ExpectedPreviousPresent || declared == "" || declared == previousIntentSHA {
		return previousIntentSHA
	}
	if err := exec.Command("git", "merge-base", "--is-ancestor", declared, baseSHA).Run(); err != nil {
		return previousIntentSHA
	}
	declaredIntent, err := loadGitIntentAt(declared, intentPath)
	if err != nil || declaredIntent != previous {
		return previousIntentSHA
	}
	return declared
}

func addProductionRuntimeChanges(registry declarativerelease.Registry, headSHA string, changed []string) ([]string, error) {
	componentsByIntent := make(map[string]declarativerelease.Component, len(registry.Components))
	for _, component := range registry.Components {
		componentsByIntent[component.IntentPath] = component
	}
	seenChanged := make(map[string]struct{}, len(changed))
	var selected *declarativerelease.Component
	for _, changedPath := range changed {
		if _, exists := seenChanged[changedPath]; exists {
			return nil, fmt.Errorf("duplicate changed path %q", changedPath)
		}
		seenChanged[changedPath] = struct{}{}
		component, exists := componentsByIntent[changedPath]
		if !exists {
			continue
		}
		if selected != nil {
			return nil, errors.New("runtime commit contains multiple production intents; split it into independent production atoms")
		}
		copy := component
		selected = &copy
	}
	if selected == nil {
		return changed, nil
	}
	intent, err := loadIntent(selected.IntentPath)
	if err != nil {
		return nil, fmt.Errorf("load component %q intent for production runtime diff: %w", selected.ID, err)
	}
	if err := intent.Validate(); err != nil {
		return nil, fmt.Errorf("component %q intent for production runtime diff: %w", selected.ID, err)
	}
	if intent.Component != selected.ID {
		return nil, fmt.Errorf("component %q production runtime intent identity mismatch", selected.ID)
	}
	if !intent.ExpectedPreviousPresent {
		return changed, nil
	}
	baseline := intent.ExpectedPreviousOCIRevision
	if err := exec.Command("git", "merge-base", "--is-ancestor", baseline, headSHA).Run(); err != nil {
		return nil, fmt.Errorf("component %q production OCI revision is not in target ancestry", selected.ID)
	}
	raw, err := exec.Command("git", "diff", "--no-renames", "--name-only", baseline, headSHA, "--").Output()
	if err != nil {
		return nil, fmt.Errorf("compute component %q production runtime diff: %w", selected.ID, err)
	}
	merged := make(map[string]struct{}, len(changed)+32)
	for _, path := range changed {
		merged[path] = struct{}{}
	}
	for _, path := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if path == "" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		if path == selected.ManifestPath {
			merged[path] = struct{}{}
			continue
		}
		for _, root := range selected.SourceRoots {
			if pathMatchesComponentRoot(path, root) {
				merged[path] = struct{}{}
				break
			}
		}
	}
	// Runtime-diff expansion is intentionally scoped to the selected component.
	// A shared Go package can be linked into several independently deployed
	// binaries; adding its historical diff as another component's manifest
	// change would either co-deploy that component or make a safe single-lane
	// atom impossible. BuildPlan still classifies the actual commit paths
	// against every expanded dependency graph and rejects any non-shared path.
	for _, component := range registry.Components {
		if component.ID == selected.ID {
			continue
		}
		delete(merged, component.ManifestPath)
		delete(merged, component.IntentPath)
	}
	result := make([]string, 0, len(merged))
	for path := range merged {
		result = append(result, path)
	}
	sort.Strings(result)
	return result, nil
}

func pathMatchesComponentRoot(filename, root string) bool {
	return filename == root || strings.HasPrefix(filename, strings.TrimSuffix(root, "/")+"/")
}

func containsPath(paths []string, want string) bool {
	for _, path := range paths {
		if path == want {
			return true
		}
	}
	return false
}

func loadEdgeGroupRegistryFile(path string) (declarativerelease.EdgeGroupRegistry, error) {
	file, err := os.Open(path)
	if err != nil {
		return declarativerelease.EdgeGroupRegistry{}, fmt.Errorf("open edge group registry: %w", err)
	}
	registry, decodeErr := declarativerelease.DecodeEdgeGroupRegistry(file)
	closeErr := file.Close()
	if decodeErr != nil {
		return declarativerelease.EdgeGroupRegistry{}, decodeErr
	}
	if closeErr != nil {
		return declarativerelease.EdgeGroupRegistry{}, closeErr
	}
	return registry, nil
}

func loadGitEdgeGroupRegistry(revision, registryPath string) (*declarativerelease.EdgeGroupRegistry, error) {
	raw, err := exec.Command("git", "show", revision+":"+registryPath).Output()
	if err != nil {
		return nil, fmt.Errorf("read previous production registry: %w", err)
	}
	base, err := declarativerelease.DecodeRegistry(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("decode previous production registry: %w", err)
	}
	if base.EdgeGroupRegistryPath == "" {
		return nil, nil
	}
	raw, err = exec.Command("git", "show", revision+":"+base.EdgeGroupRegistryPath).Output()
	if err != nil {
		return nil, fmt.Errorf("read previous edge group registry: %w", err)
	}
	edge, err := declarativerelease.DecodeEdgeGroupRegistry(bytes.NewReader(raw))
	if err != nil {
		edge, err = declarativerelease.DecodeHistoricalEdgeGroupRegistry(bytes.NewReader(raw))
	}
	if err != nil {
		return nil, fmt.Errorf("decode previous edge group registry: %w", err)
	}
	return &edge, nil
}

func loadProductionRegistry(registryPath string) (declarativerelease.Registry, error) {
	registryFile, err := os.Open(registryPath)
	if err != nil {
		return declarativerelease.Registry{}, fmt.Errorf("open registry: %w", err)
	}
	registry, decodeErr := declarativerelease.DecodeRegistry(registryFile)
	closeErr := registryFile.Close()
	if decodeErr != nil {
		return declarativerelease.Registry{}, decodeErr
	}
	if closeErr != nil {
		return declarativerelease.Registry{}, closeErr
	}
	if registry.EdgeGroupRegistryPath == "" {
		return registry, nil
	}
	edgeFile, err := os.Open(registry.EdgeGroupRegistryPath)
	if err != nil {
		return declarativerelease.Registry{}, fmt.Errorf("open edge group registry: %w", err)
	}
	edgeRegistry, decodeEdgeErr := declarativerelease.DecodeEdgeGroupRegistry(edgeFile)
	closeEdgeErr := edgeFile.Close()
	if decodeEdgeErr != nil {
		return declarativerelease.Registry{}, decodeEdgeErr
	}
	if closeEdgeErr != nil {
		return declarativerelease.Registry{}, closeEdgeErr
	}
	return declarativerelease.MergeEdgeGroupRegistry(registry, edgeRegistry)
}

func expandComponentDependencyRoots(registry declarativerelease.Registry) (declarativerelease.Registry, error) {
	rootRaw, err := boundedExternal(context.Background(), nil, 32<<10, "git", "rev-parse", "--show-toplevel")
	if err != nil {
		return declarativerelease.Registry{}, fmt.Errorf("resolve repository root: %w", err)
	}
	root, err := filepath.Abs(strings.TrimSpace(string(rootRaw)))
	if err != nil || root == "" {
		return declarativerelease.Registry{}, errors.New("repository root is invalid")
	}
	for index := range registry.Components {
		component := &registry.Components[index]
		raw, listErr := boundedExternalInDirectory(context.Background(), root, nil, 4<<20, "go", "list", "-deps", "-f", "{{if and .Module .Dir}}{{.Dir}}{{end}}", component.Artifact.BuildPackage)
		if listErr != nil {
			return declarativerelease.Registry{}, fmt.Errorf("resolve %s dependency graph: %w", component.ID, listErr)
		}
		roots := make(map[string]struct{}, len(component.SourceRoots)+32)
		for _, value := range component.SourceRoots {
			roots[value] = struct{}{}
		}
		for _, value := range []string{".dockerignore", "go.mod", "go.sum"} {
			roots[value] = struct{}{}
		}
		for _, line := range strings.Split(string(raw), "\n") {
			directory := strings.TrimSpace(line)
			if directory == "" {
				continue
			}
			absolute, absErr := filepath.Abs(directory)
			if absErr != nil {
				return declarativerelease.Registry{}, fmt.Errorf("resolve %s dependency path: %w", component.ID, absErr)
			}
			relative, relErr := filepath.Rel(root, absolute)
			if relErr != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				continue
			}
			roots[filepath.ToSlash(relative)] = struct{}{}
		}
		component.SourceRoots = component.SourceRoots[:0]
		for value := range roots {
			component.SourceRoots = append(component.SourceRoots, value)
		}
		sort.Strings(component.SourceRoots)
	}
	if err := registry.Validate(); err != nil {
		return declarativerelease.Registry{}, fmt.Errorf("validate expanded component dependency graph: %w", err)
	}
	return registry, nil
}

func runReceipt(args []string, output io.Writer) error {
	if len(args) != 4 {
		return errors.New("usage: fugue-declarative-release receipt PLAN COMPONENT REGISTRY_VERIFICATION")
	}
	planFile, err := os.Open(args[1])
	if err != nil {
		return fmt.Errorf("open release plan: %w", err)
	}
	plan, decodePlanErr := declarativerelease.DecodePlan(planFile)
	closePlanErr := planFile.Close()
	if decodePlanErr != nil {
		return decodePlanErr
	}
	if closePlanErr != nil {
		return closePlanErr
	}
	verificationFile, err := os.Open(args[3])
	if err != nil {
		return fmt.Errorf("open registry verification: %w", err)
	}
	verification, decodeVerificationErr := declarativerelease.DecodeRegistryVerification(verificationFile)
	closeVerificationErr := verificationFile.Close()
	if decodeVerificationErr != nil {
		return decodeVerificationErr
	}
	if closeVerificationErr != nil {
		return closeVerificationErr
	}
	receipt, err := declarativerelease.MaterializeArtifactReceipt(plan, args[2], verification)
	if err != nil {
		return err
	}
	encoded, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return err
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}

func runEmitGitHubOutput(args []string) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release emit-github-output PLAN GITHUB_OUTPUT")
	}
	plan, err := readPlan(args[1])
	if err != nil {
		return err
	}
	type matrixEntry struct {
		Component   string `json:"component"`
		BuildLane   string `json:"build_lane,omitempty"`
		Concurrency string `json:"concurrency,omitempty"`
	}
	type matrix struct {
		Include []matrixEntry `json:"include"`
	}
	value := matrix{Include: make([]matrixEntry, 0, len(plan.Releases))}
	edgeControl := matrix{Include: make([]matrixEntry, 0, 1)}
	edgeClient := matrix{Include: make([]matrixEntry, 0, 1)}
	edgeWorker := matrix{Include: make([]matrixEntry, 0, 1)}
	components := make([]string, 0, len(plan.Releases))
	for _, release := range plan.Releases {
		repositoryDigest := sha256.Sum256([]byte(release.Artifact.Repository))
		value.Include = append(value.Include, matrixEntry{
			Component: release.ComponentID,
			BuildLane: fmt.Sprintf("%x", repositoryDigest[:8]),
		})
		entry := matrixEntry{Component: release.ComponentID, Concurrency: release.Concurrency}
		switch {
		case strings.HasPrefix(release.ComponentID, "edge-client-"):
			edgeClient.Include = append(edgeClient.Include, entry)
		case strings.HasPrefix(release.ComponentID, "edge-control-"):
			edgeControl.Include = append(edgeControl.Include, entry)
		case strings.HasPrefix(release.ComponentID, "edge-worker-"):
			edgeWorker.Include = append(edgeWorker.Include, entry)
		}
		components = append(components, release.ComponentID)
	}
	encoded, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		return err
	}
	componentJSON, err := declarativerelease.CanonicalJSON(components)
	if err != nil {
		return err
	}
	edgeControlJSON, err := declarativerelease.CanonicalJSON(edgeControl)
	if err != nil {
		return err
	}
	edgeClientJSON, err := declarativerelease.CanonicalJSON(edgeClient)
	if err != nil {
		return err
	}
	edgeWorkerJSON, err := declarativerelease.CanonicalJSON(edgeWorker)
	if err != nil {
		return err
	}
	info, err := os.Lstat(args[2])
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 1<<20 {
		return errors.New("GITHUB_OUTPUT file is invalid")
	}
	file, err := os.OpenFile(args[2], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(
		file,
		"release_count=%d\nrelease_matrix=%s\nrelease_components=%s\nedge_client_count=%d\nedge_client_matrix=%s\nedge_control_count=%d\nedge_control_matrix=%s\nedge_worker_count=%d\nedge_worker_matrix=%s\n",
		len(plan.Releases), encoded, componentJSON, len(edgeClient.Include), edgeClientJSON, len(edgeControl.Include), edgeControlJSON, len(edgeWorker.Include), edgeWorkerJSON,
	)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func runPrepare(args []string, output io.Writer) error {
	if len(args) != 5 {
		return errors.New("usage: fugue-declarative-release prepare PLAN COMPONENT ARTIFACT_RECEIPT PLAN_DIR")
	}
	plan, err := readPlan(args[1])
	if err != nil {
		return err
	}
	if err := verifyReleaseCheckout(plan.HeadSHA); err != nil {
		return err
	}
	receiptFile, err := os.Open(args[3])
	if err != nil {
		return err
	}
	receipt, decodeReceiptErr := declarativerelease.DecodeArtifactReceipt(receiptFile)
	closeReceiptErr := receiptFile.Close()
	if decodeReceiptErr != nil {
		return decodeReceiptErr
	}
	if closeReceiptErr != nil {
		return closeReceiptErr
	}
	release, err := selectedRelease(plan, args[2])
	if err != nil {
		return err
	}
	manifestRaw, err := os.ReadFile(release.ManifestPath)
	if err != nil {
		return err
	}
	manifestRaw, err = declarativerelease.MaterializeManifestTemplate(manifestRaw, release.ManifestVariables)
	if err != nil {
		return err
	}
	var exactGuardianLKG []byte
	var lkgManifest []byte
	if release.Delivery != nil && release.Delivery.Writer == "guardian" {
		config, configErr := loadComponentLeaseClientConfig()
		if configErr != nil {
			return configErr
		}
		client, clientErr := kubernetes.NewForConfig(config)
		if clientErr != nil {
			return fmt.Errorf("create Guardian LKG client: %w", clientErr)
		}
		store, storeErr := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{{
			Key:       releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group},
			Namespace: release.Workload.Namespace, MonitorComponent: release.ComponentID,
			DependencyService: release.Delivery.DependencyService,
		}})
		if storeErr != nil {
			return storeErr
		}
		lkgContext, cancelLKG := context.WithTimeout(context.Background(), 30*time.Second)
		exactGuardianLKG, err = store.LoadStableLKG(lkgContext, releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group}, release)
		cancelLKG()
		if err != nil {
			return err
		}
		lkgManifest = exactGuardianLKG
	} else {
		var lkgErr error
		lkgManifest, lkgErr = loadLKGManifest(release)
		if lkgErr != nil {
			return lkgErr
		}
		if lkgManifest != nil {
			lkgManifest, err = declarativerelease.MaterializePredecessorManifestTemplate(lkgManifest, release.ManifestVariables)
			if err != nil {
				return err
			}
		}
	}
	var lkgReader io.Reader
	if lkgManifest != nil {
		lkgReader = bytes.NewReader(lkgManifest)
	}
	rendered, renderErr := declarativerelease.RenderManifests(plan, args[2], receipt, bytes.NewReader(manifestRaw), lkgReader)
	if renderErr != nil {
		return renderErr
	}
	if exactGuardianLKG != nil {
		rendered, err = declarativerelease.BindGuardianLKG(plan, args[2], rendered, exactGuardianLKG)
		if err != nil {
			return err
		}
	}
	cluster, err := newKubectlCluster()
	if err != nil {
		return err
	}
	if release.Transition != nil && release.Transition.EdgeGroupAB != nil && release.Delivery != nil && release.Delivery.Writer == "guardian" {
		state, stateErr := cluster.readEdgeGroupState(context.Background(), release, *release.Transition.EdgeGroupAB)
		if stateErr != nil {
			return fmt.Errorf("read edge candidate active slot: %w", stateErr)
		}
		rendered, err = declarativerelease.BindEdgeCandidateForward(plan, args[2], state.ActiveSlot, rendered)
		if err != nil {
			return err
		}
	}
	prepareTimeout := 3 * time.Minute
	if release.Transition != nil && release.Transition.EdgeGroupAB != nil {
		prepareTimeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), prepareTimeout)
	defer cancel()
	prepared, err := declarativerelease.PrepareExecution(ctx, cluster, plan, args[2], receipt, rendered, time.Now())
	if err != nil {
		return err
	}
	if err := writePlanDirectory(args[4], plan, receipt, rendered, prepared); err != nil {
		return err
	}
	encoded, err := declarativerelease.CanonicalJSON(prepared)
	if err != nil {
		return err
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}

func loadLKGManifest(release declarativerelease.PlanRelease) ([]byte, error) {
	if !release.ExpectedPreviousPresent {
		return nil, nil
	}
	manifestPath, err := historicalComponentManifestPath(release.ExpectedPreviousConfigSHA, release.ComponentID)
	if err != nil {
		return nil, err
	}
	object := release.ExpectedPreviousConfigSHA + ":" + manifestPath
	content, err := exec.Command("git", "show", object).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("read previous component manifest: %w", err)
	}
	return content, nil
}

func historicalComponentManifestPath(revision, componentID string) (string, error) {
	registry, err := loadProductionRegistry("deploy/releases/components.json")
	if err != nil {
		return "", err
	}
	for _, component := range registry.Components {
		if component.ID != componentID {
			continue
		}
		if err := exec.Command("git", "cat-file", "-e", revision+":"+component.ManifestPath).Run(); err != nil {
			return "", fmt.Errorf("previous component manifest %q is absent: %w", component.ManifestPath, err)
		}
		return component.ManifestPath, nil
	}
	return "", fmt.Errorf("previous production registry does not contain component %q", componentID)
}
func runExecute(args []string, output io.Writer) error {
	return runExecuteContext(context.Background(), args, output)
}

func runExecuteContext(parent context.Context, args []string, output io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: fugue-declarative-release execute PLAN_DIR")
	}
	files, err := readPlanDirectory(args[1])
	if err != nil {
		return err
	}
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
	if err != nil {
		return err
	}
	prepared, err := declarativerelease.DecodeExecutionPlan(bytes.NewReader(files["execution-plan.json"]), plan, files["forward.json"], files["lkg.json"])
	if err != nil {
		return err
	}
	receipt, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(files["artifact-receipt.json"]))
	if err != nil {
		return err
	}
	if prepared.ArtifactDigest != receipt.ReceiptDigest || prepared.Component != receipt.Component ||
		prepared.ConfigSHA != receipt.ConfigSHA || prepared.Forward.ImageRef != receipt.ImmutableRef {
		return errors.New("execution plan is not bound to its immutable artifact receipt")
	}
	cluster, err := newKubectlCluster()
	if err != nil {
		return err
	}
	release, err := selectedRelease(plan, prepared.Component)
	if err != nil {
		return err
	}
	executeTimeout := 5 * time.Minute
	if release.Transition != nil && release.Transition.EdgeGroupAB != nil {
		executeTimeout = 12 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, executeTimeout)
	defer cancel()
	leaseCoordinator, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	heldLease, err := leaseCoordinator.acquire(ctx, release, prepared.ConfigSHA)
	if err != nil {
		return fmt.Errorf("acquire component mutation lease: %w", err)
	}
	result := declarativerelease.Execute(ctx, cluster, plan, prepared, files["forward.json"], files["lkg.json"])
	// Every executor result is a canonical terminal result, including a
	// compensated or terminal failure result. Finalize the component lease with
	// the held UID/RV/holder CAS on every path; an unknown CAS result remains an
	// explicit error and never becomes an unconditional cleanup.
	if !finalizeComponentLeaseStatus(result.Status) {
		return errors.New("component release produced a non-terminal status")
	}
	var monitorRecordErr error
	if result.Status == "verified" {
		monitor, monitorErr := newMonitorStore()
		if monitorErr != nil {
			monitorRecordErr = monitorErr
		} else {
			finalizeCtx, finalizeCancel := componentLeaseFinalizationContext(ctx)
			_, monitorRecordErr = monitor.persistVerified(finalizeCtx, release, files, result)
			finalizeCancel()
		}
	}
	leaseCtx, leaseCancel := componentLeaseFinalizationContext(ctx)
	leaseReleaseErr := leaseCoordinator.release(leaseCtx, heldLease)
	leaseCancel()
	encoded, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return err
	}
	if _, err := output.Write(append(encoded, '\n')); err != nil {
		return err
	}
	if leaseReleaseErr != nil {
		return fmt.Errorf("component release terminal state is recorded but lease release is unproven: %w", leaseReleaseErr)
	}
	if monitorRecordErr != nil {
		return fmt.Errorf("component release is verified but its continuous rollback record is unproven: %w", monitorRecordErr)
	}
	if result.Status != "verified" {
		return fmt.Errorf("component release ended with status=%s reason=%s", result.Status, result.Reason)
	}
	return nil
}

func finalizeComponentLeaseStatus(status string) bool {
	switch status {
	case "verified", "compensated", "failed-no-write", "recovery-required":
		return true
	default:
		return false
	}
}

func runReconcile(args []string, output io.Writer) error {
	if len(args) != 2 && len(args) != 3 {
		return errors.New("usage: fugue-declarative-release reconcile PLAN_DIR [EXISTING_RESULT]")
	}
	if len(args) == 3 {
		if file, openErr := os.Open(args[2]); openErr == nil {
			_, decodeErr := declarativerelease.DecodeExecutionResult(file)
			closeErr := file.Close()
			if decodeErr == nil && closeErr == nil {
				return errors.New("executor already emitted a canonical terminal receipt; read-only replacement is forbidden")
			}
		} else if !errors.Is(openErr, os.ErrNotExist) {
			return fmt.Errorf("inspect existing executor result: %w", openErr)
		}
	}
	files, err := readPlanDirectory(args[1])
	if err != nil {
		return err
	}
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
	if err != nil {
		return err
	}
	prepared, err := declarativerelease.DecodeExecutionPlan(bytes.NewReader(files["execution-plan.json"]), plan, files["forward.json"], files["lkg.json"])
	if err != nil {
		return err
	}
	receipt, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(files["artifact-receipt.json"]))
	if err != nil || prepared.ArtifactDigest != receipt.ReceiptDigest || prepared.Component != receipt.Component || prepared.ConfigSHA != receipt.ConfigSHA || prepared.Forward.ImageRef != receipt.ImmutableRef {
		return errors.New("execution reconcile is not bound to its immutable artifact receipt")
	}
	release, err := selectedRelease(plan, prepared.Component)
	if err != nil {
		return err
	}
	reconcileTimeout := 3 * time.Minute
	if release.Transition != nil && release.Transition.EdgeGroupAB != nil {
		reconcileTimeout = 6 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), reconcileTimeout)
	defer cancel()
	cluster, err := newKubectlCluster()
	if err != nil {
		return err
	}
	result := declarativerelease.ReconcileExecution(ctx, cluster, plan, prepared, files["forward.json"], files["lkg.json"])
	encoded, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return err
	}
	if _, err := output.Write(append(encoded, '\n')); err != nil {
		return err
	}
	if result.Status != "verified" {
		return fmt.Errorf("component release reconciliation ended with status=%s reason=%s", result.Status, result.Reason)
	}
	return nil
}

func readPlan(filename string) (declarativerelease.Plan, error) {
	file, err := os.Open(filename)
	if err != nil {
		return declarativerelease.Plan{}, err
	}
	defer file.Close()
	return declarativerelease.DecodePlan(file)
}

func writePlanDirectory(directory string, plan declarativerelease.Plan, receipt declarativerelease.ArtifactReceipt, rendered declarativerelease.RenderedManifests, prepared declarativerelease.ExecutionPlan) error {
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		return errors.New("execution plan directory already exists or cannot be inspected")
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return err
	}
	values := map[string][]byte{
		"forward.json": rendered.Forward,
		"lkg.json":     rendered.LKG,
	}
	for name, value := range map[string]any{
		"release-plan.json":     plan,
		"artifact-receipt.json": receipt,
		"execution-plan.json":   prepared,
	} {
		encoded, err := declarativerelease.CanonicalJSON(value)
		if err != nil {
			return err
		}
		values[name] = encoded
	}
	for name, value := range values {
		if err := writeExclusive(filepath.Join(directory, name), append(value, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func writeExclusive(filename string, value []byte) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(value); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func readPlanDirectory(directory string) (map[string][]byte, error) {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return nil, errors.New("execution plan directory type or mode is invalid")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	want := []string{"artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "release-plan.json"}
	got := make([]string, 0, len(entries))
	result := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
		fileInfo, err := entry.Info()
		if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Mode().Perm() != 0o600 || fileInfo.Size() < 1 || fileInfo.Size() > 4<<20 {
			return nil, fmt.Errorf("execution plan file %q is invalid", entry.Name())
		}
		content, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		result[entry.Name()] = bytes.TrimSpace(content)
	}
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		return nil, errors.New("execution plan file set is invalid")
	}
	if _, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(result["artifact-receipt.json"])); err != nil {
		return nil, err
	}
	return result, nil
}

func readChangedPaths(filename string) ([]string, error) {
	info, err := os.Lstat(filename)
	if err != nil {
		return nil, fmt.Errorf("stat changed path file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maxChangedPathsBytes {
		return nil, errors.New("changed path file type/size is invalid")
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	paths := make([]string, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), maxChangedPathsBytes)
	for scanner.Scan() {
		value := scanner.Text()
		if value == "" || strings.TrimSpace(value) != value {
			return nil, errors.New("changed path line is empty or non-canonical")
		}
		paths = append(paths, value)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return paths, nil
}

func loadIntent(filename string) (declarativerelease.Intent, error) {
	file, err := os.Open(filename)
	if err != nil {
		return declarativerelease.Intent{}, err
	}
	defer file.Close()
	return declarativerelease.DecodeIntent(file)
}

func loadGitIntent(baseSHA, filename string) (declarativerelease.Intent, string, bool, error) {
	clean := filepath.ToSlash(filepath.Clean(filename))
	if clean != filename || strings.HasPrefix(clean, "../") || filepath.IsAbs(clean) {
		return declarativerelease.Intent{}, "", false, errors.New("intent path is invalid")
	}
	object := baseSHA + ":" + clean
	if err := exec.Command("git", "cat-file", "-e", object).Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return declarativerelease.Intent{}, "", false, nil
		}
		return declarativerelease.Intent{}, "", false, err
	}
	command := exec.Command("git", "show", object)
	content, err := command.CombinedOutput()
	if err != nil {
		return declarativerelease.Intent{}, "", false, err
	}
	intent, err := declarativerelease.DecodeIntent(bytes.NewReader(content))
	if err != nil {
		return declarativerelease.Intent{}, "", false, err
	}
	commitRaw, err := exec.Command("git", "rev-list", "-1", baseSHA, "--", clean).CombinedOutput()
	if err != nil {
		return declarativerelease.Intent{}, "", false, err
	}
	commit := strings.TrimSpace(string(commitRaw))
	if len(commit) != 40 || strings.Trim(commit, "0123456789abcdef") != "" {
		return declarativerelease.Intent{}, "", false, errors.New("previous component intent commit is invalid")
	}
	return intent, commit, true, nil
}

func loadGitIntentAt(commitSHA, filename string) (declarativerelease.Intent, error) {
	clean := filepath.ToSlash(filepath.Clean(filename))
	if clean != filename || strings.HasPrefix(clean, "../") || filepath.IsAbs(clean) {
		return declarativerelease.Intent{}, errors.New("intent path is invalid")
	}
	if len(commitSHA) != 40 || strings.Trim(commitSHA, "0123456789abcdef") != "" {
		return declarativerelease.Intent{}, errors.New("intent commit is invalid")
	}
	content, err := exec.Command("git", "show", commitSHA+":"+clean).CombinedOutput()
	if err != nil {
		return declarativerelease.Intent{}, err
	}
	return declarativerelease.DecodeIntent(bytes.NewReader(content))
}
