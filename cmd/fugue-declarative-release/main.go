package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
)

const maxChangedPathsBytes = 1 << 20

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "declarative-release:", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: fugue-declarative-release <plan|emit-github-output|build|receipt|adoption-receipt|prepare|execute|reconcile> ...")
	}
	switch args[0] {
	case "plan":
		return runPlan(args, output)
	case "emit-github-output":
		return runEmitGitHubOutput(args)
	case "build":
		return runBuild(args, output)
	case "receipt":
		return runReceipt(args, output)
	case "adoption-receipt":
		return runAdoptionReceipt(args, output)
	case "prepare":
		return runPrepare(args, output)
	case "execute":
		return runExecute(args, output)
	case "reconcile":
		return runReconcile(args, output)
	default:
		return errors.New("usage: fugue-declarative-release <plan|emit-github-output|build|receipt|adoption-receipt|prepare|execute|reconcile> ...")
	}
}

func runAdoptionReceipt(args []string, output io.Writer) error {
	if len(args) != 7 {
		return errors.New("usage: fugue-declarative-release adoption-receipt REGISTRY COMPONENT GROUP RUN_ID RUN_ATTEMPT RESULT")
	}
	registry, err := loadProductionRegistry(args[1])
	if err != nil {
		return err
	}
	var component *declarativerelease.Component
	for index := range registry.Components {
		if registry.Components[index].ID == args[2] {
			component = &registry.Components[index]
			break
		}
	}
	if component == nil {
		return errors.New("adoption receipt component is not registered")
	}
	runID, err := strconv.ParseInt(args[4], 10, 64)
	if err != nil {
		return errors.New("adoption receipt run ID is invalid")
	}
	runAttempt, err := strconv.Atoi(args[5])
	if err != nil {
		return errors.New("adoption receipt run attempt is invalid")
	}
	file, err := os.Open(args[6])
	if err != nil {
		return err
	}
	result, decodeErr := declarativerelease.DecodeExecutionResult(file)
	closeErr := file.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if closeErr != nil {
		return closeErr
	}
	receipt, err := declarativerelease.BuildOwnershipAdoptionReceipt(result, *component, args[3], runID, runAttempt)
	if err != nil {
		return err
	}
	raw, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return err
	}
	_, err = output.Write(append(raw, '\n'))
	return err
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
			previousConfigSHA[release.ComponentID] = priorSHA
		}
		if intent.SupersedesFailedConfigSHA != "" {
			if !found || intent.SupersedesFailedConfigSHA != priorSHA {
				return fmt.Errorf("component %q superseded failed atom is not the immediately prior component intent", release.ComponentID)
			}
			if err := exec.Command("git", "merge-base", "--is-ancestor", intent.ExpectedPreviousConfigSHA, baseSHA).Run(); err != nil {
				return fmt.Errorf("component %q recovered predecessor is not in the trusted base ancestry", release.ComponentID)
			}
			recoveredRaw, recoveredErr := exec.Command("git", "rev-list", "-1", intent.ExpectedPreviousConfigSHA, "--", release.IntentPath).CombinedOutput()
			if recoveredErr != nil || strings.TrimSpace(string(recoveredRaw)) != intent.ExpectedPreviousConfigSHA {
				return fmt.Errorf("component %q recovered predecessor is not an exact production intent atom", release.ComponentID)
			}
		}
	}
	bound, err := declarativerelease.BindIntents(registry, plan, current, previous, previousConfigSHA)
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
		if component.MigrationState != "independent" {
			continue
		}
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
		"release_count=%d\nrelease_matrix=%s\nrelease_components=%s\nedge_control_count=%d\nedge_control_matrix=%s\nedge_worker_count=%d\nedge_worker_matrix=%s\n",
		len(plan.Releases), encoded, componentJSON, len(edgeControl.Include), edgeControlJSON, len(edgeWorker.Include), edgeWorkerJSON,
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
	lkgManifest, lkgErr := loadLKGManifest(release)
	if lkgErr != nil {
		return lkgErr
	}
	if lkgManifest != nil && release.MigrationState != "adopting" {
		lkgManifest, err = declarativerelease.MaterializePredecessorManifestTemplate(lkgManifest, release.ManifestVariables)
		if err != nil {
			return err
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
	cluster, err := newKubectlCluster()
	if err != nil {
		return err
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
	if release.MigrationState == "adopting" {
		if release.BootstrapLKGPath == "" {
			return nil, errors.New("adopting declarative release with a predecessor requires bootstrapLkgPath")
		}
		content, err := os.ReadFile(release.BootstrapLKGPath)
		if err != nil {
			return nil, fmt.Errorf("read bootstrap LKG manifest: %w", err)
		}
		return content, nil
	}
	manifestPath, err := historicalComponentManifestPath(release.ExpectedPreviousConfigSHA, release.ComponentID)
	if err != nil {
		if !errors.Is(err, errHistoricalProductionRegistryAbsent) {
			return nil, err
		}
		content, receiptErr := loadReceiptBoundLKGManifest(release)
		if receiptErr != nil {
			return nil, fmt.Errorf("%w; receipt-bound LKG fallback: %v", err, receiptErr)
		}
		return content, nil
	}
	object := release.ExpectedPreviousConfigSHA + ":" + manifestPath
	content, err := exec.Command("git", "show", object).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("read previous component manifest: %w", err)
	}
	return content, nil
}

func loadReceiptBoundLKGManifest(release declarativerelease.PlanRelease) ([]byte, error) {
	expectedReceiptPath := "deploy/releases/" + release.ComponentID + "/adoption-receipt.json"
	if release.MigrationState != "independent" || release.AdoptionReceiptPath != expectedReceiptPath ||
		filepath.ToSlash(filepath.Clean(release.AdoptionReceiptPath)) != release.AdoptionReceiptPath {
		return nil, errors.New("release plan has no exact independent adoption receipt path")
	}
	registry, err := loadProductionRegistry("deploy/releases/components.json")
	if err != nil {
		return nil, fmt.Errorf("load current production registry: %w", err)
	}
	var component *declarativerelease.Component
	for index := range registry.Components {
		if registry.Components[index].ID == release.ComponentID {
			component = &registry.Components[index]
			break
		}
	}
	if component == nil || component.AdoptionReceiptPath != release.AdoptionReceiptPath {
		return nil, errors.New("component adoption receipt path does not match the release plan")
	}
	receiptFile, err := os.Open(release.AdoptionReceiptPath)
	if err != nil {
		return nil, fmt.Errorf("open ownership adoption receipt: %w", err)
	}
	receipt, decodeErr := declarativerelease.DecodeOwnershipAdoptionReceipt(receiptFile)
	closeErr := receiptFile.Close()
	if decodeErr != nil {
		return nil, decodeErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := receipt.Validate(*component, receipt.GroupID); err != nil {
		return nil, err
	}
	wantImage := component.Artifact.Repository + "@" + release.ExpectedPreviousImageDigest
	if receipt.Final.ConfigSHA != release.ExpectedPreviousConfigSHA ||
		receipt.Final.ManifestSHA != release.ExpectedPreviousManifestSHA ||
		receipt.Final.OCIRevision != release.ExpectedPreviousOCIRevision || receipt.Final.ImageRef != wantImage {
		return nil, errors.New("ownership adoption receipt does not bind the declared LKG")
	}
	lkgPath := filepath.Join(filepath.Dir(release.AdoptionReceiptPath), "lkg.json")
	if filepath.ToSlash(lkgPath) != "deploy/releases/"+release.ComponentID+"/lkg.json" {
		return nil, errors.New("receipt-bound LKG path escapes the component release directory")
	}
	content, err := os.ReadFile(lkgPath)
	if err != nil {
		return nil, fmt.Errorf("read receipt-bound LKG manifest: %w", err)
	}
	if err := validateReceiptBoundLKGManifest(content, release, *component, receipt); err != nil {
		return nil, err
	}
	return content, nil
}

func validateReceiptBoundLKGManifest(content []byte, release declarativerelease.PlanRelease, component declarativerelease.Component, receipt declarativerelease.OwnershipAdoptionReceipt) error {
	identities, err := declarativerelease.ResourceSetIdentities(content)
	if err != nil {
		return fmt.Errorf("decode receipt-bound LKG resource identities: %w", err)
	}
	wantPrimary := declarativerelease.ResourceIdentity{
		APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
		Namespace: release.Workload.Namespace, Name: release.Workload.Name,
	}
	resources := make(map[declarativerelease.ResourceIdentity]struct{}, len(receipt.Final.Resources))
	for _, resource := range receipt.Final.Resources {
		resources[resource.Identity] = struct{}{}
	}
	if len(identities) != len(resources) {
		return errors.New("receipt-bound LKG resource witness count drifted")
	}
	for _, identity := range identities {
		if _, exists := resources[identity]; !exists {
			return fmt.Errorf("receipt-bound LKG resource %s/%s is absent from the adoption witness", identity.Kind, identity.Name)
		}
	}
	primary, err := declarativerelease.ResourceSetItem(content, wantPrimary)
	if err != nil {
		return fmt.Errorf("receipt-bound LKG primary workload: %w", err)
	}
	spec, ok := primary["spec"].(map[string]any)
	if !ok {
		return errors.New("receipt-bound LKG workload spec is invalid")
	}
	template, ok := spec["template"].(map[string]any)
	if !ok {
		return errors.New("receipt-bound LKG workload template is invalid")
	}
	templateMetadata, ok := template["metadata"].(map[string]any)
	if !ok {
		return errors.New("receipt-bound LKG template metadata is invalid")
	}
	annotations, ok := templateMetadata["annotations"].(map[string]any)
	if !ok || annotations["fugue.pro/source-commit"] != release.ExpectedPreviousManifestSHA ||
		annotations["fugue.pro/oci-revision"] != release.ExpectedPreviousOCIRevision {
		return errors.New("receipt-bound LKG template source identity drifted")
	}
	templateSpec, ok := template["spec"].(map[string]any)
	if !ok {
		return errors.New("receipt-bound LKG pod spec is invalid")
	}
	containers, ok := templateSpec["containers"].([]any)
	if !ok {
		return errors.New("receipt-bound LKG containers are invalid")
	}
	wantImage := component.Artifact.Repository + "@" + release.ExpectedPreviousImageDigest
	matched := 0
	for _, rawContainer := range containers {
		container, ok := rawContainer.(map[string]any)
		if ok && container["name"] == release.Workload.Container {
			matched++
			if container["image"] != wantImage {
				return errors.New("receipt-bound LKG workload image drifted")
			}
		}
	}
	if matched != 1 {
		return errors.New("receipt-bound LKG workload container is not unique")
	}
	if _, err := declarativerelease.PredecessorConvergenceManifest(content); err != nil {
		return fmt.Errorf("build receipt-bound LKG resource witness: %w", err)
	}
	return nil
}

func historicalComponentManifestPath(revision, componentID string) (string, error) {
	const registryPath = "deploy/releases/components.json"
	raw, err := exec.Command("git", "show", revision+":"+registryPath).Output()
	if err != nil {
		if historicalGitPathAbsent(err, registryPath) {
			return "", fmt.Errorf("historical production registry is absent: %w", errHistoricalProductionRegistryAbsent)
		}
		return "", fmt.Errorf("read previous production registry: %w", err)
	}
	registry, err := declarativerelease.DecodeRegistry(bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("decode previous production registry: %w", err)
	}
	manifestPaths := make(map[string]string, len(registry.Components))
	for _, component := range registry.Components {
		if _, exists := manifestPaths[component.ID]; exists {
			return "", fmt.Errorf("previous production registry repeats component %q", component.ID)
		}
		manifestPaths[component.ID] = component.ManifestPath
	}
	if registry.EdgeGroupRegistryPath != "" {
		edgeRaw, readErr := exec.Command("git", "show", revision+":"+registry.EdgeGroupRegistryPath).Output()
		if readErr != nil {
			return "", fmt.Errorf("read previous edge group registry: %w", readErr)
		}
		historical, decodeErr := decodeHistoricalEdgeGroupManifestPaths(bytes.NewReader(edgeRaw))
		if decodeErr != nil {
			return "", fmt.Errorf("decode previous edge group registry: %w", decodeErr)
		}
		for id, path := range historical {
			if _, exists := manifestPaths[id]; exists {
				return "", fmt.Errorf("previous production registry repeats component %q", id)
			}
			manifestPaths[id] = path
		}
	}
	manifestPath := manifestPaths[componentID]
	if manifestPath != "" {
		return manifestPath, nil
	}
	return "", fmt.Errorf("previous production registry does not contain component %q", componentID)
}

var errHistoricalProductionRegistryAbsent = errors.New("historical production registry path is absent")

func historicalGitPathAbsent(err error, repositoryPath string) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 128 {
		return false
	}
	stderr := string(exitErr.Stderr)
	quoted := "path '" + repositoryPath + "'"
	return strings.Contains(stderr, quoted+" does not exist in") ||
		(strings.Contains(stderr, quoted+" exists on disk, but not in") && strings.Contains(stderr, "fatal:"))
}

type historicalEdgeGroupRegistry struct {
	APIVersion string                `json:"apiVersion"`
	Kind       string                `json:"kind"`
	Groups     []historicalEdgeGroup `json:"groups"`
}

type historicalEdgeGroup struct {
	ID      string                       `json:"id"`
	GroupID string                       `json:"groupId"`
	Control declarativerelease.Component `json:"control"`
	Worker  declarativerelease.Component `json:"worker"`
}

// decodeHistoricalEdgeGroupManifestPaths is deliberately a read-only projection.
// It accepts adoption metadata that was valid in an immutable historical tree,
// validates every component with the current Component contract, and returns
// only component IDs and manifest paths. It never feeds historical migration metadata into the current registry,
// release plan, ownership adapter, or executor.
func decodeHistoricalEdgeGroupManifestPaths(reader io.Reader) (map[string]string, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var registry historicalEdgeGroupRegistry
	if err := decoder.Decode(&registry); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, errors.New("historical edge group registry contains multiple JSON values")
		}
		return nil, err
	}
	if registry.APIVersion != declarativerelease.EdgeGroupRegistryAPIVersion || registry.Kind != declarativerelease.EdgeGroupRegistryKind ||
		len(registry.Groups) == 0 || len(registry.Groups) > 100 {
		return nil, errors.New("historical edge group registry identity is invalid")
	}
	seenGroupIDs := make(map[string]struct{}, len(registry.Groups))
	seenComponents := make(map[string]struct{}, len(registry.Groups)*2)
	manifestPaths := make(map[string]string, len(registry.Groups)*2)
	for index, group := range registry.Groups {
		if strings.TrimSpace(group.ID) == "" || strings.TrimSpace(group.GroupID) == "" ||
			(index > 0 && registry.Groups[index-1].ID >= group.ID) {
			return nil, errors.New("historical edge groups are not strictly ordered")
		}
		if _, exists := seenGroupIDs[group.GroupID]; exists {
			return nil, fmt.Errorf("historical edge group id %q is repeated", group.GroupID)
		}
		seenGroupIDs[group.GroupID] = struct{}{}
		if group.Control.ID != "edge-control-"+group.ID || group.Worker.ID != "edge-worker-"+group.ID {
			return nil, fmt.Errorf("historical edge group %q component identity is invalid", group.ID)
		}
		for _, component := range []declarativerelease.Component{group.Control, group.Worker} {
			if err := component.Validate(); err != nil {
				return nil, fmt.Errorf("historical component %q: %w", component.ID, err)
			}
			if _, exists := seenComponents[component.ID]; exists {
				return nil, fmt.Errorf("historical component %q is repeated", component.ID)
			}
			seenComponents[component.ID] = struct{}{}
			manifestPaths[component.ID] = component.ManifestPath
		}
	}
	return manifestPaths, nil
}

func runExecute(args []string, output io.Writer) error {
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
	ctx, cancel := context.WithTimeout(context.Background(), executeTimeout)
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
	leaseReleaseErr := leaseCoordinator.release(ctx, heldLease)
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
