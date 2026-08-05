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
	"path/filepath"
	"sort"
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
		return errors.New("usage: fugue-declarative-release <plan|emit-github-output|build|receipt|prepare|execute|reconcile> ...")
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
	case "prepare":
		return runPrepare(args, output)
	case "execute":
		return runExecute(args, output)
	case "reconcile":
		return runReconcile(args, output)
	default:
		return errors.New("usage: fugue-declarative-release <plan|emit-github-output|build|receipt|prepare|execute|reconcile> ...")
	}
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
	registryFile, err := os.Open(registryPath)
	if err != nil {
		return fmt.Errorf("open registry: %w", err)
	}
	registry, decodeErr := declarativerelease.DecodeRegistry(registryFile)
	closeErr := registryFile.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if closeErr != nil {
		return closeErr
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
		Component string `json:"component"`
		BuildLane string `json:"build_lane"`
	}
	type matrix struct {
		Include []matrixEntry `json:"include"`
	}
	value := matrix{Include: make([]matrixEntry, 0, len(plan.Releases))}
	components := make([]string, 0, len(plan.Releases))
	for _, release := range plan.Releases {
		repositoryDigest := sha256.Sum256([]byte(release.Artifact.Repository))
		value.Include = append(value.Include, matrixEntry{
			Component: release.ComponentID,
			BuildLane: fmt.Sprintf("%x", repositoryDigest[:8]),
		})
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
	info, err := os.Lstat(args[2])
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 1<<20 {
		return errors.New("GITHUB_OUTPUT file is invalid")
	}
	file, err := os.OpenFile(args[2], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(file, "release_count=%d\nrelease_matrix=%s\nrelease_components=%s\n", len(plan.Releases), encoded, componentJSON)
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
	manifestFile, err := os.Open(release.ManifestPath)
	if err != nil {
		return err
	}
	lkgManifest, lkgErr := loadLKGManifest(release)
	if lkgErr != nil {
		_ = manifestFile.Close()
		return lkgErr
	}
	var lkgReader io.Reader
	if lkgManifest != nil {
		lkgReader = bytes.NewReader(lkgManifest)
	}
	rendered, renderErr := declarativerelease.RenderManifests(plan, args[2], receipt, manifestFile, lkgReader)
	closeManifestErr := manifestFile.Close()
	if renderErr != nil {
		return renderErr
	}
	if closeManifestErr != nil {
		return closeManifestErr
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
	if release.IntentGeneration == 1 {
		if release.BootstrapLKGPath == "" {
			return nil, errors.New("first declarative release with a predecessor requires bootstrapLkgPath")
		}
		content, err := os.ReadFile(release.BootstrapLKGPath)
		if err != nil {
			return nil, fmt.Errorf("read bootstrap LKG manifest: %w", err)
		}
		return content, nil
	}
	object := release.ExpectedPreviousConfigSHA + ":" + release.ManifestPath
	content, err := exec.Command("git", "show", object).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("read previous component manifest: %w", err)
	}
	return content, nil
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
	var leaseReleaseErr error
	if result.Status != "recovery-required" {
		leaseReleaseErr = leaseCoordinator.release(ctx, heldLease)
	}
	encoded, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return err
	}
	if _, err := output.Write(append(encoded, '\n')); err != nil {
		return err
	}
	if leaseReleaseErr != nil {
		return fmt.Errorf("component release terminal state is verified but lease release is unproven: %w", leaseReleaseErr)
	}
	if result.Status != "verified" {
		return fmt.Errorf("component release ended with status=%s reason=%s", result.Status, result.Reason)
	}
	return nil
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
