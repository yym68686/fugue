package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	metadataclient "k8s.io/client-go/metadata"
)

const maxKubernetesOutputBytes = 4 << 20

var (
	adoptionConflictCountPattern = regexp.MustCompile(`Apply failed with ([1-9][0-9]*) conflicts?`)
	adoptionConflictPattern      = regexp.MustCompile(`conflict with "([^"]+)" using [^:]+: (\.[^[:space:]]+)`)
	adoptionConflictGroupPattern = regexp.MustCompile(`conflicts with "([^"]+)" using [^:]+:`)
)

type kubectlCluster struct {
	kubectl  string
	verifier string
	timeout  time.Duration
	metadata metadataclient.Interface
}

type healthSoakTracker struct {
	required time.Duration
	since    time.Time
}

type podHTTPEndpoint struct {
	Name string
	IP   string
	Port int
}

func (tracker *healthSoakTracker) observe(now time.Time, healthy bool) bool {
	if tracker == nil || tracker.required <= 0 {
		return healthy
	}
	if !healthy {
		tracker.since = time.Time{}
		return false
	}
	if tracker.since.IsZero() {
		tracker.since = now
	}
	return now.Sub(tracker.since) >= tracker.required
}

func newKubectlCluster() (*kubectlCluster, error) {
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		return nil, errors.New("kubectl is unavailable")
	}
	verifier := "scripts/verify_registry_image.py"
	if info, err := os.Stat(verifier); err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("registry verifier is unavailable")
	}
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes metadata client config: %w", err)
	}
	metadata, err := metadataclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes metadata client: %w", err)
	}
	return &kubectlCluster{kubectl: kubectl, verifier: verifier, timeout: 120 * time.Second, metadata: metadata}, nil
}

func (cluster *kubectlCluster) Observe(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) (declarativerelease.Observation, error) {
	return cluster.observeExpected(ctx, release, target.OCIRevision, manifest, allowsHistoricalRestarts(release, target))
}

func allowsHistoricalRestarts(release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity) bool {
	return release.MigrationState == "adopting" && release.OwnershipAdoption != nil &&
		release.ExpectedPreviousPresent && target.Present &&
		target.ConfigSHA == release.ExpectedPreviousConfigSHA &&
		target.ManifestSHA == release.ExpectedPreviousManifestSHA &&
		target.OCIRevision == release.ExpectedPreviousOCIRevision
}

func (cluster *kubectlCluster) ObserveCAS(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) (declarativerelease.Observation, error) {
	primary := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	workloadRaw, err := cluster.getResource(ctx, primary)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	resources, err := cluster.observeResources(ctx, manifest, release, workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	observation := declarativerelease.Observation{Present: len(bytes.TrimSpace(workloadRaw)) > 0, Primary: primary, Resources: resources}
	if !observation.Present {
		return observation, nil
	}
	value, err := decodeJSONObject(workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	metadata := mapField(value, "metadata")
	observation.UID = stringValue(metadata["uid"])
	observation.ResourceVersion = stringValue(metadata["resourceVersion"])
	observation.Generation = int64Value(metadata["generation"])
	return observation, nil
}

func (cluster *kubectlCluster) ObserveDegraded(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) (declarativerelease.Observation, error) {
	primary := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	workloadRaw, err := cluster.getResource(ctx, primary)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if len(bytes.TrimSpace(workloadRaw)) == 0 {
		return declarativerelease.Observation{}, errors.New("owned degraded predecessor is absent")
	}
	observation, err := parseDegradedObservation(workloadRaw, release)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	resources, err := cluster.observeResources(ctx, manifest, release, workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	observation.Resources = resources
	if err := normalizeAdoptionBootstrapDegradedIdentity(&observation, release); err != nil {
		return declarativerelease.Observation{}, err
	}
	if err := observation.ValidateDegradedPredecessor(release); err != nil {
		return declarativerelease.Observation{}, err
	}
	verificationArgs := []string{"--image", observation.ImageRef, "--platform", "linux/amd64"}
	allowMissingRevision := allowsLegacyBootstrapRegistryRevision(release, observation)
	if !allowMissingRevision {
		verificationArgs = append(verificationArgs, "--expected-revision", observation.OCIRevision)
	}
	verificationArgs = append(verificationArgs, "--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5",
		"--max-attempts", "2", "--retry-delay-seconds", "0.1")
	commandArgs := append([]string{cluster.verifier}, verificationArgs...)
	verificationRaw, err := cluster.run(ctx, nil, "python3", commandArgs...)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify degraded predecessor registry identity: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if verification.Image != observation.ImageRef || (!allowMissingRevision && verification.OCIRevision != observation.OCIRevision) ||
		(allowMissingRevision && verification.OCIRevision != "" && verification.OCIRevision != observation.OCIRevision) {
		return declarativerelease.Observation{}, errors.New("degraded predecessor registry identity mismatch")
	}
	return observation, nil
}

func allowsLegacyBootstrapRegistryRevision(release declarativerelease.PlanRelease, observation declarativerelease.Observation) bool {
	return release.MigrationState == "adopting" && release.OwnershipAdoption != nil && release.RetrySameLKG &&
		release.HeterogeneousBootstrapLKG && release.BootstrapLKGPath != "" && release.ExpectedPreviousPresent &&
		observation.Present && observation.ConfigSHA == release.ExpectedPreviousConfigSHA &&
		observation.ManifestSHA == release.ExpectedPreviousManifestSHA && observation.OCIRevision == release.ExpectedPreviousOCIRevision &&
		immutableRefDigestLocal(observation.ImageRef) == release.ExpectedPreviousImageDigest
}

// normalizeAdoptionBootstrapDegradedIdentity is the narrow compatibility
// bridge for a legacy Helm-owned LKG.  Older Edge workloads may omit the
// declarative config annotation (and, for older releases, use a source tag),
// while the adoption intent carries the exact immutable identity.  This is
// only reachable for an explicitly adopting, heterogeneous bootstrap bound
// to that exact predecessor; independent releases never get this fallback.
func normalizeAdoptionBootstrapDegradedIdentity(observation *declarativerelease.Observation, release declarativerelease.PlanRelease) error {
	if observation == nil {
		return errors.New("degraded predecessor observation is nil")
	}
	if release.MigrationState != "adopting" || release.OwnershipAdoption == nil || !release.RetrySameLKG ||
		!release.HeterogeneousBootstrapLKG || release.BootstrapLKGPath == "" || !release.ExpectedPreviousPresent {
		return nil
	}
	if observation.ManifestSHA != release.ExpectedPreviousManifestSHA || observation.OCIRevision != release.ExpectedPreviousOCIRevision {
		return errors.New("legacy bootstrap predecessor source identity is invalid")
	}
	if observation.ConfigSHA != "" && observation.ConfigSHA != release.ExpectedPreviousConfigSHA {
		return errors.New("legacy bootstrap predecessor config identity is invalid")
	}
	if observation.ConfigSHA == "" {
		observation.ConfigSHA = release.ExpectedPreviousConfigSHA
	}
	if digest := immutableRefDigestLocal(observation.ImageRef); digest != "" {
		if digest != release.ExpectedPreviousImageDigest {
			return errors.New("legacy bootstrap predecessor image identity is invalid")
		}
		return nil
	}
	if legacySourceTag(observation.ImageRef) != release.ExpectedPreviousOCIRevision {
		return errors.New("legacy bootstrap predecessor image tag is not source-bound")
	}
	if release.Artifact.Repository == "" {
		return errors.New("legacy bootstrap predecessor repository is missing")
	}
	observation.ImageRef = release.Artifact.Repository + "@" + release.ExpectedPreviousImageDigest
	return nil
}

func (cluster *kubectlCluster) VerifyTarget(ctx context.Context, target declarativerelease.TargetIdentity) error {
	if !target.Present || target.ImageRef == "" || target.OCIRevision == "" {
		return errors.New("registry target identity is incomplete")
	}
	verificationRaw, err := cluster.run(ctx, nil, "python3", cluster.verifier,
		"--image", target.ImageRef, "--platform", "linux/amd64", "--expected-revision", target.OCIRevision,
		"--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5",
		"--max-attempts", "2", "--retry-delay-seconds", "0.1")
	if err != nil {
		return fmt.Errorf("verify registry target: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil {
		return err
	}
	if verification.Image != target.ImageRef || verification.OCIRevision != target.OCIRevision {
		return errors.New("registry target identity mismatch")
	}
	return nil
}

func (cluster *kubectlCluster) VerifyBootstrapTarget(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity) error {
	if release.MigrationState != "adopting" || release.OwnershipAdoption == nil || !release.RetrySameLKG ||
		!release.HeterogeneousBootstrapLKG || release.BootstrapLKGPath == "" || !release.ExpectedPreviousPresent || !target.Present ||
		target.ConfigSHA != release.ExpectedPreviousConfigSHA || target.ManifestSHA != release.ExpectedPreviousManifestSHA ||
		target.OCIRevision != release.ExpectedPreviousOCIRevision || immutableRefDigestLocal(target.ImageRef) != release.ExpectedPreviousImageDigest {
		return errors.New("bootstrap registry compatibility is not explicitly adoption-bound")
	}
	verificationRaw, err := cluster.run(ctx, nil, "python3", cluster.verifier,
		"--image", target.ImageRef, "--platform", "linux/amd64", "--metadata-only", "--timeout-seconds", "18",
		"--request-timeout-seconds", "5", "--max-attempts", "2", "--retry-delay-seconds", "0.1")
	if err != nil {
		return fmt.Errorf("verify bootstrap registry target: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil {
		return err
	}
	if verification.Image != target.ImageRef || (verification.OCIRevision != "" && verification.OCIRevision != target.OCIRevision) {
		return errors.New("bootstrap registry target identity mismatch")
	}
	return nil
}

func immutableRefDigestLocal(ref string) string {
	parts := strings.Split(ref, "@")
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func (cluster *kubectlCluster) DryRunApply(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	if err := cluster.requireReferencedSecrets(ctx, release, manifest); err != nil {
		return err
	}
	return cluster.applyResourceSet(ctx, release, manifest, true)
}

func (cluster *kubectlCluster) DryRunOwnershipAdoption(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, lkgManifest []byte) error {
	manifest, err := declarativerelease.BuildOwnershipAdoptionManifest(lkgManifest, adoption)
	if err != nil {
		return err
	}
	return cluster.applyOwnershipAdoptionSet(ctx, release, adoption, manifest, true)
}

func (cluster *kubectlCluster) DryRunOwnershipTakeover(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, target declarativerelease.TargetIdentity, targetManifest []byte) error {
	manifest, err := declarativerelease.BuildOwnershipTakeoverManifest(targetManifest, adoption, target)
	if err != nil {
		return err
	}
	return cluster.applyOwnershipAdoptionSet(ctx, release, adoption, manifest, true)
}

func (cluster *kubectlCluster) AdoptOwnership(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, lkg declarativerelease.TargetIdentity, lkgManifest []byte) (declarativerelease.Observation, error) {
	manifest, err := declarativerelease.BuildOwnershipAdoptionManifest(lkgManifest, adoption)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	applyErr := cluster.applyOwnershipAdoptionSet(ctx, release, adoption, manifest, false)
	if err := cluster.verifyOwnershipAdoption(ctx, release, adoption); err != nil {
		if applyErr != nil {
			return declarativerelease.Observation{}, fmt.Errorf("apply ownership adoption: %v; verify ownership adoption: %w", applyErr, err)
		}
		return declarativerelease.Observation{}, err
	}
	if err := cluster.Converged(ctx, release, lkgManifest); err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify adopted bootstrap LKG convergence: %w", err)
	}
	observation, observeErr := cluster.observeExpected(ctx, release, lkg.OCIRevision, lkgManifest, true)
	if observeErr != nil {
		return declarativerelease.Observation{}, observeErr
	}
	return observation, nil
}

func (cluster *kubectlCluster) TakeoverOwnership(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, target declarativerelease.TargetIdentity, targetManifest []byte) (declarativerelease.Observation, error) {
	current, err := cluster.ObserveCAS(ctx, release, targetManifest)
	if err != nil {
		return current, fmt.Errorf("refresh ownership takeover CAS: %w", err)
	}
	if err := refreshOwnershipTakeoverCAS(&adoption, current); err != nil {
		return current, err
	}
	manifest, err := declarativerelease.BuildOwnershipTakeoverManifest(targetManifest, adoption, target)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	applyErr := cluster.applyOwnershipAdoptionSet(ctx, release, adoption, manifest, false)
	// An Edge bootstrap LKG and the immutable target can contain different
	// associative-list members inside the reviewed Pod template. SSA force
	// transfers ownership of fields that are present in the target, but it does
	// not remove legacy-only list members that remain owned by Helm. Replace the
	// already-reviewed template with UID/RV/generation JSON-patch tests, then
	// re-apply the scoped SSA manifest so the declarative manager owns the exact
	// target bytes. This compatibility step is restricted to adopting Edge-group
	// transitions and is never reachable from the independent path.
	if applyErr == nil && release.MigrationState == "adopting" && release.Transition != nil && release.Transition.Type == "edge-group-ab" {
		if replaceErr := cluster.replaceEdgeOwnershipTakeoverTemplates(ctx, release, adoption, targetManifest); replaceErr != nil {
			applyErr = replaceErr
		} else {
			refreshed, refreshErr := cluster.ObserveCAS(ctx, release, targetManifest)
			if refreshErr == nil {
				refreshErr = refreshOwnershipTakeoverPostPatchCAS(&adoption, refreshed)
			}
			if refreshErr == nil {
				manifest, refreshErr = declarativerelease.BuildOwnershipTakeoverManifest(targetManifest, adoption, target)
			}
			if refreshErr == nil {
				refreshErr = cluster.applyOwnershipAdoptionSet(ctx, release, adoption, manifest, false)
			}
			applyErr = refreshErr
		}
	}
	verifyErr := cluster.verifyOwnershipAdoption(ctx, release, adoption)
	convergedErr := cluster.Converged(ctx, release, manifest)
	observation, observeErr := cluster.ObserveCAS(ctx, release, targetManifest)
	if err := errors.Join(applyErr, verifyErr, convergedErr, observeErr); err != nil {
		return observation, fmt.Errorf("verify ownership takeover: %w", err)
	}
	return observation, nil
}

func (cluster *kubectlCluster) replaceEdgeOwnershipTakeoverTemplates(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, targetManifest []byte) error {
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes client config: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	for _, scope := range adoption.Resources {
		targetItem, itemErr := declarativerelease.ResourceSetItem(targetManifest, scope.Identity)
		if itemErr != nil {
			return itemErr
		}
		gvr, gvrErr := resourceGVR(scope.Identity)
		if gvrErr != nil {
			return gvrErr
		}
		resource := client.Resource(gvr).Namespace(scope.Identity.Namespace)
		live, getErr := resource.Get(ctx, scope.Identity.Name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("read ownership takeover target %s/%s: %w", scope.Identity.Kind, scope.Identity.Name, getErr)
		}
		patch, patchErr := edgeOwnershipTakeoverTemplatePatch(targetItem, live.Object, scope)
		if patchErr != nil {
			return fmt.Errorf("bind ownership takeover template %s/%s: %w", scope.Identity.Kind, scope.Identity.Name, patchErr)
		}
		if _, patchErr = resource.Patch(ctx, scope.Identity.Name, types.JSONPatchType, patch, metav1.PatchOptions{FieldManager: release.Workload.FieldManager}); patchErr != nil {
			return fmt.Errorf("replace reviewed ownership takeover template %s/%s: %w", scope.Identity.Kind, scope.Identity.Name, patchErr)
		}
	}
	return nil
}

func edgeOwnershipTakeoverTemplatePatch(target, live map[string]any, scope declarativerelease.OwnershipAdoptionResourcePlan) ([]byte, error) {
	if scope.Identity.Kind != "DaemonSet" || !stringInSortedSet("/spec/template", scope.Fields) {
		return nil, errors.New("Edge ownership takeover template is outside the reviewed scope")
	}
	metadata := mapField(live, "metadata")
	if stringValue(metadata["name"]) != scope.Identity.Name || stringValue(metadata["namespace"]) != scope.Identity.Namespace ||
		stringValue(metadata["uid"]) != scope.UID || stringValue(metadata["resourceVersion"]) == "" ||
		int64Value(metadata["generation"]) < scope.Generation {
		return nil, errors.New("Edge ownership takeover template CAS is invalid")
	}
	template := mapField(mapField(target, "spec"), "template")
	if len(template) == 0 {
		return nil, errors.New("Edge ownership takeover target template is absent")
	}
	operations := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": scope.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": stringValue(metadata["resourceVersion"])},
		{"op": "test", "path": "/metadata/generation", "value": int64Value(metadata["generation"])},
		{"op": "replace", "path": "/spec/template", "value": template},
	}
	return declarativerelease.CanonicalJSON(operations)
}

// refreshOwnershipTakeoverPostPatchCAS binds a second scoped SSA apply to the
// exact UID/RV/generation produced by the already-CAS-protected template
// replacement. Unlike the prewrite refresh, generation movement is expected
// here because the preceding reviewed patch changed the Pod template.
func refreshOwnershipTakeoverPostPatchCAS(adoption *declarativerelease.OwnershipAdoptionPlan, current declarativerelease.Observation) error {
	if adoption == nil || !current.Present || current.UID == "" || current.ResourceVersion == "" ||
		current.UID != adoption.UID || current.Generation < adoption.Generation {
		return errors.New("post-patch ownership takeover CAS is incomplete")
	}
	byIdentity := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(current.Resources))
	for _, resource := range current.Resources {
		byIdentity[resource.Identity] = resource
	}
	for index := range adoption.Resources {
		scope := &adoption.Resources[index]
		resource, ok := byIdentity[scope.Identity]
		if !ok || !resource.Present || resource.UID != scope.UID || resource.Generation < scope.Generation || resource.ResourceVersion == "" {
			return fmt.Errorf("post-patch ownership takeover CAS detected drift for %s/%s", scope.Identity.Kind, scope.Identity.Name)
		}
		scope.ResourceVersion = resource.ResourceVersion
		scope.Generation = resource.Generation
	}
	adoption.ResourceVersion = current.ResourceVersion
	adoption.Generation = current.Generation
	return nil
}

// refreshOwnershipTakeoverCAS rebinds only the mutable RV portion of the
// reviewed adoption scope immediately before the first takeover write. UID and
// generation remain exact CAS witnesses; a changed generation is a spec drift
// and therefore fails closed rather than being silently refreshed.
func refreshOwnershipTakeoverCAS(adoption *declarativerelease.OwnershipAdoptionPlan, current declarativerelease.Observation) error {
	if adoption == nil || !current.Present || current.UID == "" || current.ResourceVersion == "" {
		return errors.New("ownership takeover CAS refresh is incomplete")
	}
	if current.UID != adoption.UID || current.Generation != adoption.Generation {
		return errors.New("ownership takeover CAS refresh detected UID or generation drift")
	}
	byIdentity := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(current.Resources))
	for _, resource := range current.Resources {
		byIdentity[resource.Identity] = resource
	}
	for index := range adoption.Resources {
		scope := &adoption.Resources[index]
		resource, ok := byIdentity[scope.Identity]
		if !ok || !resource.Present || resource.UID != scope.UID || resource.Generation != scope.Generation || resource.ResourceVersion == "" {
			return fmt.Errorf("ownership takeover CAS refresh detected drift for %s/%s", scope.Identity.Kind, scope.Identity.Name)
		}
		scope.ResourceVersion = resource.ResourceVersion
	}
	adoption.ResourceVersion = current.ResourceVersion
	return nil
}

func (cluster *kubectlCluster) applyOwnershipAdoptionSet(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, manifest []byte, dryRun bool) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return err
	}
	arguments, err := adoptionApplyArguments(release, dryRun)
	if err != nil {
		return err
	}
	scopes := make(map[declarativerelease.ResourceIdentity]declarativerelease.OwnershipAdoptionResourcePlan, len(adoption.Resources))
	for _, scope := range adoption.Resources {
		scopes[scope.Identity] = scope
	}
	for _, identity := range identities {
		item, err := declarativerelease.ResourceSetItem(manifest, identity)
		if err != nil {
			return err
		}
		encoded, err := declarativerelease.CanonicalJSON(item)
		if err != nil {
			return err
		}
		if _, applyErr := cluster.kubectlRun(ctx, encoded, arguments...); applyErr != nil {
			scope, exists := scopes[identity]
			if !exists {
				return fmt.Errorf("adopt %s/%s has no reviewed scope", identity.Kind, identity.Name)
			}
			managers := append([]string(nil), adoption.LegacyFieldManagers...)
			if adoption.AlreadyConverged {
				managers = append(managers, release.Workload.FieldManager)
				sort.Strings(managers)
			}
			if err := validateAdoptionConflicts(applyErr, managers, scope.Fields); err != nil {
				return fmt.Errorf("adopt %s/%s: %w", identity.Kind, identity.Name, err)
			}
			forceArguments, err := adoptionForceApplyArguments(release, dryRun)
			if err != nil {
				return err
			}
			if _, forceErr := cluster.kubectlRun(ctx, encoded, forceArguments...); forceErr != nil {
				return fmt.Errorf("adopt %s/%s reviewed force-conflicts: %w", identity.Kind, identity.Name, forceErr)
			}
		}
	}
	return nil
}

func validateAdoptionConflicts(applyErr error, managers, fields []string) error {
	if applyErr == nil || len(managers) == 0 || len(fields) == 0 {
		return errors.New("ownership adoption conflict evidence is incomplete")
	}
	raw := applyErr.Error()
	countMatch := adoptionConflictCountPattern.FindStringSubmatch(raw)
	type conflict struct{ manager, field string }
	conflicts := make([]conflict, 0)
	groupManager := ""
	for _, line := range strings.Split(raw, "\n") {
		if match := adoptionConflictPattern.FindStringSubmatch(line); len(match) == 3 {
			conflicts = append(conflicts, conflict{manager: match[1], field: match[2]})
			groupManager = ""
			continue
		}
		if match := adoptionConflictGroupPattern.FindStringSubmatch(line); len(match) == 2 {
			groupManager = match[1]
			continue
		}
		trimmed := strings.TrimSpace(line)
		if groupManager != "" && strings.HasPrefix(trimmed, "- .") {
			conflicts = append(conflicts, conflict{manager: groupManager, field: strings.TrimPrefix(trimmed, "- ")})
			continue
		}
		if groupManager != "" && trimmed != "" {
			groupManager = ""
		}
	}
	if len(countMatch) != 2 || len(conflicts) == 0 {
		return errors.New("ownership adoption failure is not a typed SSA conflict")
	}
	count, err := strconv.Atoi(countMatch[1])
	if err != nil || count != len(conflicts) {
		return errors.New("ownership adoption conflict count is inconsistent")
	}
	for _, conflict := range conflicts {
		if !stringInSortedSet(conflict.manager, managers) || !adoptionFieldAllowed(conflict.field, fields) {
			return fmt.Errorf("ownership adoption conflict %s:%s is outside the reviewed allowlist", conflict.manager, conflict.field)
		}
	}
	return nil
}

func stringInSortedSet(value string, allowed []string) bool {
	index := sort.SearchStrings(allowed, value)
	return index < len(allowed) && allowed[index] == value
}

func adoptionFieldAllowed(field string, pointers []string) bool {
	for _, pointer := range pointers {
		parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
		for index, part := range parts {
			parts[index] = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		}
		prefix := "." + strings.Join(parts, ".")
		if field == prefix || strings.HasPrefix(field, prefix+".") || strings.HasPrefix(field, prefix+"[") {
			return true
		}
	}
	return false
}

func (cluster *kubectlCluster) verifyOwnershipAdoption(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan) error {
	for _, scope := range adoption.Resources {
		raw, err := cluster.getResource(ctx, scope.Identity)
		if err != nil || len(bytes.TrimSpace(raw)) == 0 {
			return fmt.Errorf("read adopted %s/%s: %w", scope.Identity.Kind, scope.Identity.Name, err)
		}
		value, err := decodeJSONObject(raw)
		if err != nil {
			return err
		}
		metadata := mapField(value, "metadata")
		if stringValue(metadata["uid"]) != scope.UID || int64Value(metadata["generation"]) < scope.Generation ||
			!managedFieldsOwnPointers(metadata, release.Workload.FieldManager, scope.Fields) {
			return fmt.Errorf("adopted %s/%s ownership is incomplete", scope.Identity.Kind, scope.Identity.Name)
		}
	}
	return nil
}

func managedFieldsOwnPointers(metadata map[string]any, manager string, pointers []string) bool {
	for _, rawEntry := range anySlice(metadata["managedFields"]) {
		entry, _ := rawEntry.(map[string]any)
		if stringValue(entry["manager"]) != manager || stringValue(entry["operation"]) != "Apply" || stringValue(entry["subresource"]) != "" {
			continue
		}
		fields := mapField(entry, "fieldsV1")
		all := true
		for _, pointer := range pointers {
			current := fields
			for _, token := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
				token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
				next, ok := current["f:"+token].(map[string]any)
				if !ok {
					all = false
					break
				}
				current = next
			}
		}
		if all {
			return true
		}
	}
	return false
}

func (cluster *kubectlCluster) Apply(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) error {
	if err := cluster.requireReferencedSecrets(ctx, release, manifest); err != nil {
		return err
	}
	if release.Transition != nil && release.Transition.Type == "edge-group-ab" {
		if isEdgeBootstrapLKGTarget(release, target) {
			return cluster.applyEdgeBootstrapLKG(ctx, release, target, manifest)
		}
		return cluster.applyEdgeGroupAB(ctx, release, target, manifest)
	}
	return cluster.applyResourceSet(ctx, release, manifest, false)
}

func (cluster *kubectlCluster) requireReferencedSecrets(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	if cluster == nil || cluster.metadata == nil {
		return errors.New("Kubernetes metadata client is unavailable")
	}
	names, err := declarativerelease.ReferencedRequiredSecrets(manifest)
	if err != nil {
		return err
	}
	secrets := cluster.metadata.Resource(schema.GroupVersionResource{Version: "v1", Resource: "secrets"}).Namespace(release.Workload.Namespace)
	for _, name := range names {
		value, getErr := secrets.Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return fmt.Errorf("required Secret %q is absent", name)
			}
			return fmt.Errorf("read required Secret metadata %q: %w", name, getErr)
		}
		if value.GetName() != name || value.GetNamespace() != release.Workload.Namespace || value.GetUID() == "" || value.GetResourceVersion() == "" {
			return fmt.Errorf("required Secret metadata %q is invalid", name)
		}
	}
	return nil
}

func (cluster *kubectlCluster) Delete(ctx context.Context, _ declarativerelease.PlanRelease, manifest []byte, observation declarativerelease.Observation) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return err
	}
	observed := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(observation.Resources))
	for _, resource := range observation.Resources {
		observed[resource.Identity] = resource
	}
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes client config: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	primary := declarativerelease.ResourceIdentity{APIVersion: observation.Primary.APIVersion, Kind: observation.Primary.Kind, Namespace: observation.Primary.Namespace, Name: observation.Primary.Name}
	order := make([]declarativerelease.ResourceIdentity, 0, len(identities))
	for _, identity := range identities {
		if identity == primary {
			order = append(order, identity)
			break
		}
	}
	for index := len(identities) - 1; index >= 0; index-- {
		if identities[index] != primary {
			order = append(order, identities[index])
		}
	}
	if len(order) != len(identities) {
		return errors.New("delete resource set does not contain its primary workload")
	}
	for _, identity := range order {
		resource, exists := observed[identity]
		if !exists || !resource.Present || resource.UID == "" || resource.ResourceVersion == "" {
			return fmt.Errorf("delete %s/%s lacks an exact observed precondition", identity.Kind, identity.Name)
		}
		if resource.RetainOnRollback {
			continue
		}
		gvr, mapErr := resourceGVR(identity)
		if mapErr != nil {
			return mapErr
		}
		uid, rv, present, refreshErr := freshDeletionPreconditions(ctx, client.Resource(gvr).Namespace(identity.Namespace), resource)
		if refreshErr != nil {
			return fmt.Errorf("refresh delete preconditions for %s/%s: %w", identity.Kind, identity.Name, refreshErr)
		}
		if !present {
			continue
		}
		foreground := metav1.DeletePropagationForeground
		options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}, PropagationPolicy: &foreground}
		if deleteErr := client.Resource(gvr).Namespace(identity.Namespace).Delete(ctx, identity.Name, options); deleteErr != nil {
			return fmt.Errorf("delete %s/%s with UID/RV preconditions: %w", identity.Kind, identity.Name, deleteErr)
		}
		deadline := time.Now().Add(cluster.timeout)
		for {
			_, getErr := client.Resource(gvr).Namespace(identity.Namespace).Get(ctx, identity.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				break
			}
			if getErr != nil {
				return fmt.Errorf("reconcile deletion of %s/%s: %w", identity.Kind, identity.Name, getErr)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("deletion of %s/%s did not converge", identity.Kind, identity.Name)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

func (cluster *kubectlCluster) DeleteCreated(ctx context.Context, _ declarativerelease.PlanRelease, forwardManifest []byte, before, after declarativerelease.Observation) error {
	return cluster.deleteCreated(ctx, forwardManifest, nil, before, after)
}

// DeleteCreatedForOwnershipTakeover preserves forward ServiceAccount
// dependencies during migration compensation. A historical LKG can omit the
// account even while the live workload references it; deleting the newly
// materialized account would make a restarted DaemonSet unschedulable.
func (cluster *kubectlCluster) DeleteCreatedForOwnershipTakeover(ctx context.Context, _ declarativerelease.PlanRelease, forwardManifest, _ []byte, before, after declarativerelease.Observation) error {
	identities, err := declarativerelease.ResourceSetIdentities(forwardManifest)
	if err != nil {
		return err
	}
	preserve := make(map[declarativerelease.ResourceIdentity]struct{})
	for _, identity := range identities {
		if identity.APIVersion == "v1" && identity.Kind == "ServiceAccount" {
			preserve[identity] = struct{}{}
		}
	}
	return cluster.deleteCreated(ctx, forwardManifest, preserve, before, after)
}

func (cluster *kubectlCluster) deleteCreated(ctx context.Context, forwardManifest []byte, preserve map[declarativerelease.ResourceIdentity]struct{}, before, after declarativerelease.Observation) error {
	identities, err := declarativerelease.ResourceSetIdentities(forwardManifest)
	if err != nil {
		return err
	}
	deletions, err := createdResourceDeletions(identities, before, after)
	if err != nil {
		return err
	}
	if len(preserve) > 0 {
		filtered := deletions[:0]
		for _, deletion := range deletions {
			if _, keep := preserve[deletion.Identity]; keep {
				continue
			}
			filtered = append(filtered, deletion)
		}
		deletions = filtered
	}
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes client config: %w", err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	for _, current := range deletions {
		identity := current.Identity
		if current.UID == "" || current.ResourceVersion == "" {
			return fmt.Errorf("created-resource rollback lacks UID/RV for %s/%s", identity.Kind, identity.Name)
		}
		gvr, mapErr := resourceGVR(identity)
		if mapErr != nil {
			return mapErr
		}
		uid, rv, present, refreshErr := freshDeletionPreconditions(ctx, client.Resource(gvr).Namespace(identity.Namespace), current)
		if refreshErr != nil {
			return fmt.Errorf("refresh created-resource delete preconditions for %s/%s: %w", identity.Kind, identity.Name, refreshErr)
		}
		if !present {
			continue
		}
		foreground := metav1.DeletePropagationForeground
		options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}, PropagationPolicy: &foreground}
		if deleteErr := client.Resource(gvr).Namespace(identity.Namespace).Delete(ctx, identity.Name, options); deleteErr != nil {
			return fmt.Errorf("delete created %s/%s with UID/RV preconditions: %w", identity.Kind, identity.Name, deleteErr)
		}
		deadline := time.Now().Add(cluster.timeout)
		for {
			_, getErr := client.Resource(gvr).Namespace(identity.Namespace).Get(ctx, identity.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				break
			}
			if getErr != nil {
				return fmt.Errorf("reconcile created %s/%s deletion: %w", identity.Kind, identity.Name, getErr)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("created %s/%s deletion did not converge", identity.Kind, identity.Name)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

// ClearOwnershipTakeoverForwardOnlyFields removes only fields that the
// reviewed forward manifest introduces and the LKG intentionally omits. This
// is a migration-only CAS patch: it never broadens the rollback manifest and
// never reads Secret values.
func (cluster *kubectlCluster) ClearOwnershipTakeoverForwardOnlyFields(ctx context.Context, release declarativerelease.PlanRelease, forwardManifest, lkgManifest []byte, observed declarativerelease.Observation) error {
	forwardIDs, err := declarativerelease.ResourceSetIdentities(forwardManifest)
	if err != nil {
		return err
	}
	clientConfig, err := loadComponentLeaseClientConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes client config: %w", err)
	}
	client, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	observedByIdentity := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(observed.Resources))
	for _, resource := range observed.Resources {
		observedByIdentity[resource.Identity] = resource
	}
	for _, identity := range forwardIDs {
		forwardItem, err := declarativerelease.ResourceSetItem(forwardManifest, identity)
		if err != nil {
			return err
		}
		lkgItem, err := declarativerelease.ResourceSetItem(lkgManifest, identity)
		if err != nil {
			// A historical LKG may omit the forward-only component
			// ServiceAccount. It is intentionally retained during
			// compensation, so there are no LKG-owned fields to clear.
			if canOmitLKGCompensationResource(identity) {
				continue
			}
			return err
		}
		paths, err := ownershipTakeoverForwardOnlyPaths(forwardItem, lkgItem)
		if err != nil {
			return fmt.Errorf("derive compensation fields for %s/%s: %w", identity.Kind, identity.Name, err)
		}
		if len(paths) == 0 {
			continue
		}
		prior, ok := observedByIdentity[identity]
		if !ok || !prior.Present || prior.UID == "" {
			return fmt.Errorf("compensation observation is incomplete for %s/%s", identity.Kind, identity.Name)
		}
		gvr, err := resourceGVR(identity)
		if err != nil {
			return err
		}
		resource := client.Resource(gvr).Namespace(identity.Namespace)
		live, err := resource.Get(ctx, identity.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read compensation target %s/%s: %w", identity.Kind, identity.Name, err)
		}
		if string(live.GetUID()) != prior.UID || live.GetResourceVersion() == "" || (prior.Generation > 0 && live.GetGeneration() < prior.Generation) {
			return fmt.Errorf("compensation target %s/%s identity drifted", identity.Kind, identity.Name)
		}
		patch := []map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": string(live.GetUID())},
			{"op": "test", "path": "/metadata/resourceVersion", "value": live.GetResourceVersion()},
		}
		for _, path := range paths {
			if _, present := ownershipJSONPointer(live.Object, path); present {
				patch = append(patch, map[string]any{"op": "remove", "path": path})
			}
		}
		if len(patch) == 2 {
			continue
		}
		payload, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		if _, err := resource.Patch(ctx, identity.Name, types.JSONPatchType, payload, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("clear compensation fields for %s/%s: %w", identity.Kind, identity.Name, err)
		}
		verified, err := resource.Get(ctx, identity.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("verify compensation fields for %s/%s: %w", identity.Kind, identity.Name, err)
		}
		for _, path := range paths {
			if _, present := ownershipJSONPointer(verified.Object, path); present {
				return fmt.Errorf("compensation field %s remains on %s/%s", path, identity.Kind, identity.Name)
			}
		}
	}
	return nil
}

func canOmitLKGCompensationResource(identity declarativerelease.ResourceIdentity) bool {
	return identity.APIVersion == "v1" && identity.Kind == "ServiceAccount"
}

func ownershipTakeoverForwardOnlyPaths(forward, lkg map[string]any) ([]string, error) {
	allowed := []string{"/spec/template/spec/serviceAccount", "/spec/template/spec/serviceAccountName", "/spec/template/spec/initContainers"}
	paths := make([]string, 0, len(allowed))
	forwardServiceAccountName, forwardServiceAccountNamePresent := ownershipJSONPointer(forward, "/spec/template/spec/serviceAccountName")
	_, lkgServiceAccountNamePresent := ownershipJSONPointer(lkg, "/spec/template/spec/serviceAccountName")
	_, lkgLegacyServiceAccountPresent := ownershipJSONPointer(lkg, "/spec/template/spec/serviceAccount")
	for _, path := range allowed {
		forwardValue, forwardPresent := ownershipJSONPointer(forward, path)
		_, lkgPresent := ownershipJSONPointer(lkg, path)
		// Kubernetes may materialize the deprecated serviceAccount alias
		// when a forward-only serviceAccountName is applied. Treat that
		// alias as part of the same reviewed compensation scope so the
		// historical LKG can actually be restored.
		if path == "/spec/template/spec/serviceAccount" && forwardServiceAccountNamePresent && !lkgServiceAccountNamePresent && !lkgLegacyServiceAccountPresent {
			forwardValue, forwardPresent = forwardServiceAccountName, true
		}
		if forwardPresent && !lkgPresent {
			if forwardValue == nil {
				return nil, fmt.Errorf("forward-only field %s is null", path)
			}
			paths = append(paths, path)
		}
	}
	return paths, nil
}

func ownershipJSONPointer(value map[string]any, pointer string) (any, bool) {
	current := any(value)
	for _, token := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		current, ok = object[token]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func freshDeletionPreconditions(ctx context.Context, resource dynamic.ResourceInterface, expected declarativerelease.ResourceObservation) (types.UID, string, bool, error) {
	if resource == nil || expected.UID == "" || expected.ResourceVersion == "" {
		return "", "", false, errors.New("deletion observation is incomplete")
	}
	current, err := resource.Get(ctx, expected.Identity.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	if string(current.GetUID()) != expected.UID || (expected.Generation > 0 && current.GetGeneration() != expected.Generation) || current.GetResourceVersion() == "" {
		return "", "", false, errors.New("deletion resource identity changed")
	}
	return current.GetUID(), current.GetResourceVersion(), true, nil
}

func createdResourceDeletions(identities []declarativerelease.ResourceIdentity, before, after declarativerelease.Observation) ([]declarativerelease.ResourceObservation, error) {
	beforeByIdentity := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(before.Resources))
	afterByIdentity := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(after.Resources))
	for _, resource := range before.Resources {
		beforeByIdentity[resource.Identity] = resource
	}
	for _, resource := range after.Resources {
		afterByIdentity[resource.Identity] = resource
	}
	result := make([]declarativerelease.ResourceObservation, 0)
	for index := len(identities) - 1; index >= 0; index-- {
		identity := identities[index]
		prior, priorExists := beforeByIdentity[identity]
		current, currentExists := afterByIdentity[identity]
		if !priorExists || !currentExists {
			return nil, fmt.Errorf("created-resource rollback lacks identity %s/%s", identity.Kind, identity.Name)
		}
		if !prior.Present && current.Present && !current.RetainOnRollback {
			result = append(result, current)
		}
	}
	return result, nil
}

func resourceGVR(identity declarativerelease.ResourceIdentity) (schema.GroupVersionResource, error) {
	known := map[string]schema.GroupVersionResource{
		"apps/v1/DaemonSet":                  {Group: "apps", Version: "v1", Resource: "daemonsets"},
		"apps/v1/Deployment":                 {Group: "apps", Version: "v1", Resource: "deployments"},
		"batch/v1/Job":                       {Group: "batch", Version: "v1", Resource: "jobs"},
		"networking.k8s.io/v1/NetworkPolicy": {Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
		"policy/v1/PodDisruptionBudget":      {Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"},
		"v1/PersistentVolumeClaim":           {Version: "v1", Resource: "persistentvolumeclaims"},
		"v1/Service":                         {Version: "v1", Resource: "services"},
		"v1/ServiceAccount":                  {Version: "v1", Resource: "serviceaccounts"},
	}
	gvr, exists := known[identity.APIVersion+"/"+identity.Kind]
	if !exists {
		return schema.GroupVersionResource{}, fmt.Errorf("resource %s/%s is not in the declarative deletion allowlist", identity.APIVersion, identity.Kind)
	}
	return gvr, nil
}

func (cluster *kubectlCluster) applyResourceSet(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte, dryRun bool) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return err
	}
	primary := declarativerelease.ResourceIdentity{
		APIVersion: release.Workload.APIVersion,
		Kind:       release.Workload.Kind,
		Namespace:  release.Workload.Namespace,
		Name:       release.Workload.Name,
	}
	ordered := make([]declarativerelease.ResourceIdentity, 0, len(identities))
	primaryFound := false
	for _, identity := range identities {
		if identity == primary {
			primaryFound = true
			continue
		}
		ordered = append(ordered, identity)
	}
	if !primaryFound {
		return errors.New("component resource set does not contain its primary workload")
	}
	// Apply the workload last. Starting a rollout can immediately update the
	// status/resourceVersion of PDBs and other dependent resources. Applying
	// those resources first preserves every prewrite UID/RV CAS until it is
	// consumed, while still keeping the workload as the atom's final write.
	identities = append(ordered, primary)
	for _, identity := range identities {
		item, itemErr := declarativerelease.ResourceSetItem(manifest, identity)
		if itemErr != nil {
			return itemErr
		}
		if identity.Kind == "Job" {
			if jobErr := cluster.applyJob(ctx, release, identity, item, dryRun); jobErr != nil {
				return fmt.Errorf("apply Job/%s: %w", identity.Name, jobErr)
			}
			continue
		}
		encoded, encodeErr := declarativerelease.CanonicalJSON(item)
		if encodeErr != nil {
			return encodeErr
		}
		if _, applyErr := cluster.kubectlRun(ctx, encoded, applyArguments(release, dryRun)...); applyErr != nil {
			return fmt.Errorf("apply %s/%s: %w", identity.Kind, identity.Name, applyErr)
		}
	}
	return nil
}

func (cluster *kubectlCluster) applyJob(ctx context.Context, release declarativerelease.PlanRelease, identity declarativerelease.ResourceIdentity, item map[string]any, dryRun bool) error {
	metadata := mapField(item, "metadata")
	uid := stringValue(metadata["uid"])
	rv := stringValue(metadata["resourceVersion"])
	delete(metadata, "uid")
	delete(metadata, "resourceVersion")
	if dryRun {
		metadata["name"] = identity.Name + "-dryrun"
		encoded, err := declarativerelease.CanonicalJSON(item)
		if err != nil {
			return err
		}
		_, err = cluster.kubectlRun(ctx, encoded, applyArguments(release, true)...)
		return err
	}
	if uid != "" || rv != "" {
		if uid == "" || rv == "" {
			return errors.New("Job replacement has a partial delete precondition")
		}
		config, err := loadComponentLeaseClientConfig()
		if err != nil {
			return fmt.Errorf("load Kubernetes client config: %w", err)
		}
		client, err := dynamic.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("create Kubernetes dynamic client: %w", err)
		}
		gvr, _ := resourceGVR(identity)
		uidValue := types.UID(uid)
		foreground := metav1.DeletePropagationForeground
		options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uidValue, ResourceVersion: &rv}, PropagationPolicy: &foreground}
		if err := client.Resource(gvr).Namespace(identity.Namespace).Delete(ctx, identity.Name, options); err != nil {
			return fmt.Errorf("delete prior Job with UID/RV preconditions: %w", err)
		}
		deadline := time.Now().Add(cluster.timeout)
		for {
			_, getErr := client.Resource(gvr).Namespace(identity.Namespace).Get(ctx, identity.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				break
			}
			if getErr != nil {
				return fmt.Errorf("reconcile prior Job deletion: %w", getErr)
			}
			if time.Now().After(deadline) {
				return errors.New("prior Job deletion did not converge")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	encoded, err := declarativerelease.CanonicalJSON(item)
	if err != nil {
		return err
	}
	_, err = cluster.kubectlRun(ctx, encoded, applyArguments(release, false)...)
	return err
}

func applyArguments(release declarativerelease.PlanRelease, dryRun bool) []string {
	arguments := []string{"apply", "--server-side", "--field-manager", release.Workload.FieldManager}
	if dryRun {
		arguments = append(arguments, "--dry-run=server")
	}
	return append(arguments, "--filename", "-", "--output", "json")
}

func adoptionApplyArguments(release declarativerelease.PlanRelease, dryRun bool) ([]string, error) {
	if release.MigrationState != "adopting" || release.OwnershipAdoption == nil {
		return nil, errors.New("ownership adoption is not explicitly authorized")
	}
	arguments := []string{"apply", "--server-side", "--field-manager", release.Workload.FieldManager}
	if dryRun {
		arguments = append(arguments, "--dry-run=server")
	}
	return append(arguments, "--filename", "-", "--output", "json"), nil
}

func adoptionForceApplyArguments(release declarativerelease.PlanRelease, dryRun bool) ([]string, error) {
	arguments, err := adoptionApplyArguments(release, dryRun)
	if err != nil {
		return nil, err
	}
	for index, argument := range arguments {
		if argument == "--filename" {
			return append(append(append([]string(nil), arguments[:index]...), "--force-conflicts"), arguments[index:]...), nil
		}
	}
	return nil, errors.New("ownership adoption apply arguments are invalid")
}

func (cluster *kubectlCluster) WaitHealthy(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) (declarativerelease.Observation, error) {
	if !target.Present {
		return declarativerelease.Observation{}, errors.New("cannot wait healthy for an absent target")
	}
	allowHistoricalRestarts := allowsHistoricalRestarts(release, target)
	soak := healthSoakDuration(release, allowHistoricalRestarts)
	deadline := time.Now().Add(cluster.timeout + soak)
	var lastErr error
	tracker := healthSoakTracker{required: soak}
	allowLegacyManager := allowHistoricalRestarts
	for {
		observation, err := cluster.observeExpected(ctx, release, target.OCIRevision, manifest, allowHistoricalRestarts)
		if err == nil && observation.Matches(target, release, allowLegacyManager) {
			probeDigest, probeErr := cluster.verifyProbes(ctx, release, target, manifest, observation)
			if probeErr == nil {
				observation.HealthDigest = digestJoin(observation.HealthDigest, probeDigest)
				if tracker.observe(time.Now(), true) {
					return observation, nil
				}
			} else {
				tracker.observe(time.Now(), false)
			}
			err = probeErr
		} else {
			tracker.observe(time.Now(), false)
		}
		lastErr = err
		if time.Now().After(deadline) {
			if lastErr == nil {
				lastErr = errors.New("workload did not converge to target")
			}
			return observation, lastErr
		}
		select {
		case <-ctx.Done():
			return observation, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func healthSoakDuration(release declarativerelease.PlanRelease, bootstrap bool) time.Duration {
	if bootstrap || release.Transition == nil || release.Transition.EdgeGroupAB == nil {
		return 0
	}
	return time.Duration(release.Transition.EdgeGroupAB.SoakSeconds) * time.Second
}

func (cluster *kubectlCluster) Converged(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return err
	}
	for _, identity := range identities {
		desired, desiredErr := declarativerelease.ResourceSetItem(manifest, identity)
		if desiredErr != nil {
			return desiredErr
		}
		liveRaw, getErr := cluster.getResource(ctx, identity)
		if getErr != nil {
			return getErr
		}
		if len(bytes.TrimSpace(liveRaw)) == 0 {
			return fmt.Errorf("declared resource %s/%s is absent", identity.Kind, identity.Name)
		}
		live, decodeErr := decodeJSONObject(liveRaw)
		if decodeErr != nil {
			return decodeErr
		}
		if !declarativerelease.ResourceDesiredSubset(desired, live) {
			return fmt.Errorf("declared resource %s/%s has not converged", identity.Kind, identity.Name)
		}
	}
	return nil
}

func (cluster *kubectlCluster) observeExpected(ctx context.Context, release declarativerelease.PlanRelease, expectedOCI string, manifest []byte, allowHistoricalRestarts bool) (declarativerelease.Observation, error) {
	primary := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	workloadRaw, err := cluster.getResource(ctx, primary)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if len(bytes.TrimSpace(workloadRaw)) == 0 {
		resources, resourceErr := cluster.observeResources(ctx, manifest, release, workloadRaw)
		if resourceErr != nil {
			return declarativerelease.Observation{}, resourceErr
		}
		return declarativerelease.Observation{Present: false, Primary: primary, Resources: resources}, nil
	}
	selector, err := selectorFromWorkload(workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	podsRaw, err := cluster.kubectlRun(ctx, nil, "get", "pods", "--namespace", release.Workload.Namespace,
		"--selector", selector, "--output", "json")
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if err := verifyDeclaredArtifactImageIDs(podsRaw, manifest, release); err != nil {
		return declarativerelease.Observation{}, err
	}
	observationWorkloadRaw := workloadRaw
	if allowHistoricalRestarts && release.MigrationState == "adopting" && release.OwnershipAdoption != nil &&
		release.RetrySameLKG && release.HeterogeneousBootstrapLKG && release.BootstrapLKGPath != "" {
		observationWorkloadRaw, err = bootstrapObservationWorkload(workloadRaw, manifest, primary)
		if err != nil {
			return declarativerelease.Observation{}, err
		}
	}
	partial, err := parseObservation(observationWorkloadRaw, podsRaw, release, allowHistoricalRestarts, true)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	verificationImage, err := observedVerificationImage(partial.ImageRef, partial.ImageID, partial.ManifestSHA, allowHistoricalRestarts)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	arguments := []string{"python3", cluster.verifier, "--image", verificationImage, "--platform", "linux/amd64"}
	if !allowHistoricalRestarts {
		arguments = append(arguments, "--expected-revision", expectedOCI)
	}
	arguments = append(arguments, "--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5", "--max-attempts", "2", "--retry-delay-seconds", "0.1")
	verificationRaw, err := cluster.run(ctx, nil, arguments[0], arguments[1:]...)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify live image provenance: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if !observedRegistryIdentityMatches(verification, verificationImage, expectedOCI, allowHistoricalRestarts) {
		return declarativerelease.Observation{}, errors.New("live registry identity mismatch")
	}
	partial.OCIRevision = expectedOCI
	resources, err := cluster.observeResources(ctx, manifest, release, workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	partial.Resources = resources
	return partial, nil
}

// bootstrapObservationWorkload binds the running bootstrap Pod cohort to the
// exact Git LKG while retaining live Kubernetes CAS, ownership and status.
// During an adoption retry the desired DaemonSet template may already be the
// reviewed forward target even though OnDelete Pods still run the heterogeneous
// bootstrap LKG. This path is explicitly limited to adopting bootstrap calls;
// convergence independently proves the live desired resource set.
func bootstrapObservationWorkload(liveRaw, manifest []byte, identity declarativerelease.ResourceIdentity) ([]byte, error) {
	live, err := decodeJSONObject(liveRaw)
	if err != nil {
		return nil, err
	}
	expected, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return nil, err
	}
	liveMetadata := mapField(live, "metadata")
	expectedMetadata := mapField(expected, "metadata")
	for _, field := range []string{"uid", "resourceVersion", "generation", "managedFields"} {
		value, exists := liveMetadata[field]
		if !exists {
			return nil, fmt.Errorf("bootstrap observation live metadata %s is absent", field)
		}
		expectedMetadata[field] = value
	}
	status, exists := live["status"]
	if !exists {
		return nil, errors.New("bootstrap observation live status is absent")
	}
	expected["status"] = status
	return declarativerelease.CanonicalJSON(expected)
}

func observedVerificationImage(imageRef, imageID, expectedSource string, allowHistorical bool) (string, error) {
	if strings.Contains(imageRef, "@sha256:") {
		return imageRef, nil
	}
	if !allowHistorical || legacySourceTag(imageRef) != expectedSource {
		return "", errors.New("legacy workload image is not exact adoption bootstrap source")
	}
	separator := strings.LastIndex(imageRef, ":")
	if separator <= strings.LastIndex(imageRef, "/") || separator < 1 || !strings.HasPrefix(imageID, "sha256:") {
		return "", errors.New("legacy workload image cannot be bound to immutable pod identity")
	}
	return imageRef[:separator] + "@" + imageID, nil
}

func observedRegistryIdentityMatches(verification declarativerelease.RegistryVerification, image, expectedOCI string, allowHistorical bool) bool {
	return verification.Image == image && ((!allowHistorical && verification.OCIRevision == expectedOCI) ||
		(allowHistorical && (verification.OCIRevision == "" || verification.OCIRevision == expectedOCI)))
}

func (cluster *kubectlCluster) observeResources(ctx context.Context, manifest []byte, release declarativerelease.PlanRelease, workloadRaw []byte) ([]declarativerelease.ResourceObservation, error) {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return nil, err
	}
	resources := make([]declarativerelease.ResourceObservation, 0, len(identities))
	for _, identity := range identities {
		raw := workloadRaw
		if identity.APIVersion != release.Workload.APIVersion || identity.Kind != release.Workload.Kind ||
			identity.Namespace != release.Workload.Namespace || identity.Name != release.Workload.Name {
			raw, err = cluster.getResource(ctx, identity)
			if err != nil {
				return nil, err
			}
		}
		resource := declarativerelease.ResourceObservation{Identity: identity}
		desired, desiredErr := declarativerelease.ResourceSetItem(manifest, identity)
		if desiredErr != nil {
			return nil, desiredErr
		}
		desiredMetadata := mapField(desired, "metadata")
		resource.RetainOnRollback = mapStringField(desiredMetadata, "annotations")["fugue.pro/release-retain-on-rollback"] == "true"
		if len(bytes.TrimSpace(raw)) == 0 {
			resources = append(resources, resource)
			continue
		}
		value, decodeErr := decodeJSONObject(raw)
		if decodeErr != nil {
			return nil, decodeErr
		}
		metadata := mapField(value, "metadata")
		resource.Present = true
		resource.UID = stringValue(metadata["uid"])
		resource.ResourceVersion = stringValue(metadata["resourceVersion"])
		resource.Generation = int64Value(metadata["generation"])
		resource.FieldManagers = managedFieldManagers(metadata)
		resource.ObjectDigest = digestJSON(sanitizeObservedResource(value))
		resources = append(resources, resource)
	}
	return resources, nil
}

func (cluster *kubectlCluster) getResource(ctx context.Context, identity declarativerelease.ResourceIdentity) ([]byte, error) {
	return cluster.kubectlRun(ctx, nil, "get", strings.ToLower(identity.Kind), identity.Name,
		"--namespace", identity.Namespace, "--output", "json", "--show-managed-fields", "--ignore-not-found")
}

func sanitizeObservedResource(value map[string]any) map[string]any {
	copyRaw, _ := json.Marshal(value)
	copyValue, _ := decodeJSONObject(copyRaw)
	delete(copyValue, "status")
	metadata := mapField(copyValue, "metadata")
	for _, key := range []string{"creationTimestamp", "generation", "managedFields", "resourceVersion", "uid"} {
		delete(metadata, key)
	}
	// The DaemonSet controller rewrites this annotation as it observes
	// template generations. It is not declarative intent and must not turn a
	// prewrite CAS into a false drift between two read-only observations.
	if annotations, ok := metadata["annotations"].(map[string]any); ok {
		delete(annotations, "deprecated.daemonset.template.generation")
	}
	return copyValue
}

func (cluster *kubectlCluster) verifyProbes(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte, observation declarativerelease.Observation) (string, error) {
	evidence := make([]string, 0, len(release.Health))
	bootstrap := allowsHistoricalRestarts(release, target)
	for _, probe := range release.Health {
		switch probe.Type {
		case "deployment", "daemonset", "job":
			kind := map[string]string{"deployment": "Deployment", "daemonset": "DaemonSet", "job": "Job"}[probe.Type]
			if probe.Name == release.Workload.Name && kind == release.Workload.Kind {
				evidence = append(evidence, probe.Type+":"+observation.TemplateDigest+":"+observation.HealthDigest)
				continue
			}
			auxiliary, err := cluster.verifyAuxiliaryWorkload(ctx, release, target, manifest, kind, probe.Name)
			if err != nil {
				return "", err
			}
			evidence = append(evidence, probe.Type+":"+probe.Name+":"+auxiliary.TemplateDigest+":"+auxiliary.HealthDigest)
		case "service-http":
			resource := fmt.Sprintf("/api/v1/namespaces/%s/services/%s:%s/proxy%s", release.Workload.Namespace, probe.Name, probe.Port, probe.Path)
			body, err := cluster.kubectlRun(ctx, nil, "get", "--raw", resource)
			if err != nil || (probe.Expected != "" && !bytes.Contains(body, []byte(probe.Expected))) {
				return "", fmt.Errorf("service health probe %q failed", probe.Name)
			}
			evidence = append(evidence, probe.Type+":"+digestBytesLocal(body))
		case "pod-http":
			if probe.Name != release.Workload.Name {
				return "", fmt.Errorf("pod health probe %q does not name the primary workload", probe.Name)
			}
			pods, err := cluster.readyPodHTTPEndpoints(ctx, release, probe.Port)
			if err != nil {
				return "", err
			}
			for _, pod := range pods {
				body, err := readPodHTTP(ctx, pod, probe.Path)
				expected := probeExpectedBody(probe, bootstrap)
				if err != nil || (expected != "" && !bytes.Contains(body, []byte(expected))) {
					return "", fmt.Errorf("pod health probe %q failed", pod.Name)
				}
				evidence = append(evidence, probe.Type+":"+pod.Name+":"+digestBytesLocal(body))
			}
		case "leader-lease":
			leaseRaw, err := cluster.kubectlRun(ctx, nil, "get", "lease", probe.Name,
				"--namespace", release.Workload.Namespace, "--output", "json")
			if err != nil {
				return "", err
			}
			holder, renew, err := parseLeaderLease(leaseRaw)
			if err != nil || time.Since(renew) > 30*time.Second {
				return "", errors.New("leader lease is stale or invalid")
			}
			evidence = append(evidence, probe.Type+":"+holder+":"+renew.UTC().Format(time.RFC3339Nano))
		case "edge-group-authority":
			if release.Transition == nil || release.Transition.EdgeGroupAB == nil || probe.Name != release.Transition.EdgeGroupAB.GroupID {
				return "", errors.New("edge group authority probe is not transition-bound")
			}
			transition := *release.Transition.EdgeGroupAB
			state, err := cluster.readEdgeGroupState(ctx, release, transition)
			if err != nil {
				return "", err
			}
			if !bootstrap {
				if err := validateEdgeGroupAuthority(state, transition); err != nil {
					return "", err
				}
			}
			items := []string{"group=" + transition.GroupID, "active_slot=" + state.ActiveSlot, "bootstrap=" + strconv.FormatBool(bootstrap)}
			for _, slot := range []struct {
				name string
				pods map[string]edgeGroupPod
			}{{"a", state.WorkerA}, {"b", state.WorkerB}} {
				for _, node := range sortedEdgeNodes(slot.pods) {
					pod := slot.pods[node]
					items = append(items, strings.Join([]string{slot.name, node, pod.RouteBundleSource, strconv.FormatUint(pod.PublicationSequence, 10), pod.ServingGeneration, strconv.FormatBool(pod.InventoryProducerActive), strconv.FormatUint(pod.InventoryHeartbeatGeneration, 10)}, ":"))
				}
			}
			evidence = append(evidence, probe.Type+":"+digestBytesLocal([]byte(strings.Join(items, "\n"))))
		default:
			return "", fmt.Errorf("unsupported health probe %q", probe.Type)
		}
	}
	sort.Strings(evidence)
	return digestBytesLocal([]byte(strings.Join(evidence, "\n"))), nil
}

func probeExpectedBody(probe declarativerelease.HealthProbe, bootstrap bool) string {
	if bootstrap && probe.Type == "pod-http" {
		return ""
	}
	return probe.Expected
}

func (cluster *kubectlCluster) verifyAuxiliaryWorkload(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte, kind, name string) (declarativerelease.Observation, error) {
	apiVersion := "apps/v1"
	if kind == "Job" {
		apiVersion = "batch/v1"
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: apiVersion, Kind: kind, Namespace: release.Workload.Namespace, Name: name}
	desired, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("health workload %s/%s is not declared: %w", kind, name, err)
	}
	container, err := healthWorkloadContainer(release, apiVersion, kind, name)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	workload, err := workloadFromDeclaredResource(desired, identity, container, release.Workload.FieldManager)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	auxiliaryRelease := release
	auxiliaryRelease.Workload = workload
	expected := target
	allowLegacy := release.MigrationState == "adopting" && release.OwnershipAdoption != nil &&
		release.HeterogeneousBootstrapLKG && release.BootstrapLKGPath != "" &&
		target.ConfigSHA == release.ExpectedPreviousConfigSHA && target.OCIRevision == release.ExpectedPreviousOCIRevision
	if allowLegacy {
		expected, err = targetIdentityFromDeclaredWorkload(desired, workload)
		if err != nil {
			return declarativerelease.Observation{}, err
		}
	}
	auxiliary, err := cluster.observeExpected(ctx, auxiliaryRelease, expected.OCIRevision, manifest, allowLegacy)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("observe health workload %s/%s: %w", kind, name, err)
	}
	if !auxiliary.Matches(expected, auxiliaryRelease, allowLegacy) {
		return declarativerelease.Observation{}, fmt.Errorf("health workload %s/%s has not converged to the immutable target", kind, name)
	}
	return auxiliary, nil
}

func healthWorkloadContainer(release declarativerelease.PlanRelease, apiVersion, kind, name string) (string, error) {
	if release.Transition != nil && release.Transition.EdgeGroupAB != nil && kind == "DaemonSet" &&
		(name == release.Transition.EdgeGroupAB.WorkerAName || name == release.Transition.EdgeGroupAB.WorkerBName) {
		container := release.Transition.EdgeGroupAB.WorkerContainer
		for _, artifactTarget := range release.ArtifactTargets {
			if artifactTarget.APIVersion == apiVersion && artifactTarget.Kind == kind && artifactTarget.Namespace == release.Workload.Namespace &&
				artifactTarget.Name == name && artifactTarget.ContainerType == "container" && artifactTarget.Container == container {
				return container, nil
			}
		}
		return "", fmt.Errorf("health workload %s/%s has no transition-bound worker container", kind, name)
	}
	container := ""
	for _, artifactTarget := range release.ArtifactTargets {
		if artifactTarget.APIVersion == apiVersion && artifactTarget.Kind == kind && artifactTarget.Namespace == release.Workload.Namespace &&
			artifactTarget.Name == name && artifactTarget.ContainerType == "container" {
			if container != "" {
				return "", fmt.Errorf("health workload %s/%s has multiple primary artifact containers", kind, name)
			}
			container = artifactTarget.Container
		}
	}
	if container == "" {
		return "", fmt.Errorf("health workload %s/%s has no declared artifact container", kind, name)
	}
	return container, nil
}

func targetIdentityFromDeclaredWorkload(desired map[string]any, workload declarativerelease.Workload) (declarativerelease.TargetIdentity, error) {
	spec := mapField(desired, "spec")
	template := mapField(spec, "template")
	templateMetadata := mapField(template, "metadata")
	templateAnnotations := mapStringField(templateMetadata, "annotations")
	templateSpec := mapField(template, "spec")
	containers, ok := templateSpec["containers"].([]any)
	if !ok {
		return declarativerelease.TargetIdentity{}, errors.New("declared workload containers are invalid")
	}
	image := ""
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			return declarativerelease.TargetIdentity{}, errors.New("declared workload container is invalid")
		}
		if stringValue(container["name"]) == workload.Container {
			if image != "" {
				return declarativerelease.TargetIdentity{}, errors.New("declared workload primary container is ambiguous")
			}
			image = stringValue(container["image"])
		}
	}
	if image == "" || !strings.Contains(image, "@sha256:") {
		return declarativerelease.TargetIdentity{}, errors.New("declared workload primary image is not immutable")
	}
	manifestSHA := templateAnnotations["fugue.pro/source-commit"]
	ociRevision := templateAnnotations["fugue.pro/oci-revision"]
	metadata := mapField(desired, "metadata")
	annotations := mapStringField(metadata, "annotations")
	configSHA := annotations["fugue.pro/production-config-sha"]
	if configSHA == "" {
		configSHA = annotations["fugue.pro/"+strings.TrimSuffix(workload.FieldManager, "-declarative")+"-manifest-revision"]
	}
	if configSHA == "" {
		configSHA = manifestSHA
	}
	if len(configSHA) != 40 || len(manifestSHA) != 40 || len(ociRevision) != 40 {
		return declarativerelease.TargetIdentity{}, errors.New("declared workload source identity is invalid")
	}
	return declarativerelease.TargetIdentity{Present: true, ImageRef: image, ConfigSHA: configSHA, ManifestSHA: manifestSHA, OCIRevision: ociRevision}, nil
}

func verifyDeclaredArtifactImageIDs(podsRaw, manifest []byte, release declarativerelease.PlanRelease) error {
	identity := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	desired, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return err
	}
	expected := make(map[string]string)
	for _, target := range release.ArtifactTargets {
		if target.APIVersion != identity.APIVersion || target.Kind != identity.Kind || target.Namespace != identity.Namespace || target.Name != identity.Name {
			continue
		}
		workload, found, imageErr := declaredContainerImageOptional(desired, target.Container, target.ContainerType)
		if imageErr != nil {
			return imageErr
		}
		if !found {
			continue
		}
		expected[target.ContainerType+"\x00"+target.Container] = workload
	}
	if len(expected) == 0 {
		image, imageErr := declaredContainerImage(desired, release.Workload.Container, "container")
		if imageErr != nil {
			return imageErr
		}
		expected["container\x00"+release.Workload.Container] = image
	}
	list, err := decodeJSONObject(podsRaw)
	if err != nil {
		return err
	}
	items, ok := list["items"].([]any)
	if !ok || len(items) == 0 {
		return errors.New("artifact workload has no Pods")
	}
	for _, raw := range items {
		pod, ok := raw.(map[string]any)
		if !ok {
			return errors.New("artifact workload Pod is invalid")
		}
		status := mapField(pod, "status")
		for key, image := range expected {
			parts := strings.SplitN(key, "\x00", 2)
			field := "containerStatuses"
			if parts[0] == "init-container" {
				field = "initContainerStatuses"
			}
			statuses, ok := status[field].([]any)
			if !ok {
				return fmt.Errorf("Pod %s are absent", field)
			}
			wantDigest := image[strings.LastIndex(image, "@")+1:]
			matched := false
			for _, rawStatus := range statuses {
				containerStatus, ok := rawStatus.(map[string]any)
				if !ok || stringValue(containerStatus["name"]) != parts[1] {
					continue
				}
				imageID := stringValue(containerStatus["imageID"])
				if imageID != wantDigest && !strings.HasSuffix(imageID, "@"+wantDigest) {
					return fmt.Errorf("Pod container %s imageID does not match declared immutable image", parts[1])
				}
				matched = true
			}
			if !matched {
				return fmt.Errorf("Pod container %s status is absent", parts[1])
			}
		}
	}
	return nil
}

func declaredContainerImage(desired map[string]any, name, containerType string) (string, error) {
	image, found, err := declaredContainerImageOptional(desired, name, containerType)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.New("declared workload container is absent")
	}
	return image, nil
}

func declaredContainerImageOptional(desired map[string]any, name, containerType string) (string, bool, error) {
	spec := mapField(desired, "spec")
	template := mapField(spec, "template")
	templateSpec := mapField(template, "spec")
	field := "containers"
	if containerType == "init-container" {
		field = "initContainers"
	}
	containers, ok := templateSpec[field].([]any)
	if !ok {
		if containerType == "init-container" {
			return "", false, nil
		}
		return "", false, fmt.Errorf("declared workload %s are invalid", field)
	}
	image := ""
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			return "", false, errors.New("declared workload container is invalid")
		}
		if stringValue(container["name"]) == name {
			if image != "" {
				return "", false, errors.New("declared workload container is ambiguous")
			}
			image = stringValue(container["image"])
		}
	}
	if image == "" {
		return "", false, nil
	}
	if !strings.Contains(image, "@sha256:") {
		return "", false, errors.New("declared workload image is not immutable")
	}
	return image, true, nil
}

func workloadFromDeclaredResource(desired map[string]any, identity declarativerelease.ResourceIdentity, container, fieldManager string) (declarativerelease.Workload, error) {
	workload := declarativerelease.Workload{
		APIVersion: identity.APIVersion, Kind: identity.Kind, Namespace: identity.Namespace, Name: identity.Name,
		Container: container, FieldManager: fieldManager,
	}
	spec := mapField(desired, "spec")
	switch identity.Kind {
	case "Deployment":
		workload.Replicas = int(int64Value(spec["replicas"]))
		strategy := mapField(spec, "strategy")
		workload.RolloutMode = map[string]string{"RollingUpdate": "rolling", "Recreate": "recreate"}[stringValue(strategy["type"])]
	case "DaemonSet":
		strategy := mapField(spec, "updateStrategy")
		workload.RolloutMode = map[string]string{"RollingUpdate": "rolling", "OnDelete": "on-delete"}[stringValue(strategy["type"])]
	case "Job":
		workload.RolloutMode = "job"
	default:
		return declarativerelease.Workload{}, fmt.Errorf("health workload kind %q is unsupported", identity.Kind)
	}
	if workload.RolloutMode == "" || (identity.Kind == "Deployment" && workload.Replicas < 1) {
		return declarativerelease.Workload{}, fmt.Errorf("health workload %s/%s has invalid rollout configuration", identity.Kind, identity.Name)
	}
	return workload, nil
}

func (cluster *kubectlCluster) readyPodHTTPEndpoints(ctx context.Context, release declarativerelease.PlanRelease, portName string) ([]podHTTPEndpoint, error) {
	workloadRaw, err := cluster.kubectlRun(ctx, nil, "get", strings.ToLower(release.Workload.Kind), release.Workload.Name,
		"--namespace", release.Workload.Namespace, "--output", "json")
	if err != nil {
		return nil, err
	}
	selector, err := selectorFromWorkload(workloadRaw)
	if err != nil {
		return nil, err
	}
	podsRaw, err := cluster.kubectlRun(ctx, nil, "get", "pods", "--namespace", release.Workload.Namespace,
		"--selector", selector, "--output", "json")
	if err != nil {
		return nil, err
	}
	return podHTTPEndpointsFromJSON(podsRaw, release.Workload.Container, portName)
}

func podHTTPEndpointsFromJSON(raw []byte, containerName, portName string) ([]podHTTPEndpoint, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return nil, err
	}
	items, ok := value["items"].([]any)
	if !ok {
		return nil, errors.New("pod list is invalid")
	}
	endpoints := make([]podHTTPEndpoint, 0, len(items))
	for _, rawItem := range items {
		pod, ok := rawItem.(map[string]any)
		if !ok {
			return nil, errors.New("pod item is invalid")
		}
		metadata := mapField(pod, "metadata")
		status := mapField(pod, "status")
		if metadata["deletionTimestamp"] != nil || !podReady(status) {
			continue
		}
		name, ip := stringValue(metadata["name"]), stringValue(status["podIP"])
		if name == "" || net.ParseIP(ip) == nil {
			return nil, errors.New("ready pod HTTP identity is invalid")
		}
		port := 0
		for _, rawContainer := range anySlice(mapField(pod, "spec")["containers"]) {
			container, _ := rawContainer.(map[string]any)
			if stringValue(container["name"]) != containerName {
				continue
			}
			for _, rawPort := range anySlice(container["ports"]) {
				candidate, _ := rawPort.(map[string]any)
				if stringValue(candidate["name"]) == portName {
					if port != 0 {
						return nil, errors.New("ready pod HTTP port is ambiguous")
					}
					port = int(int64Value(candidate["containerPort"]))
				}
			}
		}
		if port < 1 || port > 65535 {
			return nil, errors.New("ready pod HTTP port is invalid")
		}
		endpoints = append(endpoints, podHTTPEndpoint{Name: name, IP: ip, Port: port})
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Name < endpoints[j].Name })
	if len(endpoints) == 0 {
		return nil, errors.New("no ready pod HTTP endpoints")
	}
	return endpoints, nil
}

func readPodHTTP(ctx context.Context, endpoint podHTTPEndpoint, path string) ([]byte, error) {
	if endpoint.Name == "" || net.ParseIP(endpoint.IP) == nil || endpoint.Port < 1 || endpoint.Port > 65535 ||
		!strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#") {
		return nil, errors.New("pod HTTP endpoint is invalid")
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(endpoint.IP, strconv.Itoa(endpoint.Port))+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK || len(body) > 1<<20 {
		return nil, errors.New("pod HTTP response is invalid")
	}
	return body, nil
}

func (cluster *kubectlCluster) kubectlRun(ctx context.Context, input []byte, arguments ...string) ([]byte, error) {
	return cluster.run(ctx, input, cluster.kubectl, arguments...)
}

func (cluster *kubectlCluster) run(ctx context.Context, input []byte, binary string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Stdin = bytes.NewReader(input)
	var stdout, stderr limitedBuffer
	stdout.limit = maxKubernetesOutputBytes
	stderr.limit = 128 << 10
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if stdout.exceeded || stderr.exceeded {
		return nil, errors.New("command output exceeded bounded limit")
	}
	return stdout.Bytes(), nil
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (buffer *limitedBuffer) Write(value []byte) (int, error) {
	if buffer.Len()+len(value) > buffer.limit {
		buffer.exceeded = true
		remaining := buffer.limit - buffer.Len()
		if remaining > 0 {
			_, _ = buffer.Buffer.Write(value[:remaining])
		}
		return len(value), nil
	}
	return buffer.Buffer.Write(value)
}

func selectorFromWorkload(raw []byte) (string, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return "", err
	}
	spec := mapField(value, "spec")
	selector := mapField(spec, "selector")
	labels := mapField(selector, "matchLabels")
	if len(labels) == 0 {
		return "", errors.New("workload selector has no matchLabels")
	}
	keys := make([]string, 0, len(labels))
	for key, rawValue := range labels {
		if _, ok := rawValue.(string); !ok || strings.ContainsAny(key, ",=") {
			return "", errors.New("workload selector is not a canonical matchLabels selector")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key].(string))
	}
	return strings.Join(parts, ","), nil
}

func parseObservation(workloadRaw, podsRaw []byte, release declarativerelease.PlanRelease, allowHistoricalRestarts, declaredArtifactsVerified bool) (declarativerelease.Observation, error) {
	workload, err := decodeJSONObject(workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	metadata := mapField(workload, "metadata")
	spec := mapField(workload, "spec")
	status := mapField(workload, "status")
	template := mapField(spec, "template")
	templateMetadata := mapField(template, "metadata")
	templateAnnotations := mapStringField(templateMetadata, "annotations")
	templateSpec := mapField(template, "spec")
	containers, ok := templateSpec["containers"].([]any)
	if !ok {
		return declarativerelease.Observation{}, errors.New("workload containers are invalid")
	}
	image := ""
	for _, rawContainer := range containers {
		container, ok := rawContainer.(map[string]any)
		if !ok {
			return declarativerelease.Observation{}, errors.New("workload container is invalid")
		}
		if stringValue(container["name"]) == release.Workload.Container {
			if image != "" {
				return declarativerelease.Observation{}, errors.New("workload container is ambiguous")
			}
			image = stringValue(container["image"])
		}
	}
	if image == "" || (!strings.Contains(image, "@sha256:") && !allowHistoricalRestarts) {
		return declarativerelease.Observation{}, errors.New("workload image is not immutable")
	}
	manifestSHA := templateAnnotations["fugue.pro/source-commit"]
	if manifestSHA == "" && allowHistoricalRestarts {
		manifestSHA = legacySourceTag(image)
	}
	workloadAnnotations := mapStringField(metadata, "annotations")
	configSHA := workloadAnnotations["fugue.pro/production-config-sha"]
	if configSHA == "" && allowHistoricalRestarts {
		configSHA = workloadAnnotations["fugue.pro/"+release.ComponentID+"-manifest-revision"]
	}
	if configSHA == "" && allowHistoricalRestarts {
		configSHA = manifestSHA
	}
	observation := declarativerelease.Observation{
		Present: true,
		Primary: declarativerelease.ResourceIdentity{
			APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
			Namespace: release.Workload.Namespace, Name: release.Workload.Name,
		},
		UID: stringValue(metadata["uid"]), ResourceVersion: stringValue(metadata["resourceVersion"]),
		Generation: int64Value(metadata["generation"]), ObservedGeneration: int64Value(status["observedGeneration"]),
		ImageRef: image, ConfigSHA: configSHA, ManifestSHA: manifestSHA,
		TemplateDigest: digestJSON(template), FieldManagers: managedFieldManagers(metadata),
	}
	switch release.Workload.Kind {
	case "Deployment":
		observation.Desired = int32(int64Value(spec["replicas"]))
		observation.Updated = int32(int64Value(status["updatedReplicas"]))
		observation.Ready = int32(int64Value(status["readyReplicas"]))
		observation.Available = int32(int64Value(status["availableReplicas"]))
		observation.Unavailable = int32(int64Value(status["unavailableReplicas"]))
	case "DaemonSet":
		observation.Desired = int32(int64Value(status["desiredNumberScheduled"]))
		observation.Updated = int32(int64Value(status["updatedNumberScheduled"]))
		observation.Ready = int32(int64Value(status["numberReady"]))
		observation.Available = int32(int64Value(status["numberAvailable"]))
		observation.Unavailable = int32(int64Value(status["numberUnavailable"]))
	case "Job":
		observation.ObservedGeneration = observation.Generation
		observation.Desired = 1
		if int64Value(status["succeeded"]) == 1 && int64Value(status["failed"]) == 0 {
			observation.Updated = 1
			observation.Ready = 1
			observation.Available = 1
		} else {
			observation.Unavailable = 1
		}
	default:
		return declarativerelease.Observation{}, errors.New("workload kind is not implemented")
	}
	var imageID, healthDigest string
	if release.Workload.Kind == "Job" {
		imageID, healthDigest, err = parseSucceededJobPod(podsRaw, release, manifestSHA)
	} else {
		imageID, healthDigest, err = parseReadyPods(podsRaw, release, observation.Desired-int32(release.Workload.PreservedUnavailable), manifestSHA, allowHistoricalRestarts, declaredArtifactsVerified)
	}
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if allowHistoricalRestarts && release.Workload.Kind == "DaemonSet" && observation.Updated == 0 &&
		int32(int64Value(status["currentNumberScheduled"])) == observation.Desired &&
		int32(int64Value(status["numberMisscheduled"])) == 0 && observation.Ready == observation.Desired &&
		observation.Available == observation.Desired && observation.Unavailable == 0 {
		observation.Updated = observation.Desired
	}
	observation.ImageID = imageID
	observation.HealthDigest = healthDigest
	return observation, nil
}

func parseDegradedObservation(workloadRaw []byte, release declarativerelease.PlanRelease) (declarativerelease.Observation, error) {
	workload, err := decodeJSONObject(workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	metadata := mapField(workload, "metadata")
	spec := mapField(workload, "spec")
	template := mapField(spec, "template")
	templateMetadata := mapField(template, "metadata")
	templateAnnotations := mapStringField(templateMetadata, "annotations")
	templateSpec := mapField(template, "spec")
	containers, ok := templateSpec["containers"].([]any)
	if !ok {
		return declarativerelease.Observation{}, errors.New("workload containers are invalid")
	}
	image := ""
	for _, rawContainer := range containers {
		container, ok := rawContainer.(map[string]any)
		if !ok {
			return declarativerelease.Observation{}, errors.New("workload container is invalid")
		}
		if stringValue(container["name"]) == release.Workload.Container {
			if image != "" {
				return declarativerelease.Observation{}, errors.New("workload container is ambiguous")
			}
			image = stringValue(container["image"])
		}
	}
	workloadAnnotations := mapStringField(metadata, "annotations")
	return declarativerelease.Observation{
		Present: true,
		Primary: declarativerelease.ResourceIdentity{
			APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
			Namespace: release.Workload.Namespace, Name: release.Workload.Name,
		},
		UID: stringValue(metadata["uid"]), ResourceVersion: stringValue(metadata["resourceVersion"]),
		Generation: int64Value(metadata["generation"]), ImageRef: image,
		ConfigSHA:      workloadAnnotations["fugue.pro/production-config-sha"],
		ManifestSHA:    templateAnnotations["fugue.pro/source-commit"],
		OCIRevision:    templateAnnotations["fugue.pro/oci-revision"],
		TemplateDigest: digestJSON(template), FieldManagers: managedFieldManagers(metadata),
	}, nil
}

func parseSucceededJobPod(raw []byte, release declarativerelease.PlanRelease, manifestSHA string) (string, string, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return "", "", err
	}
	items, ok := value["items"].([]any)
	if !ok {
		return "", "", errors.New("Job pod list is invalid")
	}
	evidence := make([]string, 0, len(items))
	imageID := ""
	succeeded := 0
	for _, rawItem := range items {
		pod, ok := rawItem.(map[string]any)
		if !ok {
			return "", "", errors.New("Job pod item is invalid")
		}
		metadata := mapField(pod, "metadata")
		if metadata["deletionTimestamp"] != nil || mapStringField(metadata, "annotations")["fugue.pro/source-commit"] != manifestSHA {
			continue
		}
		status := mapField(pod, "status")
		if stringValue(status["phase"]) != "Succeeded" {
			continue
		}
		containerStatuses, ok := status["containerStatuses"].([]any)
		if !ok {
			continue
		}
		for _, rawStatus := range containerStatuses {
			containerStatus, ok := rawStatus.(map[string]any)
			if !ok || stringValue(containerStatus["name"]) != release.Workload.Container || int64Value(containerStatus["restartCount"]) != 0 {
				continue
			}
			terminated := mapField(mapField(containerStatus, "state"), "terminated")
			if int64Value(terminated["exitCode"]) != 0 || stringValue(terminated["reason"]) != "Completed" {
				continue
			}
			digest, digestErr := imageIDDigest(stringValue(containerStatus["imageID"]))
			if digestErr != nil {
				return "", "", digestErr
			}
			if imageID != "" && imageID != digest {
				return "", "", errors.New("succeeded Job pods have mixed image IDs")
			}
			imageID = digest
			succeeded++
			evidence = append(evidence, stringValue(metadata["uid"])+":"+digest)
		}
	}
	if succeeded != 1 || imageID == "" {
		return "", "", errors.New("Job does not have exactly one succeeded immutable pod")
	}
	sort.Strings(evidence)
	return imageID, digestBytesLocal([]byte(strings.Join(evidence, "\n"))), nil
}

func legacySourceTag(image string) string {
	separator := strings.LastIndex(image, ":")
	if separator < strings.LastIndex(image, "/") || separator < 0 {
		return ""
	}
	value := image[separator+1:]
	if len(value) != 40 {
		return ""
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return ""
		}
	}
	return value
}

func parseReadyPods(raw []byte, release declarativerelease.PlanRelease, desired int32, manifestSHA string, allowHistoricalRestarts, declaredArtifactsVerified bool) (string, string, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return "", "", err
	}
	items, ok := value["items"].([]any)
	if !ok {
		return "", "", errors.New("pod list is invalid")
	}
	evidence := make([]string, 0, len(items))
	imageID := ""
	readyCount := int32(0)
	for _, rawItem := range items {
		pod, ok := rawItem.(map[string]any)
		if !ok {
			return "", "", errors.New("pod item is invalid")
		}
		metadata := mapField(pod, "metadata")
		if metadata["deletionTimestamp"] != nil {
			continue
		}
		annotations := mapStringField(metadata, "annotations")
		podSource := annotations["fugue.pro/source-commit"]
		if podSource == "" && allowHistoricalRestarts {
			for _, rawContainer := range anySlice(mapField(pod, "spec")["containers"]) {
				container, _ := rawContainer.(map[string]any)
				if stringValue(container["name"]) == release.Workload.Container {
					image := stringValue(container["image"])
					podSource = legacySourceTag(image)
					if podSource == "" && declaredArtifactsVerified && strings.Contains(image, "@sha256:") {
						podSource = manifestSHA
					}
					break
				}
			}
		}
		if podSource != manifestSHA {
			continue
		}
		status := mapField(pod, "status")
		if !podReady(status) {
			continue
		}
		containerStatuses, ok := status["containerStatuses"].([]any)
		if !ok {
			continue
		}
		found := false
		restartCount := int64(0)
		for _, rawStatus := range containerStatuses {
			containerStatus, ok := rawStatus.(map[string]any)
			if !ok || stringValue(containerStatus["name"]) != release.Workload.Container {
				continue
			}
			restartCount = int64Value(containerStatus["restartCount"])
			if restartCount != 0 && !allowHistoricalRestarts {
				return "", "", fmt.Errorf("%w: ready workload pod restarted", declarativerelease.ErrDegradedPredecessorHealth)
			}
			digest, err := imageIDDigest(stringValue(containerStatus["imageID"]))
			if err != nil {
				return "", "", err
			}
			if imageID != "" && imageID != digest {
				return "", "", errors.New("ready workload pods have mixed image IDs")
			}
			imageID = digest
			found = true
		}
		if !found {
			continue
		}
		readyCount++
		evidence = append(evidence, stringValue(metadata["name"])+":"+stringValue(metadata["uid"])+":"+imageID+":"+strconv.FormatInt(restartCount, 10))
	}
	if readyCount != desired || desired < 1 || imageID == "" {
		return "", "", fmt.Errorf("ready workload pod count mismatch: got=%d want=%d", readyCount, desired)
	}
	sort.Strings(evidence)
	return imageID, digestBytesLocal([]byte(strings.Join(evidence, "\n"))), nil
}

func readyPodNamesFromJSON(raw []byte) ([]string, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return nil, err
	}
	items, ok := value["items"].([]any)
	if !ok {
		return nil, errors.New("pod list is invalid")
	}
	names := make([]string, 0, len(items))
	for _, rawItem := range items {
		pod, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		metadata := mapField(pod, "metadata")
		if metadata["deletionTimestamp"] == nil && podReady(mapField(pod, "status")) {
			names = append(names, stringValue(metadata["name"]))
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, errors.New("no ready workload pods")
	}
	return names, nil
}

func podReady(status map[string]any) bool {
	conditions, ok := status["conditions"].([]any)
	if !ok {
		return false
	}
	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]any)
		if ok && stringValue(condition["type"]) == "Ready" && stringValue(condition["status"]) == "True" {
			return true
		}
	}
	return false
}

func parseLeaderLease(raw []byte) (string, time.Time, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return "", time.Time{}, err
	}
	spec := mapField(value, "spec")
	holder := stringValue(spec["holderIdentity"])
	renewRaw := stringValue(spec["renewTime"])
	renew, err := time.Parse(time.RFC3339Nano, renewRaw)
	if err != nil || holder == "" {
		return "", time.Time{}, errors.New("leader lease identity is invalid")
	}
	return holder, renew, nil
}

func decodeJSONObject(raw []byte) (map[string]any, error) {
	if len(raw) == 0 || len(raw) > maxKubernetesOutputBytes {
		return nil, errors.New("JSON object size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func mapField(value map[string]any, name string) map[string]any {
	field, _ := value[name].(map[string]any)
	return field
}

func mapStringField(value map[string]any, name string) map[string]string {
	result := make(map[string]string)
	field := mapField(value, name)
	for key, raw := range field {
		if text, ok := raw.(string); ok {
			result[key] = text
		}
	}
	return result
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	case float64:
		return int64(typed)
	case int64:
		return typed
	case string:
		parsed, _ := strconv.ParseInt(typed, 10, 64)
		return parsed
	default:
		return 0
	}
}

func managedFieldManagers(metadata map[string]any) []string {
	seen := make(map[string]struct{})
	fields, _ := metadata["managedFields"].([]any)
	for _, raw := range fields {
		field, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		manager := stringValue(field["manager"])
		if manager != "" {
			seen[manager] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for manager := range seen {
		result = append(result, manager)
	}
	sort.Strings(result)
	return result
}

func imageIDDigest(value string) (string, error) {
	index := strings.LastIndex(value, "sha256:")
	if index < 0 {
		return "", errors.New("pod image ID has no sha256 digest")
	}
	digest := value[index:]
	if len(digest) != len("sha256:")+64 {
		return "", errors.New("pod image ID digest is invalid")
	}
	for _, character := range digest[len("sha256:"):] {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return "", errors.New("pod image ID digest is invalid")
		}
	}
	return digest, nil
}

func digestJSON(value any) string {
	encoded, _ := declarativerelease.CanonicalJSON(value)
	return digestBytesLocal(encoded)
}

func digestBytesLocal(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", digest)
}

func digestJoin(values ...string) string {
	return digestBytesLocal([]byte(strings.Join(values, "\n")))
}
