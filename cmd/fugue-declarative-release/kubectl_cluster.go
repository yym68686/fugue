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

type ssaConflict struct {
	manager string
	field   string
}

var imageCacheTerminalHandoffConflicts = []struct {
	pointer string
	manager string
	field   string
}{
	{pointer: "/metadata/labels/app.kubernetes.io~1managed-by", manager: "helm", field: ".metadata.labels.app.kubernetes.io/managed-by"},
	{pointer: "/spec/updateStrategy/type", manager: "kubectl-patch", field: ".spec.updateStrategy.type"},
}

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
	observation := declarativerelease.Observation{Present: !resourceAbsent(workloadRaw), Primary: primary, Resources: resources}
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
	if err := observation.ValidateDegradedPredecessor(release); err != nil {
		if explicitBootstrapDegradedObservation(release) {
			observation, err = cluster.bindExplicitBootstrapDegradedObservation(ctx, release, manifest, workloadRaw, observation)
		} else {
			observation, err = cluster.bindReceiptBoundDegradedObservation(ctx, release, workloadRaw, observation, resources)
		}
		if err != nil {
			return declarativerelease.Observation{}, err
		}
		if err := observation.ValidateDegradedPredecessor(release); err != nil {
			return declarativerelease.Observation{}, err
		}
	}
	verificationArgs := []string{"--image", observation.ImageRef, "--platform", "linux/amd64", "--expected-revision", observation.OCIRevision}
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
	if verification.Image != observation.ImageRef || verification.OCIRevision != observation.OCIRevision {
		return declarativerelease.Observation{}, errors.New("degraded predecessor registry identity mismatch")
	}
	return observation, nil
}

func explicitBootstrapDegradedObservation(release declarativerelease.PlanRelease) bool {
	return declarativerelease.InitialExplicitBootstrapFailedAtomSuccessor(release) ||
		(release.MigrationState == "adopting" && release.RetrySameLKG && release.ExpectedPreviousPresent &&
			release.BootstrapLKGPath != "" && release.BootstrapRuntime != nil && release.OwnershipAdoption != nil)
}

func (cluster *kubectlCluster) bindExplicitBootstrapDegradedObservation(ctx context.Context, release declarativerelease.PlanRelease, manifest, workloadRaw []byte, observation declarativerelease.Observation) (declarativerelease.Observation, error) {
	primary := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	desired, err := declarativerelease.ResourceSetItem(manifest, primary)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	target, err := targetIdentityFromDeclaredWorkload(desired, release.Workload)
	if err != nil || target.ImageRef != release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest ||
		target.ConfigSHA != release.ExpectedPreviousConfigSHA || target.ManifestSHA != release.ExpectedPreviousManifestSHA ||
		target.OCIRevision != release.ExpectedPreviousOCIRevision || observation.ImageRef != target.ImageRef {
		return declarativerelease.Observation{}, errors.New("explicit bootstrap degraded observation is not the exact LKG")
	}
	for label, value := range map[string]string{"config": observation.ConfigSHA, "manifest": observation.ManifestSHA, "OCI revision": observation.OCIRevision} {
		want := map[string]string{"config": target.ConfigSHA, "manifest": target.ManifestSHA, "OCI revision": target.OCIRevision}[label]
		if value != "" && value != want {
			return declarativerelease.Observation{}, fmt.Errorf("explicit bootstrap degraded observation has conflicting %s identity", label)
		}
	}
	selector, err := selectorFromWorkload(workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	podsRaw, err := cluster.kubectlRun(ctx, nil, "get", "pods", "--namespace", release.Workload.Namespace, "--selector", selector, "--output", "json")
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if err := verifyDeclaredArtifactImageIDs(podsRaw, manifest, release, true); err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify explicit bootstrap degraded Pod image: %w", err)
	}
	bootstrap := adoptingBootstrapRuntime(release, true)
	if bootstrap == nil {
		return declarativerelease.Observation{}, errors.New("explicit bootstrap degraded runtime is absent")
	}
	bootstrapImage := release.Artifact.Repository + "@" + bootstrap.ImageDigest
	verificationRaw, err := cluster.run(ctx, nil, "python3", cluster.verifier, "--image", bootstrapImage, "--platform", "linux/amd64",
		"--expected-revision", bootstrap.OCIRevision, "--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5",
		"--max-attempts", "2", "--retry-delay-seconds", "0.1")
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify explicit bootstrap degraded runtime: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil || verification.Image != bootstrapImage || verification.OCIRevision != bootstrap.OCIRevision {
		return declarativerelease.Observation{}, errors.New("explicit bootstrap degraded runtime registry identity mismatch")
	}
	observation.ConfigSHA = target.ConfigSHA
	observation.ManifestSHA = target.ManifestSHA
	observation.OCIRevision = target.OCIRevision
	return observation, nil
}

func (cluster *kubectlCluster) bindReceiptBoundDegradedObservation(ctx context.Context, release declarativerelease.PlanRelease, workloadRaw []byte, observation declarativerelease.Observation, resources []declarativerelease.ResourceObservation) (declarativerelease.Observation, error) {
	_, receipt, err := loadValidatedReceiptBoundRelease(release)
	if err != nil || len(receipt.Ownership) == 0 {
		if err == nil {
			err = errors.New("ownership adoption receipt has no exact field scopes")
		}
		return declarativerelease.Observation{}, fmt.Errorf("bind receipt-bound degraded observation: %w", err)
	}
	final := receipt.Final
	if observation.UID != final.UID || observation.ResourceVersion != final.ResourceVersion || observation.Generation != final.Generation ||
		observation.TemplateDigest != final.TemplateDigest || observation.ImageRef != final.ImageRef {
		return declarativerelease.Observation{}, errors.New("receipt-bound degraded workload CAS or template identity drifted")
	}
	for label, value := range map[string]string{
		"config": observation.ConfigSHA, "manifest": observation.ManifestSHA, "OCI revision": observation.OCIRevision,
	} {
		if value != "" {
			return declarativerelease.Observation{}, fmt.Errorf("receipt-bound degraded workload already has conflicting %s identity", label)
		}
	}
	byIdentity := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(resources))
	for _, resource := range resources {
		byIdentity[resource.Identity] = resource
	}
	finalResources := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(final.Resources))
	for _, resource := range final.Resources {
		finalResources[resource.Identity] = resource
	}
	if len(byIdentity) != len(finalResources) {
		return declarativerelease.Observation{}, errors.New("receipt-bound degraded resource witness count drifted")
	}
	adoption := declarativerelease.OwnershipAdoptionPlan{Component: release.ComponentID}
	for _, scope := range receipt.Ownership {
		current, exists := byIdentity[scope.Identity]
		prior, witnessed := finalResources[scope.Identity]
		if !exists || !witnessed || current.UID != prior.UID || current.ResourceVersion != prior.ResourceVersion ||
			current.Generation != prior.Generation || current.ObjectDigest != prior.ObjectDigest {
			return declarativerelease.Observation{}, fmt.Errorf("receipt-bound degraded resource %s/%s drifted", scope.Identity.Kind, scope.Identity.Name)
		}
		adoption.Resources = append(adoption.Resources, declarativerelease.OwnershipAdoptionResourcePlan{
			Identity: scope.Identity, Fields: append([]string(nil), scope.Fields...),
			UID: prior.UID, ResourceVersion: prior.ResourceVersion, Generation: prior.Generation,
		})
	}
	if err := cluster.verifyOwnershipAdoption(ctx, release, adoption); err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify receipt-bound degraded ownership: %w", err)
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
	if err := verifyReceiptBoundPodCohort(podsRaw, release, receipt); err != nil {
		return declarativerelease.Observation{}, err
	}
	observation.ConfigSHA = final.ConfigSHA
	observation.ManifestSHA = final.ManifestSHA
	observation.OCIRevision = final.OCIRevision
	observation.ImageID = final.ImageID
	observation.FieldManagers = append([]string(nil), final.FieldManagers...)
	observation.Resources = resources
	return observation, nil
}

func verifyReceiptBoundPodCohort(raw []byte, release declarativerelease.PlanRelease, receipt declarativerelease.OwnershipAdoptionReceipt) error {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return err
	}
	items, ok := value["items"].([]any)
	if !ok {
		return errors.New("receipt-bound pod list is invalid")
	}
	ready := int32(0)
	for _, rawItem := range items {
		pod, ok := rawItem.(map[string]any)
		if !ok {
			return errors.New("receipt-bound pod item is invalid")
		}
		metadata := mapField(pod, "metadata")
		if metadata["deletionTimestamp"] != nil {
			continue
		}
		status := mapField(pod, "status")
		if !podReady(status) {
			continue
		}
		matched := false
		for _, rawStatus := range anySlice(status["containerStatuses"]) {
			containerStatus, _ := rawStatus.(map[string]any)
			if stringValue(containerStatus["name"]) != release.Workload.Container {
				continue
			}
			digest, err := imageIDDigest(stringValue(containerStatus["imageID"]))
			if err != nil || digest != receipt.Final.ImageID || int64Value(containerStatus["restartCount"]) != 0 {
				return errors.New("receipt-bound pod image or restart witness drifted")
			}
			matched = true
		}
		if !matched {
			return errors.New("receipt-bound pod container witness is absent")
		}
		ready++
	}
	if ready != receipt.Final.Ready || ready != receipt.Final.Desired-int32(release.Workload.PreservedUnavailable) {
		return errors.New("receipt-bound ready pod cohort drifted")
	}
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
	applyErr := cluster.applyResourceSet(ctx, release, manifest, true)
	if applyErr == nil {
		return nil
	}
	return cluster.dryRunReceiptBoundImageCacheHandoff(ctx, release, manifest, applyErr)
}

func (cluster *kubectlCluster) DryRunOwnershipAdoption(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, lkgManifest []byte) error {
	manifest, err := declarativerelease.BuildOwnershipAdoptionManifest(lkgManifest, adoption)
	if err != nil {
		return err
	}
	return cluster.applyOwnershipAdoptionSet(ctx, release, adoption, manifest, true)
}

func (cluster *kubectlCluster) DryRunOwnershipTakeover(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, target declarativerelease.TargetIdentity, targetManifest []byte) error {
	if err := cluster.verifyOwnershipTakeoverScaffolds(ctx, release, adoption); err != nil {
		return err
	}
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
	if err := cluster.verifyOwnershipTakeoverScaffolds(ctx, release, adoption); err != nil {
		return current, err
	}
	manifest, err := declarativerelease.BuildOwnershipTakeoverManifest(targetManifest, adoption, target)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	applyErr := cluster.applyOwnershipAdoptionSet(ctx, release, adoption, manifest, false)
	verifyErr := cluster.verifyOwnershipTakeover(ctx, release, adoption)
	convergedErr := cluster.Converged(ctx, release, manifest)
	observation, observeErr := cluster.ObserveCAS(ctx, release, targetManifest)
	if err := errors.Join(applyErr, verifyErr, convergedErr, observeErr); err != nil {
		return observation, fmt.Errorf("verify ownership takeover: %w", err)
	}
	return observation, nil
}

func (cluster *kubectlCluster) verifyOwnershipTakeoverScaffolds(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan) error {
	for _, scope := range adoption.Resources {
		scaffolds, err := declarativerelease.OwnershipTakeoverValidationScaffolds(scope.Fields)
		if err != nil {
			return err
		}
		if len(scaffolds) == 0 {
			continue
		}
		expected := make(map[string]string, len(scope.ValidationScaffolds))
		for _, scaffold := range scope.ValidationScaffolds {
			if _, exists := expected[scaffold.Pointer]; exists {
				return fmt.Errorf("ownership takeover validation scaffold for %s/%s is duplicated", scope.Identity.Kind, scope.Identity.Name)
			}
			expected[scaffold.Pointer] = scaffold.Value
		}
		if len(expected) != len(scaffolds) {
			return fmt.Errorf("ownership takeover validation scaffold for %s/%s is incomplete", scope.Identity.Kind, scope.Identity.Name)
		}
		raw, err := cluster.getResource(ctx, scope.Identity)
		if err != nil || len(bytes.TrimSpace(raw)) == 0 {
			return fmt.Errorf("read ownership takeover scaffold %s/%s: %w", scope.Identity.Kind, scope.Identity.Name, err)
		}
		value, err := decodeJSONObject(raw)
		if err != nil {
			return err
		}
		for _, pointer := range scaffolds {
			container, ok := ownershipScaffoldContainer(pointer)
			image, found, imageErr := declaredContainerImageOptional(value, container, "container")
			if !ok || imageErr != nil || !found || image != expected[pointer] {
				return fmt.Errorf("ownership takeover validation scaffold for %s/%s is not the exact live bootstrap image", scope.Identity.Kind, scope.Identity.Name)
			}
		}
		metadata := mapField(value, "metadata")
		if stringValue(metadata["uid"]) != scope.UID || stringValue(metadata["resourceVersion"]) != scope.ResourceVersion ||
			int64Value(metadata["generation"]) != scope.Generation ||
			!managedFieldsOwnPointers(metadata, release.Workload.FieldManager, scaffolds) {
			return fmt.Errorf("ownership takeover validation scaffold for %s/%s is not CAS-bound to existing declarative ownership", scope.Identity.Kind, scope.Identity.Name)
		}
		legacyManagers := ownershipPlanLegacyManagers(adoption)
		for _, pointer := range scaffolds {
			reviewed := exactString(pointer, scope.Fields)
			for _, owner := range managedFieldsPointerOwners(metadata, pointer) {
				if owner.manager == release.Workload.FieldManager && owner.operation == "Apply" {
					continue
				}
				if reviewed && exactString(owner.manager, legacyManagers) && owner.operation == "Update" {
					continue
				}
				return fmt.Errorf("ownership takeover validation scaffold for %s/%s has unreviewed owner %s/%s", scope.Identity.Kind, scope.Identity.Name, owner.manager, owner.operation)
			}
		}
	}
	return nil
}

func ownershipScaffoldContainer(pointer string) (string, bool) {
	const prefix = "/spec/template/spec/containers[name="
	const suffix = "]/image"
	if !strings.HasPrefix(pointer, prefix) || !strings.HasSuffix(pointer, suffix) {
		return "", false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(pointer, prefix), suffix)
	return name, name != ""
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
		scope, exists := scopes[identity]
		if !exists {
			return fmt.Errorf("adopt %s/%s has no reviewed scope", identity.Kind, identity.Name)
		}
		var expectedTakeoverConflicts map[reviewedTakeoverConflict]struct{}
		if adoption.AlreadyConverged || adoption.ResumeTakeover {
			expectedTakeoverConflicts, err = cluster.expectedReviewedTakeoverConflicts(ctx, release, adoption, scope, item)
			if err != nil {
				return fmt.Errorf("adopt %s/%s conflict precondition: %w", identity.Kind, identity.Name, err)
			}
		}
		if _, applyErr := cluster.kubectlRun(ctx, encoded, arguments...); applyErr != nil {
			managers := append([]string(nil), adoption.LegacyFieldManagers...)
			if adoption.AlreadyConverged {
				managers = append(managers, release.Workload.FieldManager)
				sort.Strings(managers)
			}
			if adoption.AlreadyConverged || adoption.ResumeTakeover {
				err = validateExactReviewedTakeoverConflicts(applyErr, scope.Fields, expectedTakeoverConflicts)
			} else {
				err = validateAdoptionConflicts(applyErr, managers, scope.Fields)
			}
			if err != nil {
				return fmt.Errorf("adopt %s/%s: %w", identity.Kind, identity.Name, err)
			}
			forceArguments, err := adoptionForceApplyArguments(release, dryRun)
			if err != nil {
				return err
			}
			if _, forceErr := cluster.kubectlRun(ctx, encoded, forceArguments...); forceErr != nil {
				return fmt.Errorf("adopt %s/%s reviewed force-conflicts: %w", identity.Kind, identity.Name, forceErr)
			}
		} else if len(expectedTakeoverConflicts) > 0 {
			return fmt.Errorf("adopt %s/%s expected reviewed conflicts were absent", identity.Kind, identity.Name)
		}
	}
	return nil
}

type reviewedTakeoverConflict struct {
	manager string
	pointer string
}

func (cluster *kubectlCluster) expectedReviewedTakeoverConflicts(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan, scope declarativerelease.OwnershipAdoptionResourcePlan, desired map[string]any) (map[reviewedTakeoverConflict]struct{}, error) {
	raw, err := cluster.getResource(ctx, scope.Identity)
	if err != nil || resourceAbsent(raw) {
		return nil, fmt.Errorf("read reviewed takeover resource: %w", err)
	}
	live, err := decodeJSONObject(raw)
	if err != nil {
		return nil, err
	}
	metadata := mapField(live, "metadata")
	if stringValue(metadata["uid"]) != scope.UID || stringValue(metadata["resourceVersion"]) != scope.ResourceVersion ||
		int64Value(metadata["generation"]) != scope.Generation || !managedFieldsOwnPointers(metadata, release.Workload.FieldManager, scope.Fields) {
		return nil, errors.New("reviewed takeover conflict evidence is not CAS-bound to declarative ownership")
	}
	legacyManagers := ownershipPlanLegacyManagers(adoption)
	expected := make(map[reviewedTakeoverConflict]struct{})
	for _, pointer := range scope.Fields {
		legacyOwners := make([]managedFieldPointerOwner, 0)
		for _, owner := range managedFieldsPointerOwners(metadata, pointer) {
			if owner.manager == release.Workload.FieldManager && owner.operation == "Apply" {
				continue
			}
			if !exactString(owner.manager, legacyManagers) || owner.operation != "Update" {
				return nil, fmt.Errorf("reviewed pointer %s has unreviewed owner %s/%s", pointer, owner.manager, owner.operation)
			}
			legacyOwners = append(legacyOwners, owner)
		}
		if len(legacyOwners) == 0 {
			continue
		}
		liveValue, err := declarativerelease.OwnershipAdoptionPointerValue(live, pointer)
		if err != nil {
			return nil, err
		}
		desiredValue, err := declarativerelease.OwnershipAdoptionPointerValue(desired, pointer)
		if err != nil {
			return nil, err
		}
		liveJSON, err := declarativerelease.CanonicalJSON(liveValue)
		if err != nil {
			return nil, err
		}
		desiredJSON, err := declarativerelease.CanonicalJSON(desiredValue)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(liveJSON, desiredJSON) {
			return nil, fmt.Errorf("legacy-owned reviewed pointer %s has no takeover value transition", pointer)
		}
		for _, owner := range legacyOwners {
			expected[reviewedTakeoverConflict{manager: owner.manager, pointer: pointer}] = struct{}{}
		}
	}
	return expected, nil
}

func validateExactReviewedTakeoverConflicts(applyErr error, pointers []string, expected map[reviewedTakeoverConflict]struct{}) error {
	conflicts, err := parseSSAConflicts(applyErr)
	if err != nil {
		return err
	}
	seen := make(map[reviewedTakeoverConflict]struct{}, len(conflicts))
	for _, conflict := range conflicts {
		pointer := ""
		for _, candidate := range pointers {
			if adoptionFieldAllowed(conflict.field, []string{candidate}) {
				if pointer != "" {
					return errors.New("reviewed takeover conflict matches multiple pointers")
				}
				pointer = candidate
			}
		}
		key := reviewedTakeoverConflict{manager: conflict.manager, pointer: pointer}
		if pointer == "" {
			return fmt.Errorf("reviewed takeover conflict %s:%s is outside the exact allowlist", conflict.manager, conflict.field)
		}
		if _, ok := expected[key]; !ok {
			return fmt.Errorf("reviewed takeover conflict %s:%s is outside the exact live conflict set", conflict.manager, conflict.field)
		}
		if _, duplicate := seen[key]; duplicate {
			return errors.New("reviewed takeover conflict is duplicated")
		}
		seen[key] = struct{}{}
	}
	if len(seen) != len(expected) {
		return errors.New("reviewed takeover conflict set is incomplete")
	}
	return nil
}

func validateAdoptionConflicts(applyErr error, managers, fields []string) error {
	if applyErr == nil || len(managers) == 0 || len(fields) == 0 {
		return errors.New("ownership adoption conflict evidence is incomplete")
	}
	conflicts, err := parseSSAConflicts(applyErr)
	if err != nil {
		return err
	}
	for _, conflict := range conflicts {
		if !stringInSortedSet(conflict.manager, managers) || !adoptionFieldAllowed(conflict.field, fields) {
			return fmt.Errorf("ownership adoption conflict %s:%s is outside the reviewed allowlist", conflict.manager, conflict.field)
		}
	}
	return nil
}

func parseSSAConflicts(applyErr error) ([]ssaConflict, error) {
	if applyErr == nil {
		return nil, errors.New("ownership adoption conflict evidence is incomplete")
	}
	raw := applyErr.Error()
	countMatch := adoptionConflictCountPattern.FindStringSubmatch(raw)
	conflicts := make([]ssaConflict, 0)
	groupManager := ""
	for _, line := range strings.Split(raw, "\n") {
		if match := adoptionConflictPattern.FindStringSubmatch(line); len(match) == 3 {
			conflicts = append(conflicts, ssaConflict{manager: match[1], field: match[2]})
			groupManager = ""
			continue
		}
		if match := adoptionConflictGroupPattern.FindStringSubmatch(line); len(match) == 2 {
			groupManager = match[1]
			continue
		}
		trimmed := strings.TrimSpace(line)
		if groupManager != "" && strings.HasPrefix(trimmed, "- .") {
			conflicts = append(conflicts, ssaConflict{manager: groupManager, field: strings.TrimPrefix(trimmed, "- ")})
			continue
		}
		if groupManager != "" && trimmed != "" {
			groupManager = ""
		}
	}
	if len(countMatch) != 2 || len(conflicts) == 0 {
		return nil, errors.New("ownership adoption failure is not a typed SSA conflict")
	}
	count, err := strconv.Atoi(countMatch[1])
	if err != nil || count != len(conflicts) {
		return nil, errors.New("ownership adoption conflict count is inconsistent")
	}
	return conflicts, nil
}

func stringInSortedSet(value string, allowed []string) bool {
	index := sort.SearchStrings(allowed, value)
	return index < len(allowed) && allowed[index] == value
}

func adoptionFieldAllowed(field string, pointers []string) bool {
	for _, pointer := range pointers {
		parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
		for index, part := range parts {
			part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
			if open := strings.Index(part, "[name="); open > 0 && strings.HasSuffix(part, "]") {
				name := part[open+len("[name=") : len(part)-1]
				part = part[:open] + `[name="` + name + `"]`
			}
			parts[index] = part
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

func (cluster *kubectlCluster) verifyOwnershipTakeover(ctx context.Context, release declarativerelease.PlanRelease, adoption declarativerelease.OwnershipAdoptionPlan) error {
	if err := cluster.verifyOwnershipAdoption(ctx, release, adoption); err != nil {
		return err
	}
	for _, scope := range adoption.Resources {
		raw, err := cluster.getResource(ctx, scope.Identity)
		if err != nil || len(bytes.TrimSpace(raw)) == 0 {
			return fmt.Errorf("read ownership takeover %s/%s: %w", scope.Identity.Kind, scope.Identity.Name, err)
		}
		value, err := decodeJSONObject(raw)
		if err != nil {
			return err
		}
		metadata := mapField(value, "metadata")
		if !managedFieldsPointersExclusivelyOwned(metadata, release.Workload.FieldManager, scope.Fields) {
			return fmt.Errorf("ownership takeover %s/%s retained non-declarative reviewed ownership", scope.Identity.Kind, scope.Identity.Name)
		}
	}
	return nil
}

func ownershipPlanLegacyManagers(adoption declarativerelease.OwnershipAdoptionPlan) []string {
	if len(adoption.LegacyFieldManagers) > 0 {
		return adoption.LegacyFieldManagers
	}
	if adoption.LegacyFieldManager != "" {
		return []string{adoption.LegacyFieldManager}
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
		if managedFieldsEntryOwnsPointers(fields, pointers, true) {
			return true
		}
	}
	return false
}

func managedFieldsOwnAnyPointer(metadata map[string]any, manager string, pointers []string) bool {
	for _, rawEntry := range anySlice(metadata["managedFields"]) {
		entry, _ := rawEntry.(map[string]any)
		if stringValue(entry["manager"]) != manager || stringValue(entry["subresource"]) != "" {
			continue
		}
		if managedFieldsEntryOwnsPointers(mapField(entry, "fieldsV1"), pointers, false) {
			return true
		}
	}
	return false
}

type managedFieldPointerOwner struct {
	manager   string
	operation string
}

func managedFieldsPointerOwners(metadata map[string]any, pointer string) []managedFieldPointerOwner {
	owners := make([]managedFieldPointerOwner, 0)
	for _, rawEntry := range anySlice(metadata["managedFields"]) {
		entry, _ := rawEntry.(map[string]any)
		manager := stringValue(entry["manager"])
		operation := stringValue(entry["operation"])
		if manager == "" || operation == "" || stringValue(entry["subresource"]) != "" ||
			!managedFieldsEntryOwnsPointers(mapField(entry, "fieldsV1"), []string{pointer}, true) {
			continue
		}
		owners = append(owners, managedFieldPointerOwner{manager: manager, operation: operation})
	}
	return owners
}

func managedFieldsPointersExclusivelyOwned(metadata map[string]any, manager string, pointers []string) bool {
	if !managedFieldsOwnPointers(metadata, manager, pointers) {
		return false
	}
	for _, pointer := range pointers {
		for _, owner := range managedFieldsPointerOwners(metadata, pointer) {
			if owner.manager != manager || owner.operation != "Apply" {
				return false
			}
		}
	}
	return true
}

func exactString(value string, values []string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func managedFieldsEntryOwnsPointers(fields map[string]any, pointers []string, requireAll bool) bool {
	matched := 0
	for _, pointer := range pointers {
		current := fields
		owned := true
		for _, encoded := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
			token := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
			field := token
			selector := ""
			if open := strings.Index(token, "[name="); open > 0 && strings.HasSuffix(token, "]") {
				field = token[:open]
				selector = token[open+len("[name=") : len(token)-1]
			}
			next, ok := current["f:"+field].(map[string]any)
			if !ok {
				owned = false
				break
			}
			current = next
			if selector != "" {
				keyRaw, _ := json.Marshal(selector)
				next, ok = current[`k:{"name":`+string(keyRaw)+`}`].(map[string]any)
				if !ok {
					owned = false
					break
				}
				current = next
			}
		}
		if owned {
			matched++
			if !requireAll {
				return true
			}
		} else if requireAll {
			return false
		}
	}
	return requireAll && matched == len(pointers)
}

type imageCacheTerminalHandoff struct {
	receipt  declarativerelease.OwnershipAdoptionReceipt
	plan     declarativerelease.OwnershipAdoptionPlan
	target   declarativerelease.TargetIdentity
	manifest []byte
	preUID   string
	preRV    string
	preGen   int64
}

func (cluster *kubectlCluster) dryRunReceiptBoundImageCacheHandoff(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte, applyErr error) error {
	_, err := cluster.prepareReceiptBoundImageCacheHandoff(ctx, release, manifest, applyErr, true)
	return err
}

func (cluster *kubectlCluster) prepareReceiptBoundImageCacheHandoff(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte, applyErr error, dryRunForce bool) (imageCacheTerminalHandoff, error) {
	receipt, err := authorizeImageCacheTerminalHandoff(release)
	if err != nil {
		return imageCacheTerminalHandoff{}, errors.Join(applyErr, err)
	}
	if err := validateExactImageCacheTerminalHandoffConflicts(applyErr, receipt); err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	identity := receipt.Final.Primary
	raw, err := cluster.getResource(ctx, identity)
	if err != nil || len(bytes.TrimSpace(raw)) == 0 {
		return imageCacheTerminalHandoff{}, fmt.Errorf("read Image-cache terminal handoff workload: %w", err)
	}
	value, err := decodeJSONObject(raw)
	if err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	if err := cluster.verifyImageCacheTerminalHandoffLKG(ctx, release, receipt, manifest, raw); err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	if err := verifyImageCacheTerminalHandoffPreconditions(value, release, receipt, manifest); err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	metadata := mapField(value, "metadata")
	targetResource, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	target, err := targetIdentityFromDeclaredWorkload(targetResource, release.Workload)
	if err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	target.ManifestDigest = digestBytesLocal(manifest)
	fields := make([]string, 0, len(imageCacheTerminalHandoffConflicts))
	for _, reviewed := range imageCacheTerminalHandoffConflicts {
		fields = append(fields, reviewed.pointer)
	}
	plan := declarativerelease.OwnershipAdoptionPlan{
		Component: release.ComponentID, AlreadyConverged: true,
		Resources: []declarativerelease.OwnershipAdoptionResourcePlan{{
			Identity: identity, Fields: fields, UID: stringValue(metadata["uid"]),
			ResourceVersion: stringValue(metadata["resourceVersion"]), Generation: int64Value(metadata["generation"]),
		}},
	}
	takeoverManifest, err := declarativerelease.BuildOwnershipTakeoverManifest(manifest, plan, target)
	if err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	takeoverManifest, err = addImageCacheTerminalHandoffScaffold(takeoverManifest, identity, release.Workload.Container, receipt.Final.ImageRef)
	if err != nil {
		return imageCacheTerminalHandoff{}, err
	}
	if dryRunForce {
		if err := cluster.applyImageCacheTerminalHandoffSet(ctx, release, takeoverManifest, true); err != nil {
			return imageCacheTerminalHandoff{}, fmt.Errorf("server-side dry-run reviewed Image-cache ownership terminal handoff: %w", err)
		}
	}
	return imageCacheTerminalHandoff{
		receipt: receipt, plan: plan, target: target, manifest: takeoverManifest,
		preUID: plan.Resources[0].UID, preRV: plan.Resources[0].ResourceVersion, preGen: plan.Resources[0].Generation,
	}, nil
}

func (cluster *kubectlCluster) verifyImageCacheTerminalHandoffLKG(ctx context.Context, release declarativerelease.PlanRelease, receipt declarativerelease.OwnershipAdoptionReceipt, manifest, workloadRaw []byte) error {
	observation, err := parseDegradedObservation(workloadRaw, release)
	if err != nil {
		return err
	}
	if observation.UID != receipt.Final.UID || observation.ResourceVersion != receipt.Final.ResourceVersion || observation.Generation != receipt.Final.Generation ||
		observation.TemplateDigest != receipt.Final.TemplateDigest || observation.ImageRef != receipt.Final.ImageRef ||
		observation.ConfigSHA != "" || observation.ManifestSHA != "" || observation.OCIRevision != "" {
		return errors.New("receipt-bound Image-cache LKG workload identity drifted")
	}
	resources, err := cluster.observeResources(ctx, manifest, release, workloadRaw)
	if err != nil || len(resources) != len(receipt.Final.Resources) {
		if err == nil {
			err = errors.New("receipt-bound Image-cache LKG resource count drifted")
		}
		return err
	}
	prior := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(receipt.Final.Resources))
	for _, resource := range receipt.Final.Resources {
		prior[resource.Identity] = resource
	}
	for _, resource := range resources {
		witness, exists := prior[resource.Identity]
		if !exists || resource.UID != witness.UID || resource.ResourceVersion != witness.ResourceVersion ||
			resource.Generation != witness.Generation || resource.ObjectDigest != witness.ObjectDigest {
			return fmt.Errorf("receipt-bound Image-cache LKG resource %s/%s drifted", resource.Identity.Kind, resource.Identity.Name)
		}
	}
	selector, err := selectorFromWorkload(workloadRaw)
	if err != nil {
		return err
	}
	podsRaw, err := cluster.kubectlRun(ctx, nil, "get", "pods", "--namespace", release.Workload.Namespace, "--selector", selector, "--output", "json")
	if err != nil {
		return err
	}
	if err := verifyReceiptBoundPodCohort(podsRaw, release, receipt); err != nil {
		return err
	}
	return cluster.VerifyTarget(ctx, declarativerelease.TargetIdentity{
		Present: true, ImageRef: receipt.Final.ImageRef, ConfigSHA: receipt.Final.ConfigSHA,
		ManifestSHA: receipt.Final.ManifestSHA, OCIRevision: receipt.Final.OCIRevision,
	})
}

func addImageCacheTerminalHandoffScaffold(manifest []byte, identity declarativerelease.ResourceIdentity, container, image string) ([]byte, error) {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil || len(identities) != 1 || identities[0] != identity || container != "image-cache" || !strings.Contains(image, "@sha256:") {
		if err == nil {
			err = errors.New("Image-cache terminal handoff scaffold identity is invalid")
		}
		return nil, err
	}
	item, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return nil, err
	}
	spec := mapField(item, "spec")
	if len(spec) != 1 || mapField(spec, "updateStrategy")["type"] == nil {
		return nil, errors.New("Image-cache terminal handoff force scope expanded before scaffolding")
	}
	spec["template"] = map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": container, "image": image}}}}
	return declarativerelease.CanonicalJSON(declarativerelease.ResourceSet{
		APIVersion: declarativerelease.ResourceSetAPIVersion, Kind: declarativerelease.ResourceSetKind, Items: []map[string]any{item},
	})
}

func authorizeImageCacheTerminalHandoff(release declarativerelease.PlanRelease) (declarativerelease.OwnershipAdoptionReceipt, error) {
	if release.ComponentID != "image-cache" || release.MigrationState != "independent" || !release.RetrySameLKG ||
		release.AdoptionReceiptPath != "deploy/releases/image-cache/adoption-receipt.json" || release.OwnershipAdoption != nil || release.BootstrapLKGPath != "" {
		return declarativerelease.OwnershipAdoptionReceipt{}, errors.New("Image-cache ownership terminal handoff is not exactly receipt-bound")
	}
	component, receipt, err := loadValidatedReceiptBoundRelease(release)
	if err != nil {
		return declarativerelease.OwnershipAdoptionReceipt{}, err
	}
	if component.ID != release.ComponentID || receipt.TerminalHandoff == nil || len(receipt.Ownership) != 1 ||
		receipt.Ownership[0].Identity != receipt.Final.Primary {
		return declarativerelease.OwnershipAdoptionReceipt{}, errors.New("Image-cache terminal handoff receipt scope is invalid")
	}
	wantOwnership := []string{
		"/metadata/labels/app.kubernetes.io~1managed-by",
		"/spec/template/spec/containers[name=image-cache]/image",
		"/spec/updateStrategy",
	}
	if !equalSortedStrings(receipt.Ownership[0].Fields, wantOwnership) {
		return declarativerelease.OwnershipAdoptionReceipt{}, errors.New("Image-cache terminal handoff receipt expanded its adopted ownership scope")
	}
	if !equalSortedStrings(receipt.TerminalHandoff.Scaffolds, []string{"/spec/template/spec/containers[name=image-cache]/image"}) {
		return declarativerelease.OwnershipAdoptionReceipt{}, errors.New("Image-cache terminal handoff scaffold is not the exact preowned image leaf")
	}
	return receipt, nil
}

func equalSortedStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	a := append([]string(nil), left...)
	b := append([]string(nil), right...)
	sort.Strings(a)
	sort.Strings(b)
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

func validateExactImageCacheTerminalHandoffConflicts(applyErr error, receipt declarativerelease.OwnershipAdoptionReceipt) error {
	conflicts, err := parseSSAConflicts(applyErr)
	if err != nil {
		return err
	}
	if receipt.TerminalHandoff == nil || len(conflicts) != len(imageCacheTerminalHandoffConflicts) ||
		len(receipt.TerminalHandoff.Conflicts) != len(imageCacheTerminalHandoffConflicts) {
		return errors.New("Image-cache ownership terminal handoff conflict count is not exact")
	}
	want := make(map[string]string, len(imageCacheTerminalHandoffConflicts))
	for _, reviewed := range imageCacheTerminalHandoffConflicts {
		want[reviewed.field] = reviewed.manager
	}
	for _, authorized := range receipt.TerminalHandoff.Conflicts {
		matched := false
		for _, reviewed := range imageCacheTerminalHandoffConflicts {
			if authorized.Pointer == reviewed.pointer && authorized.LegacyManager == reviewed.manager {
				matched = true
				break
			}
		}
		if !matched {
			return errors.New("Image-cache ownership terminal handoff receipt contains an unreviewed conflict")
		}
	}
	seen := make(map[string]struct{}, len(conflicts))
	for _, conflict := range conflicts {
		if want[conflict.field] != conflict.manager {
			return fmt.Errorf("Image-cache ownership terminal handoff conflict %s:%s is outside the exact allowlist", conflict.manager, conflict.field)
		}
		key := conflict.manager + "\x00" + conflict.field
		if _, exists := seen[key]; exists {
			return errors.New("Image-cache ownership terminal handoff conflict is duplicated")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func verifyImageCacheTerminalHandoffPreconditions(value map[string]any, release declarativerelease.PlanRelease, receipt declarativerelease.OwnershipAdoptionReceipt, manifest []byte) error {
	metadata := mapField(value, "metadata")
	if stringValue(metadata["uid"]) != receipt.Final.UID || stringValue(metadata["resourceVersion"]) != receipt.Final.ResourceVersion ||
		int64Value(metadata["generation"]) != receipt.Final.Generation {
		return errors.New("Image-cache terminal handoff CAS drifted")
	}
	labelPointer := imageCacheTerminalHandoffConflicts[0].pointer
	strategyPointer := imageCacheTerminalHandoffConflicts[1].pointer
	imagePointer := "/spec/template/spec/containers[name=image-cache]/image"
	if !managedFieldsOwnPointers(metadata, release.Workload.FieldManager, []string{labelPointer, strategyPointer, imagePointer}) ||
		!managedFieldsOwnAnyPointer(metadata, "helm", []string{labelPointer}) || managedFieldsOwnAnyPointer(metadata, "helm", []string{strategyPointer, imagePointer}) ||
		!managedFieldsOwnAnyPointer(metadata, "kubectl-patch", []string{strategyPointer}) || managedFieldsOwnAnyPointer(metadata, "kubectl-patch", []string{labelPointer, imagePointer}) {
		return errors.New("Image-cache terminal handoff managedFields witness is not exact")
	}
	for _, manager := range managedFieldManagers(metadata) {
		if manager != release.Workload.FieldManager && manager != "helm" && manager != "kubectl-patch" &&
			managedFieldsOwnAnyPointer(metadata, manager, []string{labelPointer, strategyPointer}) {
			return fmt.Errorf("Image-cache terminal handoff leaf is unexpectedly owned by %s", manager)
		}
	}
	if stringValue(mapField(mapField(value, "spec"), "updateStrategy")["type"]) != "OnDelete" {
		return errors.New("Image-cache terminal handoff predecessor is not exact OnDelete")
	}
	desired, err := declarativerelease.ResourceSetItem(manifest, receipt.Final.Primary)
	if err != nil {
		return err
	}
	desiredMetadata := mapField(desired, "metadata")
	if mapStringField(desiredMetadata, "labels")["app.kubernetes.io/managed-by"] != release.Workload.FieldManager {
		return errors.New("Image-cache terminal handoff forward managed-by is invalid")
	}
	strategy := mapField(mapField(desired, "spec"), "updateStrategy")
	rolling := mapField(strategy, "rollingUpdate")
	if stringValue(strategy["type"]) != "RollingUpdate" || int64Value(rolling["maxUnavailable"]) != 2 || int64Value(rolling["maxSurge"]) != 0 {
		return errors.New("Image-cache terminal handoff forward rolling policy is invalid")
	}
	image, err := declaredContainerImage(desired, release.Workload.Container, "container")
	if err != nil || !strings.HasPrefix(image, release.Artifact.Repository+"@sha256:") {
		return errors.New("Image-cache terminal handoff forward image is not immutable")
	}
	return nil
}

func (cluster *kubectlCluster) applyImageCacheTerminalHandoffSet(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte, dryRun bool) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil || len(identities) != 1 {
		if err == nil {
			err = errors.New("Image-cache terminal handoff must contain exactly one resource")
		}
		return err
	}
	item, err := declarativerelease.ResourceSetItem(manifest, identities[0])
	if err != nil {
		return err
	}
	encoded, err := declarativerelease.CanonicalJSON(item)
	if err != nil {
		return err
	}
	arguments := applyArguments(release, dryRun)
	for index, argument := range arguments {
		if argument == "--filename" {
			arguments = append(append(append([]string(nil), arguments[:index]...), "--force-conflicts"), arguments[index:]...)
			_, err = cluster.kubectlRun(ctx, encoded, arguments...)
			return err
		}
	}
	return errors.New("Image-cache terminal handoff force arguments are invalid")
}

func (cluster *kubectlCluster) verifyImageCacheTerminalHandoffComplete(ctx context.Context, release declarativerelease.PlanRelease, handoff imageCacheTerminalHandoff) (map[string]any, error) {
	raw, err := cluster.getResource(ctx, handoff.receipt.Final.Primary)
	if err != nil || len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("read completed Image-cache terminal handoff: %w", err)
	}
	value, err := decodeJSONObject(raw)
	if err != nil {
		return nil, err
	}
	metadata := mapField(value, "metadata")
	pointers := []string{imageCacheTerminalHandoffConflicts[0].pointer, imageCacheTerminalHandoffConflicts[1].pointer, "/spec/template/spec/containers[name=image-cache]/image"}
	if stringValue(metadata["uid"]) != handoff.preUID || stringValue(metadata["resourceVersion"]) == handoff.preRV ||
		int64Value(metadata["generation"]) <= handoff.preGen || !managedFieldsOwnPointers(metadata, release.Workload.FieldManager, pointers) ||
		managedFieldsOwnAnyPointer(metadata, "helm", pointers) || managedFieldsOwnAnyPointer(metadata, "kubectl-patch", pointers) {
		return nil, errors.New("Image-cache ownership terminal handoff did not become exclusive")
	}
	return value, nil
}

func (cluster *kubectlCluster) waitImageCacheTerminalHandoffComplete(ctx context.Context, release declarativerelease.PlanRelease, handoff imageCacheTerminalHandoff) (map[string]any, error) {
	deadline := time.Now().Add(cluster.timeout)
	var lastErr error
	for {
		value, err := cluster.verifyImageCacheTerminalHandoffComplete(ctx, release, handoff)
		if err == nil {
			metadata := mapField(value, "metadata")
			status := mapField(value, "status")
			image, imageErr := declaredContainerImage(value, release.Workload.Container, "container")
			if imageErr == nil && image == handoff.receipt.Final.ImageRef &&
				int64Value(status["observedGeneration"]) == int64Value(metadata["generation"]) &&
				int32(int64Value(status["desiredNumberScheduled"])) == handoff.receipt.Final.Desired &&
				int32(int64Value(status["numberReady"])) == handoff.receipt.Final.Ready &&
				int32(int64Value(status["numberAvailable"])) == handoff.receipt.Final.Available &&
				int32(int64Value(status["numberUnavailable"])) == handoff.receipt.Final.Unavailable {
				return value, nil
			}
			lastErr = errors.New("Image-cache terminal handoff LKG cohort has not settled")
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func emitImageCacheTerminalHandoffReceipt(release declarativerelease.PlanRelease, handoff imageCacheTerminalHandoff, value map[string]any) error {
	metadata := mapField(value, "metadata")
	receipt := map[string]any{
		"apiVersion": "release.fugue.dev/v2", "kind": "OwnershipTerminalHandoffReceipt", "component": release.ComponentID,
		"sourceReceiptDigest": handoff.receipt.ReceiptDigest, "authorizationRunId": handoff.receipt.TerminalHandoff.RunID,
		"authorizationRunAttempt": handoff.receipt.TerminalHandoff.RunAttempt, "failedConfigSha": handoff.receipt.TerminalHandoff.FailedConfigSHA,
		"failedForwardImageRef": handoff.receipt.TerminalHandoff.ForwardImageRef, "artifactReceiptDigest": handoff.receipt.TerminalHandoff.ArtifactReceiptDigest,
		"targetConfigSha": handoff.target.ConfigSHA, "targetImageRef": handoff.target.ImageRef,
		"pre":          map[string]any{"uid": handoff.preUID, "resourceVersion": handoff.preRV, "generation": handoff.preGen},
		"post":         map[string]any{"uid": stringValue(metadata["uid"]), "resourceVersion": stringValue(metadata["resourceVersion"]), "generation": int64Value(metadata["generation"])},
		"fieldManager": release.Workload.FieldManager, "conflicts": handoff.receipt.TerminalHandoff.Conflicts,
		"scaffolds": handoff.receipt.TerminalHandoff.Scaffolds,
	}
	unsigned, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return err
	}
	receipt["receiptDigest"] = digestBytesLocal(unsigned)
	encoded, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "ownership-terminal-handoff-receipt=%s\n", encoded)
	return nil
}

func (cluster *kubectlCluster) Apply(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) error {
	if err := cluster.requireReferencedSecrets(ctx, release, manifest); err != nil {
		return err
	}
	if release.Transition != nil && release.Transition.Type == "edge-group-ab" {
		return cluster.applyEdgeGroupAB(ctx, release, target, manifest)
	}
	if dryRunErr := cluster.applyResourceSet(ctx, release, manifest, true); dryRunErr != nil {
		handoff, err := cluster.prepareReceiptBoundImageCacheHandoff(ctx, release, manifest, dryRunErr, true)
		if err != nil {
			return err
		}
		if err := cluster.applyImageCacheTerminalHandoffSet(ctx, release, handoff.manifest, false); err != nil {
			return fmt.Errorf("apply reviewed Image-cache ownership terminal handoff: %w", err)
		}
		post, err := cluster.waitImageCacheTerminalHandoffComplete(ctx, release, handoff)
		if err != nil {
			return err
		}
		if err := emitImageCacheTerminalHandoffReceipt(release, handoff, post); err != nil {
			return err
		}
		fresh, err := cluster.ObserveCAS(ctx, release, manifest)
		if err != nil || fresh.UID != handoff.preUID || fresh.ResourceVersion == handoff.preRV || fresh.Generation <= handoff.preGen {
			if err == nil {
				err = errors.New("Image-cache terminal handoff forward CAS did not advance")
			}
			return err
		}
		manifest, err = declarativerelease.BindManifestCAS(manifest, fresh)
		if err != nil {
			return err
		}
		if err := cluster.applyResourceSet(ctx, release, manifest, false); err != nil {
			return err
		}
		_, err = cluster.verifyImageCacheTerminalHandoffComplete(ctx, release, handoff)
		return err
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
	var lastFailure error
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
		if err != nil {
			lastFailure = err
		}
		if shouldReturnTypedPrewritePredecessorHealth(ctx, release, target, err) {
			return observation, err
		}
		if time.Now().After(deadline) {
			if lastErr == nil {
				if lastFailure != nil {
					lastErr = fmt.Errorf("continuous health window reset by: %w", lastFailure)
				} else {
					lastErr = errors.New("workload did not converge to target")
				}
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

func shouldReturnTypedPrewritePredecessorHealth(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, err error) bool {
	exactPredecessor := release.ExpectedPreviousPresent && target.Present &&
		target.ImageRef == release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest &&
		target.ConfigSHA == release.ExpectedPreviousConfigSHA && target.ManifestSHA == release.ExpectedPreviousManifestSHA &&
		target.OCIRevision == release.ExpectedPreviousOCIRevision
	return declarativerelease.IsPrewritePredecessorHealthWait(ctx) && exactPredecessor &&
		errors.Is(err, declarativerelease.ErrDegradedPredecessorHealth)
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
			converged, receiptErr := cluster.receiptBoundHistoricalResourceConverged(ctx, release, manifest, identity, desired, live, liveRaw)
			if receiptErr != nil {
				return receiptErr
			}
			if !converged {
				return fmt.Errorf("declared resource %s/%s has not converged", identity.Kind, identity.Name)
			}
		}
	}
	return nil
}

func (cluster *kubectlCluster) receiptBoundHistoricalResourceConverged(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte, identity declarativerelease.ResourceIdentity, desired, live map[string]any, liveRaw []byte) (bool, error) {
	primary := declarativerelease.ResourceIdentity{
		APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
		Namespace: release.Workload.Namespace, Name: release.Workload.Name,
	}
	if release.MigrationState != "independent" || !release.RetrySameLKG || !release.ExpectedPreviousPresent ||
		release.AdoptionReceiptPath != "deploy/releases/"+release.ComponentID+"/adoption-receipt.json" || identity != primary {
		return false, nil
	}
	spec := mapField(desired, "spec")
	strategy := mapField(spec, "updateStrategy")
	if release.Workload.Kind != "DaemonSet" || stringValue(strategy["type"]) != "OnDelete" {
		return false, nil
	}
	liveTemplate := mapField(mapField(live, "spec"), "template")
	liveAnnotations := mapStringField(mapField(liveTemplate, "metadata"), "annotations")
	if liveAnnotations["fugue.pro/source-commit"] != "" || liveAnnotations["fugue.pro/oci-revision"] != "" {
		return false, errors.New("receipt-bound historical workload has conflicting live source identity")
	}
	resources, err := cluster.observeResources(ctx, manifest, release, liveRaw)
	if err != nil {
		return false, err
	}
	observation, err := parseDegradedObservation(liveRaw, release)
	if err != nil {
		return false, err
	}
	observation.Resources = resources
	if _, err := cluster.bindReceiptBoundDegradedObservation(ctx, release, liveRaw, observation, resources); err != nil {
		return false, err
	}
	copyRaw, err := json.Marshal(desired)
	if err != nil {
		return false, err
	}
	reviewed, err := decodeJSONObject(copyRaw)
	if err != nil {
		return false, err
	}
	template := mapField(mapField(reviewed, "spec"), "template")
	templateMetadata := mapField(template, "metadata")
	if annotations, ok := templateMetadata["annotations"].(map[string]any); ok {
		delete(annotations, "fugue.pro/source-commit")
		delete(annotations, "fugue.pro/oci-revision")
	}
	return declarativerelease.ResourceDesiredSubset(reviewed, live), nil
}

func (cluster *kubectlCluster) observeExpected(ctx context.Context, release declarativerelease.PlanRelease, expectedOCI string, manifest []byte, allowHistoricalRestarts bool) (declarativerelease.Observation, error) {
	primary := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	workloadRaw, err := cluster.getResource(ctx, primary)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	trimmedWorkload := bytes.TrimSpace(workloadRaw)
	// kubectl versions differ for --ignore-not-found -o json: some emit an
	// empty stream while others emit the JSON null sentinel. Both mean that an
	// explicitly absent first-install predecessor was observed; treating null
	// as a workload makes prepare wait for a target that cannot exist yet.
	if resourceAbsent(trimmedWorkload) {
		// A first-install LKG is the canonical empty resource set. Keep an
		// absent CAS witness for every declared resource so the prewrite bind can
		// prove that no predecessor object existed, including auxiliary objects.
		identities, identityErr := declarativerelease.ResourceSetIdentities(manifest)
		if identityErr != nil {
			return declarativerelease.Observation{}, identityErr
		}
		resources := make([]declarativerelease.ResourceObservation, 0, len(identities))
		for _, identity := range identities {
			desired, desiredErr := declarativerelease.ResourceSetItem(manifest, identity)
			if desiredErr != nil {
				return declarativerelease.Observation{}, desiredErr
			}
			metadata := mapField(desired, "metadata")
			resources = append(resources, declarativerelease.ResourceObservation{
				Identity:         identity,
				RetainOnRollback: mapStringField(metadata, "annotations")["fugue.pro/release-retain-on-rollback"] == "true",
			})
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
	if err := verifyDeclaredArtifactImageIDs(podsRaw, manifest, release, allowHistoricalRestarts); err != nil {
		return declarativerelease.Observation{}, err
	}
	bootstrapRuntime := adoptingBootstrapRuntime(release, allowHistoricalRestarts)
	if bootstrapRuntime != nil {
		image := release.Artifact.Repository + "@" + bootstrapRuntime.ImageDigest
		arguments := []string{"python3", cluster.verifier, "--image", image, "--platform", "linux/amd64", "--expected-revision", bootstrapRuntime.OCIRevision,
			"--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5", "--max-attempts", "2", "--retry-delay-seconds", "0.1"}
		verificationRaw, verifyErr := cluster.run(ctx, nil, arguments[0], arguments[1:]...)
		if verifyErr != nil {
			return declarativerelease.Observation{}, fmt.Errorf("verify adoption bootstrap runtime: %w", verifyErr)
		}
		verification, decodeErr := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
		if decodeErr != nil || verification.Image != image || verification.OCIRevision != bootstrapRuntime.OCIRevision {
			return declarativerelease.Observation{}, errors.New("adoption bootstrap runtime registry identity mismatch")
		}
	}
	observationWorkloadRaw := workloadRaw
	if allowHistoricalRestarts && release.MigrationState == "adopting" && release.OwnershipAdoption != nil &&
		(release.RetrySameLKG || declarativerelease.InitialExplicitBootstrapAdoption(release)) && release.BootstrapLKGPath != "" {
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
	if err := bindAdoptingImmutableSource(&partial, verification, expectedOCI, allowHistoricalRestarts); err != nil {
		return declarativerelease.Observation{}, err
	}
	partial.OCIRevision = expectedOCI
	resources, err := cluster.observeResources(ctx, manifest, release, workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	partial.Resources = resources
	return partial, nil
}

// bindAdoptingImmutableSource is the one-time bridge for a legacy workload
// that already runs an immutable image but predates source annotations. The
// registry revision is accepted only inside an explicit adoption observation
// and only when it is the exact expected bootstrap revision. Independent
// components never enter this path.
func bindAdoptingImmutableSource(observation *declarativerelease.Observation, verification declarativerelease.RegistryVerification, expectedOCI string, allowHistorical bool) error {
	if observation.ConfigSHA != "" || observation.ManifestSHA != "" || !allowHistorical {
		return nil
	}
	if !strings.Contains(observation.ImageRef, "@sha256:") || verification.Image != observation.ImageRef || verification.OCIRevision != expectedOCI {
		return errors.New("immutable adoption bootstrap source is not registry-bound")
	}
	observation.ConfigSHA = expectedOCI
	observation.ManifestSHA = expectedOCI
	return nil
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
	reviewed := make(map[declarativerelease.ResourceIdentity][]string)
	if release.OwnershipAdoption != nil {
		for _, scope := range release.OwnershipAdoption.Resources {
			reviewed[scope.Identity] = scope.Fields
		}
	}
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
		if resourceAbsent(raw) {
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
		if fields := reviewed[identity]; len(fields) > 0 {
			resource.ReviewedOwnershipApplied = managedFieldsOwnPointers(metadata, release.Workload.FieldManager, fields)
			resource.ReviewedOwnershipExclusive = resource.ReviewedOwnershipApplied &&
				managedFieldsPointersExclusivelyOwned(metadata, release.Workload.FieldManager, fields)
		}
		resource.ObjectDigest = digestJSON(sanitizeObservedResource(value))
		resources = append(resources, resource)
	}
	return resources, nil
}

func resourceAbsent(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
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
	allowLegacy := release.MigrationState == "adopting" && release.OwnershipAdoption != nil && release.BootstrapLKGPath != "" &&
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

func verifyDeclaredArtifactImageIDs(podsRaw, manifest []byte, release declarativerelease.PlanRelease, allowHistorical bool) error {
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
	if bootstrap := adoptingBootstrapRuntime(release, allowHistorical); bootstrap != nil {
		expected["container\x00"+bootstrap.Container] = release.Artifact.Repository + "@" + bootstrap.ImageDigest
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
		if mapField(pod, "metadata")["deletionTimestamp"] != nil {
			continue
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

func adoptingBootstrapRuntime(release declarativerelease.PlanRelease, allowHistorical bool) *declarativerelease.BootstrapRuntime {
	bootstrap := release.BootstrapRuntime
	if !allowHistorical || release.MigrationState != "adopting" || release.OwnershipAdoption == nil || bootstrap == nil ||
		bootstrap.Resource.APIVersion != release.Workload.APIVersion || bootstrap.Resource.Kind != release.Workload.Kind ||
		bootstrap.Resource.Namespace != release.Workload.Namespace || bootstrap.Resource.Name != release.Workload.Name ||
		bootstrap.Container != release.Workload.Container {
		return nil
	}
	return bootstrap
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
	// Some legacy Helm Deployments share only the instance/name selector with
	// sibling control-plane workloads. The Pod template's component label is
	// immutable for the observed generation and safely narrows the read to this
	// workload without changing its Kubernetes selector.
	templateLabels := mapField(mapField(mapField(spec, "template"), "metadata"), "labels")
	if component, ok := templateLabels["app.kubernetes.io/component"].(string); ok && strings.TrimSpace(component) != "" {
		if existing, exists := labels["app.kubernetes.io/component"]; exists && existing != component {
			return "", errors.New("workload selector conflicts with Pod component label")
		}
		labels["app.kubernetes.io/component"] = component
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
	podManifestSHA := manifestSHA
	if bootstrap := adoptingBootstrapRuntime(release, allowHistoricalRestarts); bootstrap != nil {
		podManifestSHA = bootstrap.OCIRevision
	}
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
		if podSource != podManifestSHA {
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
		return "", "", fmt.Errorf("%w: ready workload pod count mismatch: got=%d want=%d", declarativerelease.ErrDegradedPredecessorHealth, readyCount, desired)
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
