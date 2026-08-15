package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
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

const (
	maxKubernetesOutputBytes     = 4 << 20
	defaultKubectlReadTimeout    = 15 * time.Second
	defaultKubectlReadAttempts   = 2
	defaultKubectlReadRetryDelay = 250 * time.Millisecond
)

var (
	emergencyConflictCountPattern = regexp.MustCompile(`Apply failed with ([1-9][0-9]*) conflicts?`)
	emergencyConflictPattern      = regexp.MustCompile(`conflict with "([^"]+)" using [^:]+: (\.[^[:space:]]+)`)
	emergencyConflictGroupPattern = regexp.MustCompile(`conflicts with "([^"]+)" using [^:]+:`)
)

type emergencySSAConflict struct {
	manager string
	field   string
}

var emergencyOwnershipManagers = map[string]bool{
	"kubectl":       true,
	"kubectl-patch": true,
	"kubectl-set":   true,
}

var legacyOwnershipManagers = map[string]bool{
	"helm": true,
}

type kubectlCluster struct {
	kubectl        string
	verifier       string
	timeout        time.Duration
	readTimeout    time.Duration
	readAttempts   int
	readRetryDelay time.Duration
	serviceHTTPURL func(string, string, int) string
	metadata       metadataclient.Interface
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
	return cluster.observeExpected(ctx, release, target.OCIRevision, manifest)
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

// ValidateEmergencyRollbackDrift proves that a continuous rollback is
// responding only to a reviewed emergency mutation of release-owned runtime
// pointers. It binds every live resource to the fresh CAS observation, keeps
// all other declared fields byte-equivalent to the recorded forward manifest,
// and requires an exact allowlisted Update managedFields witness for each
// differing pointer. The immutable verified monitor record proves that the
// declarative manager owned the target before the emergency write; Kubernetes
// transfers a leaf out of an Apply entry when a later Update claims it, so
// requiring current co-ownership would make exact recovery impossible. The
// post-apply convergence check still requires declarative-exclusive ownership.
func (cluster *kubectlCluster) ValidateEmergencyRollbackDrift(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte, current declarativerelease.Observation) (declarativerelease.Observation, error) {
	if !current.Present || len(current.Resources) == 0 {
		return declarativerelease.Observation{}, errors.New("emergency rollback drift lacks a present resource CAS")
	}
	observed := make(map[declarativerelease.ResourceIdentity]int, len(current.Resources))
	for index, resource := range current.Resources {
		observed[resource.Identity] = index
	}
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil || len(identities) != len(observed) {
		return declarativerelease.Observation{}, errors.New("emergency rollback resource inventory is inconsistent")
	}
	drifted := false
	for _, identity := range identities {
		index, ok := observed[identity]
		expected := current.Resources[index]
		if !ok || !expected.Present || expected.UID == "" || expected.ResourceVersion == "" ||
			(identity == current.Primary && expected.Generation < 1) {
			return declarativerelease.Observation{}, fmt.Errorf("emergency rollback resource %s/%s lacks exact CAS", identity.Kind, identity.Name)
		}
		raw, getErr := cluster.getResource(ctx, identity)
		if getErr != nil || resourceAbsent(raw) {
			return declarativerelease.Observation{}, fmt.Errorf("read emergency rollback resource %s/%s: %w", identity.Kind, identity.Name, getErr)
		}
		live, decodeErr := decodeJSONObject(raw)
		if decodeErr != nil {
			return declarativerelease.Observation{}, decodeErr
		}
		metadata := mapField(live, "metadata")
		freshUID, freshRV, freshGeneration := stringValue(metadata["uid"]), stringValue(metadata["resourceVersion"]), int64Value(metadata["generation"])
		if freshUID != expected.UID || freshRV == "" || freshGeneration != expected.Generation || digestJSON(sanitizeObservedResource(live)) != expected.ObjectDigest {
			return declarativerelease.Observation{}, fmt.Errorf("emergency rollback resource %s/%s changed after CAS observation", identity.Kind, identity.Name)
		}
		// Status-only controller updates may advance RV continuously while an
		// invalid Pod is failing. Keep the newest RV from the same UID,
		// generation, and desired-object digest so the subsequent SSA CAS is
		// fresh without accepting any spec movement.
		current.Resources[index].ResourceVersion = freshRV
		if identity == current.Primary {
			current.UID, current.ResourceVersion, current.Generation = freshUID, freshRV, freshGeneration
		}
		desired, desiredErr := declarativerelease.ResourceSetItem(manifest, identity)
		if desiredErr != nil {
			return declarativerelease.Observation{}, desiredErr
		}
		normalized := sanitizeObservedResource(live)
		allowed := emergencyOwnershipPointers(release, identity, desired)
		for _, pointer := range allowed {
			desiredValue, desiredFound := emergencyRuntimePointerValue(desired, pointer)
			liveValue, liveFound := emergencyRuntimePointerValue(normalized, pointer)
			if !desiredFound || !liveFound || desiredValue == liveValue {
				continue
			}
			if !reviewedEmergencyUpdateOwnsPointer(metadata, pointer, allowed) {
				return declarativerelease.Observation{}, fmt.Errorf("emergency rollback pointer %s lacks exact ownership evidence", pointer)
			}
			if !setEmergencyRuntimePointerValue(normalized, pointer, desiredValue) {
				return declarativerelease.Observation{}, fmt.Errorf("emergency rollback pointer %s is unsupported", pointer)
			}
			drifted = true
		}
		if !declarativerelease.ResourceDesiredSubset(desired, normalized) {
			return declarativerelease.Observation{}, fmt.Errorf("emergency rollback resource %s/%s drift expands beyond the exact allowlist", identity.Kind, identity.Name)
		}
	}
	if !drifted {
		return declarativerelease.Observation{}, errors.New("emergency rollback drift evidence is absent")
	}
	return current, nil
}

func reviewedEmergencyUpdateOwnsPointer(metadata map[string]any, pointer string, allowed []string) bool {
	for _, rawEntry := range anySlice(metadata["managedFields"]) {
		entry, _ := rawEntry.(map[string]any)
		if !emergencyOwnershipManagers[stringValue(entry["manager"])] || stringValue(entry["operation"]) != "Update" ||
			stringValue(entry["subresource"]) != "" || !managedFieldsEntryOwnsPointers(mapField(entry, "fieldsV1"), []string{pointer}, true) {
			continue
		}
		pointers, err := managedFieldsEntryPointers(mapField(entry, "fieldsV1"))
		if err == nil && len(pointers) > 0 && stringSubset(pointers, allowed) {
			return true
		}
	}
	return false
}

func emergencyRuntimePointerValue(resource map[string]any, pointer string) (string, bool) {
	if strings.HasPrefix(pointer, "/metadata/annotations/") {
		key := unescapeJSONPointerToken(strings.TrimPrefix(pointer, "/metadata/annotations/"))
		value, ok := mapField(mapField(resource, "metadata"), "annotations")[key].(string)
		return value, ok
	}
	if strings.HasPrefix(pointer, "/spec/template/metadata/annotations/") {
		key := unescapeJSONPointerToken(strings.TrimPrefix(pointer, "/spec/template/metadata/annotations/"))
		value, ok := mapField(mapField(mapField(mapField(resource, "spec"), "template"), "metadata"), "annotations")[key].(string)
		return value, ok
	}
	field, name, tail, ok := emergencyContainerPointerParts(pointer)
	if !ok {
		return "", false
	}
	for _, raw := range anySlice(mapField(mapField(mapField(resource, "spec"), "template"), "spec")[field]) {
		container, _ := raw.(map[string]any)
		if stringValue(container["name"]) != name {
			continue
		}
		if tail == "image" {
			value, found := container["image"].(string)
			return value, found
		}
		if scope, resource, resourceOK := resourceCPUTail(tail); resourceOK {
			value, found := mapField(mapField(container, "resources"), scope)[resource].(string)
			return value, found
		}
		probeName, probeOK := probePathTail(tail)
		if !probeOK {
			return "", false
		}
		value, found := mapField(mapField(container, probeName), "httpGet")["path"].(string)
		return value, found
	}
	return "", false
}

func setEmergencyRuntimePointerValue(resource map[string]any, pointer, value string) bool {
	if strings.HasPrefix(pointer, "/metadata/annotations/") {
		key := unescapeJSONPointerToken(strings.TrimPrefix(pointer, "/metadata/annotations/"))
		mapField(mapField(resource, "metadata"), "annotations")[key] = value
		return true
	}
	if strings.HasPrefix(pointer, "/spec/template/metadata/annotations/") {
		key := unescapeJSONPointerToken(strings.TrimPrefix(pointer, "/spec/template/metadata/annotations/"))
		mapField(mapField(mapField(mapField(resource, "spec"), "template"), "metadata"), "annotations")[key] = value
		return true
	}
	field, name, tail, ok := emergencyContainerPointerParts(pointer)
	if !ok {
		return false
	}
	for _, raw := range anySlice(mapField(mapField(mapField(resource, "spec"), "template"), "spec")[field]) {
		container, _ := raw.(map[string]any)
		if stringValue(container["name"]) == name {
			if tail == "image" {
				container["image"] = value
				return true
			}
			if scope, resource, resourceOK := resourceCPUTail(tail); resourceOK {
				resourceScope := mapField(mapField(container, "resources"), scope)
				if resourceScope == nil {
					return false
				}
				resourceScope[resource] = value
				return true
			}
			probeName, probeOK := probePathTail(tail)
			if !probeOK {
				return false
			}
			mapField(mapField(container, probeName), "httpGet")["path"] = value
			return true
		}
	}
	return false
}

func emergencyContainerPointerParts(pointer string) (string, string, string, bool) {
	const prefix = "/spec/template/spec/"
	if !strings.HasPrefix(pointer, prefix) {
		return "", "", "", false
	}
	remainder := strings.TrimPrefix(pointer, prefix)
	open := strings.Index(remainder, "[name=")
	if open < 1 {
		return "", "", "", false
	}
	close := strings.Index(remainder[open:], "]")
	if close < 0 {
		return "", "", "", false
	}
	close += open
	field, name := remainder[:open], remainder[open+len("[name="):close]
	tail := strings.TrimPrefix(remainder[close+1:], "/")
	if (field != "containers" && field != "initContainers") || name == "" {
		return "", "", "", false
	}
	if tail != "image" {
		if _, _, resourceOK := resourceCPUTail(tail); !resourceOK {
			if _, ok := probePathTail(tail); !ok {
				return "", "", "", false
			}
		}
	}
	return field, name, tail, true
}

func resourceCPUTail(tail string) (string, string, bool) {
	for _, scope := range []string{"limits", "requests"} {
		if tail == "resources/"+scope+"/cpu" {
			return scope, "cpu", true
		}
	}
	return "", "", false
}

func probePathTail(tail string) (string, bool) {
	for _, name := range []string{"startupProbe", "livenessProbe", "readinessProbe"} {
		if tail == name+"/httpGet/path" {
			return name, true
		}
	}
	return "", false
}

func unescapeJSONPointerToken(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~1", "/"), "~0", "~")
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
		return declarativerelease.Observation{}, err
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

func (cluster *kubectlCluster) DryRunApply(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	if err := cluster.requireReferencedSecrets(ctx, release, manifest); err != nil {
		return err
	}
	return cluster.applyResourceSet(ctx, release, manifest, true)
}

func (cluster *kubectlCluster) Apply(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) error {
	if err := cluster.requireReferencedSecrets(ctx, release, manifest); err != nil {
		return err
	}
	if release.Transition != nil && release.Transition.Type == "edge-group-ab" {
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
		if applyErr := cluster.applyResourceWithOwnershipConvergence(ctx, release, identity, item, encoded, dryRun); applyErr != nil {
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

// applyResourceWithOwnershipConvergence is the one bounded recovery path for
// emergency kubectl writes. It never force-applies an unreviewed field. A
// typed SSA conflict must exactly match the release-owned annotation, image,
// or HTTP probe path allowlist and an Update managedFields entry. Execute
// removes an emergency entry or only the conflicting legacy probe path leaf
// with UID/RV/entry JSON-Patch tests, then retries ordinary SSA. Prepare
// remains read-only and accepts only the same typed proof.
func (cluster *kubectlCluster) applyResourceWithOwnershipConvergence(ctx context.Context, release declarativerelease.PlanRelease, identity declarativerelease.ResourceIdentity, desired map[string]any, encoded []byte, dryRun bool) error {
	_, applyErr := cluster.kubectlRun(ctx, encoded, applyArguments(release, dryRun)...)
	if applyErr != nil {
		// A server-side apply may commit before the client loses its response.
		// Continue only when a fresh GET proves the same pre-existing UID now
		// contains the complete desired object and the declarative manager owns
		// every reviewed runtime pointer.  Creates remain fail-closed because
		// they have no prewrite UID to distinguish our object from a concurrent
		// writer.  Dry-runs never use live state to mask a validation failure.
		if !dryRun {
			converged, reconcileErr := cluster.reconcileApplyCommitUnknown(ctx, release, identity, desired)
			if reconcileErr != nil {
				return errors.Join(applyErr, reconcileErr)
			}
			if converged {
				return cluster.cleanupEmergencyOwnership(ctx, release, identity, desired, true)
			}
		}
		allowed := emergencyOwnershipPointers(release, identity, desired)
		if len(allowed) == 0 {
			return applyErr
		}
		liveRaw, getErr := cluster.getResource(ctx, identity)
		if getErr != nil || resourceAbsent(liveRaw) {
			return fmt.Errorf("read emergency ownership witness: %w", getErr)
		}
		live, decodeErr := decodeJSONObject(liveRaw)
		if decodeErr != nil {
			return decodeErr
		}
		if evidenceErr := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, applyErr); evidenceErr != nil {
			return evidenceErr
		}
		if dryRun {
			return nil
		}
		legacyPatch, legacyFound, legacyErr := nextLegacyProbeOwnershipPatch(live, allowed, applyErr)
		if legacyErr != nil {
			return legacyErr
		}
		if legacyFound {
			encodedPatch, encodeErr := declarativerelease.CanonicalJSON(legacyPatch)
			if encodeErr != nil {
				return encodeErr
			}
			if _, patchErr := cluster.kubectlRun(ctx, nil, "patch", strings.ToLower(identity.Kind), identity.Name,
				"--namespace", identity.Namespace, "--type=json", "--patch", string(encodedPatch), "--output", "json"); patchErr != nil {
				return fmt.Errorf("remove exact legacy probe ownership: %w", patchErr)
			}
		} else if cleanupErr := cluster.cleanupEmergencyOwnership(ctx, release, identity, desired, false); cleanupErr != nil {
			return cleanupErr
		}
		freshRaw, getErr := cluster.getResource(ctx, identity)
		if getErr != nil || resourceAbsent(freshRaw) {
			return fmt.Errorf("read resource after emergency ownership cleanup: %w", getErr)
		}
		fresh, decodeErr := decodeJSONObject(freshRaw)
		if decodeErr != nil {
			return decodeErr
		}
		rebound, rebindErr := rebindDesiredResourceVersionAfterOwnershipCleanup(desired, live, fresh)
		if rebindErr != nil {
			return rebindErr
		}
		if _, retryErr := cluster.kubectlRun(ctx, rebound, applyArguments(release, false)...); retryErr != nil {
			return fmt.Errorf("ordinary apply after exact emergency ownership cleanup: %w", retryErr)
		}
	}
	if dryRun {
		return nil
	}
	return cluster.cleanupEmergencyOwnership(ctx, release, identity, desired, true)
}

func (cluster *kubectlCluster) reconcileApplyCommitUnknown(ctx context.Context, release declarativerelease.PlanRelease, identity declarativerelease.ResourceIdentity, desired map[string]any) (bool, error) {
	desiredMetadata := mapField(desired, "metadata")
	desiredUID := stringValue(desiredMetadata["uid"])
	if desiredUID == "" {
		return false, nil
	}
	liveRaw, err := cluster.getResource(ctx, identity)
	if err != nil {
		return false, fmt.Errorf("read server-side apply commit-unknown state: %w", err)
	}
	if resourceAbsent(liveRaw) {
		return false, nil
	}
	live, err := decodeJSONObject(liveRaw)
	if err != nil {
		return false, err
	}
	liveMetadata := mapField(live, "metadata")
	if stringValue(liveMetadata["uid"]) != desiredUID || !declarativerelease.ResourceDesiredSubset(desired, live) {
		return false, nil
	}
	manager := release.Workload.FieldManager
	if manager == "" {
		return false, errors.New("server-side apply commit-unknown state lacks a declarative manager")
	}
	allowed := emergencyOwnershipPointers(release, identity, desired)
	if len(allowed) > 0 {
		if !managedFieldsOwnPointers(liveMetadata, manager, allowed) {
			return false, nil
		}
		return true, nil
	}
	for _, observed := range managedFieldManagers(liveMetadata) {
		if observed == manager {
			return true, nil
		}
	}
	return false, nil
}

// rebindDesiredResourceVersionAfterOwnershipCleanup permits exactly the RV
// change caused by removing reviewed emergency managedFields entries. The
// live object must otherwise be byte-equivalent after removing Kubernetes
// observation metadata, and UID/generation remain explicit CAS witnesses.
func rebindDesiredResourceVersionAfterOwnershipCleanup(desired, before, fresh map[string]any) ([]byte, error) {
	desiredMetadata := mapField(desired, "metadata")
	beforeMetadata := mapField(before, "metadata")
	freshMetadata := mapField(fresh, "metadata")
	desiredUID, desiredRV := stringValue(desiredMetadata["uid"]), stringValue(desiredMetadata["resourceVersion"])
	beforeUID, beforeRV := stringValue(beforeMetadata["uid"]), stringValue(beforeMetadata["resourceVersion"])
	freshUID, freshRV := stringValue(freshMetadata["uid"]), stringValue(freshMetadata["resourceVersion"])
	beforeGeneration, freshGeneration := int64Value(beforeMetadata["generation"]), int64Value(freshMetadata["generation"])
	if desiredUID == "" || desiredRV == "" || desiredUID != beforeUID || desiredRV != beforeRV {
		return nil, errors.New("ownership cleanup rebind is not bound to the original desired UID/RV")
	}
	if freshUID == "" || freshRV == "" || freshUID != beforeUID || freshRV == beforeRV {
		return nil, errors.New("ownership cleanup did not produce the expected fresh UID/RV")
	}
	if beforeGeneration <= 0 || freshGeneration != beforeGeneration {
		return nil, errors.New("ownership cleanup changed the workload generation")
	}
	if digestJSON(sanitizeObservedResource(before)) != digestJSON(sanitizeObservedResource(fresh)) {
		return nil, errors.New("ownership cleanup changed the live resource outside observation metadata")
	}
	copyRaw, err := declarativerelease.CanonicalJSON(desired)
	if err != nil {
		return nil, err
	}
	rebound, err := decodeJSONObject(copyRaw)
	if err != nil {
		return nil, err
	}
	mapField(rebound, "metadata")["resourceVersion"] = freshRV
	return declarativerelease.CanonicalJSON(rebound)
}

func emergencyOwnershipPointers(release declarativerelease.PlanRelease, identity declarativerelease.ResourceIdentity, desired map[string]any) []string {
	allowedSet := make(map[string]bool, 8)
	add := func(pointer string) {
		if pointer != "" {
			allowedSet[pointer] = true
		}
	}
	metadata := mapField(desired, "metadata")
	annotations := mapField(metadata, "annotations")
	for _, key := range []string{"fugue.pro/artifact-receipt-digest", "fugue.pro/production-config-sha", "fugue.pro/release-plan-digest"} {
		if _, ok := annotations[key]; ok {
			add("/metadata/annotations/" + escapeJSONPointerToken(key))
		}
	}
	templateAnnotations := mapField(mapField(mapField(desired, "spec"), "template"), "metadata")
	templateAnnotations = mapField(templateAnnotations, "annotations")
	for _, key := range []string{"fugue.pro/oci-revision", "fugue.pro/production-config-sha", "fugue.pro/source-commit"} {
		if _, ok := templateAnnotations[key]; ok {
			add("/spec/template/metadata/annotations/" + escapeJSONPointerToken(key))
		}
	}
	if release.Workload.APIVersion == identity.APIVersion && release.Workload.Kind == identity.Kind &&
		release.Workload.Namespace == identity.Namespace && release.Workload.Name == identity.Name {
		if _, found, err := declaredContainerImageOptional(desired, release.Workload.Container, "container"); err == nil && found {
			add("/spec/template/spec/containers[name=" + release.Workload.Container + "]/image")
		}
		addDeclaredContainerCPUResourcePointers(desired, release.Workload.Container, add)
	}
	for _, target := range release.ArtifactTargets {
		if target.APIVersion != identity.APIVersion || target.Kind != identity.Kind || target.Namespace != identity.Namespace || target.Name != identity.Name {
			continue
		}
		if _, found, err := declaredContainerImageOptional(desired, target.Container, target.ContainerType); err == nil && found {
			field := "containers"
			if target.ContainerType == "init-container" {
				field = "initContainers"
			}
			add("/spec/template/spec/" + field + "[name=" + target.Container + "]/image")
		}
	}
	if release.Workload.APIVersion == identity.APIVersion && release.Workload.Kind == identity.Kind &&
		release.Workload.Namespace == identity.Namespace && release.Workload.Name == identity.Name {
		addDeclaredContainerProbePointers(desired, "container", release.Workload.Container, add)
	}
	for _, target := range release.ArtifactTargets {
		if target.APIVersion != identity.APIVersion || target.Kind != identity.Kind || target.Namespace != identity.Namespace || target.Name != identity.Name {
			continue
		}
		addDeclaredContainerProbePointers(desired, target.ContainerType, target.Container, add)
	}
	allowed := make([]string, 0, len(allowedSet))
	for pointer := range allowedSet {
		allowed = append(allowed, pointer)
	}
	sort.Strings(allowed)
	return allowed
}

// addDeclaredContainerCPUResourcePointers admits only the two scalar CPU
// leaves that kubectl set resources can own. Memory, ephemeral storage,
// resource maps and every other container field remain outside the recovery
// boundary. A reviewed manifest must declare the exact value before cleanup.
func addDeclaredContainerCPUResourcePointers(desired map[string]any, containerName string, add func(string)) {
	templateSpec := mapField(mapField(mapField(desired, "spec"), "template"), "spec")
	for _, raw := range anySlice(templateSpec["containers"]) {
		container, _ := raw.(map[string]any)
		if stringValue(container["name"]) != containerName {
			continue
		}
		resources := mapField(container, "resources")
		for _, scope := range []string{"limits", "requests"} {
			if _, ok := mapField(resources, scope)["cpu"].(string); ok {
				add("/spec/template/spec/containers[name=" + containerName + "]/resources/" + scope + "/cpu")
			}
		}
		return
	}
}

// addDeclaredContainerProbePointers adds only HTTP probe path leaves to the
// emergency ownership allowlist. Probe timing, headers, ports, commands and
// every other workload field remain outside the recovery write boundary.
func addDeclaredContainerProbePointers(desired map[string]any, containerType, containerName string, add func(string)) {
	templateSpec := mapField(mapField(mapField(desired, "spec"), "template"), "spec")
	field := "containers"
	if containerType == "init-container" {
		field = "initContainers"
	} else if containerType != "container" {
		return
	}
	for _, raw := range anySlice(templateSpec[field]) {
		container, _ := raw.(map[string]any)
		if stringValue(container["name"]) != containerName {
			continue
		}
		for _, probeName := range []string{"startupProbe", "livenessProbe", "readinessProbe"} {
			probe := mapField(container, probeName)
			httpGet := mapField(probe, "httpGet")
			if _, ok := httpGet["path"].(string); !ok {
				continue
			}
			add("/spec/template/spec/" + field + "[name=" + containerName + "]" +
				"/" + probeName + "/httpGet/path")
		}
	}
}

func validateEmergencyOwnershipConflictEvidence(desired, live map[string]any, allowed []string, applyErr error) error {
	desiredMetadata, liveMetadata := mapField(desired, "metadata"), mapField(live, "metadata")
	if stringValue(desiredMetadata["uid"]) == "" || stringValue(desiredMetadata["uid"]) != stringValue(liveMetadata["uid"]) ||
		stringValue(desiredMetadata["resourceVersion"]) == "" || stringValue(desiredMetadata["resourceVersion"]) != stringValue(liveMetadata["resourceVersion"]) {
		return errors.New("emergency ownership witness is not UID/RV bound")
	}
	conflicts, err := parseEmergencySSAConflicts(applyErr)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(conflicts))
	for _, conflict := range conflicts {
		pointer := pointerForEmergencySSAField(conflict.field, allowed)
		if pointer == "" || (!emergencyOwnershipManagers[conflict.manager] && !legacyOwnershipManagers[conflict.manager]) {
			return fmt.Errorf("emergency ownership conflict %s:%s is outside the exact allowlist", conflict.manager, conflict.field)
		}
		if legacyOwnershipManagers[conflict.manager] && !emergencyProbePathPointer(pointer) {
			return fmt.Errorf("legacy ownership conflict %s:%s is outside the exact allowlist; only HTTP probe paths may transfer", conflict.manager, conflict.field)
		}
		key := conflict.manager + "\x00" + pointer
		if seen[key] {
			return errors.New("emergency ownership conflict is duplicated")
		}
		seen[key] = true
		matchedEntry := false
		for _, rawEntry := range anySlice(liveMetadata["managedFields"]) {
			entry, _ := rawEntry.(map[string]any)
			if stringValue(entry["manager"]) != conflict.manager || stringValue(entry["operation"]) != "Update" || stringValue(entry["subresource"]) != "" ||
				!managedFieldsEntryOwnsPointers(mapField(entry, "fieldsV1"), []string{pointer}, true) {
				continue
			}
			pointers, flattenErr := managedFieldsEntryPointers(mapField(entry, "fieldsV1"))
			if flattenErr != nil || len(pointers) == 0 || (emergencyOwnershipManagers[conflict.manager] && !stringSubset(pointers, allowed)) {
				return errors.New("emergency managedFields entry expands beyond the exact allowlist")
			}
			matchedEntry = true
		}
		if !matchedEntry {
			return errors.New("emergency ownership conflict lacks an exact Update managedFields witness")
		}
	}
	return nil
}

func nextLegacyProbeOwnershipPatch(live map[string]any, allowed []string, applyErr error) ([]map[string]any, bool, error) {
	conflicts, err := parseEmergencySSAConflicts(applyErr)
	if err != nil {
		return nil, false, err
	}
	metadata := mapField(live, "metadata")
	uid, rv := stringValue(metadata["uid"]), stringValue(metadata["resourceVersion"])
	if uid == "" || rv == "" {
		return nil, false, errors.New("legacy probe ownership cleanup lacks UID/RV")
	}
	hasLegacy, hasOther := false, false
	for _, conflict := range conflicts {
		if legacyOwnershipManagers[conflict.manager] {
			hasLegacy = true
		} else {
			hasOther = true
		}
	}
	if !hasLegacy {
		return nil, false, nil
	}
	if hasOther {
		return nil, false, errors.New("legacy probe ownership conflicts cannot be mixed with emergency ownership conflicts")
	}
	type removal struct {
		entryIndex int
		entry      map[string]any
		pointer    string
	}
	removals := make([]removal, 0, len(conflicts))
	for _, conflict := range conflicts {
		pointer := pointerForEmergencySSAField(conflict.field, allowed)
		if !emergencyProbePathPointer(pointer) {
			return nil, false, fmt.Errorf("legacy ownership conflict %s:%s is outside the exact HTTP probe path allowlist", conflict.manager, conflict.field)
		}
		foundIndex := -1
		var foundEntry map[string]any
		for index, rawEntry := range anySlice(metadata["managedFields"]) {
			entry, _ := rawEntry.(map[string]any)
			if stringValue(entry["manager"]) != conflict.manager || stringValue(entry["operation"]) != "Update" ||
				stringValue(entry["subresource"]) != "" || !managedFieldsEntryOwnsPointers(mapField(entry, "fieldsV1"), []string{pointer}, true) {
				continue
			}
			if foundIndex >= 0 {
				return nil, false, errors.New("legacy probe ownership witness is ambiguous")
			}
			foundIndex, foundEntry = index, entry
		}
		if foundIndex < 0 {
			return nil, false, errors.New("legacy probe ownership conflict lacks an exact Update managedFields witness")
		}
		removals = append(removals, removal{entryIndex: foundIndex, entry: foundEntry, pointer: pointer})
	}
	if len(removals) == 0 {
		return nil, false, nil
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": uid},
		{"op": "test", "path": "/metadata/resourceVersion", "value": rv},
	}
	testedEntries := make(map[int]bool)
	for _, item := range removals {
		if testedEntries[item.entryIndex] {
			continue
		}
		testedEntries[item.entryIndex] = true
		patch = append(patch, map[string]any{
			"op": "test", "path": "/metadata/managedFields/" + strconv.Itoa(item.entryIndex), "value": item.entry,
		})
	}
	sort.Slice(removals, func(i, j int) bool {
		if removals[i].entryIndex != removals[j].entryIndex {
			return removals[i].entryIndex < removals[j].entryIndex
		}
		return removals[i].pointer < removals[j].pointer
	})
	for _, item := range removals {
		path, pathErr := managedFieldsJSONPatchPath(item.entryIndex, item.pointer)
		if pathErr != nil {
			return nil, false, pathErr
		}
		patch = append(patch, map[string]any{"op": "remove", "path": path})
	}
	return patch, true, nil
}

func emergencyProbePathPointer(pointer string) bool {
	_, _, tail, ok := emergencyContainerPointerParts(pointer)
	if !ok {
		return false
	}
	_, ok = probePathTail(tail)
	return ok
}

func managedFieldsJSONPatchPath(entryIndex int, pointer string) (string, error) {
	if entryIndex < 0 || !strings.HasPrefix(pointer, "/") {
		return "", errors.New("managedFields patch path identity is invalid")
	}
	path := "/metadata/managedFields/" + strconv.Itoa(entryIndex) + "/fieldsV1"
	for _, encoded := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		token := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
		field, selector := token, ""
		if open := strings.Index(token, "[name="); open > 0 && strings.HasSuffix(token, "]") {
			field, selector = token[:open], token[open+len("[name="):len(token)-1]
		}
		if field == "" {
			return "", errors.New("managedFields patch path contains an empty field")
		}
		path += "/" + escapeJSONPointerToken("f:"+field)
		if selector != "" {
			encodedSelector, err := json.Marshal(selector)
			if err != nil {
				return "", err
			}
			path += "/" + escapeJSONPointerToken("k:{\"name\":"+string(encodedSelector)+"}")
		}
	}
	return path, nil
}

func (cluster *kubectlCluster) cleanupEmergencyOwnership(ctx context.Context, release declarativerelease.PlanRelease, identity declarativerelease.ResourceIdentity, desired map[string]any, requireDeclarativeOwner bool) error {
	allowed := emergencyOwnershipPointers(release, identity, desired)
	if len(allowed) == 0 {
		return nil
	}
	for attempts := 0; attempts < 4; attempts++ {
		liveRaw, err := cluster.getResource(ctx, identity)
		if err != nil || resourceAbsent(liveRaw) {
			return fmt.Errorf("read post-apply ownership: %w", err)
		}
		live, err := decodeJSONObject(liveRaw)
		if err != nil {
			return err
		}
		patch, found, err := nextEmergencyOwnershipPatch(live, release.Workload.FieldManager, allowed, requireDeclarativeOwner)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		encoded, err := declarativerelease.CanonicalJSON(patch)
		if err != nil {
			return err
		}
		if _, err := cluster.kubectlRun(ctx, nil, "patch", strings.ToLower(identity.Kind), identity.Name,
			"--namespace", identity.Namespace, "--type=json", "--patch", string(encoded), "--output", "json"); err != nil {
			return fmt.Errorf("remove exact emergency managedFields entry: %w", err)
		}
	}
	return errors.New("emergency ownership cleanup exceeded bounded entry count")
}

func nextEmergencyOwnershipPatch(live map[string]any, declarativeManager string, allowed []string, requireDeclarativeOwner bool) ([]map[string]any, bool, error) {
	metadata := mapField(live, "metadata")
	uid, rv := stringValue(metadata["uid"]), stringValue(metadata["resourceVersion"])
	if uid == "" || rv == "" {
		return nil, false, errors.New("emergency ownership cleanup lacks UID/RV")
	}
	entries := anySlice(metadata["managedFields"])
	for index, rawEntry := range entries {
		entry, _ := rawEntry.(map[string]any)
		manager := stringValue(entry["manager"])
		if !emergencyOwnershipManagers[manager] || stringValue(entry["operation"]) != "Update" || stringValue(entry["subresource"]) != "" {
			continue
		}
		pointers, err := managedFieldsEntryPointers(mapField(entry, "fieldsV1"))
		if err != nil || len(pointers) == 0 || !stringsOverlap(pointers, allowed) {
			continue
		}
		if !stringSubset(pointers, allowed) {
			return nil, false, errors.New("emergency managedFields cleanup would remove unreviewed ownership")
		}
		if requireDeclarativeOwner && !managedFieldsOwnPointers(metadata, declarativeManager, pointers) {
			return nil, false, errors.New("emergency ownership cleanup lacks declarative co-ownership")
		}
		path := "/metadata/managedFields/" + strconv.Itoa(index)
		return []map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": uid},
			{"op": "test", "path": "/metadata/resourceVersion", "value": rv},
			{"op": "test", "path": path, "value": entry},
			{"op": "remove", "path": path},
		}, true, nil
	}
	return nil, false, nil
}

func parseEmergencySSAConflicts(applyErr error) ([]emergencySSAConflict, error) {
	if applyErr == nil {
		return nil, errors.New("emergency ownership conflict evidence is absent")
	}
	raw := applyErr.Error()
	countMatch := emergencyConflictCountPattern.FindStringSubmatch(raw)
	conflicts := make([]emergencySSAConflict, 0)
	groupManager := ""
	for _, line := range strings.Split(raw, "\n") {
		if match := emergencyConflictPattern.FindStringSubmatch(line); len(match) == 3 {
			conflicts = append(conflicts, emergencySSAConflict{manager: match[1], field: match[2]})
			groupManager = ""
			continue
		}
		if match := emergencyConflictGroupPattern.FindStringSubmatch(line); len(match) == 2 {
			groupManager = match[1]
			continue
		}
		trimmed := strings.TrimSpace(line)
		if groupManager != "" && strings.HasPrefix(trimmed, "- .") {
			conflicts = append(conflicts, emergencySSAConflict{manager: groupManager, field: strings.TrimPrefix(trimmed, "- ")})
			continue
		}
		if groupManager != "" && trimmed != "" {
			groupManager = ""
		}
	}
	if len(countMatch) != 2 || len(conflicts) == 0 {
		return nil, errors.New("emergency ownership failure is not a typed SSA conflict")
	}
	count, err := strconv.Atoi(countMatch[1])
	if err != nil || count != len(conflicts) {
		return nil, errors.New("emergency ownership conflict count is inconsistent")
	}
	return conflicts, nil
}

func pointerForEmergencySSAField(field string, allowed []string) string {
	for _, pointer := range allowed {
		if ssaFieldForPointer(pointer) == field {
			return pointer
		}
	}
	return ""
}

func ssaFieldForPointer(pointer string) string {
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	for index, part := range parts {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		if open := strings.Index(part, "[name="); open > 0 && strings.HasSuffix(part, "]") {
			name := part[open+len("[name=") : len(part)-1]
			part = part[:open] + `[name="` + name + `"]`
		}
		parts[index] = part
	}
	return "." + strings.Join(parts, ".")
}

func managedFieldsEntryPointers(fields map[string]any) ([]string, error) {
	result := make([]string, 0)
	var walk func(map[string]any, []string) error
	walk = func(node map[string]any, path []string) error {
		keys := make([]string, 0, len(node))
		for key := range node {
			if key != "." {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		if len(keys) == 0 {
			if len(path) == 0 {
				return errors.New("managedFields entry has an empty root")
			}
			result = append(result, "/"+strings.Join(path, "/"))
			return nil
		}
		for _, key := range keys {
			child, ok := node[key].(map[string]any)
			if !ok {
				return errors.New("managedFields entry is not FieldsV1")
			}
			switch {
			case strings.HasPrefix(key, "f:"):
				next := append(append([]string(nil), path...), escapeJSONPointerToken(strings.TrimPrefix(key, "f:")))
				if err := walk(child, next); err != nil {
					return err
				}
			case strings.HasPrefix(key, "k:") && len(path) > 0:
				decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(key, "k:")))
				decoder.UseNumber()
				var selector map[string]any
				if err := decoder.Decode(&selector); err != nil || len(selector) == 0 {
					return errors.New("managedFields associative selector is invalid")
				}
				next := append([]string(nil), path...)
				if name, ok := selector["name"].(string); ok && len(selector) == 1 && name != "" {
					next[len(next)-1] += "[name=" + name + "]"
				} else {
					canonical, err := declarativerelease.CanonicalJSON(selector)
					if err != nil {
						return errors.New("managedFields associative selector is invalid")
					}
					next[len(next)-1] += "[selector=" + strings.TrimPrefix(digestBytesLocal(canonical), "sha256:") + "]"
				}
				if err := walk(child, next); err != nil {
					return err
				}
			default:
				return errors.New("managedFields entry contains an unsupported field key")
			}
		}
		return nil
	}
	if err := walk(fields, nil); err != nil {
		return nil, err
	}
	sort.Strings(result)
	return result, nil
}

func managedFieldsOwnPointers(metadata map[string]any, manager string, pointers []string) bool {
	for _, rawEntry := range anySlice(metadata["managedFields"]) {
		entry, _ := rawEntry.(map[string]any)
		if stringValue(entry["manager"]) == manager && stringValue(entry["operation"]) == "Apply" && stringValue(entry["subresource"]) == "" &&
			managedFieldsEntryOwnsPointers(mapField(entry, "fieldsV1"), pointers, true) {
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
			field, selector := token, ""
			if open := strings.Index(token, "[name="); open > 0 && strings.HasSuffix(token, "]") {
				field, selector = token[:open], token[open+len("[name="):len(token)-1]
			}
			next, ok := current["f:"+field].(map[string]any)
			if !ok {
				owned = false
				break
			}
			current = next
			if selector != "" {
				encodedSelector, _ := json.Marshal(selector)
				next, ok = current[`k:{"name":`+string(encodedSelector)+`}`].(map[string]any)
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
	return matched > 0 && (!requireAll || matched == len(pointers))
}

func stringSubset(values, allowed []string) bool {
	set := make(map[string]bool, len(allowed))
	for _, value := range allowed {
		set[value] = true
	}
	for _, value := range values {
		if !set[value] {
			return false
		}
	}
	return true
}

func stringsOverlap(left, right []string) bool {
	set := make(map[string]bool, len(right))
	for _, value := range right {
		set[value] = true
	}
	for _, value := range left {
		if set[value] {
			return true
		}
	}
	return false
}

func escapeJSONPointerToken(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func (cluster *kubectlCluster) WaitHealthy(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) (declarativerelease.Observation, error) {
	if !target.Present {
		return declarativerelease.Observation{}, errors.New("cannot wait healthy for an absent target")
	}
	soak := healthSoakDuration(release)
	deadline := time.Now().Add(cluster.timeout + soak)
	var lastErr error
	var lastFailure error
	tracker := healthSoakTracker{required: soak}
	for {
		observation, err := cluster.observeExpected(ctx, release, target.OCIRevision, manifest)
		if err == nil && observation.Matches(target, release, false) {
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
		if typed := typedPrewritePredecessorHealth(ctx, release, target, err); typed != nil {
			return observation, typed
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
			return observation, waitHealthyTerminalError(ctx.Err(), lastFailure)
		case <-time.After(2 * time.Second):
		}
	}
}

// CheckHealthyOnce performs one bounded controller reconciliation. It is used
// by the asynchronous monitor, where repeated scheduled observations provide
// the failure threshold; it deliberately does not repeat the synchronous
// rollout soak or wait loop. After exact target, probe, and manifest
// convergence are proven, it removes only reviewed emergency Update ownership
// by UID/RV/entry CAS without changing resource values.
func (cluster *kubectlCluster) CheckHealthyOnce(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) (declarativerelease.Observation, error) {
	observation, err := cluster.observeExpected(ctx, release, target.OCIRevision, manifest)
	if err != nil {
		return observation, err
	}
	if !observation.Matches(target, release, false) {
		return observation, errors.New("live workload does not match the monitored immutable target")
	}
	probeDigest, err := cluster.verifyProbes(ctx, release, target, manifest, observation)
	if err != nil {
		return observation, err
	}
	observation.HealthDigest = digestJoin(observation.HealthDigest, probeDigest)
	if err := cluster.MonitorConverged(ctx, release, manifest); err != nil {
		return observation, err
	}
	boundManifest, err := declarativerelease.BindManifestCAS(manifest, observation)
	if err != nil {
		return observation, err
	}
	if err := cluster.convergeMonitoredEmergencyOwnership(ctx, release, boundManifest); err != nil {
		return observation, err
	}
	if err := cluster.VerifyOwnershipConverged(ctx, release, manifest); err != nil {
		return observation, err
	}
	return observation, nil
}

func (cluster *kubectlCluster) convergeMonitoredEmergencyOwnership(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return err
	}
	for _, identity := range identities {
		desired, desiredErr := declarativerelease.ResourceSetItem(manifest, identity)
		if desiredErr != nil {
			return desiredErr
		}
		if len(emergencyOwnershipPointers(release, identity, desired)) == 0 {
			continue
		}
		liveRaw, getErr := cluster.getResource(ctx, identity)
		if getErr != nil || resourceAbsent(liveRaw) {
			return fmt.Errorf("read monitored emergency ownership for %s/%s: %w", identity.Kind, identity.Name, getErr)
		}
		live, decodeErr := decodeJSONObject(liveRaw)
		if decodeErr != nil {
			return decodeErr
		}
		_, found, ownershipErr := nextEmergencyOwnershipPatch(live, release.Workload.FieldManager, emergencyOwnershipPointers(release, identity, desired), false)
		if ownershipErr != nil {
			return fmt.Errorf("review monitored emergency ownership for %s/%s: %w", identity.Kind, identity.Name, ownershipErr)
		}
		if !found {
			continue
		}
		encoded, encodeErr := declarativerelease.CanonicalJSON(desired)
		if encodeErr != nil {
			return encodeErr
		}
		if applyErr := cluster.applyResourceWithOwnershipConvergence(ctx, release, identity, desired, encoded, false); applyErr != nil {
			return fmt.Errorf("converge monitored emergency ownership for %s/%s: %w", identity.Kind, identity.Name, applyErr)
		}
	}
	return nil
}

func (cluster *kubectlCluster) MonitorConverged(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
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
		if resourceAbsent(liveRaw) {
			return fmt.Errorf("declared resource %s/%s is absent", identity.Kind, identity.Name)
		}
		live, decodeErr := decodeJSONObject(liveRaw)
		if decodeErr != nil {
			return decodeErr
		}
		stripMonitorReleaseEvidence(desired)
		stripMonitorReleaseEvidence(live)
		if !declarativerelease.ResourceDesiredSubset(desired, live) {
			return fmt.Errorf("declared resource %s/%s has not converged", identity.Kind, identity.Name)
		}
	}
	return nil
}

func stripMonitorReleaseEvidence(resource map[string]any) {
	metadata := mapField(resource, "metadata")
	annotations := mapField(metadata, "annotations")
	for _, key := range []string{
		"fugue.pro/artifact-receipt-digest",
		"fugue.pro/production-config-sha",
		"fugue.pro/release-plan-digest",
	} {
		delete(annotations, key)
	}
}

func waitHealthyTerminalError(contextErr, lastFailure error) error {
	if lastFailure == nil {
		return contextErr
	}
	return fmt.Errorf("%v; last health observation: %w", contextErr, lastFailure)
}

func shouldReturnTypedPrewritePredecessorHealth(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, err error) bool {
	return typedPrewritePredecessorHealth(ctx, release, target, err) != nil
}

func typedPrewritePredecessorHealth(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, err error) error {
	exactPredecessor := release.ExpectedPreviousPresent && target.Present &&
		target.ImageRef == release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest &&
		target.ConfigSHA == release.ExpectedPreviousConfigSHA && target.ManifestSHA == release.ExpectedPreviousManifestSHA &&
		target.OCIRevision == release.ExpectedPreviousOCIRevision
	if !declarativerelease.IsPrewritePredecessorHealthWait(ctx) || !exactPredecessor || err == nil {
		return nil
	}
	if errors.Is(err, declarativerelease.ErrDegradedPredecessorHealth) {
		return err
	}
	if errors.Is(err, errWorkloadOriginatedServiceHealth) {
		return fmt.Errorf("%w: %v", declarativerelease.ErrDegradedPredecessorHealth, err)
	}
	if errors.Is(err, errServiceHTTPHealth) {
		return fmt.Errorf("%w: %v", declarativerelease.ErrDegradedPredecessorHealth, err)
	}
	if errors.Is(err, errPublicRouteHTTPHealth) {
		return fmt.Errorf("%w: %v", declarativerelease.ErrDegradedPredecessorHealth, err)
	}
	if errors.Is(err, errEdgeGroupAuthorityHealth) {
		return fmt.Errorf("%w: %v", declarativerelease.ErrDegradedPredecessorHealth, err)
	}
	return nil
}

func healthSoakDuration(release declarativerelease.PlanRelease) time.Duration {
	if release.Transition == nil || release.Transition.EdgeGroupAB == nil {
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

func (cluster *kubectlCluster) VerifyOwnershipConverged(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	identities, err := declarativerelease.ResourceSetIdentities(manifest)
	if err != nil {
		return err
	}
	for _, identity := range identities {
		desired, err := declarativerelease.ResourceSetItem(manifest, identity)
		if err != nil {
			return err
		}
		if len(emergencyOwnershipPointers(release, identity, desired)) == 0 {
			continue
		}
		liveRaw, err := cluster.getResource(ctx, identity)
		if err != nil || resourceAbsent(liveRaw) {
			return fmt.Errorf("read declared ownership %s/%s: %w", identity.Kind, identity.Name, err)
		}
		live, err := decodeJSONObject(liveRaw)
		if err != nil {
			return err
		}
		if err := verifyNoEmergencyOwnership(release, identity, desired, live); err != nil {
			return err
		}
	}
	return nil
}

func verifyNoEmergencyOwnership(release declarativerelease.PlanRelease, identity declarativerelease.ResourceIdentity, desired, live map[string]any) error {
	allowed := emergencyOwnershipPointers(release, identity, desired)
	if len(allowed) == 0 {
		return nil
	}
	metadata := mapField(live, "metadata")
	for _, rawEntry := range anySlice(metadata["managedFields"]) {
		entry, _ := rawEntry.(map[string]any)
		if !emergencyOwnershipManagers[stringValue(entry["manager"])] || stringValue(entry["subresource"]) != "" {
			continue
		}
		pointers, err := managedFieldsEntryPointers(mapField(entry, "fieldsV1"))
		if err != nil {
			return err
		}
		if stringsOverlap(pointers, allowed) {
			return fmt.Errorf("declared resource %s/%s retains emergency field ownership", identity.Kind, identity.Name)
		}
	}
	if !managedFieldsOwnPointers(metadata, release.Workload.FieldManager, allowed) {
		return fmt.Errorf("declared resource %s/%s lacks declarative ownership of the exact runtime allowlist", identity.Kind, identity.Name)
	}
	return nil
}

func (cluster *kubectlCluster) observeExpected(ctx context.Context, release declarativerelease.PlanRelease, expectedOCI string, manifest []byte) (declarativerelease.Observation, error) {
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
	partial, err := parseObservation(workloadRaw, podsRaw, release)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	verificationImage := partial.ImageRef
	verificationRevision := expectedOCI
	supersededFailedAtom := partial.MatchesSupersededFailedAtom(release)
	if supersededFailedAtom {
		// A failed atom may have reached the workload before Guardian could
		// settle it as current. Verify that exact immutable image by its own
		// reviewed revision so PrepareExecution can route it through the
		// existing owned-degraded CAS recovery path.
		verificationRevision = partial.OCIRevision
	}
	arguments := []string{"python3", cluster.verifier, "--image", verificationImage, "--platform", "linux/amd64", "--expected-revision", verificationRevision}
	arguments = append(arguments, "--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5", "--max-attempts", "2", "--retry-delay-seconds", "0.1")
	verificationRaw, err := cluster.run(ctx, nil, arguments[0], arguments[1:]...)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify live image provenance: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if verification.Image != verificationImage || verification.OCIRevision != verificationRevision {
		return declarativerelease.Observation{}, errors.New("live registry identity mismatch")
	}
	var runtimeImageErr error
	if supersededFailedAtom {
		runtimeImageErr = verifyObservedArtifactImageIDs(podsRaw, workloadRaw, release, verification.Image, verification.ManifestDigest)
	} else {
		runtimeImageErr = verifyDeclaredArtifactImageIDs(podsRaw, manifest, release, verification.Image, verification.ManifestDigest)
	}
	if runtimeImageErr != nil {
		return declarativerelease.Observation{}, runtimeImageErr
	}
	if !supersededFailedAtom {
		partial.OCIRevision = expectedOCI
	}
	resources, err := cluster.observeResources(ctx, manifest, release, workloadRaw)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	partial.Resources = resources
	return partial, nil
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
			body, err := cluster.readServiceHTTP(ctx, release.Workload.Namespace, probe)
			if err != nil || (probe.Expected != "" && !bytes.Contains(body, []byte(probe.Expected))) {
				if err == nil {
					err = errors.New("response does not contain the expected marker")
				}
				return "", fmt.Errorf("service health probe %q failed: %w: %v", probe.Name, errServiceHTTPHealth, err)
			}
			evidence = append(evidence, probe.Type+":"+digestBytesLocal(body))
		case "service-http-via-workload":
			bodies, err := cluster.readServiceHTTPViaWorkload(ctx, release.Workload.Namespace, probe)
			if err != nil {
				return "", fmt.Errorf("workload-originated service health probe %q failed: %w", probe.Name, err)
			}
			evidence = append(evidence, probe.Type+":"+digestBytesLocal(bodies))
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
				if err != nil || (probe.Expected != "" && !bytes.Contains(body, []byte(probe.Expected))) {
					return "", fmt.Errorf("pod health probe %q failed", pod.Name)
				}
				evidence = append(evidence, probe.Type+":"+pod.Name+":"+digestBytesLocal(body))
			}
		case "public-route-http":
			body, err := readPublicRouteCanary(ctx, probe)
			if err != nil {
				return "", fmt.Errorf("public route health probe %q failed: %w: %v", probe.Name, errPublicRouteHTTPHealth, err)
			}
			evidence = append(evidence, probe.Type+":"+probe.Name+":"+digestBytesLocal(body))
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
				return "", fmt.Errorf("%w: %v", errEdgeGroupAuthorityHealth, err)
			}
			if err := validateEdgeGroupAuthority(state, transition); err != nil {
				return "", fmt.Errorf("%w: %v", errEdgeGroupAuthorityHealth, err)
			}
			items := []string{"group=" + transition.GroupID, "active_slot=" + state.ActiveSlot}
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

func (cluster *kubectlCluster) readServiceHTTP(ctx context.Context, namespace string, probe declarativerelease.HealthProbe) ([]byte, error) {
	service := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: probe.Name}
	serviceRaw, err := cluster.getResource(ctx, service)
	if err != nil || resourceAbsent(serviceRaw) {
		return nil, errors.New("target Service is absent")
	}
	port, err := servicePortByName(serviceRaw, probe.Port)
	if err != nil {
		return nil, err
	}
	address := cluster.serviceHTTPDataPlaneURL(namespace, probe.Name, port)
	if address == "" {
		resource := fmt.Sprintf("/api/v1/namespaces/%s/services/%s:%s/proxy%s", namespace, probe.Name, probe.Port, probe.Path)
		return cluster.kubectlRun(ctx, nil, "get", "--raw", resource)
	}
	return readServiceHTTPDataPlane(ctx, address, probe.Path)
}

func (cluster *kubectlCluster) serviceHTTPDataPlaneURL(namespace, name string, port int) string {
	if cluster != nil && cluster.serviceHTTPURL != nil {
		return cluster.serviceHTTPURL(namespace, name, port)
	}
	if net.ParseIP(strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))) == nil {
		return ""
	}
	return "http://" + net.JoinHostPort(name+"."+namespace+".svc", strconv.Itoa(port))
}

func readServiceHTTPDataPlane(ctx context.Context, address, path string) ([]byte, error) {
	if !strings.HasPrefix(address, "http://") || !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#\r\n\x00") {
		return nil, errors.New("service HTTP endpoint is invalid")
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+path, nil)
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
		return nil, errors.New("service HTTP response is invalid")
	}
	return body, nil
}

func (cluster *kubectlCluster) readServiceHTTPViaWorkload(ctx context.Context, namespace string, probe declarativerelease.HealthProbe) ([]byte, error) {
	source := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: probe.SourceWorkload}
	sourceRaw, err := cluster.getResource(ctx, source)
	if err != nil || resourceAbsent(sourceRaw) {
		return nil, errors.New("source workload is absent")
	}
	selector, err := selectorFromWorkload(sourceRaw)
	if err != nil {
		return nil, err
	}
	podsRaw, err := cluster.kubectlRun(ctx, nil, "get", "pods", "--namespace", namespace, "--selector", selector, "--output", "json")
	if err != nil {
		return nil, err
	}
	service := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: probe.Name}
	serviceRaw, err := cluster.getResource(ctx, service)
	if err != nil || resourceAbsent(serviceRaw) {
		return nil, errors.New("target Service is absent")
	}
	port, err := servicePortByName(serviceRaw, probe.Port)
	if err != nil {
		return nil, err
	}
	pods, err := readyWorkloadPods(podsRaw, probe.SourceContainer)
	if err != nil {
		return nil, err
	}
	results := make([]string, 0, len(pods))
	url := "http://" + probe.Name + ":" + strconv.Itoa(port) + probe.Path
	for _, pod := range pods {
		body, runErr := cluster.kubectlRun(ctx, nil, "exec", "--namespace", namespace, pod, "--container", probe.SourceContainer, "--",
			"wget", "-qO-", "-T", "5", url)
		if runErr != nil || (probe.Expected != "" && !bytes.Contains(body, []byte(probe.Expected))) {
			return nil, fmt.Errorf("%w: source Pod %s did not observe the expected service response", errWorkloadOriginatedServiceHealth, pod)
		}
		results = append(results, pod+":"+digestBytesLocal(body))
	}
	return []byte(strings.Join(results, "\n")), nil
}

var errWorkloadOriginatedServiceHealth = errors.New("workload-originated service health is degraded")

// errServiceHTTPHealth marks an ordinary Service probe failure. It is only
// promoted to ErrDegradedPredecessorHealth by typedPrewritePredecessorHealth
// when the target is the exact immutable LKG in the bounded prewrite context.
// Forward targets and ordinary successors therefore remain fail-closed.
var errServiceHTTPHealth = errors.New("service-http health is degraded")

// errPublicRouteHTTPHealth marks a failure of an existing public route canary.
// It is promoted only for the exact immutable predecessor during the bounded
// prewrite repair path, so a broken route can be repaired without weakening
// forward or post-deploy route verification.
var errPublicRouteHTTPHealth = errors.New("public-route-http health is degraded")

// errEdgeGroupAuthorityHealth marks only a failure of the declared group
// publication/inventory health contract. It is promoted to a degraded LKG
// witness exclusively by typedPrewritePredecessorHealth, which additionally
// proves the exact predecessor artifact and bounded prewrite context. Forward
// health and identity failures remain ordinary fail-closed errors.
var errEdgeGroupAuthorityHealth = errors.New("edge-group authority health is degraded")

func servicePortByName(raw []byte, name string) (int, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return 0, err
	}
	port := 0
	for _, rawPort := range anySlice(mapField(value, "spec")["ports"]) {
		candidate, _ := rawPort.(map[string]any)
		if stringValue(candidate["name"]) != name {
			continue
		}
		if port != 0 {
			return 0, errors.New("target Service port is ambiguous")
		}
		port = int(int64Value(candidate["port"]))
	}
	if port < 1 || port > 65535 {
		return 0, errors.New("target Service port is invalid")
	}
	return port, nil
}

func readyWorkloadPods(raw []byte, container string) ([]string, error) {
	value, err := decodeJSONObject(raw)
	if err != nil {
		return nil, err
	}
	pods := make([]string, 0)
	for _, rawItem := range anySlice(value["items"]) {
		pod, _ := rawItem.(map[string]any)
		metadata, status := mapField(pod, "metadata"), mapField(pod, "status")
		if metadata["deletionTimestamp"] != nil || !podReady(status) {
			continue
		}
		containerPresent := false
		for _, rawContainer := range anySlice(mapField(pod, "spec")["containers"]) {
			candidate, _ := rawContainer.(map[string]any)
			containerPresent = containerPresent || stringValue(candidate["name"]) == container
		}
		name := stringValue(metadata["name"])
		if name == "" || !containerPresent {
			return nil, errors.New("ready source Pod identity is invalid")
		}
		pods = append(pods, name)
	}
	sort.Strings(pods)
	if len(pods) == 0 {
		return nil, errors.New("source workload has no ready Pods")
	}
	return pods, nil
}

func (cluster *kubectlCluster) verifyAuxiliaryWorkload(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte, kind, name string) (declarativerelease.Observation, error) {
	_ = target
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
	desiredTarget, err := targetIdentityFromDeclaredWorkload(desired, workload)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	auxiliary, err := cluster.observeExpected(ctx, auxiliaryRelease, desiredTarget.OCIRevision, manifest)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("observe health workload %s/%s: %w", kind, name, err)
	}
	if !auxiliary.Matches(desiredTarget, auxiliaryRelease, false) {
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

func verifyDeclaredArtifactImageIDs(podsRaw, manifest []byte, release declarativerelease.PlanRelease, verifiedImage, platformManifestDigest string) error {
	if !strings.Contains(verifiedImage, "@sha256:") || !strings.HasPrefix(platformManifestDigest, "sha256:") || len(platformManifestDigest) != len("sha256:")+64 {
		return errors.New("verified artifact runtime identity is invalid")
	}
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
		if workload != verifiedImage {
			return fmt.Errorf("declared workload container %s is not bound to the verified artifact", target.Container)
		}
		expected[target.ContainerType+"\x00"+target.Container] = workload
	}
	if len(expected) == 0 {
		image, imageErr := declaredContainerImage(desired, release.Workload.Container, "container")
		if imageErr != nil {
			return imageErr
		}
		if image != verifiedImage {
			return errors.New("declared workload primary image is not bound to the verified artifact")
		}
		expected["container\x00"+release.Workload.Container] = image
	}
	return verifyArtifactPodImageIDs(podsRaw, expected, verifiedImage, platformManifestDigest)
}

func verifyObservedArtifactImageIDs(podsRaw, workloadRaw []byte, release declarativerelease.PlanRelease, verifiedImage, platformManifestDigest string) error {
	if !strings.Contains(verifiedImage, "@sha256:") || !strings.HasPrefix(platformManifestDigest, "sha256:") || len(platformManifestDigest) != len("sha256:")+64 {
		return errors.New("verified artifact runtime identity is invalid")
	}
	workload, err := decodeJSONObject(workloadRaw)
	if err != nil {
		return err
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	expected := make(map[string]string)
	for _, target := range release.ArtifactTargets {
		if target.APIVersion != identity.APIVersion || target.Kind != identity.Kind || target.Namespace != identity.Namespace || target.Name != identity.Name {
			continue
		}
		image, found, imageErr := declaredContainerImageOptional(workload, target.Container, target.ContainerType)
		if imageErr != nil {
			return imageErr
		}
		if !found {
			continue
		}
		if image != verifiedImage {
			return fmt.Errorf("observed workload container %s is not bound to the verified failed atom", target.Container)
		}
		expected[target.ContainerType+"\x00"+target.Container] = image
	}
	if len(expected) == 0 {
		image, imageErr := declaredContainerImage(workload, release.Workload.Container, "container")
		if imageErr != nil {
			return imageErr
		}
		if image != verifiedImage {
			return errors.New("observed workload primary image is not bound to the verified failed atom")
		}
		expected["container\x00"+release.Workload.Container] = image
	}
	return verifyArtifactPodImageIDs(podsRaw, expected, verifiedImage, platformManifestDigest)
}

func verifyArtifactPodImageIDs(podsRaw []byte, expected map[string]string, verifiedImage, platformManifestDigest string) error {
	topDigest := verifiedImage[strings.LastIndex(verifiedImage, "@")+1:]
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
		for key := range expected {
			parts := strings.SplitN(key, "\x00", 2)
			field := "containerStatuses"
			if parts[0] == "init-container" {
				field = "initContainerStatuses"
			}
			statuses, ok := status[field].([]any)
			if !ok {
				return fmt.Errorf("Pod %s are absent", field)
			}
			matched := false
			for _, rawStatus := range statuses {
				containerStatus, ok := rawStatus.(map[string]any)
				if !ok || stringValue(containerStatus["name"]) != parts[1] {
					continue
				}
				imageID := stringValue(containerStatus["imageID"])
				if !imageIDMatchesAnyDigest(imageID, topDigest, platformManifestDigest) {
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

func imageIDMatchesAnyDigest(imageID string, digests ...string) bool {
	for _, digest := range digests {
		if imageID == digest || strings.HasSuffix(imageID, "@"+digest) || strings.HasSuffix(imageID, "://"+digest) {
			return true
		}
	}
	return false
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

func readPublicRouteCanary(ctx context.Context, probe declarativerelease.HealthProbe) ([]byte, error) {
	return readPublicRouteCanaryWithRoots(ctx, probe, nil)
}

func readPublicRouteCanaryWithRoots(ctx context.Context, probe declarativerelease.HealthProbe, roots *x509.CertPool) ([]byte, error) {
	address, err := netip.ParseAddrPort(probe.Address)
	if err != nil || address.Port() == 0 || probe.Host == "" || !strings.HasPrefix(probe.Path, "/") ||
		strings.ContainsAny(probe.Path, "?#\r\n\x00") || probe.Expected == "" {
		return nil, errors.New("public route canary is invalid")
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: probe.Host,
			RootCAs:    roots,
		},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			if network != "tcp" && network != "tcp4" && network != "tcp6" {
				return nil, errors.New("public route canary network is invalid")
			}
			return dialer.DialContext(ctx, "tcp", probe.Address)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   8 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("public route canary redirect is forbidden")
		},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+probe.Host+probe.Path, nil)
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
	if response.StatusCode != http.StatusOK || len(body) > 1<<20 || !bytes.Contains(body, []byte(probe.Expected)) {
		return nil, errors.New("public route canary response is invalid")
	}
	return body, nil
}

func (cluster *kubectlCluster) kubectlRun(ctx context.Context, input []byte, arguments ...string) ([]byte, error) {
	if len(arguments) == 0 || arguments[0] != "get" {
		return cluster.run(ctx, input, cluster.kubectl, arguments...)
	}
	timeout := cluster.readTimeout
	if timeout <= 0 {
		timeout = defaultKubectlReadTimeout
	}
	attempts := cluster.readAttempts
	if attempts <= 0 {
		attempts = defaultKubectlReadAttempts
	}
	delay := cluster.readRetryDelay
	if delay <= 0 {
		delay = defaultKubectlReadRetryDelay
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		readCtx, cancel := context.WithTimeout(ctx, timeout)
		output, err := cluster.run(readCtx, input, cluster.kubectl, arguments...)
		cancel()
		if err == nil {
			return output, nil
		}
		lastErr = err
		if ctx.Err() != nil || attempt+1 == attempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("read-only kubectl get failed after %d attempts: %w", attempts, lastErr)
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

func parseObservation(workloadRaw, podsRaw []byte, release declarativerelease.PlanRelease) (declarativerelease.Observation, error) {
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
	if image == "" || !strings.Contains(image, "@sha256:") {
		return declarativerelease.Observation{}, errors.New("workload image is not immutable")
	}
	manifestSHA := templateAnnotations["fugue.pro/source-commit"]
	workloadAnnotations := mapStringField(metadata, "annotations")
	configSHA := workloadAnnotations["fugue.pro/production-config-sha"]
	observation := declarativerelease.Observation{
		Present: true,
		Primary: declarativerelease.ResourceIdentity{
			APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
			Namespace: release.Workload.Namespace, Name: release.Workload.Name,
		},
		UID: stringValue(metadata["uid"]), ResourceVersion: stringValue(metadata["resourceVersion"]),
		Generation: int64Value(metadata["generation"]), ObservedGeneration: int64Value(status["observedGeneration"]),
		ImageRef: image, ConfigSHA: configSHA, ManifestSHA: manifestSHA,
		OCIRevision:    templateAnnotations["fugue.pro/oci-revision"],
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
		imageID, healthDigest, err = parseReadyPods(podsRaw, release, observation.Desired-int32(release.Workload.PreservedUnavailable), manifestSHA)
	}
	if err != nil {
		return declarativerelease.Observation{}, err
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

func parseReadyPods(raw []byte, release declarativerelease.PlanRelease, desired int32, manifestSHA string) (string, string, error) {
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
			if restartCount != 0 {
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
