package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	"k8s.io/client-go/tools/clientcmd"
)

const maxKubernetesOutputBytes = 4 << 20

type kubectlCluster struct {
	kubectl  string
	verifier string
	timeout  time.Duration
}

type healthSoakTracker struct {
	required time.Duration
	since    time.Time
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
	return &kubectlCluster{kubectl: kubectl, verifier: verifier, timeout: 120 * time.Second}, nil
}

func (cluster *kubectlCluster) Observe(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) (declarativerelease.Observation, error) {
	return cluster.observeExpected(ctx, release, target.OCIRevision, manifest, allowsHistoricalRestarts(release, target))
}

func allowsHistoricalRestarts(release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity) bool {
	return release.ExpectedPreviousPresent && target.Present &&
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

func (cluster *kubectlCluster) DryRunApply(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	return cluster.applyResourceSet(ctx, release, manifest, true)
}

func (cluster *kubectlCluster) Apply(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) error {
	if release.Transition != nil && release.Transition.Type == "edge-group-ab" {
		return cluster.applyEdgeGroupAB(ctx, release, target, manifest)
	}
	return cluster.applyResourceSet(ctx, release, manifest, false)
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
	config, err := clientcmd.BuildConfigFromFlags("", "")
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
		uid := types.UID(resource.UID)
		rv := resource.ResourceVersion
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
	identities, err := declarativerelease.ResourceSetIdentities(forwardManifest)
	if err != nil {
		return err
	}
	deletions, err := createdResourceDeletions(identities, before, after)
	if err != nil {
		return err
	}
	config, err := clientcmd.BuildConfigFromFlags("", "")
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
		uid := types.UID(current.UID)
		rv := current.ResourceVersion
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
		config, err := clientcmd.BuildConfigFromFlags("", "")
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
	if release.IntentGeneration == 1 {
		arguments = append(arguments, "--force-conflicts")
	}
	if dryRun {
		arguments = append(arguments, "--dry-run=server")
	}
	return append(arguments, "--filename", "-", "--output", "json")
}

func (cluster *kubectlCluster) WaitHealthy(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, manifest []byte) (declarativerelease.Observation, error) {
	if !target.Present {
		return declarativerelease.Observation{}, errors.New("cannot wait healthy for an absent target")
	}
	soak := time.Duration(0)
	if release.Transition != nil && release.Transition.EdgeGroupAB != nil {
		soak = time.Duration(release.Transition.EdgeGroupAB.SoakSeconds) * time.Second
	}
	deadline := time.Now().Add(cluster.timeout + soak)
	var lastErr error
	tracker := healthSoakTracker{required: soak}
	allowHistoricalRestarts := allowsHistoricalRestarts(release, target)
	for {
		observation, err := cluster.observeExpected(ctx, release, target.OCIRevision, manifest, allowHistoricalRestarts)
		if err == nil && observation.Matches(target, release, release.IntentGeneration == 1) {
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
	partial, err := parseObservation(workloadRaw, podsRaw, release, allowHistoricalRestarts)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	verificationRaw, err := cluster.run(ctx, nil, "python3", cluster.verifier,
		"--image", partial.ImageRef, "--platform", "linux/amd64", "--expected-revision", expectedOCI,
		"--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5",
		"--max-attempts", "2", "--retry-delay-seconds", "0.1")
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("verify live image provenance: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	if verification.Image != partial.ImageRef || verification.OCIRevision != expectedOCI {
		return declarativerelease.Observation{}, errors.New("live registry identity mismatch")
	}
	partial.OCIRevision = verification.OCIRevision
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
			pods, err := cluster.readyPodNames(ctx, release)
			if err != nil {
				return "", err
			}
			for _, pod := range pods {
				resource := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%s/proxy%s", release.Workload.Namespace, pod, probe.Port, probe.Path)
				body, err := cluster.kubectlRun(ctx, nil, "get", "--raw", resource)
				if err != nil || (probe.Expected != "" && !bytes.Contains(body, []byte(probe.Expected))) {
					return "", fmt.Errorf("pod health probe %q failed", pod)
				}
				evidence = append(evidence, probe.Type+":"+pod+":"+digestBytesLocal(body))
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
		default:
			return "", fmt.Errorf("unsupported health probe %q", probe.Type)
		}
	}
	sort.Strings(evidence)
	return digestBytesLocal([]byte(strings.Join(evidence, "\n"))), nil
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
	container := ""
	for _, artifactTarget := range release.ArtifactTargets {
		if artifactTarget.APIVersion == apiVersion && artifactTarget.Kind == kind && artifactTarget.Namespace == release.Workload.Namespace &&
			artifactTarget.Name == name && artifactTarget.ContainerType == "container" {
			if container != "" {
				return declarativerelease.Observation{}, fmt.Errorf("health workload %s/%s has multiple primary artifact containers", kind, name)
			}
			container = artifactTarget.Container
		}
	}
	if container == "" {
		return declarativerelease.Observation{}, fmt.Errorf("health workload %s/%s has no declared artifact container", kind, name)
	}
	workload, err := workloadFromDeclaredResource(desired, identity, container, release.Workload.FieldManager)
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	auxiliaryRelease := release
	auxiliaryRelease.Workload = workload
	auxiliary, err := cluster.observeExpected(ctx, auxiliaryRelease, target.OCIRevision, manifest, false)
	if err != nil {
		return declarativerelease.Observation{}, fmt.Errorf("observe health workload %s/%s: %w", kind, name, err)
	}
	if !auxiliary.Matches(target, auxiliaryRelease, release.IntentGeneration == 1) {
		return declarativerelease.Observation{}, fmt.Errorf("health workload %s/%s has not converged to the immutable target", kind, name)
	}
	return auxiliary, nil
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

func (cluster *kubectlCluster) readyPodNames(ctx context.Context, release declarativerelease.PlanRelease) ([]string, error) {
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
	return readyPodNamesFromJSON(podsRaw)
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

func parseObservation(workloadRaw, podsRaw []byte, release declarativerelease.PlanRelease, allowHistoricalRestarts bool) (declarativerelease.Observation, error) {
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
	if manifestSHA == "" {
		manifestSHA = legacySourceTag(image)
	}
	workloadAnnotations := mapStringField(metadata, "annotations")
	configSHA := workloadAnnotations["fugue.pro/production-config-sha"]
	if configSHA == "" {
		configSHA = workloadAnnotations["fugue.pro/"+release.ComponentID+"-manifest-revision"]
	}
	if configSHA == "" {
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
		imageID, healthDigest, err = parseReadyPods(podsRaw, release, observation.Desired-int32(release.Workload.PreservedUnavailable), manifestSHA, allowHistoricalRestarts)
	}
	if err != nil {
		return declarativerelease.Observation{}, err
	}
	observation.ImageID = imageID
	observation.HealthDigest = healthDigest
	return observation, nil
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

func parseReadyPods(raw []byte, release declarativerelease.PlanRelease, desired int32, manifestSHA string, allowHistoricalRestarts bool) (string, string, error) {
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
		if annotations["fugue.pro/source-commit"] != manifestSHA {
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
				return "", "", errors.New("ready workload pod restarted")
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
