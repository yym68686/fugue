package declarativerelease

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const maxWorkloadManifestBytes = 4 << 20

type RenderedManifests struct {
	Forward       []byte
	LKG           []byte
	ForwardDigest string
	LKGDigest     string
}

// RenderManifests creates digest-pinned forward and LKG resources from one
// reviewed base workload. It cannot add or remove Kubernetes objects and only
// writes release identity plus the selected container image.
func RenderManifests(plan Plan, componentID string, receipt ArtifactReceipt, manifest, lkgManifest io.Reader) (RenderedManifests, error) {
	if err := plan.ValidateBound(); err != nil {
		return RenderedManifests{}, err
	}
	if _, err := DecodeArtifactReceipt(bytes.NewReader(mustCanonical(receipt))); err != nil {
		return RenderedManifests{}, err
	}
	var release *PlanRelease
	for index := range plan.Releases {
		if plan.Releases[index].ComponentID == componentID {
			release = &plan.Releases[index]
			break
		}
	}
	if release == nil || receipt.Component != componentID || receipt.ConfigSHA != plan.HeadSHA ||
		receipt.PlanDigest != plan.PlanDigest || receipt.IntentDigest != release.IntentDigest ||
		receipt.Repository != release.Artifact.Repository {
		return RenderedManifests{}, errors.New("artifact receipt is not bound to selected release")
	}
	base, err := DecodeResourceSet(manifest)
	if err != nil {
		return RenderedManifests{}, err
	}
	if _, err := base.Primary(release.Workload); err != nil {
		return RenderedManifests{}, err
	}
	forward := deepCopyResourceSet(base)
	lkg := ResourceSet{APIVersion: ResourceSetAPIVersion, Kind: ResourceSetKind}
	if err := patchResourceSet(&forward, *release, receipt.Repository+"@"+receipt.TopDigest, plan.HeadSHA, plan.HeadSHA, plan.HeadSHA, plan.PlanDigest, receipt.ReceiptDigest); err != nil {
		return RenderedManifests{}, err
	}
	if err := validateImmutableResourceSetImages(forward); err != nil {
		return RenderedManifests{}, fmt.Errorf("validate forward images: %w", err)
	}
	if release.ExpectedPreviousPresent {
		if lkgManifest == nil {
			return RenderedManifests{}, errors.New("declared predecessor requires an explicit LKG resource set")
		}
		lkg, err = DecodeResourceSet(lkgManifest)
		if err != nil {
			return RenderedManifests{}, fmt.Errorf("decode LKG resource set: %w", err)
		}
		if _, err := lkg.Primary(release.Workload); err != nil {
			return RenderedManifests{}, fmt.Errorf("validate LKG primary workload: %w", err)
		}
		if !lkgResourceIdentitiesSubset(forward, lkg) {
			return RenderedManifests{}, errors.New("LKG resource identities are not a subset of forward")
		}
		bootstrap := release.MigrationState == "adopting" && release.OwnershipAdoption != nil && release.BootstrapLKGPath != ""
		if bootstrap {
			if err := validateBootstrapLKGIdentity(lkg, *release); err != nil {
				return RenderedManifests{}, err
			}
		} else {
			if err := patchResourceSet(&lkg, *release, receipt.Repository+"@"+release.ExpectedPreviousImageDigest, release.ExpectedPreviousConfigSHA, release.ExpectedPreviousManifestSHA, release.ExpectedPreviousOCIRevision, plan.PlanDigest, receipt.ReceiptDigest); err != nil {
				return RenderedManifests{}, err
			}
		}
		if err := validateImmutableResourceSetImages(lkg); err != nil {
			return RenderedManifests{}, fmt.Errorf("validate LKG images: %w", err)
		}
	}
	forwardBytes, err := CanonicalJSON(forward)
	if err != nil {
		return RenderedManifests{}, err
	}
	lkgBytes, err := CanonicalJSON(lkg)
	if err != nil {
		return RenderedManifests{}, err
	}
	forwardDigest := sha256.Sum256(forwardBytes)
	lkgDigest := sha256.Sum256(lkgBytes)
	return RenderedManifests{
		Forward: forwardBytes, LKG: lkgBytes,
		ForwardDigest: fmt.Sprintf("sha256:%x", forwardDigest),
		LKGDigest:     fmt.Sprintf("sha256:%x", lkgDigest),
	}, nil
}

// validateBootstrapLKGIdentity permits a first ownership handoff from a
// heterogeneous legacy resource set. Each reviewed LKG container must already
// be immutable; the primary workload remains bound to the intent's exact
// source and digest while auxiliary front/slot/sidecar images retain their own
// reviewed LKG identities.
func validateBootstrapLKGIdentity(lkg ResourceSet, release PlanRelease) error {
	primary, err := lkg.Primary(release.Workload)
	if err != nil {
		return err
	}
	image, err := workloadContainerImage(primary, release.Workload.Container, "container")
	if err != nil {
		return err
	}
	if image != release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest {
		return errors.New("bootstrap LKG primary image does not match the declared predecessor")
	}
	spec, err := objectField(primary, "spec")
	if err != nil {
		return err
	}
	template, err := objectField(spec, "template")
	if err != nil {
		return err
	}
	metadata, err := objectField(template, "metadata")
	if err != nil {
		return err
	}
	annotations := ensureReadStringMap(metadata, "annotations")
	if annotations["fugue.pro/source-commit"] != release.ExpectedPreviousManifestSHA ||
		annotations["fugue.pro/oci-revision"] != release.ExpectedPreviousOCIRevision {
		return errors.New("bootstrap LKG primary source identity does not match the declared predecessor")
	}
	return nil
}

func workloadContainerImage(workload map[string]any, name, containerType string) (string, error) {
	spec, err := objectField(workload, "spec")
	if err != nil {
		return "", err
	}
	template, err := objectField(spec, "template")
	if err != nil {
		return "", err
	}
	templateSpec, err := objectField(template, "spec")
	if err != nil {
		return "", err
	}
	field := "containers"
	if containerType == "init-container" {
		field = "initContainers"
	}
	containers, ok := templateSpec[field].([]any)
	if !ok {
		return "", fmt.Errorf("bootstrap LKG %s are invalid", field)
	}
	image := ""
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			return "", errors.New("bootstrap LKG container is invalid")
		}
		if stringField(container, "name") == name {
			if image != "" {
				return "", errors.New("bootstrap LKG container is ambiguous")
			}
			image = stringField(container, "image")
		}
	}
	if image == "" {
		return "", errors.New("bootstrap LKG container is absent")
	}
	return image, nil
}

func validateImmutableResourceSetImages(set ResourceSet) error {
	for _, item := range set.Items {
		identity, err := resourceIdentity(item)
		if err != nil {
			return err
		}
		if identity.Kind != "Deployment" && identity.Kind != "DaemonSet" && identity.Kind != "Job" {
			continue
		}
		spec, err := objectField(item, "spec")
		if err != nil {
			return err
		}
		template, err := objectField(spec, "template")
		if err != nil {
			return err
		}
		podSpec, err := objectField(template, "spec")
		if err != nil {
			return err
		}
		for _, field := range []string{"initContainers", "containers"} {
			containers, _ := podSpec[field].([]any)
			for _, raw := range containers {
				container, ok := raw.(map[string]any)
				if !ok {
					return fmt.Errorf("%s/%s %s is invalid", identity.Kind, identity.Name, field)
				}
				name, _ := container["name"].(string)
				image, _ := container["image"].(string)
				if !immutableImageRef(image) {
					return fmt.Errorf("%s/%s container %q is not pinned to a nonzero immutable digest", identity.Kind, identity.Name, name)
				}
			}
		}
	}
	return nil
}

func immutableImageRef(value string) bool {
	separator := strings.LastIndex(value, "@")
	if separator < 1 || !repositoryPattern.MatchString(value[:separator]) || !digestPattern.MatchString(value[separator+1:]) {
		return false
	}
	return value[separator+1:] != "sha256:"+strings.Repeat("0", 64) && !strings.HasPrefix(value, "registry.invalid/")
}

func lkgResourceIdentitiesSubset(forward, lkg ResourceSet) bool {
	if len(lkg.Items) > len(forward.Items) {
		return false
	}
	forwardIdentities := make(map[ResourceIdentity]struct{}, len(forward.Items))
	for _, item := range forward.Items {
		identity, err := resourceIdentity(item)
		if err != nil {
			return false
		}
		forwardIdentities[identity] = struct{}{}
	}
	for _, item := range lkg.Items {
		identity, err := resourceIdentity(item)
		if err != nil {
			return false
		}
		if _, exists := forwardIdentities[identity]; !exists {
			return false
		}
	}
	return true
}

func mustCanonical(value any) []byte {
	encoded, err := CanonicalJSON(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func decodeManifest(reader io.Reader) (map[string]any, error) {
	if reader == nil {
		return nil, errors.New("workload manifest reader is nil")
	}
	decoder := json.NewDecoder(io.LimitReader(reader, maxWorkloadManifestBytes))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode workload manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("workload manifest must contain exactly one JSON object")
	}
	if value == nil {
		return nil, errors.New("workload manifest is empty")
	}
	return value, nil
}

func patchResourceSet(set *ResourceSet, release PlanRelease, image, configSHA, manifestSHA, ociRevision, planDigest, receiptDigest string) error {
	if _, err := set.Primary(release.Workload); err != nil {
		return err
	}
	for _, item := range set.Items {
		metadata, metadataErr := objectField(item, "metadata")
		if metadataErr != nil {
			return metadataErr
		}
		annotations := ensureStringMap(metadata, "annotations")
		annotations["fugue.pro/production-config-sha"] = configSHA
		annotations["fugue.pro/release-plan-digest"] = planDigest
		annotations["fugue.pro/artifact-receipt-digest"] = receiptDigest
	}
	targets := release.ArtifactTargets
	if len(targets) == 0 {
		targets = []ArtifactTarget{{
			APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
			Namespace: release.Workload.Namespace, Name: release.Workload.Name,
			Container: release.Workload.Container, ContainerType: "container",
		}}
	}
	patchedWorkloads := make(map[string]struct{})
	for _, target := range targets {
		item, err := resourceSetTarget(set, target)
		if err != nil {
			return err
		}
		workloadKey := target.APIVersion + "\x00" + target.Kind + "\x00" + target.Namespace + "\x00" + target.Name
		if _, exists := patchedWorkloads[workloadKey]; !exists {
			if err := patchWorkloadIdentity(item, configSHA, manifestSHA, ociRevision, planDigest, receiptDigest); err != nil {
				return err
			}
			patchedWorkloads[workloadKey] = struct{}{}
		}
		if err := patchWorkloadContainer(item, target, image); err != nil {
			return err
		}
	}
	return nil
}

func validateManifestIdentity(value map[string]any, workload Workload) error {
	if stringField(value, "apiVersion") != workload.APIVersion || stringField(value, "kind") != workload.Kind {
		return errors.New("workload manifest apiVersion/kind mismatch")
	}
	metadata, err := objectField(value, "metadata")
	if err != nil || stringField(metadata, "name") != workload.Name || stringField(metadata, "namespace") != workload.Namespace {
		return errors.New("workload manifest namespace/name mismatch")
	}
	return nil
}

func patchWorkloadIdentity(value map[string]any, configSHA, manifestSHA, ociRevision, planDigest, receiptDigest string) error {
	metadata, err := objectField(value, "metadata")
	if err != nil {
		return err
	}
	annotations := ensureStringMap(metadata, "annotations")
	annotations["fugue.pro/production-config-sha"] = configSHA
	annotations["fugue.pro/release-plan-digest"] = planDigest
	annotations["fugue.pro/artifact-receipt-digest"] = receiptDigest
	spec, err := objectField(value, "spec")
	if err != nil {
		return err
	}
	template, err := objectField(spec, "template")
	if err != nil {
		return err
	}
	templateMetadata, err := objectField(template, "metadata")
	if err != nil {
		return err
	}
	templateAnnotations := ensureStringMap(templateMetadata, "annotations")
	templateAnnotations["fugue.pro/source-commit"] = manifestSHA
	templateAnnotations["fugue.pro/oci-revision"] = ociRevision
	templateAnnotations["fugue.pro/production-config-sha"] = configSHA
	return nil
}

func patchWorkloadContainer(value map[string]any, target ArtifactTarget, image string) error {
	spec, err := objectField(value, "spec")
	if err != nil {
		return err
	}
	template, err := objectField(spec, "template")
	if err != nil {
		return err
	}
	templateSpec, err := objectField(template, "spec")
	if err != nil {
		return err
	}
	field := "containers"
	if target.ContainerType == "init-container" {
		field = "initContainers"
	}
	containers, ok := templateSpec[field].([]any)
	if !ok || len(containers) == 0 {
		return fmt.Errorf("artifact target %s are invalid", field)
	}
	matches := 0
	for _, item := range containers {
		container, ok := item.(map[string]any)
		if !ok {
			return errors.New("artifact target container is not an object")
		}
		if stringField(container, "name") != target.Container {
			continue
		}
		container["image"] = image
		matches++
	}
	if matches != 1 {
		return fmt.Errorf("workload manifest must contain exactly one %q %s", target.Container, target.ContainerType)
	}
	return nil
}

func resourceSetTarget(set *ResourceSet, target ArtifactTarget) (map[string]any, error) {
	for _, item := range set.Items {
		identity, err := resourceIdentity(item)
		if err != nil {
			return nil, err
		}
		if identity.APIVersion == target.APIVersion && identity.Kind == target.Kind &&
			identity.Namespace == target.Namespace && identity.Name == target.Name {
			return item, nil
		}
	}
	return nil, fmt.Errorf("artifact target workload %s/%s is absent from resource set", target.Kind, target.Name)
}

func objectField(value map[string]any, name string) (map[string]any, error) {
	field, ok := value[name].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("workload manifest field %s is not an object", name)
	}
	return field, nil
}

func stringField(value map[string]any, name string) string {
	field, _ := value[name].(string)
	return field
}

func ensureStringMap(value map[string]any, name string) map[string]any {
	if existing, ok := value[name].(map[string]any); ok {
		return existing
	}
	created := make(map[string]any)
	value[name] = created
	return created
}

func deepCopyMap(value map[string]any) map[string]any {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var copied map[string]any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&copied); err != nil {
		panic(err)
	}
	return copied
}

func deepCopyResourceSet(value ResourceSet) ResourceSet {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var copied ResourceSet
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&copied); err != nil {
		panic(err)
	}
	return copied
}
