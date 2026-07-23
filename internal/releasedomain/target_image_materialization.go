package releasedomain

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// MaterializeTargetPublishedImageRefs returns a canonical report-only target
// manifest whose exact target-commit image tags are replaced by the matching
// immutable references already sealed in the build artifact plan. It does not
// change the target manifest consumed by the activation resolver or an apply
// adapter.
func MaterializeTargetPublishedImageRefs(
	targetManifest, ownership []byte,
	defaultNamespace, trustedTarget string,
	buildPlan BuildArtifactPlan,
) ([]byte, error) {
	if err := validateTrustedGitCommit(trustedTarget, "immutable target manifest target commit"); err != nil {
		return nil, err
	}
	if err := VerifyBuildArtifactPlan(buildPlan); err != nil {
		return nil, fmt.Errorf("verify build artifact plan: %w", err)
	}
	if buildPlan.TargetCommit != trustedTarget {
		return nil, fmt.Errorf("immutable target manifest and build plan target mismatch")
	}
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return nil, fmt.Errorf("load ownership: %w", err)
	}
	objects, unknown := decodeManifest(targetManifest, spec, defaultNamespace, "immutable target")
	if len(unknown) != 0 {
		return nil, manifestEvidenceError(unknown)
	}
	if _, duplicates := indexManifestObjects(objects, "immutable target"); len(duplicates) != 0 {
		return nil, manifestEvidenceError(duplicates)
	}

	publishedByRepository := make(map[string]string, len(buildPlan.Artifacts))
	for _, artifact := range buildPlan.Artifacts {
		if artifact.PublishedImageRef == "" {
			continue
		}
		repository := strings.TrimSuffix(artifact.PublishedImageRef, "@"+artifact.ArtifactDigest)
		if repository == "" || repository == artifact.PublishedImageRef {
			return nil, fmt.Errorf("build artifact published image reference is not repository-bound")
		}
		if _, duplicate := publishedByRepository[repository]; duplicate {
			return nil, fmt.Errorf("build artifact published image repository is ambiguous")
		}
		publishedByRepository[repository] = artifact.PublishedImageRef
	}

	targetSuffix := ":" + trustedTarget
	for index := range objects {
		containers, workload, containerErr := workloadContainers(objects[index])
		if containerErr != nil {
			return nil, containerErr
		}
		if !workload {
			continue
		}
		for _, container := range sortedRenderedContainers(containers) {
			if strings.Contains(container.Image, "@") || !strings.HasSuffix(container.Image, targetSuffix) {
				continue
			}
			repository := strings.TrimSuffix(container.Image, targetSuffix)
			published, exists := publishedByRepository[repository]
			if !exists {
				return nil, fmt.Errorf("target-commit workload image has no exact published artifact")
			}
			if err := setRenderedContainerImage(objects[index], container.Pointer, published); err != nil {
				return nil, err
			}
		}
	}
	return encodeMaterializedTargetObjects(objects)
}

// MaterializeLiveRelativeTargetPublishedImageRefs additionally removes one
// Helm-only source of false activation evidence. The public-edge chart hashes
// its image values into rollout checksum annotations even when an immutable
// digest keeps the rendered workload image unchanged. When edge was built but
// not activated, and the checksum is the object's only rendered drift, retain
// the live checksum. Any authoritative-DNS source change, actual image change,
// ambiguous ownership, or additional object drift leaves the target intact or
// fails closed.
func MaterializeLiveRelativeTargetPublishedImageRefs(
	baseManifest, targetManifest, ownership []byte,
	defaultNamespace, trustedTarget string,
	buildPlan BuildArtifactPlan,
	releasePlan Plan,
) ([]byte, error) {
	if err := VerifyPlanDigest(releasePlan); err != nil {
		return nil, fmt.Errorf("verify release plan: %w", err)
	}
	if buildPlan.ChangedFilesDigest != releasePlan.Digests.ChangedFiles {
		return nil, fmt.Errorf("build artifact and release plan binding mismatch")
	}
	if digestBytesSHA256(baseManifest) != releasePlan.Digests.BaseManifest ||
		digestBytesSHA256(targetManifest) != releasePlan.Digests.TargetManifest ||
		digestBytesSHA256(ownership) != releasePlan.Digests.Ownership {
		return nil, fmt.Errorf("live-relative target manifest or ownership digest mismatch")
	}
	context := releasePlan.Digests.ClassificationContext
	if err := VerifyClassificationContextEvidence(context); err != nil {
		return nil, fmt.Errorf("verify classification context: %w", err)
	}
	if defaultNamespace != context.DefaultNamespace {
		return nil, fmt.Errorf("live-relative target default namespace mismatch")
	}

	materialized, err := MaterializeTargetPublishedImageRefs(
		targetManifest, ownership, defaultNamespace, trustedTarget, buildPlan,
	)
	if err != nil {
		return nil, err
	}
	if containsDomain(releasePlan.Files.Domains, DomainAuthoritativeDNS) {
		return materialized, nil
	}

	edgeRepository, found, err := publishedArtifactRepository(buildPlan, "edge")
	if err != nil {
		return nil, err
	}
	if !found {
		return materialized, nil
	}
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return nil, fmt.Errorf("load ownership: %w", err)
	}
	if err := spec.ValidateBindings(context.BindingMap()); err != nil {
		return nil, fmt.Errorf("validate ownership bindings: %w", err)
	}
	baseObjects, baseUnknown := decodeManifest(baseManifest, spec, defaultNamespace, "live-relative base")
	targetObjects, targetUnknown := decodeManifest(materialized, spec, defaultNamespace, "live-relative target")
	if len(baseUnknown) != 0 || len(targetUnknown) != 0 {
		return nil, manifestEvidenceError(append(baseUnknown, targetUnknown...))
	}
	baseByIdentity, duplicateBase := indexManifestObjects(baseObjects, "live-relative base")
	_, duplicateTarget := indexManifestObjects(targetObjects, "live-relative target")
	if len(duplicateBase) != 0 || len(duplicateTarget) != 0 {
		return nil, manifestEvidenceError(append(duplicateBase, duplicateTarget...))
	}

	for index := range targetObjects {
		base, exists := baseByIdentity[identityKey(targetObjects[index].Identity)]
		if !exists {
			continue
		}
		if err := preserveBuiltOnlyPublicEdgeChecksum(
			base, targetObjects[index], spec, context, edgeRepository,
		); err != nil {
			return nil, err
		}
	}
	return encodeMaterializedTargetObjects(targetObjects)
}

func containsDomain(domains []Domain, expected Domain) bool {
	for _, domain := range domains {
		if domain == expected {
			return true
		}
	}
	return false
}

func publishedArtifactRepository(plan BuildArtifactPlan, name string) (string, bool, error) {
	for _, artifact := range plan.Artifacts {
		if artifact.Name != name {
			continue
		}
		if artifact.PublishedImageRef == "" {
			return "", false, nil
		}
		repository := strings.TrimSuffix(artifact.PublishedImageRef, "@"+artifact.ArtifactDigest)
		if repository == "" || repository == artifact.PublishedImageRef {
			return "", false, fmt.Errorf("build artifact published image reference is not repository-bound")
		}
		return repository, true, nil
	}
	return "", false, nil
}

func preserveBuiltOnlyPublicEdgeChecksum(
	base, target manifestObject,
	spec *OwnershipSpec,
	context ClassificationContextEvidence,
	edgeRepository string,
) error {
	checksumKey, containerName, matches := publicEdgeChecksumBinding(target)
	if !matches {
		return nil
	}
	pointer := "/spec/template/metadata/annotations/" + escapeJSONPointerToken(checksumKey)
	rule, err := uniqueActivationObjectRule(spec, base, target, context)
	if err != nil {
		return fmt.Errorf("public-edge checksum ownership is not unique: %w", err)
	}
	if rule.domainForPointer(pointer) != DomainAuthoritativeDNS {
		return fmt.Errorf("public-edge checksum is not owned by authoritative-dns")
	}

	baseAnnotations, err := podTemplateAnnotations(base)
	if err != nil {
		return err
	}
	targetAnnotations, err := podTemplateAnnotations(target)
	if err != nil {
		return err
	}
	baseChecksum, baseOK := baseAnnotations[checksumKey].(string)
	targetChecksum, targetOK := targetAnnotations[checksumKey].(string)
	if !baseOK || !targetOK || baseChecksum == "" || targetChecksum == "" {
		return fmt.Errorf("public-edge checksum annotation is missing or invalid")
	}
	if baseChecksum == targetChecksum {
		return nil
	}

	pointers := make([]string, 0, 1)
	diffJSON(normalizedObject(base), true, normalizedObject(target), true, "", &pointers)
	pointers = uniqueSortedStrings(pointers)
	if len(pointers) != 1 || pointers[0] != pointer {
		return nil
	}
	baseContainers, baseWorkload, err := workloadContainers(base)
	if err != nil {
		return err
	}
	targetContainers, targetWorkload, err := workloadContainers(target)
	if err != nil {
		return err
	}
	baseContainer, baseFound := baseContainers[containerName]
	targetContainer, targetFound := targetContainers[containerName]
	if !baseWorkload || !targetWorkload || !baseFound || !targetFound {
		return fmt.Errorf("public-edge checksum workload container is missing")
	}
	if imageRepository(baseContainer.Image) != edgeRepository || imageRepository(targetContainer.Image) != edgeRepository {
		return fmt.Errorf("public-edge checksum workload is not bound to the edge artifact repository")
	}
	if baseContainer.Image != targetContainer.Image {
		return nil
	}
	targetAnnotations[checksumKey] = baseChecksum
	return nil
}

func publicEdgeChecksumBinding(object manifestObject) (string, string, bool) {
	if object.Identity.APIGroup != "apps" || object.Identity.Version != "v1" || object.Identity.Kind != "DaemonSet" ||
		object.Labels["fugue.io/rollout-subsystem"] != "public-data-plane" {
		return "", "", false
	}
	switch object.Labels["fugue.io/rollout-mode"] {
	case "node-local-blue-green-front":
		return "checksum/edge-blue-green-front", "edge-front", true
	case "node-local-blue-green-worker":
		return "checksum/edge-blue-green-worker", "edge", true
	default:
		return "", "", false
	}
}

func podTemplateAnnotations(object manifestObject) (map[string]any, error) {
	metadata, ok := nestedManifestMap(object.Object, "spec", "template", "metadata")
	if !ok {
		return nil, fmt.Errorf("pod template metadata is missing for %s", object.Identity.String())
	}
	annotations, ok := metadata["annotations"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("pod template annotations are missing for %s", object.Identity.String())
	}
	return annotations, nil
}

func imageRepository(reference string) string {
	if separator := strings.LastIndexByte(reference, '@'); separator > 0 {
		return reference[:separator]
	}
	slash := strings.LastIndexByte(reference, '/')
	if separator := strings.LastIndexByte(reference, ':'); separator > slash {
		return reference[:separator]
	}
	return reference
}

func setRenderedContainerImage(object manifestObject, pointer, image string) error {
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	if len(parts) < 4 || parts[len(parts)-1] != "image" {
		return fmt.Errorf("rendered workload image pointer is invalid")
	}
	current := any(object.Object)
	for index, part := range parts[:len(parts)-1] {
		switch typed := current.(type) {
		case map[string]any:
			next, exists := typed[part]
			if !exists {
				return fmt.Errorf("rendered workload image pointer is missing")
			}
			current = next
		case []any:
			position := -1
			if _, scanErr := fmt.Sscanf(part, "%d", &position); scanErr != nil || position < 0 || position >= len(typed) {
				return fmt.Errorf("rendered workload image pointer index is invalid")
			}
			current = typed[position]
		default:
			return fmt.Errorf("rendered workload image pointer cannot be traversed at segment %d", index)
		}
	}
	container, ok := current.(map[string]any)
	if !ok {
		return fmt.Errorf("rendered workload image container is invalid")
	}
	container["image"] = image
	return nil
}

func encodeMaterializedTargetObjects(objects []manifestObject) ([]byte, error) {
	indexed, duplicates := indexManifestObjects(objects, "immutable target")
	if len(duplicates) != 0 {
		return nil, manifestEvidenceError(duplicates)
	}
	identities := make([]string, 0, len(indexed))
	for identity := range indexed {
		identities = append(identities, identity)
	}
	sort.Strings(identities)

	var output bytes.Buffer
	for index, identity := range identities {
		root, err := canonicalManifestNode(normalizedObject(indexed[identity]))
		if err != nil {
			return nil, fmt.Errorf("canonicalize immutable target %s: %w", indexed[identity].Identity.String(), err)
		}
		var document bytes.Buffer
		encoder := yaml.NewEncoder(&document)
		encoder.SetIndent(2)
		if err := encoder.Encode(root); err != nil {
			_ = encoder.Close()
			return nil, fmt.Errorf("encode immutable target %s: %w", indexed[identity].Identity.String(), err)
		}
		if err := encoder.Close(); err != nil {
			return nil, fmt.Errorf("close immutable target encoder for %s: %w", indexed[identity].Identity.String(), err)
		}
		if index != 0 {
			output.WriteString("---\n")
		}
		output.Write(document.Bytes())
		if output.Len() > maxRenderedManifestBytes {
			return nil, fmt.Errorf("immutable target manifest bytes exceed limit %d", maxRenderedManifestBytes)
		}
	}
	return append([]byte(nil), output.Bytes()...), nil
}
