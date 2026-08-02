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
	return materializeTargetPublishedImageRefs(
		nil,
		targetManifest,
		ownership,
		defaultNamespace,
		trustedTarget,
		buildPlan,
	)
}

func materializeTargetPublishedImageRefs(
	baseManifest, targetManifest, ownership []byte,
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
	liveImages := map[string]string{}
	if baseManifest != nil {
		baseObjects, unknown := decodeManifest(baseManifest, spec, defaultNamespace, "immutable live")
		if len(unknown) != 0 {
			return nil, manifestEvidenceError(unknown)
		}
		baseByIdentity, duplicates := indexManifestObjects(baseObjects, "immutable live")
		if len(duplicates) != 0 {
			return nil, manifestEvidenceError(duplicates)
		}
		for identity, object := range baseByIdentity {
			containers, workload, containerErr := workloadContainers(object)
			if containerErr != nil {
				return nil, containerErr
			}
			if !workload {
				continue
			}
			for name, container := range containers {
				liveImages[renderedContainerKey(identity, name)] = container.Image
			}
		}
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
			if liveImage, exists := liveImages[renderedContainerKey(identityKey(objects[index].Identity), container.Name)]; exists &&
				liveImage == container.Image {
				// An exact live/target image match is not an activation. A
				// same-SHA reconciliation therefore needs no newly built
				// artifact for this container, while every changed or newly
				// introduced target-commit image still fails closed below.
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

// MaterializeLiveRelativeTargetPublishedImageRefs treats an exact live/target
// container image match as a non-activation, so a reconciliation of a commit
// that is already live does not require a duplicate build artifact. Changed
// and newly introduced target-commit images retain the strict artifact
// requirement. It also removes one Helm-only source of false activation
// evidence: the public-edge chart hashes its image values into rollout
// checksum annotations and pod-template fields even when the rendered
// workload images remain unchanged. When edge was built but not activated,
// retain the exact observed-live objects matched by the three public-edge
// DaemonSet ownership rules. Any actual image change, ambiguous ownership, or
// other public-data-plane object drift remains in the target and therefore
// remains fail-closed in activation authorization.
func MaterializeLiveRelativeTargetPublishedImageRefs(
	baseManifest, targetManifest, ownership []byte,
	defaultNamespace, trustedTarget string,
	buildPlan BuildArtifactPlan,
	releasePlan Plan,
) ([]byte, error) {
	return materializeObservedLiveRelativeTargetPublishedImageRefs(
		baseManifest, baseManifest, targetManifest, ownership,
		defaultNamespace, trustedTarget, buildPlan, releasePlan,
	)
}

// MaterializeObservedLiveRelativeTargetPublishedImageRefs uses a separately
// attested image-only projection of the actual Kubernetes workload state when
// deciding whether a target image is already active. The Helm base remains the
// release-plan and rollback source of truth; observedLiveManifest is accepted
// only after VerifyObservedLiveImageManifest proves that it differs from that
// base exclusively at existing container image fields.
func MaterializeObservedLiveRelativeTargetPublishedImageRefs(
	baseManifest, observedLiveManifest, targetManifest, ownership []byte,
	defaultNamespace, trustedTarget string,
	buildPlan BuildArtifactPlan,
	releasePlan Plan,
) ([]byte, error) {
	return materializeObservedLiveRelativeTargetPublishedImageRefs(
		baseManifest, observedLiveManifest, targetManifest, ownership,
		defaultNamespace, trustedTarget, buildPlan, releasePlan,
	)
}

func materializeObservedLiveRelativeTargetPublishedImageRefs(
	baseManifest, observedLiveManifest, targetManifest, ownership []byte,
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
	if !bytes.Equal(baseManifest, observedLiveManifest) {
		if err := VerifyObservedLiveImageManifest(
			baseManifest, observedLiveManifest, ownership, defaultNamespace,
		); err != nil {
			return nil, fmt.Errorf("verify observed live image manifest: %w", err)
		}
	}

	materialized, err := materializeTargetPublishedImageRefs(
		observedLiveManifest, targetManifest, ownership, defaultNamespace, trustedTarget, buildPlan,
	)
	if err != nil {
		return nil, err
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
	baseObjects, baseUnknown := decodeManifest(observedLiveManifest, spec, defaultNamespace, "live-relative observed base")
	targetObjects, targetUnknown := decodeManifest(materialized, spec, defaultNamespace, "live-relative target")
	if len(baseUnknown) != 0 || len(targetUnknown) != 0 {
		return nil, manifestEvidenceError(append(baseUnknown, targetUnknown...))
	}
	baseByIdentity, duplicateBase := indexManifestObjects(baseObjects, "live-relative base")
	targetByIdentity, duplicateTarget := indexManifestObjects(targetObjects, "live-relative target")
	if len(duplicateBase) != 0 || len(duplicateTarget) != 0 {
		return nil, manifestEvidenceError(append(duplicateBase, duplicateTarget...))
	}

	for identity, base := range baseByIdentity {
		if _, exists := targetByIdentity[identity]; !exists && isPublicDataPlaneObject(base) {
			return nil, fmt.Errorf("public-data-plane object is missing from target: %s", base.Identity.String())
		}
	}
	for index := range targetObjects {
		base, exists := baseByIdentity[identityKey(targetObjects[index].Identity)]
		if !exists {
			if isPublicDataPlaneObject(targetObjects[index]) {
				return nil, fmt.Errorf("public-data-plane object is absent from observed live: %s", targetObjects[index].Identity.String())
			}
			continue
		}
		preserve, err := preserveBuiltOnlyPublicEdgeObject(
			base, targetObjects[index], spec, context, edgeRepository,
		)
		if err != nil {
			return nil, err
		}
		if preserve {
			targetObjects[index] = base
		}
	}
	return encodeMaterializedTargetObjects(targetObjects)
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

func preserveBuiltOnlyPublicEdgeObject(
	base, target manifestObject,
	spec *OwnershipSpec,
	context ClassificationContextEvidence,
	edgeRepository string,
) (bool, error) {
	pointers := make([]string, 0, 1)
	diffJSON(normalizedObject(base), true, normalizedObject(target), true, "", &pointers)
	if len(pointers) == 0 {
		return false, nil
	}
	if !isPublicDataPlaneObject(base) && !isPublicDataPlaneObject(target) {
		return false, nil
	}
	if !isPublicDataPlaneObject(base) || !isPublicDataPlaneObject(target) {
		return false, fmt.Errorf("public-data-plane object identity changed across live and target")
	}
	rule, err := uniqueActivationObjectRule(spec, base, target, context)
	if err != nil {
		return false, fmt.Errorf("public-data-plane ownership is not unique: %w", err)
	}
	if !isExactPublicEdgePreserveRule(rule.ID) {
		return false, nil
	}
	if rule.Domain != DomainAuthoritativeDNS {
		return false, fmt.Errorf("public-edge object is not owned by authoritative-dns")
	}
	baseContainers, baseWorkload, err := workloadContainers(base)
	if err != nil {
		return false, err
	}
	targetContainers, targetWorkload, err := workloadContainers(target)
	if err != nil {
		return false, err
	}
	if !baseWorkload || !targetWorkload || len(baseContainers) != len(targetContainers) {
		return false, fmt.Errorf("public-edge workload containers are incomplete")
	}
	edgeRepositoryFound := false
	for name, baseContainer := range baseContainers {
		targetContainer, found := targetContainers[name]
		if !found {
			return false, fmt.Errorf("public-edge workload container set changed")
		}
		if imageRepository(baseContainer.Image) == edgeRepository || imageRepository(targetContainer.Image) == edgeRepository {
			edgeRepositoryFound = true
		}
		if baseContainer.Image != targetContainer.Image {
			return false, nil
		}
	}
	if !edgeRepositoryFound {
		return false, fmt.Errorf("public-edge workload is not bound to the edge artifact repository")
	}
	return true, nil
}

func isPublicDataPlaneObject(object manifestObject) bool {
	return object.Labels["fugue.io/rollout-subsystem"] == "public-data-plane"
}

func isExactPublicEdgePreserveRule(ruleID string) bool {
	switch ruleID {
	case "authoritative-dns-public-edge-front-daemon-set",
		"authoritative-dns-public-edge-worker-a-daemon-set",
		"authoritative-dns-public-edge-worker-b-daemon-set":
		return true
	default:
		return false
	}
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

func renderedContainerKey(identity, container string) string {
	return identity + "\x00" + container
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
