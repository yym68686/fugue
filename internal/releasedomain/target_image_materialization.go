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
