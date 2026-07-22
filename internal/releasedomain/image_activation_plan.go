package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ImageActivationPlanInput is the complete side-effect-free input for
// deriving actual image activations from a conservative build plan and the
// exact live-relative rendered workload diff.
type ImageActivationPlanInput struct {
	BuildPlan               BuildArtifactPlan
	ReleasePlan             Plan
	Ownership               []byte
	BaseManifest            []byte
	TargetManifest          []byte
	ImmutableTargetManifest []byte
}

type renderedContainer struct {
	Name    string
	Image   string
	Pointer string
}

// BuildImageActivationPlanFromManifests preserves the strict complete-only
// API used by authorization callers. Report-only producers must use
// BuildImageActivationReportFromManifests so unresolved changes remain
// explicit instead of being discarded or promoted into authorization.
func BuildImageActivationPlanFromManifests(input ImageActivationPlanInput) (ImageActivationPlan, error) {
	plan, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		return ImageActivationPlan{}, err
	}
	if !evidence.Complete {
		return ImageActivationPlan{}, fmt.Errorf("image activation evidence is incomplete: %s", evidence.Unresolved[0].Reason)
	}
	return plan, nil
}

// BuildImageActivationReportFromManifests derives resolved image activations
// and a lossless report of every unresolved rendered image change. Extra
// verified builds that appear in neither set remain built-only. The result is
// side-effect free and never authorizes a mutation.
func BuildImageActivationReportFromManifests(input ImageActivationPlanInput) (ImageActivationPlan, ImageActivationEvidence, error) {
	if err := VerifyBuildArtifactPlan(input.BuildPlan); err != nil {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("verify build artifact plan: %w", err)
	}
	if err := VerifyPlanDigest(input.ReleasePlan); err != nil {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("verify release plan: %w", err)
	}
	// BuildArtifactPlan revision bounds are exact Git commits from the verified
	// changed-file evidence. Plan.Digests.Base/Target are opaque SSoT digests
	// and intentionally have a different namespace. The common changed-file
	// digest is the revision-to-rendered-plan binding at this report-only layer.
	if input.BuildPlan.ChangedFilesDigest != input.ReleasePlan.Digests.ChangedFiles {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("build artifact and release plan binding mismatch")
	}
	if digestBytesSHA256(input.BaseManifest) != input.ReleasePlan.Digests.BaseManifest ||
		digestBytesSHA256(input.TargetManifest) != input.ReleasePlan.Digests.TargetManifest ||
		digestBytesSHA256(input.Ownership) != input.ReleasePlan.Digests.Ownership {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("rendered manifest or ownership digest mismatch")
	}
	context := input.ReleasePlan.Digests.ClassificationContext
	if err := VerifyClassificationContextEvidence(context); err != nil {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("verify classification context: %w", err)
	}
	spec, err := LoadOwnership(strings.NewReader(string(input.Ownership)))
	if err != nil {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("load ownership: %w", err)
	}
	if err := spec.ValidateBindings(context.BindingMap()); err != nil {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("validate ownership bindings: %w", err)
	}
	activationTargetManifest := input.TargetManifest
	if len(input.ImmutableTargetManifest) != 0 {
		expected, err := MaterializeTargetPublishedImageRefs(
			input.TargetManifest,
			input.Ownership,
			context.DefaultNamespace,
			input.BuildPlan.TargetCommit,
			input.BuildPlan,
		)
		if err != nil {
			return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("materialize immutable target manifest: %w", err)
		}
		if !bytes.Equal(expected, input.ImmutableTargetManifest) {
			return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("immutable target manifest binding mismatch")
		}
		activationTargetManifest = input.ImmutableTargetManifest
	}

	baseObjects, baseUnknown := decodeManifest(input.BaseManifest, spec, context.DefaultNamespace, "base")
	targetObjects, targetUnknown := decodeManifest(activationTargetManifest, spec, context.DefaultNamespace, "target")
	if len(baseUnknown) != 0 || len(targetUnknown) != 0 {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("rendered manifests contain incomplete object evidence")
	}
	baseByIdentity, duplicateBase := indexManifestObjects(baseObjects, "base")
	targetByIdentity, duplicateTarget := indexManifestObjects(targetObjects, "target")
	if len(duplicateBase) != 0 || len(duplicateTarget) != 0 {
		return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("rendered manifests contain duplicate object identities")
	}

	artifactsByDigest := make(map[string][]BuildArtifact, len(input.BuildPlan.Artifacts))
	for _, artifact := range input.BuildPlan.Artifacts {
		artifactsByDigest[artifact.ArtifactDigest] = append(artifactsByDigest[artifact.ArtifactDigest], artifact)
	}

	keys := make([]string, 0, len(targetByIdentity))
	for key := range targetByIdentity {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	activations := make([]ImageActivation, 0)
	unresolved := make([]ImageActivationGap, 0)
	usedArtifacts := map[string]struct{}{}
	for _, key := range keys {
		target := targetByIdentity[key]
		targetContainers, targetIsWorkload, err := workloadContainers(target)
		if err != nil {
			return ImageActivationPlan{}, ImageActivationEvidence{}, err
		}
		if !targetIsWorkload {
			continue
		}
		base, baseExists := baseByIdentity[key]
		baseContainers := map[string]renderedContainer{}
		if baseExists {
			var baseIsWorkload bool
			baseContainers, baseIsWorkload, err = workloadContainers(base)
			if err != nil {
				return ImageActivationPlan{}, ImageActivationEvidence{}, err
			}
			if !baseIsWorkload {
				return ImageActivationPlan{}, ImageActivationEvidence{}, fmt.Errorf("workload kind changed for %s", target.Identity.String())
			}
		}

		for _, targetContainer := range sortedRenderedContainers(targetContainers) {
			baseContainer, baseContainerExists := baseContainers[targetContainer.Name]
			if baseContainerExists && baseContainer.Image == targetContainer.Image {
				continue
			}

			forwardDigest, err := renderedObjectDigest(target)
			if err != nil {
				return ImageActivationPlan{}, ImageActivationEvidence{}, err
			}
			reverseDigest := ""
			if baseExists && baseContainerExists {
				reverseDigest, err = renderedObjectDigest(base)
				if err != nil {
					return ImageActivationPlan{}, ImageActivationEvidence{}, err
				}
			}

			artifactDigest, immutable := imageArtifactDigest(targetContainer.Image)
			matchingArtifacts := artifactsByDigest[artifactDigest]
			matchingNames := make([]string, 0, len(matchingArtifacts))
			for _, artifact := range matchingArtifacts {
				matchingNames = append(matchingNames, artifact.Name)
				usedArtifacts[artifact.Name] = struct{}{}
			}
			domains, err := activationObjectDomains(spec, base, baseExists, target, targetContainer.Pointer, context)
			if err != nil {
				return ImageActivationPlan{}, ImageActivationEvidence{}, err
			}

			reason := ""
			switch {
			case !immutable:
				reason = ImageActivationGapTargetNotImmutable
			case len(matchingArtifacts) == 0:
				reason = ImageActivationGapArtifactNotBuilt
			case len(matchingArtifacts) > 1:
				reason = ImageActivationGapArtifactAmbiguous
			case !baseExists || !baseContainerExists:
				reason = ImageActivationGapAbsentCreate
			case len(domains) == 0:
				reason = ImageActivationGapOwnershipMissing
			case len(domains) > 1:
				reason = ImageActivationGapOwnershipAmbiguous
			}

			adapter := ""
			if reason == "" {
				var ok bool
				adapter, ok = fixedAdapterForDomain(domains[0])
				if !ok {
					reason = ImageActivationGapAdapterMissing
				}
			}

			if reason != "" {
				gap := ImageActivationGap{
					ID:           imageActivationGapID(target.Identity, targetContainer.Name),
					Workload:     activationWorkload(target.Identity, targetContainer.Name),
					LiveImageRef: baseContainer.Image, TargetImageRef: targetContainer.Image,
					ArtifactDigest: artifactDigest, MatchingBuildArtifacts: matchingNames,
					OwnershipDomains: domains, Reason: reason,
					ForwardRenderedDigest: forwardDigest, ReverseRenderedDigest: reverseDigest,
				}
				if reason == ImageActivationGapTargetNotImmutable {
					gap.ArtifactDigest = ""
					gap.MatchingBuildArtifacts = []string{}
				}
				if reason == ImageActivationGapAbsentCreate {
					gap.LiveImageRef = ""
					gap.ReverseRenderedDigest = ""
				}
				unresolved = append(unresolved, gap)
				continue
			}

			artifact := matchingArtifacts[0]
			activations = append(activations, ImageActivation{
				ID:                    imageActivationID(artifact.Name, target.Identity, targetContainer.Name),
				ArtifactName:          artifact.Name,
				ArtifactDigest:        artifact.ArtifactDigest,
				Workload:              activationWorkload(target.Identity, targetContainer.Name),
				Domain:                domains[0],
				Adapter:               adapter,
				LiveImageRef:          baseContainer.Image,
				TargetImageRef:        targetContainer.Image,
				ForwardRenderedDigest: forwardDigest,
				ReverseRenderedDigest: reverseDigest,
			})
		}
	}

	activationPlan, err := NewImageActivationPlan(
		input.BuildPlan.BaseCommit,
		input.BuildPlan.TargetCommit,
		input.BuildPlan.Digest,
		input.ReleasePlan.Digests.BaseManifest,
		activations,
	)
	if err != nil {
		return ImageActivationPlan{}, ImageActivationEvidence{}, err
	}
	builtOnly := make([]string, 0, len(input.BuildPlan.Artifacts))
	for _, artifact := range input.BuildPlan.Artifacts {
		if _, used := usedArtifacts[artifact.Name]; !used {
			builtOnly = append(builtOnly, artifact.Name)
		}
	}
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: input.BuildPlan.BaseCommit, TargetCommit: input.BuildPlan.TargetCommit,
		BuildArtifactPlanDigest:           input.BuildPlan.Digest,
		ResolvedImageActivationPlanDigest: activationPlan.Digest,
		BuiltOnlyArtifacts:                builtOnly, Unresolved: unresolved,
	})
	if err != nil {
		return ImageActivationPlan{}, ImageActivationEvidence{}, err
	}
	return activationPlan, evidence, nil
}

func workloadContainers(object manifestObject) (map[string]renderedContainer, bool, error) {
	path := []string{"spec", "template", "spec"}
	switch object.Identity.Kind {
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job":
	case "CronJob":
		path = []string{"spec", "jobTemplate", "spec", "template", "spec"}
	case "Pod":
		path = []string{"spec"}
	default:
		return nil, false, nil
	}
	podSpec, ok := nestedManifestMap(object.Object, path...)
	if !ok {
		return nil, false, fmt.Errorf("workload pod spec is missing for %s", object.Identity.String())
	}
	containers := map[string]renderedContainer{}
	for _, field := range []string{"containers", "initContainers"} {
		values, exists := podSpec[field]
		if !exists {
			if field == "containers" {
				return nil, false, fmt.Errorf("workload containers are missing for %s", object.Identity.String())
			}
			continue
		}
		list, ok := values.([]any)
		if !ok || len(list) == 0 {
			return nil, false, fmt.Errorf("workload %s are invalid for %s", field, object.Identity.String())
		}
		for index, value := range list {
			container, ok := value.(map[string]any)
			if !ok {
				return nil, false, fmt.Errorf("workload container is invalid for %s", object.Identity.String())
			}
			name, nameOK := container["name"].(string)
			image, imageOK := container["image"].(string)
			if !nameOK || !imageOK || !validContractText(name, 253) || !validContractText(image, 1024) {
				return nil, false, fmt.Errorf("workload container identity is invalid for %s", object.Identity.String())
			}
			if _, duplicate := containers[name]; duplicate {
				return nil, false, fmt.Errorf("workload container name is duplicated for %s", object.Identity.String())
			}
			containers[name] = renderedContainer{
				Name: name, Image: image,
				Pointer: "/" + strings.Join(append(append([]string(nil), path...), field, fmt.Sprintf("%d", index), "image"), "/"),
			}
		}
	}
	return containers, true, nil
}

func nestedManifestMap(root map[string]any, path ...string) (map[string]any, bool) {
	current := root
	for _, name := range path {
		next, ok := current[name].(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func sortedRenderedContainers(values map[string]renderedContainer) []renderedContainer {
	result := make([]renderedContainer, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	return result
}

func imageArtifactDigest(reference string) (string, bool) {
	separator := strings.LastIndexByte(reference, '@')
	if separator <= 0 || separator == len(reference)-1 {
		return "", false
	}
	digest := reference[separator+1:]
	if validateCanonicalSHA256Digest(digest, "rendered image artifact digest") != nil {
		return "", false
	}
	return digest, true
}

func uniqueActivationObjectRule(spec *OwnershipSpec, base, target manifestObject, context ClassificationContextEvidence) (ObjectRule, error) {
	bindings := context.BindingMap()
	matches := make([]ObjectRule, 0, 1)
	for _, rule := range spec.ObjectRules {
		baseMatches, err := rule.matches(base, context.DefaultNamespace, bindings)
		if err != nil {
			return ObjectRule{}, fmt.Errorf("base ownership match failed: %w", err)
		}
		targetMatches, err := rule.matches(target, context.DefaultNamespace, bindings)
		if err != nil {
			return ObjectRule{}, fmt.Errorf("target ownership match failed: %w", err)
		}
		if baseMatches && targetMatches {
			matches = append(matches, rule)
		}
	}
	if len(matches) != 1 {
		return ObjectRule{}, fmt.Errorf("image activation workload does not have one exact ownership matcher")
	}
	return matches[0], nil
}

func activationObjectDomains(spec *OwnershipSpec, base manifestObject, baseExists bool, target manifestObject, pointer string, context ClassificationContextEvidence) ([]Domain, error) {
	bindings := context.BindingMap()
	domains := make([]Domain, 0, 1)
	matchedRules := 0
	for _, rule := range spec.ObjectRules {
		targetMatches, err := rule.matches(target, context.DefaultNamespace, bindings)
		if err != nil {
			return nil, fmt.Errorf("target ownership match failed: %w", err)
		}
		if !targetMatches {
			continue
		}
		if baseExists {
			baseMatches, err := rule.matches(base, context.DefaultNamespace, bindings)
			if err != nil {
				return nil, fmt.Errorf("base ownership match failed: %w", err)
			}
			if !baseMatches {
				continue
			}
		}
		matchedRules++
		domains = append(domains, rule.domainForPointer(pointer))
	}
	domains = canonicalDomains(domains)
	if len(domains) != matchedRules {
		return nil, fmt.Errorf("image activation ownership rules overlap without distinct recovery domains")
	}
	return domains, nil
}

func renderedObjectDigest(object manifestObject) (string, error) {
	encoded, err := json.Marshal(normalizedObject(object))
	if err != nil {
		return "", fmt.Errorf("marshal rendered workload: %w", err)
	}
	return digestOperationalBytes(encoded), nil
}

func imageActivationID(artifact string, identity ObjectIdentity, container string) string {
	payload := strings.Join([]string{
		identity.APIGroup, identity.Version, identity.Kind,
		identity.Namespace, identity.Name, container,
	}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return "activate-" + artifact + "-" + hex.EncodeToString(digest[:8])
}

func imageActivationGapID(identity ObjectIdentity, container string) string {
	payload := strings.Join([]string{
		identity.APIGroup, identity.Version, identity.Kind,
		identity.Namespace, identity.Name, container,
	}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return "activation-gap-" + hex.EncodeToString(digest[:8])
}

func activationWorkload(identity ObjectIdentity, container string) ActivationWorkload {
	apiVersion := identity.Version
	if identity.APIGroup != "" {
		apiVersion = identity.APIGroup + "/" + identity.Version
	}
	return ActivationWorkload{
		APIVersion: apiVersion,
		Kind:       identity.Kind,
		Namespace:  identity.Namespace,
		Name:       identity.Name,
		Container:  container,
	}
}
