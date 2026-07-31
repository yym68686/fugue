package releasedomain

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
)

// MaterializeObservedLiveImageManifest projects the currently observed
// workload images onto the canonical Helm base manifest. The returned
// manifest deliberately retains every non-image field from the Helm base, so
// it can be used only as an image-activation witness and never as a substitute
// for the Helm revision or rollback manifest.
func MaterializeObservedLiveImageManifest(
	baseManifest, observedWorkloads, ownership []byte,
	defaultNamespace string,
) ([]byte, error) {
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return nil, fmt.Errorf("load ownership: %w", err)
	}
	baseObjects, baseUnknown := decodeManifest(baseManifest, spec, defaultNamespace, "observed-live base")
	observedObjects, observedUnknown := decodeManifest(observedWorkloads, spec, defaultNamespace, "observed-live workload snapshot")
	if len(baseUnknown) != 0 || len(observedUnknown) != 0 {
		return nil, manifestEvidenceError(append(baseUnknown, observedUnknown...))
	}
	baseByIdentity, duplicateBase := indexManifestObjects(baseObjects, "observed-live base")
	observedByIdentity, duplicateObserved := indexManifestObjects(observedObjects, "observed-live workload snapshot")
	if len(duplicateBase) != 0 || len(duplicateObserved) != 0 {
		return nil, manifestEvidenceError(append(duplicateBase, duplicateObserved...))
	}

	materialized := make([]manifestObject, 0, len(baseByIdentity))
	for _, key := range sortedManifestIdentityKeys(baseByIdentity) {
		base := baseByIdentity[key]
		base.Object = cloneManifestMap(base.Object)
		baseContainers, baseIsWorkload, containerErr := workloadContainers(base)
		if containerErr != nil {
			return nil, containerErr
		}
		if !baseIsWorkload {
			materialized = append(materialized, base)
			continue
		}
		observed, exists := observedByIdentity[key]
		if !exists {
			return nil, fmt.Errorf("observed live workload is missing for %s", base.Identity.String())
		}
		observedContainers, observedIsWorkload, observedErr := workloadContainers(observed)
		if observedErr != nil {
			return nil, observedErr
		}
		if !observedIsWorkload {
			return nil, fmt.Errorf("observed live object is not a workload for %s", base.Identity.String())
		}
		for _, baseContainer := range sortedRenderedContainers(baseContainers) {
			observedContainer, found := observedContainers[baseContainer.Name]
			if !found {
				return nil, fmt.Errorf("observed live workload container is missing for %s/%s", base.Identity.String(), baseContainer.Name)
			}
			if err := setRenderedContainerImage(base, baseContainer.Pointer, observedContainer.Image); err != nil {
				return nil, err
			}
		}
		materialized = append(materialized, base)
	}
	result, err := encodeMaterializedTargetObjects(materialized)
	if err != nil {
		return nil, fmt.Errorf("encode observed live image manifest: %w", err)
	}
	if err := VerifyObservedLiveImageManifest(baseManifest, result, ownership, defaultNamespace); err != nil {
		return nil, err
	}
	return result, nil
}

// VerifyObservedLiveImageManifest proves that an observed-live witness is a
// canonical, identity-preserving copy of the Helm base whose only possible
// differences are image fields of containers already declared by that base.
func VerifyObservedLiveImageManifest(
	baseManifest, observedLiveManifest, ownership []byte,
	defaultNamespace string,
) error {
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return fmt.Errorf("load ownership: %w", err)
	}
	baseObjects, baseUnknown := decodeManifest(baseManifest, spec, defaultNamespace, "observed-live verification base")
	observedObjects, observedUnknown := decodeManifest(observedLiveManifest, spec, defaultNamespace, "observed-live verification witness")
	if len(baseUnknown) != 0 || len(observedUnknown) != 0 {
		return manifestEvidenceError(append(baseUnknown, observedUnknown...))
	}
	baseByIdentity, duplicateBase := indexManifestObjects(baseObjects, "observed-live verification base")
	observedByIdentity, duplicateObserved := indexManifestObjects(observedObjects, "observed-live verification witness")
	if len(duplicateBase) != 0 || len(duplicateObserved) != 0 {
		return manifestEvidenceError(append(duplicateBase, duplicateObserved...))
	}
	if len(baseByIdentity) != len(observedByIdentity) {
		return fmt.Errorf("observed live image manifest object set differs from Helm base")
	}
	canonicalObserved, err := encodeMaterializedTargetObjects(observedObjects)
	if err != nil {
		return fmt.Errorf("canonicalize observed live image manifest: %w", err)
	}
	if !bytes.Equal(canonicalObserved, observedLiveManifest) {
		return fmt.Errorf("observed live image manifest is not canonical")
	}

	for _, key := range sortedManifestIdentityKeys(baseByIdentity) {
		base := baseByIdentity[key]
		observed, exists := observedByIdentity[key]
		if !exists {
			return fmt.Errorf("observed live image manifest is missing %s", base.Identity.String())
		}
		observed.Object = cloneManifestMap(observed.Object)
		baseContainers, baseIsWorkload, baseErr := workloadContainers(base)
		if baseErr != nil {
			return baseErr
		}
		observedContainers, observedIsWorkload, observedErr := workloadContainers(observed)
		if observedErr != nil {
			return observedErr
		}
		if baseIsWorkload != observedIsWorkload {
			return fmt.Errorf("observed live workload kind differs for %s", base.Identity.String())
		}
		if baseIsWorkload {
			if len(baseContainers) != len(observedContainers) {
				return fmt.Errorf("observed live workload container set differs for %s", base.Identity.String())
			}
			for _, baseContainer := range sortedRenderedContainers(baseContainers) {
				observedContainer, found := observedContainers[baseContainer.Name]
				if !found || observedContainer.Pointer != baseContainer.Pointer {
					return fmt.Errorf("observed live workload container set differs for %s", base.Identity.String())
				}
				if err := setRenderedContainerImage(observed, observedContainer.Pointer, baseContainer.Image); err != nil {
					return err
				}
			}
		}
		if !reflect.DeepEqual(normalizedObject(base), normalizedObject(observed)) {
			return fmt.Errorf("observed live image manifest changes a non-image field for %s", base.Identity.String())
		}
	}
	return nil
}

func sortedManifestIdentityKeys(values map[string]manifestObject) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
