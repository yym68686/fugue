package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// DisabledPublicEdgeWorkerObservationAnnotation is reserved for the private,
// pre-write observed-live witness. Helm-rendered manifests must never set it.
const DisabledPublicEdgeWorkerObservationAnnotation = "release-domain.fugue.dev/disabled-public-edge-worker-v1"

const disabledPublicEdgeWorkerObservationPolicy = "disabled-public-edge-worker-v1"

type disabledPublicEdgeWorkerObservation struct {
	Policy                 string `json:"policy"`
	IdentityDigest         string `json:"identityDigest"`
	UIDDigest              string `json:"uidDigest"`
	ResourceVersionDigest  string `json:"resourceVersionDigest"`
	Generation             int64  `json:"generation"`
	ObservedGeneration     int64  `json:"observedGeneration"`
	DesiredNumberScheduled int64  `json:"desiredNumberScheduled"`
	CurrentNumberScheduled int64  `json:"currentNumberScheduled"`
	NumberReady            int64  `json:"numberReady"`
	NumberAvailable        int64  `json:"numberAvailable"`
	UpdatedNumberScheduled int64  `json:"updatedNumberScheduled"`
	NumberUnavailable      int64  `json:"numberUnavailable"`
	NumberMisscheduled     int64  `json:"numberMisscheduled"`
	DeletionTimestampEmpty bool   `json:"deletionTimestampEmpty"`
	RuntimeMetadataDigest  string `json:"runtimeMetadataDigest,omitempty"`
	NonImageRenderedDigest string `json:"nonImageRenderedDigest,omitempty"`
}

// CaptureDisabledPublicEdgeWorkerObservation returns a private witness marker
// only for the exact dynamic public Edge worker DaemonSet whose observed
// generation is fully converged at zero. Missing Kubernetes zero-value status
// fields normalize to zero; malformed, negative, or non-zero values fail
// closed. The caller still has to bind the marker to the Helm base and target.
func CaptureDisabledPublicEdgeWorkerObservation(data []byte, defaultNamespace string) (string, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return "", false, fmt.Errorf("decode disabled public edge worker observation: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return "", false, fmt.Errorf("disabled public edge worker observation contains trailing data")
	}
	identity, labels, candidate, err := disabledPublicEdgeWorkerRawIdentity(raw, defaultNamespace)
	if err != nil || !candidate {
		return "", candidate, err
	}
	metadata, _ := raw["metadata"].(map[string]any)
	status, ok := raw["status"].(map[string]any)
	if !ok {
		return "", true, fmt.Errorf("disabled public edge worker status is missing")
	}
	uid, err := requiredObservedString(metadata, "uid")
	if err != nil {
		return "", true, err
	}
	resourceVersion, err := requiredObservedString(metadata, "resourceVersion")
	if err != nil {
		return "", true, err
	}
	generation, err := requiredObservedPositiveInteger(metadata, "generation")
	if err != nil {
		return "", true, err
	}
	observedGeneration, err := requiredObservedPositiveInteger(status, "observedGeneration")
	if err != nil {
		return "", true, err
	}
	if generation != observedGeneration {
		return "", true, fmt.Errorf("disabled public edge worker generation is not observed")
	}
	annotations, err := stringMap(metadata["annotations"], "metadata.annotations")
	if err != nil {
		return "", true, err
	}
	for key, expected := range map[string]string{
		"deprecated.daemonset.template.generation": strconv.FormatInt(generation, 10),
		"meta.helm.sh/release-name":                labels["app.kubernetes.io/instance"],
		"meta.helm.sh/release-namespace":           identity.Namespace,
	} {
		if value, exists := annotations[key]; exists && value != expected {
			return "", true, fmt.Errorf("disabled public edge worker server annotation %s is invalid", key)
		}
	}
	if deletion, exists := metadata["deletionTimestamp"]; exists && deletion != nil {
		return "", true, fmt.Errorf("disabled public edge worker is deleting")
	}
	runtimeMetadataDigest, err := disabledPublicEdgeWorkerRuntimeMetadataDigest(raw)
	if err != nil {
		return "", true, err
	}

	counts := make(map[string]int64, 7)
	for _, field := range []string{
		"desiredNumberScheduled", "currentNumberScheduled", "numberReady",
		"numberAvailable", "updatedNumberScheduled", "numberUnavailable",
		"numberMisscheduled",
	} {
		value, err := normalizedObservedNonNegativeInteger(status, field)
		if err != nil {
			return "", true, err
		}
		if value != 0 {
			return "", true, fmt.Errorf("disabled public edge worker %s is non-zero", field)
		}
		counts[field] = value
	}
	marker := disabledPublicEdgeWorkerObservation{
		Policy:                disabledPublicEdgeWorkerObservationPolicy,
		IdentityDigest:        disabledPublicEdgeWorkerIdentityDigest(identity, labels),
		UIDDigest:             digestDisabledObservationText(uid),
		ResourceVersionDigest: digestDisabledObservationText(resourceVersion),
		Generation:            generation, ObservedGeneration: observedGeneration,
		DesiredNumberScheduled: counts["desiredNumberScheduled"],
		CurrentNumberScheduled: counts["currentNumberScheduled"],
		NumberReady:            counts["numberReady"], NumberAvailable: counts["numberAvailable"],
		UpdatedNumberScheduled: counts["updatedNumberScheduled"],
		NumberUnavailable:      counts["numberUnavailable"],
		NumberMisscheduled:     counts["numberMisscheduled"],
		DeletionTimestampEmpty: true,
		RuntimeMetadataDigest:  runtimeMetadataDigest,
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return "", true, fmt.Errorf("encode disabled public edge worker observation: %w", err)
	}
	return string(encoded), true, nil
}

func disabledPublicEdgeWorkerRuntimeMetadataDigest(raw map[string]any) (string, error) {
	templateMetadata, ok := nestedManifestMap(raw, "spec", "template", "metadata")
	if !ok {
		return "", fmt.Errorf("disabled public edge worker template metadata is missing")
	}
	annotations, err := stringMap(templateMetadata["annotations"], "spec.template.metadata.annotations")
	if err != nil {
		return "", err
	}
	releaseID, hasReleaseID := annotations["fugue.io/public-data-plane-release-id"]
	releaseMode, hasReleaseMode := annotations["fugue.io/public-data-plane-release-mode"]
	if !hasReleaseID && !hasReleaseMode {
		return "", nil
	}
	if !hasReleaseID || !hasReleaseMode || !validContractText(releaseID, 253) ||
		!strings.HasPrefix(releaseID, "pdp-") || releaseMode != "node-local-blue-green-worker" {
		return "", fmt.Errorf("disabled public edge worker runtime metadata is invalid")
	}
	return digestDisabledObservationText(releaseID + "\x00" + releaseMode), nil
}

// IsDisabledPublicEdgeWorkerServerAnnotation identifies the exact API/Helm
// metadata annotations validated by CaptureDisabledPublicEdgeWorkerObservation
// and excluded from the Helm-rendered equality comparison.
func IsDisabledPublicEdgeWorkerServerAnnotation(key string) bool {
	switch key {
	case "deprecated.daemonset.template.generation",
		"meta.helm.sh/release-name",
		"meta.helm.sh/release-namespace":
		return true
	default:
		return false
	}
}

// IsDisabledPublicEdgeWorkerRuntimeAnnotation identifies operational
// blue-green metadata that is validated and digest-bound in the private
// marker instead of being treated as Helm-rendered scheduling state.
func IsDisabledPublicEdgeWorkerRuntimeAnnotation(key string) bool {
	return key == "fugue.io/public-data-plane-release-id" ||
		key == "fugue.io/public-data-plane-release-mode"
}

func disabledPublicEdgeWorkerRawIdentity(raw map[string]any, defaultNamespace string) (ObjectIdentity, map[string]string, bool, error) {
	apiVersion, _ := raw["apiVersion"].(string)
	kind, _ := raw["kind"].(string)
	if apiVersion != "apps/v1" || kind != "DaemonSet" {
		return ObjectIdentity{}, nil, false, nil
	}
	metadata, ok := raw["metadata"].(map[string]any)
	if !ok {
		return ObjectIdentity{}, nil, false, nil
	}
	name, _ := metadata["name"].(string)
	slot := disabledPublicEdgeWorkerSlot(name)
	if slot == "" {
		return ObjectIdentity{}, nil, false, nil
	}
	namespace, _ := metadata["namespace"].(string)
	if namespace == "" {
		namespace = defaultNamespace
	}
	if namespace == "" || namespace != defaultNamespace {
		return ObjectIdentity{}, nil, false, nil
	}
	labels, err := stringMap(metadata["labels"], "metadata.labels")
	if err != nil {
		return ObjectIdentity{}, nil, true, err
	}
	if !disabledPublicEdgeWorkerLabels(labels, slot) {
		return ObjectIdentity{}, nil, false, nil
	}
	return ObjectIdentity{APIGroup: "apps", Version: "v1", Kind: "DaemonSet", Namespace: namespace, Name: name}, labels, true, nil
}

func disabledPublicEdgeWorkerIdentity(object manifestObject) (string, bool) {
	if object.Identity.APIGroup != "apps" || object.Identity.Version != "v1" || object.Identity.Kind != "DaemonSet" || object.Identity.Namespace == "" {
		return "", false
	}
	slot := disabledPublicEdgeWorkerSlot(object.Identity.Name)
	return slot, slot != "" && disabledPublicEdgeWorkerLabels(object.Labels, slot)
}

func disabledPublicEdgeWorkerSlot(name string) string {
	for _, slot := range []string{"a", "b"} {
		if strings.HasSuffix(name, "-edge-dynamic-worker-"+slot) {
			return slot
		}
	}
	return ""
}

func disabledPublicEdgeWorkerLabels(labels map[string]string, slot string) bool {
	return labels["app.kubernetes.io/instance"] != "" &&
		labels["app.kubernetes.io/component"] == "edge-dynamic-worker-"+slot &&
		labels["fugue.io/rollout-subsystem"] == "public-data-plane" &&
		labels["fugue.io/rollout-mode"] == "node-local-blue-green-worker" &&
		labels["fugue.io/downtime-class"] == "online-required" &&
		labels["fugue.io/edge-slot"] == slot
}

func requiredObservedString(values map[string]any, field string) (string, error) {
	value, ok := values[field].(string)
	if !ok || value == "" || strings.TrimSpace(value) != value {
		return "", fmt.Errorf("disabled public edge worker %s is invalid", field)
	}
	return value, nil
}

func requiredObservedPositiveInteger(values map[string]any, field string) (int64, error) {
	value, exists := values[field]
	if !exists {
		return 0, fmt.Errorf("disabled public edge worker %s is missing", field)
	}
	parsed, err := observedInteger(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("disabled public edge worker %s is invalid", field)
	}
	return parsed, nil
}

func normalizedObservedNonNegativeInteger(values map[string]any, field string) (int64, error) {
	value, exists := values[field]
	if !exists {
		return 0, nil
	}
	parsed, err := observedInteger(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("disabled public edge worker %s is invalid", field)
	}
	return parsed, nil
}

func observedInteger(value any) (int64, error) {
	switch typed := value.(type) {
	case json.Number:
		return typed.Int64()
	case int:
		return int64(typed), nil
	case int64:
		return typed, nil
	default:
		return 0, fmt.Errorf("not an integer")
	}
}

func digestDisabledObservationText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func disabledPublicEdgeWorkerIdentityDigest(identity ObjectIdentity, labels map[string]string) string {
	slot := disabledPublicEdgeWorkerSlot(identity.Name)
	return digestDisabledObservationText(strings.Join([]string{
		identity.APIGroup, identity.Version, identity.Kind, identity.Namespace, identity.Name,
		labels["app.kubernetes.io/instance"], labels["app.kubernetes.io/component"],
		labels["fugue.io/rollout-subsystem"], labels["fugue.io/rollout-mode"],
		labels["fugue.io/downtime-class"], slot,
	}, "\x00"))
}

func parseDisabledPublicEdgeWorkerObservation(value string, requireRenderedDigest bool) (disabledPublicEdgeWorkerObservation, error) {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var marker disabledPublicEdgeWorkerObservation
	if err := decoder.Decode(&marker); err != nil {
		return marker, fmt.Errorf("disabled public edge worker observation is invalid: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return marker, fmt.Errorf("disabled public edge worker observation contains trailing data")
	}
	canonical, err := json.Marshal(marker)
	if err != nil || string(canonical) != value {
		return marker, fmt.Errorf("disabled public edge worker observation is not canonical")
	}
	if marker.Policy != disabledPublicEdgeWorkerObservationPolicy ||
		validateCanonicalSHA256Digest(marker.IdentityDigest, "disabled worker identity digest") != nil ||
		validateCanonicalSHA256Digest(marker.UIDDigest, "disabled worker UID digest") != nil ||
		validateCanonicalSHA256Digest(marker.ResourceVersionDigest, "disabled worker resourceVersion digest") != nil ||
		marker.Generation <= 0 || marker.ObservedGeneration != marker.Generation ||
		!marker.DeletionTimestampEmpty ||
		marker.DesiredNumberScheduled != 0 || marker.CurrentNumberScheduled != 0 ||
		marker.NumberReady != 0 || marker.NumberAvailable != 0 ||
		marker.UpdatedNumberScheduled != 0 || marker.NumberUnavailable != 0 ||
		marker.NumberMisscheduled != 0 {
		return marker, fmt.Errorf("disabled public edge worker observation is not a converged zero-state witness")
	}
	if marker.RuntimeMetadataDigest != "" {
		if err := validateCanonicalSHA256Digest(marker.RuntimeMetadataDigest, "disabled worker runtime metadata digest"); err != nil {
			return marker, err
		}
	}
	if requireRenderedDigest {
		if err := validateCanonicalSHA256Digest(marker.NonImageRenderedDigest, "disabled worker non-image rendered digest"); err != nil {
			return marker, err
		}
	} else if marker.NonImageRenderedDigest != "" {
		return marker, fmt.Errorf("capture-time disabled public edge worker observation already has a rendered digest")
	}
	return marker, nil
}

func disabledPublicEdgeWorkerNonImageObject(object manifestObject) (map[string]any, error) {
	result := normalizedObject(object)
	if result == nil {
		return nil, fmt.Errorf("disabled public edge worker object is empty")
	}
	metadata, ok := result["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("disabled public edge worker metadata is missing")
	}
	if annotations, ok := metadata["annotations"].(map[string]any); ok {
		delete(annotations, DisabledPublicEdgeWorkerObservationAnnotation)
		if len(annotations) == 0 {
			delete(metadata, "annotations")
		}
	}
	containers, workload, err := workloadContainers(manifestObject{Identity: object.Identity, Object: result})
	if err != nil {
		return nil, err
	}
	if !workload || len(containers) != 3 {
		return nil, fmt.Errorf("disabled public edge worker must have exactly the edge, caddy, and Edge identity init containers")
	}
	edge, edgeExists := containers["edge"]
	caddy, caddyExists := containers["caddy"]
	identity, identityExists := containers[publicDataPlaneEdgeIdentityContainer]
	if !edgeExists || edge.Pointer != "/spec/template/spec/containers/0/image" ||
		!caddyExists || caddy.Pointer != "/spec/template/spec/containers/1/image" ||
		!identityExists || identity.Pointer != "/spec/template/spec/initContainers/0/image" || identity.Image != edge.Image {
		return nil, fmt.Errorf("disabled public edge worker container identity is invalid")
	}
	if err := deleteRenderedContainerImage(result, edge.Pointer); err != nil {
		return nil, err
	}
	if err := deleteRenderedContainerImage(result, identity.Pointer); err != nil {
		return nil, err
	}
	return result, nil
}

func deleteRenderedContainerImage(root map[string]any, pointer string) error {
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	if len(parts) < 4 || parts[len(parts)-1] != "image" {
		return fmt.Errorf("rendered workload image pointer is invalid")
	}
	current := any(root)
	for _, part := range parts[:len(parts)-1] {
		switch typed := current.(type) {
		case map[string]any:
			var exists bool
			current, exists = typed[part]
			if !exists {
				return fmt.Errorf("rendered workload image pointer is missing")
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return fmt.Errorf("rendered workload image pointer index is invalid")
			}
			current = typed[index]
		default:
			return fmt.Errorf("rendered workload image pointer is invalid")
		}
	}
	container, ok := current.(map[string]any)
	if !ok {
		return fmt.Errorf("rendered workload container is invalid")
	}
	delete(container, "image")
	return nil
}

func disabledPublicEdgeWorkerNonImageDigest(object manifestObject) (string, error) {
	nonImage, err := disabledPublicEdgeWorkerNonImageObject(object)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(nonImage)
	if err != nil {
		return "", fmt.Errorf("encode disabled public edge worker non-image object: %w", err)
	}
	return digestOperationalBytes(encoded), nil
}

func disabledPublicEdgeWorkerBaseMatchesLive(base, live manifestObject) (string, error) {
	baseNonImage, err := disabledPublicEdgeWorkerNonImageObject(base)
	if err != nil {
		return "", err
	}
	liveNonImage, err := disabledPublicEdgeWorkerNonImageObject(live)
	if err != nil {
		return "", err
	}
	return manifestObservedMismatch(baseNonImage, liveNonImage, ""), nil
}

func manifestObservedMismatch(expected, actual any, path string) string {
	switch expectedValue := expected.(type) {
	case map[string]any:
		actualValue, ok := actual.(map[string]any)
		if !ok {
			return path
		}
		for key, value := range expectedValue {
			actualChild, exists := actualValue[key]
			childPath := path + "/" + escapeJSONPointerToken(key)
			if !exists {
				if allowedDisabledWorkerObservedOmission(childPath, value) {
					continue
				}
				return childPath
			}
			if mismatch := manifestObservedMismatch(value, actualChild, childPath); mismatch != "" {
				return mismatch
			}
		}
		for key, value := range actualValue {
			if _, exists := expectedValue[key]; !exists &&
				!allowedDisabledWorkerObservedDefault(path+"/"+escapeJSONPointerToken(key), value) {
				return fmt.Sprintf("%s/%s (unexpected %T)", path, escapeJSONPointerToken(key), value)
			}
		}
		return ""
	case []any:
		actualValue, ok := actual.([]any)
		if !ok || len(actualValue) != len(expectedValue) {
			return path
		}
		for index := range expectedValue {
			if mismatch := manifestObservedMismatch(expectedValue[index], actualValue[index], path+"/"+strconv.Itoa(index)); mismatch != "" {
				return mismatch
			}
		}
		return ""
	default:
		if reflect.DeepEqual(expected, actual) {
			return ""
		}
		expectedInteger, expectedOK := manifestInteger(expected)
		actualInteger, actualOK := manifestInteger(actual)
		if expectedOK && actualOK && expectedInteger == actualInteger {
			return ""
		}
		return fmt.Sprintf("%s (expected %T, observed %T)", path, expected, actual)
	}
}

func manifestInteger(value any) (int64, bool) {
	if typed, ok := value.(manifestNumber); ok {
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return parsed, err == nil
	}
	if typed, ok := value.(json.Number); ok {
		parsed, err := typed.Int64()
		return parsed, err == nil
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return 0, false
	}
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflected.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		unsigned := reflected.Uint()
		if unsigned <= math.MaxInt64 {
			return int64(unsigned), true
		}
	case reflect.Float32, reflect.Float64:
		floating := reflected.Float()
		if !math.IsNaN(floating) && !math.IsInf(floating, 0) && math.Trunc(floating) == floating &&
			floating >= math.MinInt64 && floating < -math.MinInt64 {
			return int64(floating), true
		}
	}
	return 0, false
}

func allowedDisabledWorkerObservedDefault(path string, value any) bool {
	switch path {
	case "/spec/template/metadata/creationTimestamp":
		return value == nil
	case "/spec/template/spec/dnsPolicy":
		return value == "ClusterFirst"
	case "/spec/template/spec/restartPolicy":
		return value == "Always"
	case "/spec/template/spec/schedulerName":
		return value == "default-scheduler"
	case "/spec/template/spec/securityContext":
		object, ok := value.(map[string]any)
		return ok && len(object) == 0
	case "/spec/template/spec/terminationGracePeriodSeconds":
		integer, ok := manifestInteger(value)
		return ok && integer == 30
	case "/spec/template/spec/enableServiceLinks":
		boolean, ok := value.(bool)
		return ok && boolean
	case "/spec/template/spec/serviceAccount", "/spec/template/spec/serviceAccountName":
		return value == "default"
	}
	if strings.HasPrefix(path, "/spec/template/spec/containers/") ||
		strings.HasPrefix(path, "/spec/template/spec/initContainers/") {
		switch {
		case strings.Contains(path, "/env/") && strings.HasSuffix(path, "/valueFrom/fieldRef/apiVersion"):
			return value == "v1"
		case (strings.Contains(path, "/livenessProbe/") || strings.Contains(path, "/readinessProbe/") ||
			strings.Contains(path, "/startupProbe/")) && strings.HasSuffix(path, "/httpGet/scheme"):
			return value == "HTTP"
		case (strings.Contains(path, "/livenessProbe/") || strings.Contains(path, "/readinessProbe/") ||
			strings.Contains(path, "/startupProbe/")) && strings.HasSuffix(path, "/successThreshold"):
			integer, ok := manifestInteger(value)
			return ok && integer == 1
		case strings.HasSuffix(path, "/terminationMessagePath"):
			return value == "/dev/termination-log"
		case strings.HasSuffix(path, "/terminationMessagePolicy"):
			return value == "File"
		case strings.HasSuffix(path, "/resources"):
			object, ok := value.(map[string]any)
			return ok && len(object) == 0
		}
	}
	if strings.HasPrefix(path, "/spec/template/spec/volumes/") && strings.HasSuffix(path, "/secret/defaultMode") {
		integer, ok := manifestInteger(value)
		return ok && integer == 420
	}
	return false
}

func allowedDisabledWorkerObservedOmission(path string, value any) bool {
	return strings.HasPrefix(path, "/spec/template/spec/containers/") &&
		strings.Contains(path, "/env/") && strings.HasSuffix(path, "/value") && value == ""
}

func setDisabledPublicEdgeWorkerObservation(object *manifestObject, marker disabledPublicEdgeWorkerObservation) error {
	metadata, ok := object.Object["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("disabled public edge worker metadata is missing")
	}
	annotations, _ := metadata["annotations"].(map[string]any)
	if annotations == nil {
		annotations = map[string]any{}
		metadata["annotations"] = annotations
	}
	if _, exists := annotations[DisabledPublicEdgeWorkerObservationAnnotation]; exists {
		return fmt.Errorf("disabled public edge worker observation annotation already exists")
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode disabled public edge worker observation: %w", err)
	}
	annotations[DisabledPublicEdgeWorkerObservationAnnotation] = string(encoded)
	return nil
}

func removeDisabledPublicEdgeWorkerObservation(object *manifestObject) {
	metadata, _ := object.Object["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	delete(annotations, DisabledPublicEdgeWorkerObservationAnnotation)
	if len(annotations) == 0 {
		delete(metadata, "annotations")
	}
}

func disabledPublicEdgeWorkerImageChangeIsInactive(
	spec *OwnershipSpec,
	context ClassificationContextEvidence,
	base, target manifestObject,
	targetContainer renderedContainer,
) (bool, error) {
	value, marked := base.Annotations[DisabledPublicEdgeWorkerObservationAnnotation]
	if !marked {
		return false, nil
	}
	baseSlot, baseCandidate := disabledPublicEdgeWorkerIdentity(base)
	targetSlot, targetCandidate := disabledPublicEdgeWorkerIdentity(target)
	if !baseCandidate || !targetCandidate || baseSlot != targetSlot || base.Identity != target.Identity ||
		(targetContainer.Name != "edge" && targetContainer.Name != publicDataPlaneEdgeIdentityContainer) {
		return false, nil
	}
	if _, reserved := target.Annotations[DisabledPublicEdgeWorkerObservationAnnotation]; reserved {
		return false, fmt.Errorf("target manifest contains reserved disabled public edge worker observation")
	}
	marker, err := parseDisabledPublicEdgeWorkerObservation(value, true)
	if err != nil {
		return false, err
	}
	if marker.IdentityDigest != disabledPublicEdgeWorkerIdentityDigest(base.Identity, base.Labels) {
		return false, fmt.Errorf("disabled public edge worker observation identity binding mismatch")
	}
	digest, err := disabledPublicEdgeWorkerNonImageDigest(base)
	if err != nil {
		return false, err
	}
	if digest != marker.NonImageRenderedDigest {
		return false, fmt.Errorf("disabled public edge worker rendered binding mismatch")
	}
	baseNonImage, err := disabledPublicEdgeWorkerNonImageObject(base)
	if err != nil {
		return false, err
	}
	targetNonImage, err := disabledPublicEdgeWorkerNonImageObject(target)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(baseNonImage, targetNonImage) {
		return false, nil
	}
	rule, err := uniqueActivationObjectRule(spec, base, target, context)
	if err != nil {
		return false, nil
	}
	if rule.domainForPointer(targetContainer.Pointer) != DomainAuthoritativeDNS {
		return false, nil
	}
	return true, nil
}
