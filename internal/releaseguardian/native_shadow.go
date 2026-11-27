package releaseguardian

import (
	"context"
	"errors"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NativeReleaseShadow compares the real legacy publisher's planning path
// with a Fugue-native draft. It does not execute or authorize either plan.
type NativeReleaseShadow struct {
	Kind           string             `json:"kind"`
	IntentDigest   string             `json:"intentDigest"`
	DeploymentUID  string             `json:"deploymentUid"`
	Legacy         PublicationPreview `json:"legacy"`
	Native         PublicationPreview `json:"native"`
	EvidenceDigest string             `json:"evidenceDigest"`
}

func (store *KubeStore) CompareNativeReleaseShadow(ctx context.Context, key Key, clusterID, requestID string, files map[string][]byte) (NativeReleaseShadow, error) {
	legacy, err := store.PreviewDesired(ctx, key, files)
	if err != nil {
		return NativeReleaseShadow{}, err
	}
	native, err := store.DraftNativeRelease(ctx, key, clusterID, requestID, files)
	if err != nil {
		return NativeReleaseShadow{}, err
	}
	if !equivalentShadowPreviews(legacy, native.Preview) {
		return NativeReleaseShadow{}, errors.New("native and legacy release previews differ; migration remains shadow-only")
	}
	bundle, err := DecodeExecutionBundle(files, key)
	if err != nil {
		return NativeReleaseShadow{}, err
	}
	readWorkload := func() (string, error) {
		workload := native.Preview.PrewriteWorkload
		live, err := store.client.AppsV1().Deployments(workload.Namespace).Get(ctx, workload.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if string(live.UID) != bundle.Prepared.Prewrite.UID || live.ResourceVersion != native.Preview.PrewriteResourceVersion {
			return "", errors.New("live Deployment no longer matches the shared prewrite binding")
		}
		return string(live.UID), nil
	}
	uid, err := readWorkload()
	if err != nil {
		return NativeReleaseShadow{}, err
	}
	fresh, err := store.PreviewDesired(ctx, key, files)
	if err != nil {
		return NativeReleaseShadow{}, err
	}
	if !equivalentShadowPreviews(legacy, fresh) {
		return NativeReleaseShadow{}, errors.New("provider evidence changed during release shadow comparison")
	}
	if _, err := readWorkload(); err != nil {
		return NativeReleaseShadow{}, err
	}
	intentDigest, err := native.Intent.Digest()
	if err != nil {
		return NativeReleaseShadow{}, err
	}
	observation := NativeReleaseShadow{Kind: "GuardianReleaseShadowComparison", IntentDigest: intentDigest, DeploymentUID: uid, Legacy: legacy, Native: native.Preview}
	raw, err := declarativerelease.CanonicalJSON(observation)
	if err != nil {
		return NativeReleaseShadow{}, err
	}
	observation.EvidenceDigest = digest(raw)
	return observation, nil
}

func equivalentShadowPreviews(a, b PublicationPreview) bool {
	// Local and dependency observation clocks are stamped at read time; the
	// route timestamp is an independent canary observation and must agree.
	a.Health.Local.ObservedAt, b.Health.Local.ObservedAt = "", ""
	a.Health.Dependency.ObservedAt, b.Health.Dependency.ObservedAt = "", ""
	return a == b
}
