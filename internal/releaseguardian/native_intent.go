package releaseguardian

import (
	"context"
	"errors"
	"strings"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
)

// NativeReleaseDraft binds a preflight observation to the exact execution
// bundle for persistent owner review. It cannot advance DesiredRelease.
type NativeReleaseDraft struct {
	Intent  productioncontrol.Intent
	Payload []byte
	Preview PublicationPreview
}

type nativeReleasePayload struct {
	Namespace             string            `json:"namespace"`
	ProposedRecord        ReleaseRecord     `json:"proposedRecord"`
	ExpectedDesiredDigest string            `json:"expectedDesiredDigest"`
	RollbackManifest      string            `json:"rollbackManifestDigest"`
	Files                 map[string][]byte `json:"files"`
}

// DraftNativeRelease reuses the legacy publisher's exact admission preview.
// The initial native lane supports only an existing Deployment with a healthy,
// settled predecessor and no shared/delegated writes. Repair and takeover of
// an in-flight legacy operation require separate proofs.
func (store *KubeStore) DraftNativeRelease(ctx context.Context, key Key, clusterID, requestID string, files map[string][]byte) (NativeReleaseDraft, error) {
	publication, err := store.planDesiredPublicationFor(ctx, key, files, true)
	if err != nil {
		return NativeReleaseDraft{}, err
	}
	preview := publication.preview
	workload := preview.PrewriteWorkload
	if workload.Namespace != preview.Namespace || workload.Kind == "" || workload.Name == "" {
		return NativeReleaseDraft{}, errors.New("native release draft does not identify a workload in the configured namespace")
	}
	binding := nativeReleasePayload{
		Namespace: preview.Namespace, ProposedRecord: preview.Record, ExpectedDesiredDigest: preview.CurrentDesired.RecordDigest,
		RollbackManifest: preview.RollbackManifestDigest, Files: publication.bundle.Files,
	}
	payload, err := declarativerelease.CanonicalJSON(binding)
	if err != nil {
		return NativeReleaseDraft{}, err
	}
	resourceID := nativeKubernetesResourceID(workload)
	intent, err := productioncontrol.NewIntent(clusterID, resourceID, requestID, productioncontrol.ComponentRelease, preview.CurrentDesired.Generation,
		preview.CurrentRecordDigest, preview.Record.RecordDigest, preview.Record.LKGRecordDigest, payload)
	if err != nil {
		return NativeReleaseDraft{}, err
	}
	if _, err := bindNativePublication(intent, binding, publication); err != nil {
		return NativeReleaseDraft{}, err
	}
	return NativeReleaseDraft{Intent: intent, Payload: payload, Preview: preview}, nil
}

func nativeKubernetesResourceID(workload declarativerelease.ResourceIdentity) string {
	return "kubernetes/" + workload.Namespace + "/" + strings.ToLower(workload.Kind) + "/" + workload.Name
}
