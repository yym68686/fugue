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
// The initial native lane supports only healthy, settled predecessors; repair
// and takeover of an in-flight legacy operation require separate proofs.
func (store *KubeStore) DraftNativeRelease(ctx context.Context, key Key, clusterID, requestID string, files map[string][]byte) (NativeReleaseDraft, error) {
	publication, err := store.planDesiredPublication(ctx, key, files)
	if err != nil {
		return NativeReleaseDraft{}, err
	}
	preview := publication.preview
	if preview.CurrentRecordDigest != preview.CurrentDesired.RecordDigest ||
		preview.CurrentRecordDigest != preview.Record.LKGRecordDigest ||
		preview.Health.Local.State != HealthHealthy || preview.Health.Dependency.State != HealthHealthy || preview.Health.Route.State != HealthHealthy {
		return NativeReleaseDraft{}, errors.New("native release draft requires a healthy settled Guardian predecessor")
	}
	boundFiles, err := executionFilesFromStrings(publication.record.Data)
	if err != nil {
		return NativeReleaseDraft{}, err
	}
	workload := preview.PrewriteWorkload
	if workload.Namespace != preview.Namespace || workload.Kind == "" || workload.Name == "" {
		return NativeReleaseDraft{}, errors.New("native release draft does not identify a workload in the configured namespace")
	}
	payload, err := declarativerelease.CanonicalJSON(nativeReleasePayload{
		Namespace: preview.Namespace, ProposedRecord: preview.Record, ExpectedDesiredDigest: preview.CurrentDesired.RecordDigest,
		RollbackManifest: preview.RollbackManifestDigest, Files: boundFiles,
	})
	if err != nil {
		return NativeReleaseDraft{}, err
	}
	resourceID := "kubernetes/" + workload.Namespace + "/" + strings.ToLower(workload.Kind) + "/" + workload.Name
	intent, err := productioncontrol.NewIntent(clusterID, resourceID, requestID, productioncontrol.ComponentRelease, preview.CurrentDesired.Generation,
		preview.CurrentRecordDigest, preview.Record.RecordDigest, preview.Record.LKGRecordDigest, payload)
	if err != nil {
		return NativeReleaseDraft{}, err
	}
	return NativeReleaseDraft{Intent: intent, Payload: payload, Preview: preview}, nil
}
