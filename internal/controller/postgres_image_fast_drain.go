package controller

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"
)

type postgresImageDrainWitness struct {
	BudgetSeconds   int    `json:"budget_seconds"`
	OriginalSeconds int    `json:"original_seconds"`
	Marker          string `json:"marker"`
	HadPostgresql   bool   `json:"had_postgresql"`
	HadParameters   bool   `json:"had_parameters"`
}

func configurePostgresImageDrain(w *postgresImageWitness, cluster map[string]any, opID string, budget int) error {
	spec := imageObject(cluster["spec"])
	original, ok := spec["smartShutdownTimeout"].(float64)
	stop, stopOK := spec["stopDelay"].(float64)
	rawPostgresql, hadPostgresql := spec["postgresql"]
	pg := imageObject(rawPostgresql)
	_, hadParameters := pg["parameters"]
	params := imageObject(pg["parameters"])
	_, reserved := params[pgimage.DrainMarker]
	if !ok || original != float64(int(original)) || original <= float64(budget) || !stopOK || stop <= original || reserved {
		return fmt.Errorf("fast drain requires an explicit longer smart shutdown timeout, intact stop delay and an unused drain marker")
	}
	w.Drain = &postgresImageDrainWitness{BudgetSeconds: budget, OriginalSeconds: int(original), Marker: pgimage.Hash([]string{opID, w.ClusterUID, w.PodUID}), HadPostgresql: hadPostgresql, HadParameters: hadParameters}
	return nil
}

// Return the original spec view only for this exact operation's temporary
// settings. Partial writes, a foreign marker or any independent change fail
// closed; the resulting spec is hashed by the caller before any mutation.
func originalImageDrainSpec(cluster map[string]any, drain *postgresImageDrainWitness) (map[string]any, bool, error) {
	if drain == nil {
		return cluster, false, nil
	}
	spec := imageObject(cluster["spec"])
	pg := imageObject(spec["postgresql"])
	params := imageObject(pg["parameters"])
	marker, exists := params[pgimage.DrainMarker]
	timeout := spec["smartShutdownTimeout"]
	if !exists && timeout == float64(drain.OriginalSeconds) {
		return cluster, false, nil
	}
	if !exists || marker != drain.Marker || timeout != float64(drain.BudgetSeconds) {
		return nil, false, fmt.Errorf("database drain settings differ from the original or operation-owned settings")
	}
	original := maps.Clone(spec)
	originalPG := maps.Clone(pg)
	originalParams := maps.Clone(params)
	delete(originalParams, pgimage.DrainMarker)
	if drain.HadParameters {
		originalPG["parameters"] = originalParams
	} else {
		if len(originalParams) != 0 {
			return nil, false, fmt.Errorf("database parameters were added during fast drain")
		}
		delete(originalPG, "parameters")
	}
	if drain.HadPostgresql {
		original["postgresql"] = originalPG
	} else {
		if len(originalPG) != 0 {
			return nil, false, fmt.Errorf("database configuration was added during fast drain")
		}
		delete(original, "postgresql")
	}
	original["smartShutdownTimeout"] = float64(drain.OriginalSeconds)
	return map[string]any{"spec": original, "metadata": cluster["metadata"]}, true, nil
}

func verifyImageDrainCluster(w postgresImageWitness, cluster map[string]any) (staged bool, err error) {
	original, staged, err := originalImageDrainSpec(cluster, w.Drain)
	if err != nil {
		return false, err
	}
	image := imageString(imageObject(cluster["spec"]), "imageName")
	target := image == w.TargetImage
	expected := w.SpecHash
	if target && w.RestartResourceTarget != nil {
		expected = w.RestartSpecHash
	}
	meta := imageObject(cluster["metadata"])
	if imageString(meta, "uid") != w.ClusterUID || imageString(imageObject(meta["annotations"]), coldMigrationAnnotation) != w.ColdMigration ||
		(!target && image != w.SourceImage) || imagePreservedSpecHash(original) != expected {
		return false, fmt.Errorf("database preserved specification changed during fast drain")
	}
	return staged, nil
}

func (c *kubeClient) patchImageDrain(ctx context.Context, w postgresImageWitness, cluster map[string]any, stage bool) error {
	staged, err := verifyImageDrainCluster(w, cluster)
	if err != nil || staged == stage {
		return err
	}
	meta, spec := imageObject(cluster["metadata"]), imageObject(cluster["spec"])
	if stage && imageString(spec, "imageName") != w.SourceImage {
		return fmt.Errorf("cannot stage fast drain after the image change")
	}
	rv := imageString(meta, "resourceVersion")
	if rv == "" {
		return fmt.Errorf("missing Cluster resourceVersion for fast drain compare-and-swap")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": w.ClusterUID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": rv},
		{"op": "test", "path": "/spec", "value": spec},
	}
	if stage {
		patch = append(patch, map[string]any{"op": "replace", "path": "/spec/smartShutdownTimeout", "value": w.Drain.BudgetSeconds})
		switch {
		case !w.Drain.HadPostgresql:
			patch = append(patch, map[string]any{"op": "add", "path": "/spec/postgresql", "value": map[string]any{"parameters": map[string]any{pgimage.DrainMarker: w.Drain.Marker}}})
		case !w.Drain.HadParameters:
			patch = append(patch, map[string]any{"op": "add", "path": "/spec/postgresql/parameters", "value": map[string]any{pgimage.DrainMarker: w.Drain.Marker}})
		default:
			patch = append(patch, map[string]any{"op": "add", "path": "/spec/postgresql/parameters/" + pgimage.DrainMarker, "value": w.Drain.Marker})
		}
	} else {
		patch = append(patch, map[string]any{"op": "replace", "path": "/spec/smartShutdownTimeout", "value": w.Drain.OriginalSeconds})
		switch {
		case !w.Drain.HadPostgresql:
			patch = append(patch, map[string]any{"op": "remove", "path": "/spec/postgresql"})
		case !w.Drain.HadParameters:
			patch = append(patch, map[string]any{"op": "remove", "path": "/spec/postgresql/parameters"})
		default:
			patch = append(patch, map[string]any{"op": "remove", "path": "/spec/postgresql/parameters/" + pgimage.DrainMarker})
		}
	}
	_, err = c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(w.Namespace, w.Cluster), "application/json-patch+json", patch, nil)
	return err
}

func readPostgresImageDrain(ctx context.Context, c *kubeClient, w postgresImageWitness) pgimage.DrainObservation {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := coldExec(ctx, c, w.Namespace, w.Pod, managedPostgresMainContainerName, pgimage.DrainObservationCommand(), &output); err != nil {
		return pgimage.UnavailableDrainObservation()
	}
	return pgimage.DecodeDrainObservation(output.Bytes(), w.Drain.BudgetSeconds)
}

func (s *Service) imageDrainSample(ctx context.Context, c *kubeClient, w postgresImageWitness) pgimage.DrainObservation {
	if s.postgresImageDrainProbe != nil {
		return s.postgresImageDrainProbe(ctx, c, w)
	}
	return readPostgresImageDrain(ctx, c, w)
}

func (s *Service) recordImageDrainSample(op model.Operation, sample pgimage.DrainObservation) {
	p := map[string]any{"status": sample.Status}
	if sample.Sessions != nil && sample.ActiveTransactions != nil && sample.IdleInTransaction != nil && sample.OldestTransactionMS != nil {
		p["sessions"], p["active_transactions"], p["idle_in_transaction"], p["oldest_transaction_ms"] =
			*sample.Sessions, *sample.ActiveTransactions, *sample.IdleInTransaction, *sample.OldestTransactionMS
	}
	_, _ = s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: op.TenantID, AppID: op.AppID, OperationID: op.ID,
		Type: pgimage.DrainSampleEvidence, ObservedAt: time.Now().UTC(), Summary: "Fast drain session sample: " + sample.Status, Reason: sample.Reason, PayloadVersion: 1, Payload: p})
}

func (s *Service) abortImageDrain(ctx context.Context, c *kubeClient, op model.Operation, w postgresImageWitness, cluster map[string]any, postgres model.AppPostgresSpec, alreadyRecorded bool, reason string) error {
	if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
		return err
	}
	if !alreadyRecorded {
		if _, err := s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: op.TenantID, AppID: op.AppID, OperationID: op.ID,
			Type: pgimage.DrainAbortEvidence, Summary: "Fast drain abandoned before image replacement", Reason: reason, PayloadVersion: 1}); err != nil {
			return err
		}
	}
	staged, err := verifyImageDrainCluster(w, cluster)
	if err != nil {
		return err
	}
	if !staged {
		// Invalidate a delayed staging PATCH even if the spec already appears
		// unchanged. A no-op JSON Patch would not advance resourceVersion.
		const fence = "fugue.pro/image-drain-abort"
		meta := imageObject(cluster["metadata"])
		annotations := maps.Clone(imageObject(meta["annotations"]))
		if annotations == nil {
			annotations = map[string]any{}
		}
		if owner, found := annotations[fence]; found && owner != w.Drain.Marker {
			return fmt.Errorf("fast drain abort fence belongs to another operation")
		}
		fenced := annotations[fence] == w.Drain.Marker
		if !fenced {
			annotations[fence] = w.Drain.Marker
			if err := c.patchDrainAbortFence(ctx, w, cluster, annotations); err != nil {
				return err
			}
			cluster, err = c.imageMigrationObject(ctx, cloudNativePGClusterAPIPath(w.Namespace, w.Cluster))
			if err != nil {
				return err
			}
		}
		staged, err = verifyImageDrainCluster(w, cluster)
		meta = imageObject(cluster["metadata"])
		annotations = maps.Clone(imageObject(meta["annotations"]))
		if err != nil || staged || annotations[fence] != w.Drain.Marker || imageString(imageObject(cluster["spec"]), "imageName") != w.SourceImage {
			return fmt.Errorf("could not verify the fast drain abort fence")
		}
		delete(annotations, fence)
		if err := c.patchDrainAbortFence(ctx, w, cluster, annotations); err != nil {
			return err
		}
	}
	if err := s.restoreImageDrain(ctx, c, op, w, cluster); err != nil {
		return err
	}
	// A source container might have restarted independently. Verify the
	// original Cluster, image, storage and primary identity rather than
	// releasing the interlock based on the Pod name or UID alone.
	current, pod, snapshot, err := c.observeImageMigration(ctx, w.Namespace, op.ServiceID, postgres)
	if err != nil {
		return err
	}
	if err := validateImageWitness(w, current, pod, snapshot); err != nil {
		return err
	}
	if imageString(imageObject(current["spec"]), "imageName") != w.SourceImage ||
		imageString(imageContainer(pod), "image") != w.SourceImage {
		return fmt.Errorf("waiting for the source database image after drain settings restoration")
	}
	volumes, err := c.imageMigrationVolumes(ctx, w.Namespace, pod, nil)
	if err != nil || pgimage.Hash(volumes) != pgimage.Hash(w.Volumes) {
		return fmt.Errorf("source database storage identity changed during fast drain abort")
	}
	sample := s.imageDrainSample(ctx, c, w)
	if sample.SampledAt == nil || sample.LoadedMarker != "" {
		return fmt.Errorf("waiting for original instance to acknowledge restored shutdown settings")
	}
	return nil
}

func (c *kubeClient) patchDrainAbortFence(ctx context.Context, w postgresImageWitness, cluster map[string]any, annotations map[string]any) error {
	meta := imageObject(cluster["metadata"])
	rv := imageString(meta, "resourceVersion")
	if rv == "" {
		return fmt.Errorf("missing Cluster resourceVersion for fast drain abort")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": w.ClusterUID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": rv},
		{"op": "test", "path": "/spec", "value": cluster["spec"]},
	}
	op, path := "add", "/metadata/annotations"
	if imageObject(meta["annotations"]) != nil {
		op = "replace"
	}
	patch = append(patch, map[string]any{"op": op, "path": path, "value": annotations})
	_, err := c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(w.Namespace, w.Cluster), "application/json-patch+json", patch, nil)
	return err
}

func imageDrainPodPath(w postgresImageWitness) string {
	return "/api/v1/namespaces/" + url.PathEscape(w.Namespace) + "/pods/" + url.PathEscape(w.Pod)
}

func imageDrainReplacement(pod map[string]any, w postgresImageWitness) bool {
	meta := imageObject(pod["metadata"])
	uid := imageString(meta, "uid")
	if uid == "" || uid == w.PodUID || imageString(meta, "deletionTimestamp") != "" ||
		imageString(imageContainer(pod), "image") != w.TargetImage {
		return false
	}
	for _, owner := range imageObjects(meta["ownerReferences"]) {
		if owner["controller"] == true && owner["kind"] == "Cluster" && owner["apiVersion"] == "postgresql.cnpg.io/v1" &&
			owner["name"] == w.Cluster && owner["uid"] == w.ClusterUID {
			return true
		}
	}
	return false
}

// Confirm restoration by reading back the complete spec. Recovery can safely
// retry when either the settings PATCH or its response was lost.
func (s *Service) restoreImageDrain(ctx context.Context, c *kubeClient, op model.Operation, w postgresImageWitness, cluster map[string]any) error {
	if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
		return err
	}
	if err := c.patchImageDrain(ctx, w, cluster, false); err != nil {
		return err
	}
	current, err := c.imageMigrationObject(ctx, cloudNativePGClusterAPIPath(w.Namespace, w.Cluster))
	if err != nil {
		return err
	}
	staged, err := verifyImageDrainCluster(w, current)
	if err != nil {
		return err
	}
	if staged {
		return fmt.Errorf("original database drain settings are not restored")
	}
	return nil
}
