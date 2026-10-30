package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

// An explicit class-only recovery retry may replace an unpublished destination.
// It never reuses its seed, replays a promoted copy, or discards any data claim.
// All source identity/receipt witnesses survive; a new seed is independently
// copied and verified from the still-stopped original volume.
func (s *Service) replanColdStorage(ctx context.Context, c *kubeClient, ns string, op model.Operation, app model.App, old *coldPostgresState, rv string, pg model.AppPostgresSpec) (*coldPostgresState, string, error) {
	if !coldStorageReplanAllowed(old, app.ID, pg) {
		return nil, "", fmt.Errorf("conflicting cold migration intent; only a class-only retry of an unpublished, verified offline recovery is allowed")
	}
	for _, attempt := range old.RetainedAttempts {
		if attempt.StorageClass == pg.StorageClassName {
			return nil, "", fmt.Errorf("requested storage class already has a retained recovery attempt; refusing to overwrite it")
		}
	}
	service, err := s.Store.GetBackingService(op.ServiceID)
	if err != nil {
		return nil, "", err
	}
	if service.Spec.Postgres == nil || service.Spec.Postgres.ServiceName != old.SourceName || store.PostgresSpecFingerprint(*service.Spec.Postgres) != old.SourceSpecHash {
		return nil, "", fmt.Errorf("recovery source is no longer selected unchanged; refusing destination replacement")
	}
	if err := s.verifyColdSourceIdentity(ctx, c, ns, old); err != nil {
		return nil, "", err
	}
	if err := verifyColdOfflineQuiesced(ctx, c, ns, old); err != nil {
		return nil, "", err
	}
	// The stable Service is checked before and after gracefully stopping the
	// abandoned destination. Neither check writes serving configuration.
	if err := coldEndpointStillSelectsSource(ctx, c, ns, old); err != nil {
		return nil, "", err
	}
	ev, err := coldSourceEvidenceForState(ctx, c, ns, old)
	if err != nil {
		return nil, "", err
	}
	if ev.SystemID != old.SystemID || ev.ControlDigest != old.ControlDigest || ev.FileDigest != old.FileDigest {
		return nil, "", fmt.Errorf("retained source changed; refusing recovery reseed")
	}
	if err := s.stopUnpublishedColdTarget(ctx, c, ns, old, op.ID); err != nil {
		return nil, "", err
	}
	if err := coldEndpointStillSelectsSource(ctx, c, ns, old); err != nil {
		return nil, "", err
	}
	ev, err = coldSourceEvidenceForState(ctx, c, ns, old)
	if err != nil {
		return nil, "", err
	}
	if ev.SystemID != old.SystemID || ev.ControlDigest != old.ControlDigest || ev.FileDigest != old.FileDigest {
		return nil, "", fmt.Errorf("retained source changed; refusing recovery reseed")
	}
	next := coldReplannedState(old, pg)
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return nil, "", err
	}
	if err := saveColdState(ctx, c, ns, &next, &rv); err != nil {
		return nil, "", err
	}
	return &next, rv, nil
}

func coldReplannedState(old *coldPostgresState, pg model.AppPostgresSpec) coldPostgresState {
	next := *old
	next.RetainedAttempts = append(append([]coldRetainedAttempt(nil), old.RetainedAttempts...), coldRetainedAttempt{TargetName: old.TargetName, TargetUID: old.TargetUID, SeedName: old.SeedName, SeedUID: old.SeedUID, StorageClass: old.TargetClass, IntentHash: old.IntentHash})
	next.TargetClass, next.IntentHash = pg.StorageClassName, store.PostgresSpecFingerprint(pg)
	h := sha256.Sum256([]byte(old.ServiceID + old.SourceUID + pg.RuntimeID + pg.StorageClassName))
	suffix := hex.EncodeToString(h[:8])
	next.SeedName, next.TargetName = "pg-seed-"+suffix, "pg-restored-"+suffix
	next.SeedUID, next.TargetUID, next.StreamDigest, next.Phase = "", "", "", "planned"
	return next
}

func coldStorageReplanAllowed(old *coldPostgresState, appID string, pg model.AppPostgresSpec) bool {
	previousIntent := *model.CloneAppPostgresSpec(&pg)
	previousIntent.StorageClassName = old.TargetClass
	return old.AppID == appID && old.OfflineSource && old.TargetClass != pg.StorageClassName && old.TargetRuntime == pg.RuntimeID && old.TargetNode == pg.PrimaryNodeName && old.IntentHash != "" && store.PostgresSpecFingerprint(previousIntent) == old.IntentHash && (old.Phase == "copied" || old.Phase == "restoring") && len(old.RetainedAttempts) < 4
}

func coldEndpointStillSelectsSource(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState) error {
	obj, found, err := c.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/services/"+url.PathEscape(st.Endpoint))
	if err != nil {
		return err
	}
	meta := normalizeKubeMap(obj["metadata"])
	selector := normalizeKubeMap(normalizeKubeMap(obj["spec"])["selector"])
	if !found || meta["uid"] == nil || meta["uid"] == "" || len(selector) != 2 || selector["cnpg.io/cluster"] != st.SourceName || selector["cnpg.io/instanceRole"] != "primary" {
		return fmt.Errorf("stable endpoint is no longer the retained source; refusing recovery reseed")
	}
	return nil
}

func (s *Service) stopUnpublishedColdTarget(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, operationID string) error {
	stable := 0
	for {
		if err := s.ensureOperationStillActive(operationID); err != nil {
			return err
		}
		cluster, found, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
		if err != nil {
			return err
		}
		if !found {
			if st.TargetUID == "" {
				return nil
			}
			return fmt.Errorf("recorded recovery target disappeared")
		}
		if cluster.Metadata.UID != st.TargetUID || cluster.Metadata.ResourceVersion == "" || cluster.Metadata.Annotations[coldMigrationAnnotation] != coldStateName(st.ServiceID) {
			return fmt.Errorf("unpublished recovery target identity changed")
		}
		if cluster.Metadata.Annotations[coldReconciliationAnnotation] != coldReconciliationDisabled || cluster.Metadata.Annotations["cnpg.io/fencedInstances"] != `["*"]` || cluster.Metadata.Annotations[runtimepkg.CloudNativePGHibernationAnno] != runtimepkg.CloudNativePGHibernationOn {
			body := map[string]any{"metadata": map[string]any{"uid": st.TargetUID, "resourceVersion": cluster.Metadata.ResourceVersion, "annotations": map[string]string{coldReconciliationAnnotation: coldReconciliationDisabled, "cnpg.io/fencedInstances": `["*"]`, runtimepkg.CloudNativePGHibernationAnno: runtimepkg.CloudNativePGHibernationOn}}}
			if _, err := c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(ns, st.TargetName), "application/merge-patch+json", body, nil); err != nil {
				return err
			}
		}
		pods, err := c.listPodsBySelector(ctx, ns, "cnpg.io/cluster="+st.TargetName)
		if err != nil {
			return err
		}
		remaining := 0
		for _, pod := range pods {
			if managedPostgresPodFinished(pod) {
				continue
			}
			remaining++
			if pod.Spec.NodeName != "" {
				node, found, err := c.getNode(ctx, pod.Spec.NodeName)
				if err != nil || !found || !kubeNodeReady(node) {
					return fmt.Errorf("recovery target node must acknowledge normal shutdown")
				}
			}
			if pod.Metadata.DeletionTimestamp != "" {
				continue
			}
			if !coldClusterOwnsPod(cluster, pod) {
				// A CNPG bootstrap Pod is owned indirectly through an exact Job.
				jobPath, jobUID := "", ""
				for _, ref := range pod.ObservedOwnerReferences {
					if ref.Controller && ref.APIVersion == "batch/v1" && ref.Kind == "Job" && ref.UID != "" {
						jobPath, jobUID = "/apis/batch/v1/namespaces/"+url.PathEscape(ns)+"/jobs/"+url.PathEscape(ref.Name), ref.UID
					}
				}
				if jobPath == "" {
					return fmt.Errorf("unpublished target contains an unowned Pod")
				}
				job, found, err := c.getRawObject(ctx, jobPath)
				if err != nil {
					return err
				}
				meta := normalizeKubeMap(job["metadata"])
				owned := false
				for _, ref := range mapSlice(meta["ownerReferences"]) {
					owned = owned || (ref["uid"] == st.TargetUID && ref["kind"] == "Cluster" && ref["apiVersion"] == runtimepkg.CloudNativePGAPIVersion && ref["controller"] == true)
				}
				if jobPath == "" || !found || meta["uid"] != jobUID || !owned {
					return fmt.Errorf("unpublished target contains an unowned Pod/Job")
				}
				body := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground", "preconditions": map[string]string{"uid": jobUID, "resourceVersion": objectStringField(meta, "resourceVersion")}}
				if objectStringField(meta, "resourceVersion") == "" {
					return fmt.Errorf("recovery join Job resource version unavailable")
				}
				if _, err := c.doRequest(ctx, http.MethodDelete, jobPath, "application/json", body, nil); err != nil {
					return err
				}
				// Job deletion uses normal GC. Do not force its Pod or data claim.
				continue
			}
			if pod.ObservedUID == "" {
				return fmt.Errorf("recovery target Pod UID unavailable")
			}
			if err := c.deleteObjectWithUID(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(pod.Metadata.Name), pod.ObservedUID); err != nil {
				return err
			}
		}
		if remaining == 0 {
			stable++
		} else {
			stable = 0
		}
		if stable >= 3 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("unpublished target shutdown not verified: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}
