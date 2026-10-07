package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

// Source storage is never expanded or deleted. One independent ConfigMap is
// the retry anchor; after a seed is verified it is immutable, and after cutover
// neither seed nor destination may be recopied or rolled back to the old source.
func (s *Service) executeManagedDatabaseColdMigration(ctx context.Context, op model.Operation, app model.App, source kubeCloudNativePGCluster, pg model.AppPostgresSpec, targetRuntime model.Runtime) error {
	c, err := s.kubeClient()
	if err != nil {
		return err
	}
	ns := runtimepkg.NamespaceForTenant(app.TenantID)
	st, rv, err := s.prepareColdState(ctx, c, op, app, source, pg, targetRuntime)
	if err != nil {
		return err
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	if err := s.verifyColdSourceIdentity(ctx, c, ns, st); err != nil {
		return err
	}
	currentService, err := s.Store.GetBackingService(op.ServiceID)
	if err != nil {
		return err
	}
	if currentService.Spec.Postgres == nil {
		return fmt.Errorf("source service is no longer PostgreSQL")
	}
	alreadySelected := currentService.Spec.Postgres != nil && currentService.Spec.Postgres.ServiceName == st.TargetName
	if !alreadySelected && store.PostgresSpecFingerprint(*currentService.Spec.Postgres) != st.SourceSpecHash {
		return fmt.Errorf("cold source desired configuration changed; refusing stale migration")
	}
	source, found, err := c.getCloudNativePGCluster(ctx, ns, st.SourceName)
	if err != nil || !found {
		return fmt.Errorf("cold source cluster missing: %v", err)
	}
	if err := patchColdSourceFence(ctx, c, ns, source, coldStateName(st.ServiceID)); err != nil {
		return err
	}
	scheduling := runtimepkg.SchedulingForRuntime(targetRuntime)
	if scheduling.NodeSelector == nil {
		scheduling.NodeSelector = map[string]string{}
	}
	scheduling.NodeSelector[kubeHostnameLabelKey] = st.TargetNode
	pg.ServiceName = st.TargetName
	pg.EndpointServiceName = st.Endpoint
	pg.PrimaryNodeName = st.TargetNode
	pg.Image = st.Image
	pg.StorageSize, err = maximumRecoveryStorageSize(pg.StorageSize, st.TargetSize)
	if err != nil {
		return err
	}
	pg.Suspended = false
	pg.Instances = 1
	pg.SynchronousReplicas = 0
	pg.FailoverTargetRuntimeID = ""
	if st.Phase == "planned" || st.Phase == "copying" {
		if alreadySelected {
			return fmt.Errorf("cold target selected before verified seed record")
		}
		s.updateManagedPostgresTransitionProgress(op.ID, "verifying fenced PostgreSQL process and physical source identity")
		ev, err := waitColdSourceEvidence(ctx, c, ns, st.SourcePVC)
		if err != nil {
			return err
		}
		if st.ControlDigest != "" && (ev.ControlDigest != st.ControlDigest || ev.SystemID != st.SystemID) {
			return fmt.Errorf("source PostgreSQL checkpoint changed after fencing")
		}
		st.SystemID, st.ControlDigest = ev.SystemID, ev.ControlDigest
		pvc, found, err := c.getPersistentVolumeClaim(ctx, ns, st.SeedName)
		if err != nil {
			return err
		}
		if found {
			if (st.SeedUID != "" && pvc.Metadata.UID != st.SeedUID) || pvc.Metadata.Labels[coldMigrationAnnotation] != coldStateName(st.ServiceID) || pvc.Spec.StorageClassName != st.TargetClass || pvc.Spec.Resources.Requests["storage"] != st.TargetSize {
				return fmt.Errorf("cold seed ownership/UID mismatch")
			}
			st.SeedUID = pvc.Metadata.UID
		} else {
			if st.SeedUID != "" {
				return fmt.Errorf("recorded cold seed disappeared")
			}
			obj := coldSeedPVC(ns, st)
			if err := c.applyObject(ctx, obj, nil); err != nil {
				return err
			}
			pvc, found, err = c.getPersistentVolumeClaim(ctx, ns, st.SeedName)
			if err != nil || !found {
				return fmt.Errorf("created seed not observable")
			}
			st.SeedUID = pvc.Metadata.UID
		}
		st.Phase = "copying"
		if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
			return err
		}
		// Never copy over an existing recovery cluster, even after lost phase writes.
		_, targetExists, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
		if err != nil {
			return err
		}
		if targetExists {
			return fmt.Errorf("refusing seed recopy: recovery cluster already exists")
		}
		s.updateManagedPostgresTransitionProgress(op.ID, "copying fenced PGDATA directly into target storage; source capacity unchanged")
		digest, err := s.copyColdSeed(ctx, c, ns, st, ev, scheduling)
		if err != nil {
			return err
		}
		if err := s.verifyColdSourceIdentity(ctx, c, ns, st); err != nil {
			return err
		}
		after, err := coldSourceEvidence(ctx, c, ns, st.SourcePVC)
		if err != nil {
			return err
		}
		if ev.FileDigest != after.FileDigest || ev.ControlDigest != after.ControlDigest || ev.SystemID != after.SystemID {
			return fmt.Errorf("source physical files changed during copy")
		}
		verified, err := verifyColdSeedFiles(ctx, c, ns, st, ev, scheduling)
		if err != nil {
			return err
		}
		if verified.FileDigest != ev.FileDigest || verified.ControlDigest != ev.ControlDigest || verified.SystemID != ev.SystemID {
			detail := ""
			switch {
			case verified.ContentDigest != ev.ContentDigest && verified.MetadataDigest != ev.MetadataDigest:
				detail = "content and metadata"
			case verified.ContentDigest != ev.ContentDigest:
				detail = "file content"
			case verified.MetadataDigest != ev.MetadataDigest:
				detail = coldMetadataMismatchDetail(ev.MetadataEntries, verified.MetadataEntries)
			default:
				detail = "manifest"
			}
			return fmt.Errorf("destination seed physical data differs from fenced source (%s)", detail)
		}
		st.FileDigest, st.StreamDigest, st.Phase = ev.FileDigest, digest, "copied"
		if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
			return err
		}
	}
	if st.Phase != "copied" && st.Phase != "restoring" && st.Phase != "validated" && st.Phase != "cutover" && st.Phase != "complete" {
		return fmt.Errorf("unknown cold migration phase %q", st.Phase)
	}
	pvc, found, err := c.getPersistentVolumeClaim(ctx, ns, st.SeedName)
	if err != nil || !found || pvc.Metadata.UID != st.SeedUID || pvc.Spec.StorageClassName != st.TargetClass {
		return fmt.Errorf("verified seed identity changed")
	}
	s.updateManagedPostgresTransitionProgress(op.ID, "recovering target CNPG cluster from verified destination seed")
	if st.Phase == "copied" {
		st.Phase = "restoring"
		if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
			return err
		}
	}
	if err := s.ensureColdTarget(ctx, c, ns, st, app, pg, scheduling); err != nil {
		return err
	}
	tc, found, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
	if err != nil || !found {
		return fmt.Errorf("target recovery cluster unavailable")
	}
	if st.TargetUID == "" {
		st.TargetUID = tc.Metadata.UID
		if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
			return err
		}
	}
	if st.TargetUID != tc.Metadata.UID {
		return fmt.Errorf("target recovery cluster UID changed")
	}
	primary, err := s.waitColdPostgresPrimary(ctx, c, ns, st.TargetName, op.ID, pg)
	if err != nil {
		return err
	}
	tc, _, err = c.getCloudNativePGCluster(ctx, ns, st.TargetName)
	if err != nil {
		return err
	}
	if tc.Status.SystemID == "" || tc.Status.SystemID != st.SystemID {
		return fmt.Errorf("target PostgreSQL system ID differs from source pg_control")
	}
	pod, found, err := c.getPod(ctx, ns, primary)
	if err != nil || !found || pod.Spec.NodeName != st.TargetNode {
		return fmt.Errorf("recovered primary is not on the requested destination node")
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	if !alreadySelected {
		// A resumed bootstrap must not promote a copy if someone restarted or
		// changed the retained source while this operation was interrupted.
		evidence, err := coldSourceEvidence(ctx, c, ns, st.SourcePVC)
		if err != nil {
			return err
		}
		if evidence.SystemID != st.SystemID || evidence.ControlDigest != st.ControlDigest || evidence.FileDigest != st.FileDigest {
			return fmt.Errorf("retained source changed after verified copy; refusing cutover")
		}
	}
	if st.Phase == "restoring" {
		st.Phase = "validated"
		if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
			return err
		}
	}
	// Commit the chosen cluster before its Service is reconciled. Subsequent
	// releases/configuration render the same stable hostname and target selector.
	if _, err := s.Store.CommitColdPostgresTarget(op.ID, st.SourceSpecHash, pg); err != nil {
		return err
	}
	st.Phase = "cutover"
	if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
		return err
	}
	fresh, err := s.Store.GetApp(app.ID)
	if err != nil {
		return err
	}
	finalApp, err := appWithBackingServicePostgres(op.ServiceID, fresh, pg)
	if err != nil {
		return err
	}
	bundle, err := s.applyManagedDesiredAppState(ctx, op.ID, finalApp, finalApp.Spec)
	if err != nil {
		return err
	}
	// The stable application Service must select the exact validated primary.
	ip, found, err := c.getPodIP(ctx, ns, primary)
	if err != nil || !found {
		return fmt.Errorf("target primary address unavailable")
	}
	if err := s.waitColdStableEndpoint(ctx, st.Endpoint+"."+ns+".svc.cluster.local", ip, pg); err != nil {
		return fmt.Errorf("stable database endpoint cutover not verified: %w", err)
	}
	st.Phase = "complete"
	if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
		return err
	}
	_, err = s.Store.CompleteManagedOperationWithResult(op.ID, bundle.ManifestPath, "verified cold database migration to "+st.TargetRuntime, &fresh.Spec, nil)
	return err
}

func coldMetadataMismatchDetail(source, target []coldMetadataEntry) string {
	byPath := make(map[string]coldMetadataEntry, len(source))
	for _, entry := range source {
		byPath[entry.Path] = entry
	}
	for _, entry := range target {
		want, ok := byPath[entry.Path]
		if !ok {
			return fmt.Sprintf("unexpected target entry %s", entry.Path)
		}
		if want.Mode != entry.Mode || want.UID != entry.UID || want.GID != entry.GID || want.Size != entry.Size || want.Link != entry.Link {
			return fmt.Sprintf("metadata %s source(mode=%o uid=%d gid=%d size=%d link=%q) target(mode=%o uid=%d gid=%d size=%d link=%q)", entry.Path, want.Mode, want.UID, want.GID, want.Size, want.Link, entry.Mode, entry.UID, entry.GID, entry.Size, entry.Link)
		}
		delete(byPath, entry.Path)
	}
	for path := range byPath {
		return "missing target entry " + path
	}
	return "metadata digest"
}

func (s *Service) verifyColdSourceIdentity(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState) error {
	cluster, found, err := c.getCloudNativePGCluster(ctx, ns, st.SourceName)
	if err != nil {
		return err
	}
	if !found || cluster.Metadata.UID != st.SourceUID {
		return fmt.Errorf("source cluster identity changed")
	}
	pvc, found, err := c.getPersistentVolumeClaim(ctx, ns, st.SourcePVC)
	if err != nil {
		return err
	}
	if !found || pvc.Metadata.UID != st.SourcePVCUID || pvc.Spec.VolumeName != st.SourcePV {
		return fmt.Errorf("source PVC/PV identity changed")
	}
	if st.Phase == "planned" || st.Phase == "copying" {
		pod, found, err := c.getPod(ctx, ns, st.SourcePVC)
		if err != nil {
			return err
		}
		if !found || pod.ObservedUID != st.SourcePodUID || pod.Spec.NodeName != st.SourceNode || pod.Metadata.DeletionTimestamp != "" {
			return fmt.Errorf("source Pod identity changed")
		}
		matchesImage := false
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == "postgres" {
				matchesImage = strings.TrimPrefix(cs.ImageID, "docker-pullable://") == st.SourceImageID
			}
		}
		if !matchesImage {
			return fmt.Errorf("source PostgreSQL binary changed")
		}
	}
	return nil
}
func patchColdSourceFence(ctx context.Context, c *kubeClient, ns string, cluster kubeCloudNativePGCluster, plan string) error {
	if cluster.Metadata.UID == "" || cluster.Metadata.ResourceVersion == "" {
		return fmt.Errorf("missing source fencing preconditions")
	}
	if other := cluster.Metadata.Annotations[coldMigrationAnnotation]; other != "" && other != plan {
		return fmt.Errorf("another cold migration owns the source")
	}
	body := map[string]any{"metadata": map[string]any{"uid": cluster.Metadata.UID, "resourceVersion": cluster.Metadata.ResourceVersion, "annotations": map[string]string{"cnpg.io/fencedInstances": `["*"]`, coldMigrationAnnotation: plan}}}
	_, err := c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(ns, cluster.Metadata.Name), "application/merge-patch+json", body, nil)
	return err
}
func coldSeedPVC(ns string, st *coldPostgresState) map[string]any {
	// Do not inherit cnpg.io/cluster or node-serial labels: the seed is not an
	// instance of the source and must never be discovered or adopted by it.
	return map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"name": st.SeedName, "namespace": ns, "labels": map[string]string{coldMigrationAnnotation: coldStateName(st.ServiceID), "cnpg.io/pvcRole": "PG_DATA"}}, "spec": map[string]any{"accessModes": []string{"ReadWriteOnce"}, "storageClassName": st.TargetClass, "resources": map[string]any{"requests": map[string]string{"storage": st.TargetSize}}}}
}
func (s *Service) copyColdSeed(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, ev coldFileEvidence, scheduling runtimepkg.SchedulingConstraints) (string, error) {
	names := movableRWOMigrationResourceNames(model.App{ID: st.ServiceID}, st.SeedName)
	// Only the exact isolated receiver may be replaced before activation.
	if err := deleteColdPod(ctx, c, ns, names.targetPod, coldStateName(st.ServiceID)); err != nil {
		return "", err
	}
	obj := buildMovableRWOTargetPod(ns, names.targetPod, names.labels, st.SeedName, ".", scheduling)
	m := normalizeKubeMap(obj["metadata"])
	labels := normalizeKubeMap(m["labels"])
	labels[coldMigrationAnnotation] = coldStateName(st.ServiceID)
	m["labels"] = labels
	obj["metadata"] = m
	spec := normalizeKubeMap(obj["spec"])
	spec["automountServiceAccountToken"] = false
	obj["spec"] = spec
	if err := c.applyObject(ctx, obj, nil); err != nil {
		return "", err
	}
	defer deleteColdPod(context.Background(), c, ns, names.targetPod, coldStateName(st.ServiceID))
	if err := waitForMovableRWOPodReady(ctx, c, ns, names.targetPod, 5*time.Minute); err != nil {
		return "", err
	}
	ip, found, err := c.getPodIP(ctx, ns, names.targetPod)
	if err != nil || !found || ip == "" {
		return "", fmt.Errorf("target receiver address unavailable")
	}
	digest, err := execColdPostgresTar(ctx, c, ns, st.SourcePVC, ev.DataPath, ip)
	if err != nil {
		return "", err
	}
	if err := waitForMovableRWOPodSucceeded(ctx, c, ns, names.targetPod, 10*time.Minute); err != nil {
		return "", err
	}
	targetDigest, err := movableRWOTransferDigest(ctx, c, ns, names.targetPod, "receiver")
	if err != nil {
		return "", err
	}
	if digest != targetDigest {
		return "", fmt.Errorf("cold stream digest mismatch")
	}
	return digest, nil
}
func deleteColdPod(ctx context.Context, c *kubeClient, ns, name, plan string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	p, found, err := c.getPod(ctx, ns, name)
	if err != nil || !found {
		return err
	}
	if p.Metadata.Labels[coldMigrationAnnotation] != plan || p.ObservedUID == "" {
		return fmt.Errorf("refusing to delete unowned cold transfer Pod")
	}
	// Normal graceful deletion with UID precondition. Never force-delete a
	// data-mounting Pod while using API disappearance as proof of stopped I/O.
	if err := c.deleteObjectWithUID(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(name), p.ObservedUID); err != nil {
		return err
	}
	return waitForMovableRWOPodDeleted(ctx, c, ns, name)
}
func verifyColdSeedFiles(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, source coldFileEvidence, scheduling runtimepkg.SchedulingConstraints) (coldFileEvidence, error) {
	var ev coldFileEvidence
	name := st.SeedName + "-verify"
	plan := coldStateName(st.ServiceID)
	if err := deleteColdPod(ctx, c, ns, name, plan); err != nil {
		return ev, err
	}
	obj := movableRWOPodObject(ns, name, map[string]string{coldMigrationAnnotation: plan}, map[string]any{"restartPolicy": "Never", "automountServiceAccountToken": false, "containers": []map[string]any{{"name": "verify", "image": st.Image, "command": []string{"python3", "-c", coldProbeProgram, "seed", path.Join("/seed", path.Base(source.DataPath))}, "securityContext": map[string]any{"runAsUser": source.UID, "runAsGroup": source.GID, "allowPrivilegeEscalation": false}, "volumeMounts": []map[string]any{{"name": "seed", "mountPath": "/seed", "readOnly": true}}}}, "volumes": []map[string]any{{"name": "seed", "persistentVolumeClaim": map[string]any{"claimName": st.SeedName, "readOnly": true}}}})
	ps := normalizeKubeMap(obj["spec"])
	applyMovableRWOScheduling(ps, scheduling)
	obj["spec"] = ps
	if err := c.applyObject(ctx, obj, nil); err != nil {
		return ev, err
	}
	defer deleteColdPod(context.Background(), c, ns, name, plan)
	if err := waitForMovableRWOPodSucceeded(ctx, c, ns, name, 5*time.Minute); err != nil {
		return ev, err
	}
	logs, found, err := c.getPodLogs(ctx, ns, name, "verify", false, 5)
	if err != nil || !found {
		return ev, fmt.Errorf("seed verification logs missing")
	}
	err = json.Unmarshal([]byte(strings.TrimSpace(logs)), &ev)
	return ev, err
}
func (s *Service) ensureColdTarget(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, app model.App, pg model.AppPostgresSpec, scheduling runtimepkg.SchedulingConstraints) error {
	if st.FileDigest == "" || st.SystemID == "" || st.SeedUID == "" {
		return fmt.Errorf("recovery bootstrap requires durable copy evidence")
	}
	current, found, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
	if err != nil {
		return err
	}
	if found {
		if current.Metadata.Annotations[coldMigrationAnnotation] != coldStateName(st.ServiceID) || (st.TargetUID != "" && st.TargetUID != current.Metadata.UID) {
			return fmt.Errorf("target cluster ownership changed")
		}
		if current.Spec.Storage.StorageClass != st.TargetClass {
			return fmt.Errorf("target cluster storage class changed")
		}
		return nil
	}
	if st.TargetUID != "" {
		return fmt.Errorf("recorded target disappeared; refusing new bootstrap")
	}
	targetApp, err := appWithBackingServicePostgres(st.ServiceID, app, pg)
	if err != nil {
		return err
	}
	placements := map[string][]runtimepkg.SchedulingConstraints{st.TargetName: {scheduling}}
	objects := runtimepkg.BuildManagedAppChildObjectsWithPlacements(targetApp, runtimepkg.SchedulingConstraints{}, placements, nil)
	for _, obj := range objects {
		if objectStringField(obj, "kind") != "Cluster" || objectStringField(normalizeKubeMap(obj["metadata"]), "name") != st.TargetName {
			continue
		}
		spec := normalizeKubeMap(obj["spec"])
		cred := runtimepkg.ManagedPostgresCredentialSecretName(st.ServiceID, app.ID, pg)
		spec["bootstrap"] = map[string]any{"recovery": map[string]any{"database": pg.Database, "owner": pg.User, "secret": map[string]string{"name": cred}, "volumeSnapshots": map[string]any{"storage": map[string]string{"kind": "PersistentVolumeClaim", "name": st.SeedName}}}}
		spec["storage"] = map[string]any{"size": st.TargetSize, "storageClass": st.TargetClass, "resizeInUseVolumes": true}
		obj["spec"] = spec
		m := normalizeKubeMap(obj["metadata"])
		annotations := normalizeKubeMap(m["annotations"])
		annotations[coldMigrationAnnotation] = coldStateName(st.ServiceID)
		m["annotations"] = annotations
		obj["metadata"] = m
		// Only CNPG and the existing service-owned credential are involved. No app
		// Service or Deployment is applied before recovery validation.
		return c.applyObject(ctx, obj, nil)
	}
	return fmt.Errorf("target CNPG object not rendered for exact service")
}
func (s *Service) waitColdPostgresPrimary(ctx context.Context, c *kubeClient, ns, name, opID string, pg model.AppPostgresSpec) (string, error) {
	for {
		if err := s.ensureOperationStillActive(opID); err != nil {
			return "", err
		}
		cluster, found, err := c.getCloudNativePGCluster(ctx, ns, name)
		if err != nil {
			return "", err
		}
		if found && cluster.Status.CurrentPrimary != "" && cluster.Status.ReadyInstances >= 1 {
			_, err := s.waitForManagedPostgresPrimary(ctx, c, ns, name, cluster.Status.CurrentPrimary, opID, pg)
			return cluster.Status.CurrentPrimary, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Wait for the instance manager's acknowledgement, not just a desired annotation.
func waitColdSourceEvidence(ctx context.Context, c *kubeClient, ns, pod string) (coldFileEvidence, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		ev, err := coldSourceEvidence(ctx, c, ns, pod)
		if err == nil {
			return ev, nil
		}
		if !strings.Contains(err.Error(), "has not acknowledged fencing") {
			return ev, err
		}
		select {
		case <-ctx.Done():
			return ev, fmt.Errorf("wait for source fencing: %w", err)
		case <-time.After(2 * time.Second):
		}
	}
}
func (s *Service) waitColdStableEndpoint(ctx context.Context, host, ip string, pg model.AppPostgresSpec) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		err := s.probeManagedPostgresPrimarySQL(ctx, host, ip, pg)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ctx.Err(), err)
		case <-time.After(2 * time.Second):
		}
	}
}
