package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/store"
)

var errPostgresSourceRetirementUnsettled = errors.New("source retirement awaits verified settlement")

// Only tiny process/control-file reads: never hash, copy, repair, or start PGDATA.
const sourceRetirementProbeProgram = `import json,os,re,subprocess,urllib.request
data=os.environ.get('PGDATA')
if not data or not os.path.isabs(data):raise RuntimeError('PGDATA identity unavailable')
metrics=urllib.request.urlopen('http://127.0.0.1:9187/metrics',timeout=3).read().decode()
if not re.search(r'^cnpg_collector_fencing_on 1(?:\.0)?$',metrics,re.M):raise RuntimeError('source manager has not acknowledged fencing')
if subprocess.run(['pg_ctl','-D',data,'status'],stdout=subprocess.PIPE,stderr=subprocess.PIPE).returncode!=3:raise RuntimeError('source PostgreSQL is running or unknown')
for pid in os.listdir('/proc'):
 if not pid.isdigit():continue
 try:
  if open('/proc/'+pid+'/comm').read().strip() in ('postgres','postmaster'):raise RuntimeError('source PostgreSQL process remains')
 except FileNotFoundError:pass
control=subprocess.check_output(['pg_controldata',data],env={**os.environ,'LC_ALL':'C'},text=True)
identifier=re.search(r'^Database system identifier:\s*(\d+)\s*$',control,re.M)
if not identifier:raise RuntimeError('source system ID unavailable')
print(json.dumps({'stopped':True,'system_id':identifier[1]}))`

func probePostgresRetirementSource(ctx context.Context, c *kubeClient, p model.PostgresSourceRetirementPlan, id model.PostgresSourceRetirementPod) error {
	pod, err := pgretire.Required(ctx, c.getRawObject, pgretire.Path(p.Namespace, "pods", id.Name))
	if err != nil {
		return err
	}
	if pgretire.String(pgretire.Meta(pod), "uid") != id.UID || pgretire.String(pgretire.Object(pod["spec"]), "nodeName") != id.Node {
		return fmt.Errorf("source Pod identity changed before stopped-state probe")
	}
	if id.Node == "" {
		status := pgretire.Object(pod["status"])
		if status["phase"] == "Pending" && len(pgretire.Objects(status["containerStatuses"])) == 0 && len(pgretire.Objects(status["initContainerStatuses"])) == 0 && len(pgretire.Objects(status["ephemeralContainerStatuses"])) == 0 {
			return nil // A never-bound, never-started Pod cannot own a running process.
		}
		return fmt.Errorf("unbound source Pod execution state is unknown")
	}
	if err := pgretire.NodeReady(ctx, c.getRawObject, id.Node); err != nil {
		return err
	}
	var out bytes.Buffer
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := coldExec(call, c, p.Namespace, id.Name, "postgres", []string{"python3", "-c", sourceRetirementProbeProgram}, &out); err != nil {
		return err
	}
	var proof struct {
		Stopped  bool   `json:"stopped"`
		SystemID string `json:"system_id"`
	}
	if json.Unmarshal(out.Bytes(), &proof) != nil || !proof.Stopped || proof.SystemID != p.SystemID {
		return fmt.Errorf("source stopped/system identity proof failed")
	}
	return nil
}

func retirementPatch(ctx context.Context, c *kubeClient, path string, obj map[string]any, changes ...map[string]any) error {
	meta := pgretire.Meta(obj)
	if pgretire.String(meta, "uid") == "" || pgretire.String(meta, "resourceVersion") == "" || pgretire.String(meta, "deletionTimestamp") != "" {
		return fmt.Errorf("retirement mutation identity unavailable")
	}
	patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": meta["uid"]}, {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]}}
	patch = append(patch, changes...)
	_, err := c.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil)
	return err
}

func (s *Service) retirementEvidence(op model.Operation, kind string, p model.PostgresSourceRetirementPlan) error {
	existing, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{kind}, Limit: 1})
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		if pgretire.Hash(existing[0].Payload) != pgretire.Hash(p) {
			return fmt.Errorf("durable retirement evidence differs from the confirmed plan")
		}
		return nil
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	var payload map[string]any
	if err = json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	recorded, err := s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: op.TenantID, AppID: op.AppID, OperationID: op.ID, Type: kind, SubjectKind: "Cluster", SubjectName: p.SourceCluster, SubjectNamespace: p.Namespace, SubjectUID: p.SourceUID, Summary: kind, PayloadVersion: 1, Payload: payload})
	if err != nil {
		return err
	}
	if pgretire.Hash(recorded.Payload) != pgretire.Hash(p) {
		return fmt.Errorf("retirement evidence was truncated or changed; mutation refused")
	}
	return nil
}

func (s *Service) executePostgresSourceRetirement(ctx context.Context, op model.Operation, app model.App) (executionErr error) {
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{pgretire.IntentEvidence}, Limit: 1})
	if err != nil {
		return fmt.Errorf("%w: durable intent unavailable: %v", errPostgresSourceRetirementUnsettled, err)
	}
	intent := len(evidence) > 0
	defer func() {
		if executionErr != nil && intent && !errors.Is(executionErr, context.Canceled) && !errors.Is(executionErr, errOperationNoLongerActive) {
			_, _ = s.Store.UpdateOperationProgress(op.ID, "source retirement waiting for verified settlement: "+executionErr.Error())
			executionErr = fmt.Errorf("%w: %v", errPostgresSourceRetirementUnsettled, executionErr)
		}
	}()
	if !s.Config.KubectlApply {
		return fmt.Errorf("source retirement requires Kubernetes apply mode")
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.SourceRetirement == nil {
		return fmt.Errorf("retirement intent missing")
	}
	p := *op.DesiredSpec.Postgres.SourceRetirement
	target, err := store.PostgresImageMigrationTargetForApp(app, op.ServiceID)
	if err != nil {
		return err
	}
	if p.ConfigHash != pgretire.ConfigHash(app, *target.Service) || p.PlanHash != pgretire.PlanHash(p) || p.ServiceID != op.ServiceID || p.AppID != app.ID {
		return fmt.Errorf("retirement configuration/identity changed")
	}
	c, err := s.kubeClient()
	if err != nil {
		return err
	}
	check := func() error {
		if err := s.ensureOperationStillActiveContext(ctx, op.ID); err != nil {
			return err
		}
		fresh, err := s.Store.GetApp(app.ID)
		if err != nil {
			return err
		}
		t, err := store.PostgresImageMigrationTargetForApp(fresh, op.ServiceID)
		if err != nil {
			return err
		}
		if pgretire.ConfigHash(fresh, *t.Service) != p.ConfigHash {
			return fmt.Errorf("database configuration changed during retirement")
		}
		return pgretire.Validate(ctx, c.getRawObject, p, intent)
	}
	sql := func() error {
		pod, err := pgretire.Required(ctx, c.getRawObject, pgretire.Path(p.Namespace, "pods", p.TargetPod))
		if err != nil {
			return err
		}
		return s.probeManagedPostgresPrimarySQL(ctx, managedPostgresRWServiceHost(p.Namespace, p.Endpoint), pgretire.String(pgretire.Object(pod["status"]), "podIP"), target.Postgres)
	}
	probe := s.postgresSourceRetirementProbe
	if probe == nil {
		probe = probePostgresRetirementSource
	}
	if !intent {
		live, err := pgretire.Inspect(ctx, c.getRawObject, app, *target.Service)
		if err != nil {
			return err
		}
		if live.PlanHash != p.PlanHash {
			return fmt.Errorf("retirement live plan changed before execution")
		}
		if err = sql(); err != nil {
			return fmt.Errorf("destination SQL is not healthy: %w", err)
		}
		for _, id := range p.Pods {
			if err = probe(ctx, c, p, id); err != nil {
				return err
			}
		}
		if err = s.retirementEvidence(op, pgretire.IntentEvidence, p); err != nil {
			return err
		}
		intent = true
	} else if pgretire.Hash(evidence[0].Payload) != pgretire.Hash(p) {
		return fmt.Errorf("durable retirement intent differs from operation")
	}
	if err = check(); err != nil {
		return err
	}
	if err = sql(); err != nil {
		return err
	}
	s.updateManagedPostgresTransitionProgress(op.ID, "retaining source storage and pausing the exact superseded Cluster; destination untouched")
	path := pgretire.Path(p.Namespace, "clusters", p.SourceCluster)
	source, found, err := c.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if found && pgretire.String(pgretire.Meta(source), "deletionTimestamp") == "" {
		annotations := map[string]any{}
		for k, v := range pgretire.Object(pgretire.Meta(source)["annotations"]) {
			annotations[k] = v
		}
		if annotations["cnpg.io/reconciliationLoop"] != "disabled" || annotations["cnpg.io/hibernation"] != "on" || annotations["cnpg.io/fencedInstances"] != `["*"]` {
			annotations["cnpg.io/reconciliationLoop"] = "disabled"
			annotations["cnpg.io/hibernation"] = "on"
			annotations["cnpg.io/fencedInstances"] = `["*"]`
			if err = retirementPatch(ctx, c, path, source, map[string]any{"op": "add", "path": "/metadata/annotations", "value": annotations}); err != nil {
				return err
			}
		}
	}
	for _, v := range p.Volumes {
		if err = check(); err != nil {
			return err
		}
		pvPath := "/api/v1/persistentvolumes/" + url.PathEscape(v.PV)
		pv, e := pgretire.Required(ctx, c.getRawObject, pvPath)
		if e != nil {
			return e
		}
		if pgretire.Object(pv["spec"])["persistentVolumeReclaimPolicy"] != "Retain" {
			if err = retirementPatch(ctx, c, pvPath, pv, map[string]any{"op": "add", "path": "/spec/persistentVolumeReclaimPolicy", "value": "Retain"}); err != nil {
				return err
			}
		}
		if err = check(); err != nil {
			return err
		}
		pvcPath := pgretire.Path(p.Namespace, "persistentvolumeclaims", v.PVC)
		pvc, e := pgretire.Required(ctx, c.getRawObject, pvcPath)
		if e != nil {
			return e
		}
		if pgretire.Owned(pvc, p.SourceUID) {
			kept := []any{}
			for _, ref := range pgretire.Objects(pgretire.Meta(pvc)["ownerReferences"]) {
				if ref["uid"] != p.SourceUID {
					kept = append(kept, ref)
				}
			}
			annotations := map[string]any{}
			for k, val := range pgretire.Object(pgretire.Meta(pvc)["annotations"]) {
				annotations[k] = val
			}
			annotations[pgretire.RetainedUIDAnnotation] = p.SourceUID
			annotations[pgretire.RetainedPlanAnnotation] = p.PlanHash
			annotations["fugue.pro/retired-source-cluster"] = p.SourceCluster
			labels := map[string]any{}
			for k, val := range pgretire.Object(pgretire.Meta(pvc)["labels"]) {
				if !strings.HasPrefix(k, "cnpg.io/") {
					labels[k] = val
				}
			}
			if err = retirementPatch(ctx, c, pvcPath, pvc, map[string]any{"op": "add", "path": "/metadata/ownerReferences", "value": kept}, map[string]any{"op": "add", "path": "/metadata/annotations", "value": annotations}, map[string]any{"op": "add", "path": "/metadata/labels", "value": labels}); err != nil {
				return err
			}
		}
	}
	if err = check(); err != nil {
		return err
	}
	if err = verifyRetiredStorage(ctx, c, p); err != nil {
		return err
	}
	source, found, err = c.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if found {
		if pgretire.String(pgretire.Meta(source), "deletionTimestamp") == "" {
			// Verify every remaining source instance is still fenced and stopped.
			for _, id := range p.Pods {
				if _, exists, e := c.getRawObject(ctx, pgretire.Path(p.Namespace, "pods", id.Name)); e != nil {
					return e
				} else if exists {
					if e = probe(ctx, c, p, id); e != nil {
						return e
					}
				}
			}
			if err = check(); err != nil {
				return err
			}
			_, err = c.doJSON(ctx, http.MethodDelete, path, map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Orphan", "preconditions": map[string]any{"uid": p.SourceUID, "resourceVersion": pgretire.Meta(source)["resourceVersion"]}}, nil)
			if err != nil {
				return err
			}
		}
		_, exists, e := c.getRawObject(ctx, path)
		if e != nil {
			return e
		}
		if exists {
			return fmt.Errorf("waiting for exact source Cluster orphan deletion")
		}
	}
	for _, id := range p.Pods {
		if err = check(); err != nil {
			return err
		}
		podPath := pgretire.Path(p.Namespace, "pods", id.Name)
		pod, exists, e := c.getRawObject(ctx, podPath)
		if e != nil {
			return e
		}
		if !exists {
			continue
		}
		if pgretire.String(pgretire.Meta(pod), "uid") != id.UID {
			return fmt.Errorf("source Pod UID changed; refusing removal")
		}
		if pgretire.String(pgretire.Meta(pod), "deletionTimestamp") == "" {
			_, err = c.doJSON(ctx, http.MethodDelete, podPath, map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 30, "preconditions": map[string]any{"uid": id.UID, "resourceVersion": pgretire.Meta(pod)["resourceVersion"]}}, nil)
			if err != nil {
				return err
			}
		}
	}
	for _, id := range p.Pods {
		if _, exists, e := c.getRawObject(ctx, pgretire.Path(p.Namespace, "pods", id.Name)); e != nil {
			return e
		} else if exists {
			return fmt.Errorf("waiting for graceful source Pod termination")
		}
	}
	if err = check(); err != nil {
		return err
	}
	if err = verifyRetiredStorage(ctx, c, p); err != nil {
		return err
	}
	if err = sql(); err != nil {
		return err
	}
	if err = s.retirementEvidence(op, pgretire.VerifiedEvidence, p); err != nil {
		return err
	}
	_, err = s.Store.CompleteManagedOperationGuarded(op, nil, "", "source Cluster and Pods retired; all PVCs/PVs and the destination retained", nil, nil, nil)
	return err
}

func verifyRetiredStorage(ctx context.Context, c *kubeClient, p model.PostgresSourceRetirementPlan) error {
	for _, v := range p.Volumes {
		pvc, err := pgretire.Required(ctx, c.getRawObject, pgretire.Path(p.Namespace, "persistentvolumeclaims", v.PVC))
		if err != nil {
			return err
		}
		if pgretire.Owned(pvc, p.SourceUID) || pgretire.Object(pgretire.Meta(pvc)["annotations"])[pgretire.RetainedUIDAnnotation] != p.SourceUID || pgretire.Object(pgretire.Meta(pvc)["annotations"])[pgretire.RetainedPlanAnnotation] != p.PlanHash || pgretire.Object(pgretire.Meta(pvc)["labels"])[pgretire.ClusterLabel] != nil {
			return fmt.Errorf("source PVC retention is not verified")
		}
		pv, err := pgretire.Required(ctx, c.getRawObject, "/api/v1/persistentvolumes/"+url.PathEscape(v.PV))
		if err != nil {
			return err
		}
		if pgretire.Object(pv["spec"])["persistentVolumeReclaimPolicy"] != "Retain" {
			return fmt.Errorf("source PV reclaim policy is not Retain")
		}
	}
	return nil
}
