package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"fugue/internal/dataprewarm"
	"fugue/internal/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const prewarmLabel = "fugue.pro/data-prewarm"

func prewarmName(id string) string {
	return fmt.Sprintf("fugue-prewarm-%x", sha256.Sum256([]byte(id)))[:38]
}

func (s *Service) runDataPrewarmLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.reconcileDataPrewarms(ctx); err != nil && ctx.Err() == nil {
			s.Logger.Printf("data prewarm reconciliation failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Service) reconcileDataPrewarms(ctx context.Context) error {
	if !s.Config.KubectlApply {
		return nil
	}
	transfers, err := s.Store.ListDataPrewarmsForReconcile(100)
	if err != nil {
		return err
	}
	if len(transfers) == 0 {
		return nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	for _, transfer := range transfers {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = s.reconcileDataPrewarm(bounded, client, transfer)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.Logger.Printf("data prewarm %s: %v", transfer.ID, err)
		}
	}
	return nil
}
func (s *Service) reconcileDataPrewarm(ctx context.Context, c *kubeClient, t model.DataTransfer) error {
	now := time.Now().UTC()
	if t.Cache == nil {
		t.Cache = &model.DataPrewarmCache{State: "removed", ObservedAt: now}
		return s.failDataPrewarm(t, "legacy prewarm plan was never authorized for execution; submit a new data prewarm request")
	}
	if t.Cache.Job == "" {
		rt, err := s.Store.GetRuntime(t.Target)
		if err != nil {
			return err
		}
		namespace := c.effectiveNamespace("")
		name := prewarmName(t.ID)
		t.Cache = &model.DataPrewarmCache{Namespace: namespace, Claim: name, Job: name, Node: rt.ClusterNodeName, ManifestDigest: t.Manifest.Digest, State: "planned", ObservedAt: now}
		// Persist resource names before creation; cancellation/restart can clean up
		// objects even when the Kubernetes response never reaches the controller.
		t, err = s.Store.UpdateDataTransfer(t)
		if err != nil {
			return err
		}
	}
	terminal := t.Status == model.DataTransferStatusCompleted || t.Status == model.DataTransferStatusFailed || t.Status == model.DataTransferStatusCanceled
	expired := t.ExpiresAt != nil && !t.ExpiresAt.After(now)
	if terminal || expired {
		removeCache := t.Status != model.DataTransferStatusCompleted || expired
		done, err := cleanupPrewarm(ctx, c, *t.Cache, t.ID, removeCache)
		if err != nil {
			return err
		}
		if removeCache {
			if done {
				t.Cache.State = "removed"
			} else {
				t.Cache.State = "cleanup_pending"
			}
		}
		if expired && !terminal {
			t.Status = model.DataTransferStatusFailed
			t.ErrorCode = "prewarm_expired"
			t.ErrorMessage = "prewarm deadline expired"
			t.FinishedAt = &now
		}
		if !removeCache && done {
			t.Cache.WorkerCleaned = true
		}
		t.Cache.ObservedAt = now
		_, err = s.Store.UpdateDataTransfer(t)
		return err
	}
	if t.Status == model.DataTransferStatusPlanned {
		if now.Sub(t.CreatedAt) > 30*time.Minute {
			return s.failDataPrewarm(t, "prewarm could not be scheduled within 30 minutes")
		}
		if err := s.createDataPrewarmJob(ctx, c, t); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return s.failDataPrewarm(t, "prewarm scheduling failed; inspect controller logs for resource diagnostics")
		}
		t.Status = model.DataTransferStatusRunning
		t.StartedAt = &now
		t.Cache.State = "downloading"
		t.Cache.ObservedAt = now
		_, err := s.Store.UpdateDataTransfer(t)
		return err
	}
	// A job's own UID and a successful worker on the exact node are required.
	var job struct {
		Metadata struct {
			UID    string            `json:"uid"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Status struct {
			Succeeded int `json:"succeeded"`
			Failed    int `json:"failed"`
		} `json:"status"`
	}
	status, err := c.doJSON(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+t.Cache.Namespace+"/jobs/"+t.Cache.Job, nil, &job)
	if status == 404 {
		return s.failDataPrewarm(t, "runtime job disappeared; no cache success was inferred")
	}
	if err != nil {
		return err
	}
	if job.Metadata.Labels[prewarmLabel] != t.ID {
		return fmt.Errorf("prewarm job identity mismatch")
	}
	t.Cache.JobUID = job.Metadata.UID
	pods, err := c.listPodsBySelector(ctx, t.Cache.Namespace, "job-name="+t.Cache.Job)
	if err != nil {
		return err
	}
	ready := false
	for _, pod := range pods {
		owner := false
		for _, ref := range pod.ObservedOwnerReferences {
			if ref.Kind == "Job" && ref.UID == job.Metadata.UID {
				owner = true
			}
		}
		if !owner || pod.Spec.NodeName != t.Cache.Node {
			continue
		}
		logs, ok, err := c.getPodLogs(ctx, t.Cache.Namespace, pod.Metadata.Name, "prewarm", false, 4)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		for _, line := range strings.Split(logs, "\n") {
			var p dataprewarm.Progress
			if json.Unmarshal([]byte(line), &p) != nil || p.TransferID != t.ID || p.ManifestDigest != t.Manifest.Digest || p.BytesDone < 0 || p.BytesDone > t.BytesTotal || p.FilesDone < 0 || p.FilesDone > t.FilesTotal || p.ObservedAt.Before(t.CreatedAt) || p.ObservedAt.After(now.Add(30*time.Second)) {
				continue
			}
			t.BytesDone = maxPrewarmBytes(t.BytesDone, p.BytesDone)
			t.FilesDone = max(t.FilesDone, p.FilesDone)
			t.Cache.ObservedAt = p.ObservedAt
			if p.State == "ready" && pod.Status.Phase == "Succeeded" && p.BytesDone == t.BytesTotal && p.FilesDone == t.FilesTotal {
				ready = true
			}
		}
	}
	if ready && job.Status.Succeeded > 0 {
		t.Status = model.DataTransferStatusCompleted
		t.FinishedAt = &now
		t.Cache.State = "ready"
		t.Message = "Snapshot bytes verified in runtime cache; applications have not been remounted."
	} else if job.Status.Failed > 0 || now.Sub(t.CreatedAt) > 35*time.Minute {
		return s.failDataPrewarm(t, "runtime prewarm failed or timed out; cache cleanup is pending")
	}
	_, err = s.Store.UpdateDataTransfer(t)
	return err
}
func (s *Service) failDataPrewarm(t model.DataTransfer, message string) error {
	now := time.Now().UTC()
	t.Status = model.DataTransferStatusFailed
	t.FinishedAt = &now
	t.ErrorCode = "prewarm_failed"
	t.ErrorMessage = message
	if t.Cache != nil && t.Cache.State != "removed" {
		t.Cache.State = "cleanup_pending"
		t.Cache.ObservedAt = now
	}
	_, err := s.Store.UpdateDataTransfer(t)
	return err
}
func (s *Service) createDataPrewarmJob(ctx context.Context, c *kubeClient, t model.DataTransfer) error {
	rt, err := s.Store.GetRuntime(t.Target)
	if err != nil {
		return err
	}
	if rt.TenantID != t.TenantID || rt.Type != model.RuntimeTypeManagedOwned || rt.ClusterNodeName == "" || rt.ClusterNodeName != t.Cache.Node {
		return fmt.Errorf("target runtime is not a stable managed node")
	}
	backend, err := s.Store.GetDataBackendForUse(t.Source, t.TenantID, true)
	if err != nil {
		return err
	}
	plan, err := prewarmDownloadPlan(ctx, t, backend)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	if len(raw) > 512<<10 {
		return fmt.Errorf("prewarm plan exceeds 512 KiB")
	}
	// Reuse this deployed controller's immutable image; do not guess a tag or
	// give runtime workers control-plane database credentials.
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		return fmt.Errorf("controller pod identity unavailable")
	}
	pod, ok, err := c.getPod(ctx, t.Cache.Namespace, podName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("controller pod unavailable")
	}
	image := ""
	for _, container := range pod.Spec.Containers {
		if container.Name == "controller" {
			image = container.Image
		}
	}
	if !strings.Contains(image, "@sha256:") {
		return fmt.Errorf("prewarm worker requires the controller's digest-pinned image")
	}
	labels := map[string]string{prewarmLabel: t.ID}
	metadata := map[string]any{"name": t.Cache.Job, "namespace": t.Cache.Namespace, "labels": labels}
	pvcSpec := map[string]any{"accessModes": []string{"ReadWriteOnce"}, "resources": map[string]any{"requests": map[string]string{"storage": fmt.Sprintf("%dMi", maxPrewarmBytes(int64(64), (t.BytesTotal+(64<<20)+(1<<20)-1)>>20))}}}
	if class := strings.TrimSpace(os.Getenv("FUGUE_DATA_PREWARM_STORAGE_CLASS")); class != "" {
		pvcSpec["storageClassName"] = class
	}
	pvc := map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": metadata, "spec": pvcSpec}
	secret := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": metadata, "type": "Opaque", "data": map[string][]byte{"plan.json": raw}}
	for _, item := range []struct {
		resource string
		body     any
	}{{"persistentvolumeclaims", pvc}, {"secrets", secret}} {
		apiPath := "/api/v1/namespaces/" + t.Cache.Namespace + "/" + item.resource
		status, err := c.doJSON(ctx, http.MethodPost, apiPath, item.body, nil)
		if status == 409 {
			var existing struct {
				Metadata struct {
					Labels map[string]string `json:"labels"`
				} `json:"metadata"`
			}
			_, err = c.doJSON(ctx, http.MethodGet, apiPath+"/"+t.Cache.Job, nil, &existing)
			if err != nil {
				return err
			}
			if existing.Metadata.Labels[prewarmLabel] != t.ID {
				return fmt.Errorf("prewarm resource identity mismatch")
			}
		} else if err != nil {
			return err
		}
	}
	job := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": metadata, "spec": map[string]any{
		"backoffLimit": 0, "activeDeadlineSeconds": 1800, "template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{
			"restartPolicy": "Never", "automountServiceAccountToken": false, "nodeSelector": map[string]string{"kubernetes.io/hostname": t.Cache.Node},
			"containers": []any{map[string]any{"name": "prewarm", "image": image, "imagePullPolicy": "IfNotPresent", "command": []string{"/usr/local/bin/fugue-controller", "--data-prewarm-worker"}, "securityContext": map[string]any{"allowPrivilegeEscalation": false, "capabilities": map[string]any{"drop": []string{"ALL"}}, "readOnlyRootFilesystem": true}, "resources": map[string]any{"requests": map[string]string{"cpu": "50m", "memory": "32Mi"}, "limits": map[string]string{"cpu": "1", "memory": "128Mi"}}, "volumeMounts": []any{map[string]any{"name": "plan", "mountPath": "/plan", "readOnly": true}, map[string]any{"name": "cache", "mountPath": "/cache"}}}},
			"volumes":    []any{map[string]any{"name": "plan", "secret": map[string]any{"secretName": t.Cache.Job}}, map[string]any{"name": "cache", "persistentVolumeClaim": map[string]any{"claimName": t.Cache.Claim}}},
		}},
	}}
	return c.createJob(ctx, t.Cache.Namespace, job)
}
func prewarmDownloadPlan(ctx context.Context, t model.DataTransfer, backend model.DataBackend) (dataprewarm.Plan, error) {
	plan := dataprewarm.Plan{TransferID: t.ID, Manifest: t.Manifest, ExpiresAt: time.Now().UTC().Add(30 * time.Minute)}
	if backend.Bucket == "" || backend.Credentials.AccessKeyID == "" || backend.Credentials.SecretAccessKey == "" {
		return plan, fmt.Errorf("prewarm requires an S3-compatible backend with configured credentials")
	}
	region := backend.Region
	if region == "" {
		region = "auto"
	}
	opts := s3.Options{Region: region, Credentials: credentials.NewStaticCredentialsProvider(backend.Credentials.AccessKeyID, backend.Credentials.SecretAccessKey, backend.Credentials.Token), UsePathStyle: backend.Endpoint != ""}
	if backend.Endpoint != "" {
		opts.BaseEndpoint = aws.String(strings.TrimRight(backend.Endpoint, "/"))
	}
	presign := s3.NewPresignClient(s3.New(opts))
	seen := map[string]bool{}
	for _, entry := range t.Manifest.Entries {
		if entry.Kind != "file" || seen[entry.SHA256] {
			continue
		}
		seen[entry.SHA256] = true
		// The snapshot's canonical object key is immutable. Never allow a manifest
		// to escape a configured backend prefix by injecting an absolute path.
		if entry.ObjectKey == "" || strings.HasPrefix(entry.ObjectKey, "/") || path.Clean(entry.ObjectKey) != entry.ObjectKey || strings.HasPrefix(entry.ObjectKey, "../") {
			return plan, fmt.Errorf("invalid snapshot object key")
		}
		signed, err := presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(backend.Bucket), Key: aws.String(path.Join(strings.Trim(backend.Prefix, "/"), entry.ObjectKey))}, func(o *s3.PresignOptions) { o.Expires = 35 * time.Minute })
		if err != nil {
			return plan, fmt.Errorf("could not authorize prewarm blob download")
		}
		plan.Blobs = append(plan.Blobs, dataprewarm.Blob{SHA256: entry.SHA256, Size: entry.Size, URL: signed.URL})
	}
	return plan, dataprewarm.Validate(plan)
}
func cleanupPrewarm(ctx context.Context, c *kubeClient, cache model.DataPrewarmCache, transferID string, removeCache bool) (bool, error) {
	paths := []string{"/apis/batch/v1/namespaces/" + cache.Namespace + "/jobs/" + url.PathEscape(cache.Job), "/api/v1/namespaces/" + cache.Namespace + "/secrets/" + url.PathEscape(cache.Job)}
	if removeCache {
		paths = append(paths, "/api/v1/namespaces/"+cache.Namespace+"/persistentvolumeclaims/"+url.PathEscape(cache.Claim))
	}
	done := true
	for _, apiPath := range paths {
		var object struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
		}
		status, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &object)
		if status == 404 {
			continue
		}
		if err != nil {
			return false, err
		}
		if object.Metadata.Labels[prewarmLabel] != transferID {
			return false, fmt.Errorf("refusing cleanup of an unrelated resource")
		}
		status, err = c.doJSON(ctx, http.MethodDelete, apiPath, map[string]any{"propagationPolicy": "Foreground"}, nil)
		if status == 404 {
			continue
		}
		if err != nil {
			return false, err
		}
		status, err = c.doJSON(ctx, http.MethodGet, apiPath, nil, nil)
		if status == 404 {
			continue
		}
		if err != nil {
			return false, err
		}
		done = false
	}
	return done, nil
}

func maxPrewarmBytes(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
