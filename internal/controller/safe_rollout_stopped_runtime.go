package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/diagnosticadmission"
	"fugue/internal/livediagnostics"
	"fugue/internal/model"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func captureStoppedPodIdentity(pod map[string]any, p *releaseDrainPod) error {
	if p.Version == "" || p.Node == "" || len(mapSlice(nestedObjectValue(pod, "spec", "ephemeralContainers"))) > 0 || nestedObjectValue(pod, "spec", "hostNetwork") == true {
		return errors.New("stopped Pod has incomplete or unsupported runtime identity")
	}
	p.StoppedContainers = map[string]string{}
	for _, pair := range [][2]string{{"containers", "containerStatuses"}, {"initContainers", "initContainerStatuses"}} {
		expected := map[string]bool{}
		for _, c := range mapSlice(nestedObjectValue(pod, "spec", pair[0])) {
			name := objectStringField(c, "name")
			if name == "" || expected[name] {
				return errors.New("duplicate container declaration")
			}
			expected[name] = true
		}
		statuses := mapSlice(nestedObjectValue(pod, "status", pair[1]))
		if len(statuses) != len(expected) {
			return errors.New("stopped Pod container set is incomplete")
		}
		for _, c := range statuses {
			name, id := objectStringField(c, "name"), objectStringField(c, "containerID")
			finished, err := time.Parse(time.RFC3339Nano, objectStringField(objectMapValue(nestedObjectValue(c, "state", "terminated")), "finishedAt"))
			if !expected[name] || p.StoppedContainers[name] != "" || !strings.HasPrefix(id, "containerd://") || !validCRIHex(strings.TrimPrefix(id, "containerd://")) || c["ready"] != false || err != nil || finished.After(time.Now()) || nestedObjectValue(c, "state", "running") != nil || nestedObjectValue(c, "state", "waiting") != nil {
				return errors.New("Pod has no complete stopped container identity")
			}
			p.StoppedContainers[name] = strings.TrimPrefix(id, "containerd://")
			p.Containers = append(p.Containers, fmt.Sprintf("%s:%s:%v:%s", name, id, c["restartCount"], finished.Format(time.RFC3339Nano)))
		}
	}
	if len(p.StoppedContainers) == 0 {
		return errors.New("stopped Pod has no container identity")
	}
	sort.Strings(p.Containers)
	return nil
}

func captureDrainNodeIdentity(ctx context.Context, c *kubeClient, p *releaseDrainPod) error {
	obj, found, err := c.getRawObject(ctx, "/api/v1/nodes/"+url.PathEscape(p.Node))
	if err != nil {
		return err
	}
	if !found {
		return errors.New("stopped Pod node missing")
	}
	m := objectMapField(obj, "metadata")
	p.NodeUID = objectStringField(m, "uid")
	p.BootID = objectStringField(objectMapValue(nestedObjectValue(obj, "status", "nodeInfo")), "bootID")
	if p.NodeUID == "" || p.BootID == "" || objectStringField(m, "deletionTimestamp") != "" {
		return errors.New("stopped Pod node identity incomplete")
	}
	for _, condition := range mapSlice(nestedObjectValue(obj, "status", "conditions")) {
		if condition["type"] != "Ready" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, objectStringField(condition, "lastHeartbeatTime"))
		if err == nil && condition["status"] == "True" && time.Since(at) >= -5*time.Second && time.Since(at) < 2*time.Minute {
			return nil
		}
	}
	return errors.New("stopped Pod node has no fresh Ready heartbeat")
}

func validCRIHex(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 32 && strings.ToLower(id) == id
}

func (s *Service) observeStoppedReleaseRuntime(ctx context.Context, c *kubeClient, app model.App, release model.AppRelease, workload releaseDrainWorkload) ([]livediagnostics.ProbeReport, error) {
	// The existing diagnostic admission lease bounds concurrent privileged
	// observations across Controller, API and CLI. Serving resources are read-only.
	client, err := kubernetes.NewForConfig(&rest.Config{Host: c.baseURL, BearerToken: c.bearerToken, Transport: c.client.Transport, Timeout: 10 * time.Second, ContentConfig: rest.ContentConfig{ContentType: "application/json", AcceptContentTypes: "application/json"}})
	if err != nil {
		return nil, err
	}
	ns := c.namespace
	if ns == "" {
		ns = s.Config.KubectlNamespace
	}
	if ns == "" {
		return nil, errors.New("diagnostic namespace unavailable")
	}
	catalog, err := loadDrainDiagnosticCatalog(ctx, client, ns)
	if err != nil {
		return nil, err
	}
	var receipts []livediagnostics.ProbeReport
	for _, pod := range workload.Pods {
		params := map[string]string{"namespace": release.RevisionWorkload.Namespace, "pod": pod.Name, "pod_uid": pod.UID}
		probe, params, err := catalog.Resolve("pod-runtime-state", params)
		if err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(struct {
			Release string
			Pod     releaseDrainPod
			Catalog string
		}{release.ID, pod, catalog.Digest})
		digest := sha256.Sum256(raw)
		name := fmt.Sprintf("diagnostic-retire-%x", digest[:20])
		target := livediagnostics.Target{Type: livediagnostics.TargetNode, Node: pod.Node}
		expected, err := livediagnostics.BuildProbeJob(catalog, probe, target, name, ns, "api", livediagnostics.StartRequest{Kind: livediagnostics.ProbeRegistered, DurationSeconds: 12}, params)
		if err != nil {
			return nil, err
		}
		expected.Annotations["fugue.pro/retirement-release-id"] = release.ID
		expected.Annotations["fugue.pro/retirement-pod-version"] = pod.Version
		job, err := client.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			job, err = diagnosticadmission.AdmitJob(ctx, client, ns, &expected)
		}
		if err != nil {
			return nil, err
		}
		if err := validateDrainDiagnosticJob(*job, expected); err != nil {
			return nil, err
		}
		// A completed report can be reused only briefly. Delete just this exact
		// diagnostic Job before retrying, never an application object.
		if job.Status.CompletionTime != nil && time.Since(job.Status.CompletionTime.Time) > 30*time.Second || job.Status.Failed > 0 {
			uid := job.UID
			prop := metav1.DeletePropagationBackground
			if err := client.BatchV1().Jobs(ns).Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}, PropagationPolicy: &prop}); err != nil {
				return nil, err
			}
			return nil, errors.New("expired diagnostic scheduled for fresh observation")
		}
		for job.Status.Succeeded != 1 {
			if job.Status.Failed > 0 {
				return nil, errors.New("stopped runtime diagnostic failed")
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
			current, err := client.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if current.UID != job.UID {
				return nil, errors.New("diagnostic Job replaced")
			}
			job = current
			if err := validateDrainDiagnosticJob(*job, expected); err != nil {
				return nil, err
			}
		}
		pods, err := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
		if err != nil {
			return nil, err
		}
		if len(pods.Items) != 1 {
			return nil, errors.New("diagnostic Pod set is ambiguous")
		}
		runner := pods.Items[0]
		owned := false
		for _, ref := range runner.OwnerReferences {
			if ref.Controller != nil && *ref.Controller && ref.UID == job.UID && ref.Kind == "Job" && ref.Name == name {
				owned = true
			}
		}
		if !owned || runner.Status.Phase != corev1.PodSucceeded || runner.Spec.NodeName != pod.Node || len(runner.Spec.Containers) != 1 || runner.Spec.Containers[0].Image != probe.Image || runner.DeletionTimestamp != nil {
			return nil, errors.New("diagnostic Pod execution identity differs")
		}
		stream, err := client.CoreV1().Pods(ns).GetLogs(runner.Name, &corev1.PodLogOptions{Container: livediagnostics.DiagnosticAgentContainer}).Stream(ctx)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(stream, (4<<20)+1))
		stream.Close()
		if readErr != nil || len(body) > 4<<20 {
			return nil, errors.New("diagnostic report unavailable or oversized")
		}
		var report livediagnostics.ProbeReport
		if err := json.Unmarshal(body, &report); err != nil {
			return nil, err
		}
		if err := livediagnostics.ValidateProbeReport(report, *job); err != nil {
			return nil, err
		}
		if err := validateStoppedRuntimeReport(report, pod, release.RevisionWorkload.Namespace, time.Now().UTC()); err != nil {
			return nil, err
		}
		receipts = append(receipts, report)
	}
	current, err := loadDrainDiagnosticCatalog(ctx, client, ns)
	if err != nil || current.Digest != catalog.Digest {
		return nil, errors.New("diagnostic trust or catalog changed during observation")
	}
	return receipts, nil
}

func loadDrainDiagnosticCatalog(ctx context.Context, client kubernetes.Interface, ns string) (livediagnostics.VerifiedCatalog, error) {
	c, err := client.CoreV1().ConfigMaps(ns).Get(ctx, livediagnostics.CatalogConfigMap, metav1.GetOptions{})
	if err != nil {
		return livediagnostics.VerifiedCatalog{}, err
	}
	t, err := client.CoreV1().ConfigMaps(ns).Get(ctx, livediagnostics.TrustConfigMap, metav1.GetOptions{})
	if err != nil {
		return livediagnostics.VerifiedCatalog{}, err
	}
	keys := map[string]string{}
	if err := livediagnostics.DecodeStrict([]byte(t.Data["keys.json"]), &keys); err != nil {
		return livediagnostics.VerifiedCatalog{}, err
	}
	return livediagnostics.LoadPublishedCatalog([]byte(c.Data["catalog.json"]), []byte(c.Data["previous-catalog.json"]), keys, nil)
}

func validateDrainDiagnosticJob(actual, expected batchv1.Job) error {
	for key, value := range expected.Annotations {
		if actual.Annotations[key] != value {
			return errors.New("diagnostic Job annotations differ")
		}
	}
	for key, value := range expected.Labels {
		if actual.Labels[key] != value {
			return errors.New("diagnostic Job labels differ")
		}
	}
	a, _ := json.Marshal(actual.Spec)
	b, _ := json.Marshal(expected.Spec)
	var got, want any
	_ = json.Unmarshal(a, &got)
	_ = json.Unmarshal(b, &want)
	if actual.UID == "" || actual.DeletionTimestamp != nil || !revisionAppliedFieldsMatch("job.spec", got, want) {
		return errors.New("diagnostic Job spec differs")
	}
	return nil
}

func validateStoppedRuntimeReport(report livediagnostics.ProbeReport, pod releaseDrainPod, namespace string, now time.Time) error {
	if report.ProbeID != "pod-runtime-state" || report.Quality.Status != "complete" || report.Quality.Truncated || len(report.Quality.Gaps) > 0 || report.StartedAt.IsZero() || report.FinishedAt.Before(report.StartedAt) || now.Sub(report.FinishedAt) > 30*time.Second || report.FinishedAt.After(now.Add(5*time.Second)) {
		return errors.New("runtime report is not fresh complete evidence")
	}
	count := 0
	for _, e := range report.Evidence {
		if e.Source != "pod-runtime-state" {
			continue
		}
		var d struct {
			Node   string    `json:"node"`
			BootID string    `json:"boot_id"`
			From   time.Time `json:"observed_from"`
			Until  time.Time `json:"observed_until"`
			Peer   struct {
				PID   int    `json:"pid"`
				Start string `json:"start_ticks"`
			} `json:"runtime_peer"`
			Facts struct {
				Target    struct{ Namespace, Name, UID string } `json:"target"`
				Quiescent *bool                                 `json:"quiescent"`
				Sandboxes []struct {
					ID, State     string
					Metadata      struct{ Namespace, Name, UID string }
					PID           *int64
					ProcessStatus string `json:"process_status"`
					Closed        *bool  `json:"net_namespace_closed"`
				} `json:"sandboxes"`
				Containers []struct {
					ID, Name, State string
					SandboxID       string `json:"sandbox_id"`
					PID             *int64
					Present         *bool `json:"process_present"`
				} `json:"containers"`
			} `json:"facts"`
		}
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		if e.Status != "complete" || e.Error != "" || d.Node != pod.Node || d.BootID != pod.BootID || d.Peer.PID <= 1 || d.Peer.Start == "" || d.From.Before(report.StartedAt) || d.Until.After(report.FinishedAt) || d.Until.Before(d.From) || now.Sub(d.Until) > 30*time.Second || d.Facts.Quiescent == nil || !*d.Facts.Quiescent || d.Facts.Target.Namespace != namespace || d.Facts.Target.Name != pod.Name || d.Facts.Target.UID != pod.UID {
			return errors.New("stopped runtime observation identity or time differs")
		}
		if len(d.Facts.Sandboxes) == 0 || len(d.Facts.Containers) == 0 {
			return errors.New("empty stopped runtime inventory")
		}
		sandboxes := map[string]bool{}
		for _, sandbox := range d.Facts.Sandboxes {
			if !validCRIHex(sandbox.ID) || sandboxes[sandbox.ID] || sandbox.Metadata != d.Facts.Target || sandbox.State != "SANDBOX_NOTREADY" || sandbox.PID == nil || *sandbox.PID != 0 || sandbox.ProcessStatus != "deleted" || sandbox.Closed == nil || !*sandbox.Closed {
				return errors.New("sandbox is not positively stopped")
			}
			sandboxes[sandbox.ID] = true
		}
		seen := map[string]bool{}
		observed := map[string]string{}
		for _, c := range d.Facts.Containers {
			if !validCRIHex(c.ID) || seen[c.ID] || !sandboxes[c.SandboxID] || c.State != "CONTAINER_EXITED" || c.PID == nil || *c.PID < 0 || c.Present == nil || *c.Present || c.Name == "" {
				return errors.New("container is not positively stopped")
			}
			seen[c.ID] = true
			if pod.StoppedContainers[c.Name] == c.ID {
				observed[c.Name] = c.ID
			}
		}
		if !reflect.DeepEqual(observed, pod.StoppedContainers) {
			return errors.New("runtime observation does not cover Kubernetes container identities")
		}
		count++
	}
	if count == 0 {
		return errors.New("report has no stopped runtime facts")
	}
	return nil
}
