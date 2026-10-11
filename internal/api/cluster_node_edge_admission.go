package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/model/edgecohort"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Server) edgeEnrollmentClusterClient() (*clusterNodeClient, error) {
	f := s.newClusterNodeClient
	if f == nil {
		f = newClusterNodeClient
	}
	return f()
}

func (s *Server) validateManagedEdgeCohortCapability(ctx context.Context) error {
	c, err := s.edgeEnrollmentClusterClient()
	if err != nil {
		return err
	}
	defer c.closeIdleConnections()
	var d appsv1.Deployment
	if err = c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/namespaces/"+url.PathEscape(s.controlPlaneNamespace)+"/deployments/fugue-release-guardian", &d); err != nil {
		return err
	}
	if d.Annotations[edgecohort.CapabilityAnnotation] != edgecohort.CapabilityV1 || d.Spec.Replicas == nil || *d.Spec.Replicas < 1 || d.Status.ObservedGeneration != d.Generation || d.Status.UpdatedReplicas != *d.Spec.Replicas || d.Status.ReadyReplicas != *d.Spec.Replicas {
		return errors.New("release Guardian must support managed Edge membership and be fully ready")
	}
	return nil
}

func acquireEdgeEnrollmentLease(ctx context.Context, c *clusterNodeClient, namespace, group string) (func() error, error) {
	path := "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(namespace) + "/leases/" + url.PathEscape("fugue-authority-"+group)
	var lease coordinationv1.Lease
	if err := c.doJSON(ctx, http.MethodGet, path, &lease); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" && lease.Spec.RenewTime != nil && lease.Spec.LeaseDurationSeconds != nil && now.Before(lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds)*time.Second)) {
		return nil, errors.New("group authority is being updated; retry after release completes")
	}
	holder := model.NewID("edge-admission")
	seconds := int32(60)
	stamp := metav1.NewMicroTime(now.Truncate(time.Microsecond))
	lease.Spec.HolderIdentity = &holder
	lease.Spec.LeaseDurationSeconds = &seconds
	lease.Spec.AcquireTime = &stamp
	lease.Spec.RenewTime = &stamp
	var acquired coordinationv1.Lease
	if err := c.doJSONWithBody(ctx, http.MethodPut, path, &lease, &acquired); err != nil {
		return nil, err
	}
	return func() error {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var current coordinationv1.Lease
		if err := c.doJSON(releaseCtx, http.MethodGet, path, &current); err != nil {
			return err
		}
		if current.UID != acquired.UID || current.ResourceVersion != acquired.ResourceVersion || current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != holder {
			return errors.New("Edge admission lease changed")
		}
		empty := ""
		current.Spec.HolderIdentity = &empty
		current.Spec.AcquireTime = nil
		current.Spec.RenewTime = nil
		return c.doJSONWithBody(releaseCtx, http.MethodPut, path, &current, nil)
	}, nil
}

func (s *Server) admitManagedEdgeEnrollment(parent context.Context, machine model.Machine, id string) (result error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	var intent model.EdgeNodeEnrollment
	if json.Unmarshal([]byte(machine.Labels[model.MachineEdgeEnrollmentLabel]), &intent) != nil || intent.ID != id || !machine.Policy.AllowEdge {
		return errors.New("enrollment intent does not match")
	}
	if err := intent.Validate(machine.ClusterNodeName, intent.EdgeGroupID, time.Now().UTC()); err != nil {
		return err
	}
	c, err := s.edgeEnrollmentClusterClient()
	if err != nil {
		return err
	}
	defer c.closeIdleConnections()
	release, err := acquireEdgeEnrollmentLease(ctx, c, s.controlPlaneNamespace, intent.EdgeGroupID)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, release()) }()
	current, err := s.prepareManagedEdgeEnrollment(ctx, machine, intent.EdgeGroupID)
	if err != nil {
		return err
	}
	if current != intent {
		return errors.New("group authority changed before membership admission")
	}
	pods, err := c.listCorePods(ctx, s.controlPlaneNamespace, "fugue.io/edge-group-id="+intent.EdgeGroupID)
	if err != nil {
		return err
	}
	workers := map[string]corev1.Pod{}
	front := false
	var frontPod corev1.Pod
	for _, p := range pods {
		if p.Spec.NodeName != machine.ClusterNodeName || p.DeletionTimestamp != nil {
			continue
		}
		slot := p.Labels["fugue.io/edge-slot"]
		isFront := false
		isWorker := false
		for _, container := range p.Spec.Containers {
			isFront = isFront || container.Name == "edge-front"
			isWorker = isWorker || container.Name == "edge"
		}
		if !isFront && !isWorker {
			continue
		}
		ready := false
		for _, condition := range p.Status.Conditions {
			ready = ready || condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue
		}
		if !ready || p.Status.Phase != corev1.PodRunning {
			return errors.New("node Edge workloads are not all ready")
		}
		if isFront {
			if front {
				return errors.New("ambiguous Edge Front")
			}
			front = true
			frontPod = p
		}
		if isWorker {
			if slot != "a" && slot != "b" || workers[slot].Name != "" {
				return errors.New("ambiguous Edge worker slots")
			}
			workers[slot] = p
		}
	}
	if !front || len(workers) != 2 {
		return errors.New("both Edge slots and Front are required")
	}
	worker := workers[intent.Slot]
	imageMatches := false
	for _, c := range worker.Status.ContainerStatuses {
		imageMatches = imageMatches || c.Name == "edge" && c.Ready && strings.HasSuffix(c.ImageID, "@"+intent.WorkerImageDigest)
	}
	if !imageMatches {
		return errors.New("current Edge worker image differs from authority")
	}
	port := int32(0)
	for _, c := range worker.Spec.Containers {
		if c.Name == "edge" {
			for _, p := range c.Ports {
				if p.Name == "health" {
					port = p.ContainerPort
				}
			}
		}
	}
	if port < 1 || net.ParseIP(worker.Status.PodIP) == nil {
		return errors.New("private Edge health endpoint is unavailable")
	}
	endpoint := "http://" + net.JoinHostPort(worker.Status.PodIP, strconv.Itoa(int(port))) + "/healthz"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	resp, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var health struct {
		Healthy   bool   `json:"healthy"`
		EdgeID    string `json:"edge_id"`
		Group     string `json:"edge_group_id"`
		Bundle    string `json:"bundle_version"`
		Applied   string `json:"caddy_applied_version"`
		Stale     bool   `json:"stale_cache"`
		Candidate bool   `json:"candidate_bundle_loaded"`
		Serving   struct {
			State       string    `json:"state"`
			Bundle      string    `json:"bundle_version"`
			RouteProbes int       `json:"route_probes"`
			TLSProbes   int       `json:"tls_probes"`
			Verified    time.Time `json:"verified_at"`
		} `json:"platform_serving"`
	}
	if resp.StatusCode != 200 || json.NewDecoder(io.LimitReader(resp.Body, 128<<10)).Decode(&health) != nil || !health.Healthy || health.EdgeID != intent.NodeID || health.Group != intent.EdgeGroupID || health.Bundle == "" || health.Bundle != health.Applied || health.Stale || health.Candidate || health.Serving.State != "serving_verified" || health.Serving.Bundle != health.Bundle || health.Serving.RouteProbes < 1 || health.Serving.TLSProbes < 1 || time.Since(health.Serving.Verified) > 90*time.Second || health.Serving.Verified.After(time.Now().Add(30*time.Second)) {
		return errors.New("fresh exact-node route and TLS execution proofs are required")
	}
	frontPort := int32(0)
	for _, container := range frontPod.Spec.Containers {
		if container.Name == "edge-front" {
			for _, port := range container.Ports {
				if port.Name == "health" {
					frontPort = port.ContainerPort
				}
			}
		}
	}
	if frontPort < 1 || net.ParseIP(frontPod.Status.PodIP) == nil {
		return errors.New("Front health endpoint is unavailable")
	}
	frontRequest, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(frontPod.Status.PodIP, strconv.Itoa(int(frontPort)))+"/healthz", nil)
	frontResponse, err := (&http.Client{Timeout: 5 * time.Second}).Do(frontRequest)
	if err != nil {
		return err
	}
	defer frontResponse.Body.Close()
	var frontState struct {
		Slot       string `json:"active_slot"`
		Generation uint64 `json:"activation_generation"`
		Bundle     string `json:"bundle_generation"`
		Source     string `json:"worker_source_commit"`
		Image      string `json:"worker_image_digest"`
	}
	if frontResponse.StatusCode != 200 || json.NewDecoder(io.LimitReader(frontResponse.Body, 32<<10)).Decode(&frontState) != nil || frontState.Slot != intent.Slot || frontState.Generation != intent.FenceSequence || frontState.Bundle != intent.BundleVersion || frontState.Source != intent.WorkerSourceSHA || frontState.Image != intent.WorkerImageDigest {
		return errors.New("Front activation differs from admitted authority")
	}
	admitted, err := s.store.AdmitMachineEdgeEnrollment(machine.ClusterNodeName, id)
	if err != nil {
		return err
	}
	runtime, err := s.loadClusterNodePolicyRuntime(admitted)
	if err != nil {
		return err
	}
	_, reconcileErr := s.reconcileClusterNodePolicy(ctx, admitted, runtime)
	if reconcileErr != "" {
		return errors.New(reconcileErr)
	}
	s.clusterNodeInventoryCache.clear(clusterNodeInventoryCacheKey)
	return nil
}
