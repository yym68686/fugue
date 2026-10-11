package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/model/edgecohort"
	runtimepkg "fugue/internal/runtime"
)

func prepareEdgePolicyEnrollment(client *Client, node, group string, request setClusterNodePolicyRequest) (setClusterNodePolicyRequest, string, error) {
	policy, err := client.GetClusterNodePolicy(node)
	if err != nil {
		return request, "", err
	}
	if policy.NodeName != node || !policy.Ready || !policy.NodeSchedulable || policy.BlockRollout {
		return request, "", fmt.Errorf("node is not ready for Edge enrollment: %s", firstNonEmpty(policy.GateReason, policy.ReconcileError, "node health gate is closed"))
	}
	bound := strings.TrimSpace(policy.Labels[runtimepkg.EdgeGroupIDLabelKey])
	group = strings.TrimSpace(group)
	edge, edgeErr := client.GetEdgeNode(node)
	if edgeErr != nil {
		var apiErr *apiServerError
		if !errors.As(edgeErr, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
			return request, "", edgeErr
		}
	} else {
		if edge.Node.ID != node || bound != "" && bound != edge.Node.EdgeGroupID {
			return request, "", fmt.Errorf("Edge node identity disagrees with the node's authority binding")
		}
		bound = edge.Node.EdgeGroupID
	}
	if group == "" {
		group = bound
	}
	if !edgetopology.ValidAuthorityID(group) {
		return request, "", fmt.Errorf("new Edge enrollment requires --edge-group with an explicit authority; country and hostname do not select an authority")
	}
	if bound != "" && bound != group {
		return request, "", fmt.Errorf("node is already bound to Edge authority %s; role changes cannot migrate it", bound)
	}
	// Preserve deployed static identities. New nodes and safe retries use the
	// node updater's scoped credential path, including partially enrolled nodes.
	if edgeErr == nil && edge.Node.WorkloadMode != model.EdgeWorkloadModeDynamic && (edge.Node.Healthy || edge.Node.LastHeartbeatAt != nil || edge.Node.RouteBundleVersion != "") {
		return request, group, nil
	}
	request.EdgeGroupID = &group
	return request, group, nil
}

func (c *CLI) waitForEdgeEnrollment(parent context.Context, client *Client, node, group string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	bounded := *client
	bounded.context = ctx
	previous := ""
	lastProbe := time.Time{}
	for {
		reason, probe, err := observeEdgeEnrollment(&bounded, node, group, time.Now())
		if err != nil {
			var apiErr *apiServerError
			if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden) {
				return err
			}
			reason = err.Error()
		}
		if reason == "" {
			return nil
		}
		if probe && time.Since(lastProbe) >= 30*time.Second {
			lastProbe = time.Now()
			if _, probeErr := bounded.ProbeEdgeNode(node); probeErr != nil {
				reason = "public Edge probe: " + probeErr.Error()
			}
		}
		if reason != previous {
			fmt.Fprintf(c.stderr, "Edge enrollment pending: %s\n", reason)
			previous = reason
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("Edge enrollment incomplete: %s: %w; rerun the same command after resolving the reported condition", reason, ctx.Err())
		case <-timer.C:
		}
	}
}

func observeEdgeEnrollment(client *Client, node, group string, now time.Time) (string, bool, error) {
	policy, err := client.GetClusterNodePolicy(node)
	if err != nil {
		return "", false, err
	}
	if policy.Policy == nil || !policy.Policy.AllowEdge || !policy.Policy.EffectiveEdge || !policy.Ready || !policy.Reconciled {
		return "waiting for healthy, reconciled Edge node policy", false, nil
	}
	if policy.Labels[runtimepkg.EdgeGroupIDLabelKey] != group {
		return "waiting for the exact Edge authority label", false, nil
	}
	pods, err := client.ListClusterPods(clusterPodsOptions{Node: node, LabelSelector: runtimepkg.EdgeGroupIDLabelKey + "=" + group})
	if err != nil {
		return "", false, err
	}
	if reason := edgeEnrollmentPodReason(pods, node, group); reason != "" {
		if preparation, prepareErr := prepareEdgeEnrollmentActivation(client, pods, node, group, now); prepareErr != nil {
			return "", false, prepareErr
		} else if preparation != "" {
			return preparation, false, nil
		}
		return reason, false, nil
	}
	edge, err := client.GetEdgeNode(node)
	if err != nil {
		return "", false, err
	}
	if edge.Node.ID != node || edge.Node.EdgeGroupID != group {
		return "Edge runtime identity does not match enrollment", false, nil
	}
	heartbeat := edge.Node.LastHeartbeatAt
	if heartbeat == nil || now.Sub(*heartbeat) > 90*time.Second || heartbeat.After(now.Add(30*time.Second)) {
		return "waiting for a fresh Edge heartbeat", false, nil
	}
	if !edge.Node.Healthy || edge.Node.LastError != "" || edge.Node.CaddyLastError != "" {
		return firstNonEmpty(edge.Node.LastError, edge.Node.CaddyLastError, "Edge runtime is not healthy"), false, nil
	}
	state, err := client.GetAdminEdgeNodeDesiredState(node)
	if err != nil {
		return "", false, err
	}
	desired := state.DesiredState
	if desired.EdgeID != node || desired.EdgeGroupID != group || desired.Draining || !desired.RouteReady || !desired.TLSReady {
		return "waiting for the exact node's route and TLS readiness", false, nil
	}
	if desired.PublicProbeStatus != model.EdgePublicProbeStatusPassing {
		return "waiting for public 80/443 reachability", true, nil
	}
	if policy.Labels[edgecohort.PhaseLabel] == edgecohort.Preparing {
		if desired.Enrollment == nil {
			return "waiting for an active enrollment permission before admission", false, nil
		}
		_, err := client.SetClusterNodePolicy(node, setClusterNodePolicyRequest{EdgeAdmissionID: &desired.Enrollment.ID})
		if err != nil {
			return "", false, err
		}
		return "waiting for admitted release membership", false, nil
	}
	return "", false, nil
}

func prepareEdgeEnrollmentActivation(client *Client, pods []model.ClusterPod, node, group string, now time.Time) (string, error) {
	desired, err := client.GetAdminEdgeNodeDesiredState(node)
	if err != nil {
		return "", err
	}
	intent := desired.DesiredState.Enrollment
	if intent == nil {
		return "waiting for current Edge enrollment authority", nil
	}
	if err = intent.Validate(node, group, now); err != nil {
		return "", err
	}
	var selected *model.ClusterPod
	for i := range pods {
		p := &pods[i]
		if p.NodeName != node || p.Labels[runtimepkg.EdgeGroupIDLabelKey] != group || p.Labels["fugue.io/edge-slot"] != intent.Slot {
			continue
		}
		for _, c := range p.Containers {
			if c.Name == "edge" && c.State == "running" && strings.HasSuffix(c.ImageID, "@"+intent.WorkerImageDigest) {
				if selected != nil {
					return "", errors.New("Edge enrollment worker identity is ambiguous")
				}
				selected = p
			}
		}
	}
	if selected == nil {
		return "waiting for the current authority's exact worker image", nil
	}
	exec := func(command ...string) (string, error) {
		r, e := client.ExecClusterPod(clusterExecRequest{Namespace: selected.Namespace, Pod: selected.Name, Container: "edge", Command: command, Timeout: 15 * time.Second})
		return r.Output, e
	}
	// The path is the same projected Front state contract used by the worker.
	const statePath = "/var/lib/fugue-edge-front/activation.json"
	state, err := exec("sh", "-c", "if test -e /var/lib/fugue-edge-front/activation.json; then cat /var/lib/fugue-edge-front/activation.json; else printf absent; fi")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(state) != "absent" {
		return "", nil
	}
	// Both slots need a verified local bundle before Front is initialized.
	// Afterwards an idle slot reads only candidates and cannot seed an empty
	// cache from the current stream. Do not strand it behind an empty candidate.
	warmed := map[string]bool{}
	for _, pod := range pods {
		if pod.NodeName == node && pod.Labels[runtimepkg.EdgeGroupIDLabelKey] == group && pod.Phase == "Running" && pod.Ready {
			warmed[pod.Labels["fugue.io/edge-slot"]] = true
		}
	}
	if !warmed[model.EdgeSlotA] || !warmed[model.EdgeSlotB] {
		return "waiting for both worker slots to prepare private signed routes before initial activation", nil
	}
	identity, err := exec("sh", "-ec", "for field in edge_id edge_group_id slot release_epoch; do cat /var/run/fugue/edge-identity/$field; printf '\\n'; done")
	if err != nil {
		return "", err
	}
	fields := strings.Fields(identity)
	if len(fields) != 4 || fields[0] != node || fields[1] != group || fields[2] != intent.Slot || fields[3] != intent.WorkerSourceSHA {
		return "", errors.New("Edge enrollment projected worker identity differs from current authority")
	}
	healthRaw, err := exec("wget", "-qO-", "http://127.0.0.1:7832/healthz")
	if err != nil {
		return "waiting for the private worker's signed route preparation", nil
	}
	var health struct {
		Healthy     bool       `json:"healthy"`
		EdgeID      string     `json:"edge_id"`
		GroupID     string     `json:"edge_group_id"`
		Bundle      string     `json:"bundle_version"`
		Source      string     `json:"route_bundle_source"`
		Applied     string     `json:"caddy_applied_version"`
		Routes      int        `json:"route_count"`
		Stale       bool       `json:"stale_cache"`
		Candidate   bool       `json:"candidate_bundle_loaded"`
		LastSuccess *time.Time `json:"last_success_at"`
		LastError   string     `json:"last_error"`
		CaddyError  string     `json:"caddy_last_error"`
		Serving     struct {
			State       string    `json:"state"`
			Bundle      string    `json:"bundle_version"`
			RouteProbes int       `json:"route_probes"`
			TLSProbes   int       `json:"tls_probes"`
			VerifiedAt  time.Time `json:"verified_at"`
		} `json:"platform_serving"`
	}
	if json.Unmarshal([]byte(healthRaw), &health) != nil || !health.Healthy || health.EdgeID != node || health.GroupID != group || health.Source != "edge-control-group-authority/v1" || health.Bundle == "" || health.Bundle != health.Applied || health.Routes < 1 || health.Stale || health.Candidate || health.LastError != "" || health.CaddyError != "" || health.LastSuccess == nil || now.Sub(*health.LastSuccess) > 90*time.Second || health.LastSuccess.After(now.Add(30*time.Second)) {
		return "waiting for fresh, verified private worker route health", nil
	}
	if health.Serving.State != "serving_verified" || health.Serving.Bundle != health.Bundle || health.Serving.RouteProbes < 1 || health.Serving.TLSProbes < 1 || now.Sub(health.Serving.VerifiedAt) > 90*time.Second || health.Serving.VerifiedAt.After(now.Add(30*time.Second)) {
		return "waiting for authenticated route and TLS execution proofs on the new node", nil
	}
	// Repeat authority discovery immediately before the create-only local CAS.
	latest, err := client.GetAdminEdgeNodeDesiredState(node)
	if err != nil {
		return "", err
	}
	if latest.DesiredState.Enrollment == nil || *latest.DesiredState.Enrollment != *intent {
		return "", errors.New("Edge authority changed before initial activation; rerun enrollment")
	}
	_, err = exec("/usr/local/bin/fugue-edge-front-cas", "--state-file", statePath, "--group", group, "--expected-generation", "0", "--initial-generation", strconv.FormatUint(intent.FenceSequence, 10), "--expected-slot", intent.Slot, "--target-slot", intent.Slot, "--bundle-generation", intent.BundleVersion, "--worker-source-commit", intent.WorkerSourceSHA, "--worker-image-digest", intent.WorkerImageDigest, "--operation", "initialize", "--reason", "Verified node enrollment "+intent.ID)
	if err != nil {
		return "", err
	}
	return "initial Edge activation created after private route verification", nil
}

func edgeEnrollmentPodReason(pods []model.ClusterPod, node, group string) string {
	slots := map[string]bool{}
	front := false
	for _, pod := range pods {
		if pod.NodeName != node || pod.Labels[runtimepkg.EdgeGroupIDLabelKey] != group {
			continue
		}
		slot := pod.Labels["fugue.io/edge-slot"]
		worker := slot == model.EdgeSlotA || slot == model.EdgeSlotB
		isFront := false
		for _, container := range pod.Containers {
			isFront = isFront || container.Name == "edge-front"
		}
		if !worker && !isFront {
			continue
		}
		if !pod.Ready || pod.Phase != "Running" {
			reason := "not ready"
			for _, container := range pod.Containers {
				if !container.Ready {
					reason = firstNonEmpty(container.Message, container.Reason, container.State, reason)
					break
				}
			}
			return pod.Name + ": " + reason
		}
		if worker {
			slots[slot] = true
		}
		front = front || isFront
	}
	if !slots[model.EdgeSlotA] || !slots[model.EdgeSlotB] || !front {
		return "waiting for both Edge worker slots and the node's public Front"
	}
	return ""
}
